"""
数据库存储区 - MongoDB数据库
==================================================
【文件职责】
这个文件只做一件事：接收调度器推送的数据，然后写入MongoDB数据库。

【重要提醒】
1. 这个文件不提供任何读取接口（修复区如果需要读数据库，应该直接连接数据库）
2. 这个文件只与调度器对话，不与任何其他文件对话
3. 所有表字段都是中文，MongoDB直接存储字典，完全保留中文字段名
4. 数据库会完整保存所有字段，空值就是 null，不需要过滤

【表结构】
数据库里有两个集合（Collections），都在同一个MongoDB数据库里：

1. active_positions（持仓集合）
   - 作用：存储当前正在持仓的数据
   - 特点：覆盖更新，每个交易所只能有一条数据
   - id生成：交易所_合约名_开仓时间（作为普通字段存储）
   - 操作：根据id进行upsert（存在则更新，不存在则插入）
   - 清理：根据交易所字段删除该交易所所有数据
   - 时间字段：updated_at（每次写入/更新时自动填充，北京时间）
   - MongoDB会自动生成 _id 字段，与业务id共存

2. closed_positions（历史集合）
   - 作用：永久保存所有已平仓记录
   - 特点：追加写入，永不删除，永不覆盖
   - id生成：交易所_合约名_平仓时间（强制使用平仓时间，确保格式统一）
   - 操作：根据id进行幂等写入（只写一次，双重保护）
   - 时间字段：created_at（首次写入时自动填充，北京时间）
   - MongoDB会自动生成 _id 字段，与业务id共存

【重要设计】
- 历史表的 id 强制使用平仓时间重新生成，不受上游数据影响
- 即使传入的数据中带有 id，也会被删除并用平仓时间重新生成
- 确保所有历史记录的 id 格式统一：交易所_开仓合约名_平仓时间

【调用关系】
调度器 (scheduler.py) 
    ↓ 推送 {tag, data}
数据库 (database.py) 
    ↓ 根据tag执行不同逻辑
MongoDB Atlas数据库

【重要变化 - 从Turso迁移到MongoDB】
1. 启动时不强制获取配置，等真正需要连接时才从 data_manager 获取
2. 连接方式从 aiohttp + SQL 改为 pymongo + run_in_executor
3. 不再需要SQL语句，直接操作Python字典
4. 数据格式完全不变，字段名、字段值原样存储
5. 时间字段自动填充：updated_at（持仓表）、created_at（历史表），使用北京时间（UTC+8）
==================================================
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

# 配置日志 - 统一前缀
logger = logging.getLogger(__name__)


def get_beijing_time() -> str:
    """
    获取北京时间（UTC+8）
    ==================================================
    MongoDB 服务器使用 UTC 时间，比北京时间晚8小时。
    此函数返回北京时间字符串，格式：YYYY-MM-DD HH:MM:SS
    
    :return: 北京时间字符串
    ==================================================
    """
    # 获取 UTC 时间，加上8小时得到北京时间
    beijing_time = datetime.utcnow() + timedelta(hours=8)
    return beijing_time.strftime('%Y-%m-%d %H:%M:%S')


class Database:
    """
    数据库操作类
    ==================================================
    这个类负责所有数据库写入操作，不提供任何读取接口。
    所有方法都是私有的（_开头），对外只暴露 handle_data 一个入口。
    
    【MongoDB迁移说明】
    - 启动时不获取配置，等调度器调用 handle_data 时才获取
    - 使用 run_in_executor 将同步的pymongo操作包装成异步
    - 所有数据格式与原Turso版本完全一致
    - 中文字段名完美支持
    - 时间字段自动填充：updated_at（持仓表）、created_at（历史表），使用北京时间
    
    【id生成规则】
    - 持仓表：交易所_开仓合约名_开仓时间（唯一标识一次开仓）
    - 历史表：交易所_开仓合约名_平仓时间（强制重新生成，确保格式统一）
    ==================================================
    """
    
    def __init__(self):
        """
        初始化数据库
        启动时不获取配置，等真正需要时才获取
        """
        # ----- 延迟获取配置，不在启动时强制要求 -----
        self.mongo_uri = None
        
        # ----- MongoDB客户端对象（懒加载）-----
        self._client = None
        self._db = None
        self._active = None  # active_positions 集合
        self._closed = None  # closed_positions 集合
        
        # ----- 初始化日志记录集合 -----
        self._logged_active_ids = set()
        
        # ----- 初始化最后日志时间记录 -----
        self._last_log_time = 0
        self._log_interval = 60
        
        logger.info("✅ 【数据库】初始化完成，等待调度器调用...")
    
    async def _get_db(self):
        """
        获取MongoDB数据库连接（懒加载）
        ==================================================
        首次调用时才从 data_manager 获取配置并建立连接
        ==================================================
        """
        if self._client is None:
            # 延迟获取配置
            if not self.mongo_uri:
                try:
                    from smart_brain import get_brain_instance
                    brain = get_brain_instance()
                    if brain and brain.data_manager:
                        self.mongo_uri = brain.data_manager.get_database_config('mongodb_uri')
                        logger.debug("✅ 【数据库】从 data_manager 获取 MongoDB 配置")
                except Exception as e:
                    logger.error(f"❌ 【数据库】获取 MongoDB 配置失败: {e}")
                    raise ConnectionError(f"无法获取 MongoDB 配置: {e}")
            
            if not self.mongo_uri:
                raise ConnectionError("❌ 【数据库】MongoDB 连接信息未配置")
            
            logger.info("✅ 【数据库】MongoDB配置获取成功")
            
            loop = asyncio.get_event_loop()
            try:
                # 在线程池中执行同步的MongoDB连接
                self._client = await loop.run_in_executor(
                    None,
                    lambda: MongoClient(
                        self.mongo_uri,
                        serverSelectionTimeoutMS=5000,  # 5秒连接超时
                        connectTimeoutMS=5000,
                        socketTimeoutMS=10000
                    )
                )
                # 测试连接
                await loop.run_in_executor(
                    None,
                    lambda: self._client.admin.command('ping')
                )
                
                # 获取数据库和集合
                self._db = self._client["trading_db"]
                self._active = self._db["active_positions"]
                self._closed = self._db["closed_positions"]
                
                logger.info("✅ 【数据库】MongoDB连接成功")
                
            except Exception as e:
                logger.error(f"❌ 【数据库】MongoDB连接失败: {e}")
                raise ConnectionError(f"无法连接到MongoDB: {e}")
        
        return self._db
    
    async def initialize(self):
        """
        异步初始化数据库连接和集合
        ==================================================
        - 建立连接
        - 创建索引（集合会自动创建，只需要建索引）
        - 验证连接是否成功
        ==================================================
        """
        # ----- 测试数据库连接 -----
        try:
            db = await self._get_db()
            logger.info("✅ 【数据库】连接测试成功")
        except Exception as e:
            raise ConnectionError(f"❌ 【数据库】无法连接到MongoDB: {e}")
        
        # ----- 初始化索引 -----
        await self._init_indexes()
        
        logger.info("✅ 【数据库】异步初始化完成")
    
    async def close(self):
        """
        关闭MongoDB连接
        """
        if self._client:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self._client.close()
            )
            logger.debug("🔌 【数据库】MongoDB连接已关闭")
    
    # ==================== 对外唯一入口 ====================
    
    async def handle_data(self, tag: str, data: Dict[str, Any]):
        """
        接收调度器推送的数据 - 这是数据库文件的唯一入口
        """
        try:
            exchange = data.get('交易所')
            if not exchange:
                logger.error("❌ 【数据库】数据中没有'交易所'字段，无法处理")
                return
            
            if tag == '平仓完整':
                logger.debug(f"📦 【数据库】收到平仓完整数据: {exchange}")
                await self._handle_closed(data, exchange)
                
            elif tag == '持仓完整':
                contract = data.get('开仓合约名', 'unknown')
                
                current_time = time.time()
                time_since_last_log = current_time - self._last_log_time
                
                if time_since_last_log >= self._log_interval:
                    logger.debug(f"📦 【数据库】收到持仓完整数据: {exchange} - {contract}")
                    self._last_log_time = current_time
                else:
                    logger.debug(f"📦 【数据库】收到持仓完整数据: {exchange} - {contract} (已抑制)")
                
                await self._handle_active(data)
                
            else:
                logger.warning(f"⚠️ 【数据库】收到未知标签: {tag}")
                
        except Exception as e:
            logger.error(f"❌ 【数据库】处理数据失败: {e}", exc_info=True)
    
    # ==================== 内部处理方法 ====================
    
    async def _handle_closed(self, data: Dict[str, Any], exchange: str):
        """处理平仓完整数据"""
        await self._insert_closed_position(data)
        await self._delete_active_position(exchange)
        logger.debug(f"✅ 【数据库】平仓完整处理完成: {exchange}")
    
    async def _handle_active(self, data: Dict[str, Any]):
        """处理持仓完整数据"""
        await self._save_active_position(data)
    
    # ==================== MongoDB数据库操作 ====================
    
    async def _save_active_position(self, data: Dict[str, Any]):
        """
        持仓区：覆盖更新（根据id）
        ==================================================
        逻辑：
            - 根据 id 进行 upsert（存在则更新，不存在则插入）
            - id 格式：交易所_开仓合约名_开仓时间
            - 唯一索引保证同一个 id 只有一条记录
            - 自动添加 updated_at 时间戳（北京时间）
        ==================================================
        """
        # 生成id（如果不存在）
        if 'id' not in data or not data['id']:
            exchange = data.get('交易所', 'unknown')
            contract = data.get('开仓合约名', 'unknown')
            open_time = data.get('开仓时间', '')
            data['id'] = f"{exchange}_{contract}_{open_time}"
            logger.debug(f"🔑 【数据库】持仓表生成id: {data['id']}")
        
        # 🔥 添加更新时间戳（北京时间，每次写入/更新都刷新）
        data['updated_at'] = get_beijing_time()
        
        record_id = data['id']
        exchange = data.get('交易所', 'unknown')
        contract = data.get('开仓合约名', 'unknown')
        
        db = await self._get_db()
        loop = asyncio.get_event_loop()
        
        # 检查是否首次写入（用于日志控制）
        exists = await loop.run_in_executor(
            None,
            lambda: self._active.find_one({"id": record_id}) is not None
        )
        
        # 覆盖更新（upsert = True：存在就更新，不存在就插入）
        await loop.run_in_executor(
            None,
            lambda: self._active.update_one(
                {"id": record_id},  # 查询条件
                {"$set": data},      # 更新操作（$set 保留未指定的字段）
                upsert=True          # 不存在则插入
            )
        )
        
        # 日志控制
        if not exists:
            logger.debug(f"✅ 【数据库】成功写入持仓区{exchange}数据 - {contract}（首次）")
            self._logged_active_ids.add(record_id)
        else:
            # 抑制重复日志，只打印debug
            logger.debug(f"📝 【数据库】更新持仓区{exchange}数据 - {contract}")
    
    async def _insert_closed_position(self, data: Dict[str, Any]):
        """
        历史区：幂等写入（根据id）
        ==================================================
        逻辑：
            - 强制使用平仓时间重新生成 id，确保格式统一
            - id 格式：交易所_开仓合约名_平仓时间
            - 优先：根据 id 检查是否存在，不存在才插入
            - 备选：唯一索引拦截重复插入
            - 确保同一个平仓记录只写一次
            - 自动添加 created_at 时间戳（北京时间）
        
        【重要】即使传入的数据中带有 id，也会被删除并用平仓时间重新生成
        ==================================================
        """
        # 创建副本，避免修改原始数据
        clean_data = data.copy()
        
        # ===== 强制删除可能存在的旧 id =====
        # 确保历史表使用自己的 id 格式（交易所_开仓合约名_平仓时间）
        if 'id' in clean_data:
            logger.debug(f"🗑️ 【数据库】历史表删除旧 id: {clean_data['id']}")
            del clean_data['id']
        
        # ===== 删除历史表不存在的字段 =====
        if 'updated_at' in clean_data:
            logger.debug(f"🗑️ 【数据库】删除历史表不存在的字段: updated_at")
            del clean_data['updated_at']
        
        # ===== 重新生成 id（使用平仓时间）=====
        exchange = clean_data.get('交易所', 'unknown')
        contract = clean_data.get('开仓合约名', 'unknown')
        close_time = clean_data.get('平仓时间', '')
        
        # 如果平仓时间为空，不应该发生，但为了安全加上判断
        if not close_time:
            logger.error(f"❌ 【数据库】历史表缺少平仓时间，无法生成id: {exchange}_{contract}")
            # 使用当前时间作为降级方案
            close_time = get_beijing_time()
        
        clean_data['id'] = f"{exchange}_{contract}_{close_time}"
        logger.debug(f"🔑 【数据库】历史表生成新 id: {clean_data['id']}")
        
        # 🔥 添加创建时间戳（北京时间，只在首次写入时记录）
        clean_data['created_at'] = get_beijing_time()
        
        record_id = clean_data['id']
        exchange_name = clean_data.get('交易所', 'unknown')
        contract_name = clean_data.get('开仓合约名', 'unknown')
        
        db = await self._get_db()
        loop = asyncio.get_event_loop()
        
        # ===== 第一层保护：代码层幂等性检查 =====
        exists = await loop.run_in_executor(
            None,
            lambda: self._closed.find_one({"id": record_id}) is not None
        )
        
        if exists:
            logger.debug(f"⏭️ 【数据库】历史区已存在记录，跳过写入: {record_id}")
            return
        
        # ===== 第二层保护：数据库层唯一索引拦截 =====
        try:
            await loop.run_in_executor(
                None,
                lambda: self._closed.insert_one(clean_data)
            )
            logger.debug(f"✅ 【数据库】成功写入历史区{exchange_name}数据 - {contract_name} 平仓时间:{close_time}")
        except DuplicateKeyError:
            logger.debug(f"⏭️ 【数据库】历史区已存在记录（唯一索引拦截），跳过写入: {record_id}")
            return
    
    async def _check_closed_exists(self, record_id: str) -> bool:
        """
        检查历史表中是否已存在该记录
        
        用于历史表的幂等性保护，避免重复写入。
        查询集合: closed_positions（历史集合）
        
        :param record_id: 记录ID (格式: 交易所_合约名_平仓时间)
        :return: 
            True = 记录已存在（跳过写入）
            False = 记录不存在（可以写入）
        """
        if not record_id:
            return False
        
        try:
            db = await self._get_db()
            loop = asyncio.get_event_loop()
            
            exists = await loop.run_in_executor(
                None,
                lambda: self._closed.find_one({"id": record_id}) is not None
            )
            
            if exists:
                logger.debug(f"🔍 【数据库】历史表已存在记录: {record_id}")
            return exists
            
        except Exception as e:
            logger.error(f"❌ 【数据库】检查历史表记录失败: {e}")
            return False  # 出错时假设不存在，让写入流程继续
    
    async def _check_active_exists(self, record_id: str) -> bool:
        """
        检查持仓表中是否已存在该记录
        
        用于持仓表的日志控制，判断是首次写入还是覆盖更新。
        查询集合: active_positions（持仓集合）
        
        :param record_id: 记录ID (格式: 交易所_合约名_开仓时间)
        :return: 
            True = 记录已存在（覆盖更新，不打印首次日志）
            False = 记录不存在（首次写入，打印首次日志）
        """
        if not record_id:
            return False
        
        try:
            db = await self._get_db()
            loop = asyncio.get_event_loop()
            
            exists = await loop.run_in_executor(
                None,
                lambda: self._active.find_one({"id": record_id}) is not None
            )
            
            if exists:
                logger.debug(f"🔍 【数据库】持仓表已存在记录: {record_id}")
            return exists
            
        except Exception as e:
            logger.error(f"❌ 【数据库】检查持仓表记录失败: {e}")
            return False  # 出错时假设不存在，允许打印首次写入日志
    
    async def _delete_active_position(self, exchange: str):
        """
        清理持仓区 - 根据交易所删除所有数据
        ==================================================
        逻辑：
            - 删除该交易所的所有持仓记录
            - 防止遗漏，确保完全清理
        ==================================================
        """
        if not exchange:
            logger.error("❌ 【数据库】清理持仓必须传入交易所参数，本次操作已取消")
            return
        
        db = await self._get_db()
        loop = asyncio.get_event_loop()
        
        result = await loop.run_in_executor(
            None,
            lambda: self._active.delete_many({"交易所": exchange})
        )
        
        logger.debug(f"✅ 【数据库】成功清除持仓区{exchange}数据，删除了{result.deleted_count}条")
    
    # ==================== 集合查询方法（用于兼容原代码）====================
    
    async def _get_collections(self) -> List[str]:
        """
        获取当前数据库中的所有集合名称
        ==================================================
        【MongoDB迁移】
        对应原来的 _get_tables() 方法
        返回所有集合名，用于验证和调试
        ==================================================
        """
        db = await self._get_db()
        loop = asyncio.get_event_loop()
        
        collections = await loop.run_in_executor(
            None,
            lambda: db.list_collection_names()
        )
        
        logger.info(f"📋 【数据库】当前数据库中的集合: {collections}")
        return collections
    
    # ==================== 连接测试 ====================
    
    async def test_connection(self) -> bool:
        """测试数据库连接是否正常"""
        try:
            db = await self._get_db()
            loop = asyncio.get_event_loop()
            
            # 发送ping命令测试连接
            await loop.run_in_executor(
                None,
                lambda: self._client.admin.command('ping')
            )
            
            logger.debug("✅ 【数据库】连接测试成功")
            return True
                
        except Exception as e:
            logger.error(f"❌ 【数据库】连接测试失败: {e}")
            return False
    
    # ==================== 初始化/建索引 ====================
    
    async def _init_indexes(self):
        """
        初始化数据库索引
        ==================================================
        两个集合的 id 字段都设为唯一索引：
            - 持仓集合：防止同一个开仓记录重复
            - 历史集合：防止同一个平仓记录重复写入
        
        索引列表：
            持仓集合：
                - id: 唯一索引（保证业务id不重复）
                - 交易所: 普通索引（用于按交易所查询和删除）
                - 开仓合约名: 普通索引（用于查询）
            
            历史集合：
                - id: 唯一索引（防止重复写入）
                - 交易所: 普通索引
                - 平仓时间: 普通索引（用于时间范围查询）
        ==================================================
        """
        try:
            # 先获取集合列表，用于调试
            collections_before = await self._get_collections()
            logger.debug(f"📋 【数据库】初始化前数据库中的集合: {collections_before}")
            
            db = await self._get_db()
            loop = asyncio.get_event_loop()
            
            # ==================== 持仓集合索引 ====================
            # 1. id 唯一索引（防止重复开仓记录）
            await loop.run_in_executor(
                None,
                lambda: self._active.create_index("id", unique=True, background=True)
            )
            logger.debug("📝 【数据库】持仓集合 id 唯一索引创建完成")
            
            # 2. 交易所索引（用于按交易所查询和删除）
            await loop.run_in_executor(
                None,
                lambda: self._active.create_index("交易所", background=True)
            )
            logger.debug("📝 【数据库】持仓集合 交易所 索引创建完成")
            
            # 3. 开仓合约名索引
            await loop.run_in_executor(
                None,
                lambda: self._active.create_index("开仓合约名", background=True)
            )
            logger.debug("📝 【数据库】持仓集合 开仓合约名 索引创建完成")
            
            # ==================== 历史集合索引 ====================
            # 1. id 唯一索引（防止重复写入平仓记录）
            await loop.run_in_executor(
                None,
                lambda: self._closed.create_index("id", unique=True, background=True)
            )
            logger.debug("📝 【数据库】历史集合 id 唯一索引创建完成")
            
            # 2. 交易所索引
            await loop.run_in_executor(
                None,
                lambda: self._closed.create_index("交易所", background=True)
            )
            logger.debug("📝 【数据库】历史集合 交易所 索引创建完成")
            
            # 3. 平仓时间索引（用于时间范围查询）
            await loop.run_in_executor(
                None,
                lambda: self._closed.create_index("平仓时间", background=True)
            )
            logger.debug("📝 【数据库】历史集合 平仓时间 索引创建完成")
            
            # 验证集合是否存在
            collections_after = await self._get_collections()
            logger.debug(f"📋 【数据库】初始化后数据库中的集合: {collections_after}")
            
            logger.info("✅ 【数据库】MongoDB索引初始化完成（id字段已设为唯一）")
            
        except Exception as e:
            logger.error(f"❌ 【数据库】索引初始化失败: {e}")
            raise