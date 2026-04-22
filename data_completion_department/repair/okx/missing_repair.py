"""
欧意持仓缺失修复
==================================================
【文件职责】
这个文件是欧意修复区的核心，负责处理两种信息标签：
1. "欧意持仓缺失" - 启动修复流程（循环运行）
2. "欧意空仓"     - 停止修复流程

【重要提醒】
这个文件会被两个地方调用：
1. 调度器直接调用 handle_info() 推送信息标签
2. receiver 通过币安修复区入口间接调用 handle_store_snapshot() 推送数据

【数据来源】
1. 门外存储区快照：通过 handle_store_snapshot() 接收
2. 数据库持仓区：从MongoDB数据库读取历史持仓数据（第1步需要）
3. 本文件缓存：保存修复过程中的中间数据

【门外标签】
调度器推送的信息标签，永远只有1个（覆盖更新）
   - 持仓缺失标签 = 开（启动循环）
   - 空仓标签     = 关（停止循环）

【门外数据】
receiver推送的存储区快照，永远只有1份（覆盖更新）
   - 包含最新的user_data
   - 修复区从这里获取欧意数据

【修复流程 - 共6步】（完全按照你的设计文档）
第1步：获取缓存数据（从数据库或直接用缓存）
第2步：检测资金费状态（4种情况）
第3步：资金费融合（只有情况D需要）
第4步：覆盖更新（从门外存储区读最新数据，保护资金费4字段）
第5步：计算固定字段（包括6个原有字段+4个平仓相关字段）
第6步：检测平仓价并打对应标签推送

【数据库迁移 - 从Turso到MongoDB】
2026-03-20 修改：将数据库查询从Turso改为MongoDB
- 通过全局大脑实例获取 data_manager，再从 data_manager 获取数据库连接字符串
- 查询方式从 SQL 改为直接字典查询
- 连接方式从 aiohttp 改为 pymongo（按需连接，用完即弃）
- 结果处理从 _row_to_dict() 转换改为直接使用（MongoDB返回就是字典）
- 读取逻辑优化：按开仓时间倒序取最新一条，防止残留数据干扰
- 数据清洗：删除 _id 字段 + 按标准顺序排序
==================================================
"""

import logging
import asyncio
import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from pymongo import MongoClient

# 导入常量 - 使用修正后的常量名
from ...constants import (
    TAG_COMPLETE,
    TAG_CLOSED_COMPLETE,
    INFO_OKX_MISSING,
    INFO_OKX_EMPTY,
    EXCHANGE_OKX,
    FIELD_EXCHANGE,
    FIELD_OPEN_CONTRACT,
    FIELD_OPEN_PRICE,
    FIELD_OPEN_DIRECTION,
    FIELD_POSITION_SIZE,
    FIELD_POSITION_CONTRACTS,
    FIELD_CONTRACT_VALUE,
    FIELD_LEVERAGE,
    FIELD_OPEN_POSITION_VALUE,
    FIELD_OPEN_MARGIN,
    FIELD_MARK_PRICE,
    FIELD_MARK_POSITION_VALUE,
    FIELD_MARK_MARGIN,
    FIELD_LATEST_PRICE,
    FIELD_LATEST_POSITION_VALUE,
    FIELD_LATEST_MARGIN,
    FIELD_MARK_PNL_PERCENT,
    FIELD_LATEST_PNL_PERCENT,
    FIELD_CLOSE_TIME,
    FIELD_CLOSE_PRICE,
    FIELD_CLOSE_POSITION_VALUE,
    FIELD_CLOSE_PNL_PERCENT,
    FIELD_CLOSE_PNL,
    FIELD_CLOSE_PNL_PERCENT_OF_MARGIN,
    FIELD_AVG_FUNDING_RATE,
    FIELD_FUNDING_THIS,
    FIELD_FUNDING_TOTAL,
    FIELD_FUNDING_COUNT,
    FIELD_FUNDING_TIME,
)

# 导入工具函数 - 字典排序
from ..utils import order_dict

logger = logging.getLogger(__name__)


class OkxMissingRepair:
    """
    欧意持仓缺失修复类
    ==================================================
    这个类负责：
        1. 接收门外标签（通过 handle_info）
        2. 接收门外存储区快照（通过 handle_store_snapshot）
        3. 根据标签启动/停止修复循环
        4. 执行6步修复流程

    门外标签规则：
        - 永远只有1个标签（覆盖更新）
        - 持仓缺失 = 开（启动循环）
        - 空仓     = 关（停止循环）

    门外数据规则：
        - 永远只有1份存储区快照（覆盖更新）
        - 包含最新的 user_data

    修复循环规则：
        - 每秒执行一次
        - 每次执行前检查门外标签
        - 如果标签变了，自己停止
        - 如果修复过程出错，等待5秒后重试
    ==================================================
    """

    def __init__(self, scheduler):
        """
        初始化修复区

        :param scheduler: 调度器实例，用于推送修复结果
        """
        self.scheduler = scheduler

        # ===== 门外标签状态（不是缓存，只是记录）=====
        self.current_info = None

        # ===== 门外存储区快照（receiver推送过来的）=====
        self.latest_snapshot = None

        # ===== 修复循环控制 =====
        self.is_running = False
        self.repair_task = None

        # ===== 本文件数据缓存（用于修复计算）=====
        self.cache = None

        # 临时存储门外数据（供第3步使用）
        self._snapshot_data = None

        logger.info("✅【欧易持仓缺失修复区】 初始化完成")

    # ==================== 对外入口 ====================

    async def handle_info(self, info: str):
        """
        接收调度器推送的信息标签
        ==================================================
        这是修复区的标签入口，调度器会把两种标签推送到这里：
            - "欧意持仓缺失"
            - "欧意空仓"

        门外标签规则：
            - 永远只有1个标签（覆盖更新）
            - 新标签到来直接覆盖旧标签

        动作：
            - 记录当前标签（self.current_info）
            - 根据标签类型启动或停止修复循环
        ==================================================

        :param info: 信息标签，只能是"欧意持仓缺失"或"欧意空仓"
        """
        old_info = self.current_info
        self.current_info = info

        logger.debug(f"📨【欧易持仓缺失修复区】 门外标签更新: {old_info} → {info}")

        if info == INFO_OKX_MISSING:
            await self._start_repair()
        elif info == INFO_OKX_EMPTY:
            await self._stop_repair()
        else:
            logger.warning(f"⚠️【欧易持仓缺失修复区】 收到未知信息标签: {info}")

    async def handle_store_snapshot(self, snapshot: Dict):
        """
        接收receiver推送的存储区快照
        ==================================================
        这是修复区的数据入口，receiver会把整个存储区推送到这里：
            {
                'market_data': {...},
                'user_data': {...},
                'timestamp': '...'
            }

        门外数据规则：
            - 永远只有1份快照（覆盖更新）
            - 新快照到来直接覆盖旧快照

        动作：
            - 保存最新的快照（self.latest_snapshot）
        ==================================================

        :param snapshot: 完整的存储区快照
        """
        self.latest_snapshot = snapshot
        logger.debug(f"📦【欧易持仓缺失修复区】 收到存储区快照，时间戳: {snapshot.get('timestamp')}")

    # ==================== 修复循环控制 ====================

    async def _start_repair(self):
        """启动修复流程（循环运行）"""
        if self.is_running:
            logger.debug("【欧易持仓缺失修复区】修复流程已在运行中")
            return

        self.is_running = True
        self.repair_task = asyncio.create_task(self._repair_loop())
        logger.info("🚀【欧易持仓缺失修复区】 修复流程已启动（循环运行）")

    async def _stop_repair(self):
        """停止修复流程"""
        if not self.is_running:
            return

        self.is_running = False
        if self.repair_task:
            self.repair_task.cancel()
            try:
                await self.repair_task
            except asyncio.CancelledError:
                pass
            self.repair_task = None
        logger.info("🛑 【欧易持仓缺失修复区】修复流程已停止")

    async def _repair_loop(self):
        """
        修复循环
        ==================================================
        只要门外标签是"欧意持仓缺失"，就一直运行

        循环频率：每秒执行一次
        安全机制：
            - 每次执行前检查门外标签
            - 如果标签变了，自己停止
            - 如果修复过程出错，等待5秒后重试
        ==================================================
        """
        logger.debug("🔄【欧易持仓缺失修复区】 修复循环开始")

        while self.is_running:
            await asyncio.sleep(0)
            try:
                if self.current_info != INFO_OKX_MISSING:
                    logger.debug("【欧易持仓缺失修复区】门外标签已不是持仓缺失，停止修复循环")
                    await self._stop_repair()
                    break

                await self._repair_once()
                await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info("【欧易持仓缺失修复区】修复循环被取消")
                break
            except Exception as e:
                logger.error(f"❌ 【欧易持仓缺失修复区】修复循环出错: {e}", exc_info=True)
                await asyncio.sleep(5)

        logger.info("🔄【欧易持仓缺失修复区】 修复循环结束")

    async def _repair_once(self):
        """
        执行一轮修复流程
        ==================================================
        完全按照你的6步设计文档：
            第1步：获取缓存数据
            第2步：检测资金费状态
            第3步：资金费融合（仅情况D需要）
            第4步：覆盖更新（从门外存储区读最新数据，保护资金费4字段）
            第5步：计算固定字段（包括6个原有字段+4个平仓相关字段）
            第6步：检测平仓价并打对应标签推送
        ==================================================
        """
        logger.debug("执行一轮欧意持仓缺失修复")

        if not await self._step1_get_cache():
            logger.error("❌【欧易持仓缺失修复区】 第1步失败：无法获取缓存数据，本次修复终止")
            return

        if not self.latest_snapshot:
            logger.warning("⚠️【欧易持仓缺失修复区】 门外还没有存储区数据，等待下次循环")
            return

        funding_action = await self._step2_check_funding()
        if funding_action is None:
            logger.error("❌【欧易持仓缺失修复区】 第2步失败：检测资金费状态出错，本次修复终止")
            return

        if funding_action == 'do_fusion':
            await self._step3_funding_fusion()

        await self._step4_update_from_snapshot()
        await self._step5_calc_fixed_fields()
        await self._step6_push_complete()

        logger.debug("【欧易持仓缺失修复区】一轮修复流程执行完成")

    # ==================== 辅助函数：安全转换 ====================
    
    def _safe_float(self, value, default=0.0):
        """安全转换为float，如果是字符串则转换，如果是None则返回默认值"""
        if value is None:
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            logger.debug(f"【欧易持仓缺失修复区】 类型转换失败: {value}, 使用默认值 {default}")
            return default

    # ==================== 6步修复流程 ====================

    async def _step1_get_cache(self) -> bool:
        """
        第1步：获取缓存数据
        ==================================================
        【MongoDB迁移说明】
        按照"按需连接"原则重构：
            1. 先检查缓存是否有数据
            2. 如果有缓存，直接使用，不需要连接数据库
            3. 如果没缓存，才去连接MongoDB读取
        
        【读取逻辑优化】
        - 按开仓时间倒序排序，取最新一条数据
        - 防止平仓失败时残留的旧数据干扰修复流程
        
        【数据清洗】
        - 删除 MongoDB 的 _id 字段（避免 JSON 序列化报错）
        - 按标准顺序重新排列字段（让路由数据显示整齐）
        ==================================================
        """
        if self.cache is not None:
            logger.debug("✅【欧易持仓缺失修复区】 第1步：使用现有缓存")
            return True

        logger.info("🔍【欧易持仓缺失修复区】 第1步：缓存为空，准备从MongoDB读取")

        # ----- 第2层：获取MongoDB连接信息（通过全局大脑实例获取）-----
        try:
            from smart_brain import get_brain_instance
            brain = get_brain_instance()
            if brain is None:
                logger.error("❌【欧易持仓缺失修复区】 大脑实例尚未初始化")
                return False
            
            data_manager = brain.data_manager
            mongo_uri = data_manager.get_database_config('mongodb_uri')
            
            if not mongo_uri:
                logger.error("❌【欧易持仓缺失修复区】 MongoDB 连接信息未配置")
                return False
            
            logger.info("✅【欧易持仓缺失修复区】 成功从 data_manager 读取MongoDB连接信息")
            
        except ImportError as e:
            logger.error(f"❌【欧易持仓缺失修复区】 无法导入 smart_brain 模块: {e}")
            return False
        except AttributeError as e:
            logger.error(f"❌【欧易持仓缺失修复区】 大脑实例结构异常: {e}")
            return False

        # ----- 第3层：连接MongoDB并查询欧意数据 -----
        loop = asyncio.get_event_loop()
        client = None
        try:
            client = await loop.run_in_executor(
                None,
                lambda: MongoClient(
                    mongo_uri,
                    serverSelectionTimeoutMS=5000,
                    connectTimeoutMS=5000
                )
            )
            
            await loop.run_in_executor(
                None,
                lambda: client.admin.command('ping')
            )
            logger.debug("✅【欧易持仓缺失修复区】 MongoDB连接成功")
            
            db = client["trading_db"]
            collection = db["active_positions"]
            
            cursor = await loop.run_in_executor(
                None,
                lambda: collection.find({"交易所": "okx"}).sort("开仓时间", -1).limit(1)
            )
            
            results = await loop.run_in_executor(
                None,
                lambda: list(cursor)
            )
            
            result = results[0] if results else None
            
            if not result:
                logger.warning("⚠️【欧易持仓缺失修复区】 未找到交易所为 'okx' 的数据，尝试其他写法")
                test_exchanges = ['OKX', 'Okx', 'okex', 'OKEX', '欧意', '欧易']
                for test_exchange in test_exchanges:
                    await asyncio.sleep(0)
                    cursor = await loop.run_in_executor(
                        None,
                        lambda ex=test_exchange: collection.find({"交易所": ex}).sort("开仓时间", -1).limit(1)
                    )
                    results = await loop.run_in_executor(
                        None,
                        lambda: list(cursor)
                    )
                    if results:
                        result = results[0]
                        logger.debug(f"✅【欧易持仓缺失修复区】 找到数据！交易所字段实际为: {test_exchange}")
                        break
            
            if not result:
                logger.error("❌【欧易持仓缺失修复区】 尝试了所有可能的交易所名称，都没有找到数据")
                return False
            
            if '_id' in result:
                del result['_id']
            
            result = order_dict(result)
            
            self.cache = result

            logger.info(f"✅【欧易持仓缺失修复区】 第1步：成功读取到欧意数据（最新开仓时间）")
            logger.info(f"   交易所: {self.cache.get(FIELD_EXCHANGE)}")
            logger.info(f"   开仓合约名: {self.cache.get(FIELD_OPEN_CONTRACT)}")
            logger.info(f"   开仓时间: {self.cache.get('开仓时间')}")
            logger.info(f"   ID: {self.cache.get('id')}")
            
            logger.debug(f"   开仓价: {self.cache.get(FIELD_OPEN_PRICE)}")
            logger.debug(f"   持仓张数: {self.cache.get(FIELD_POSITION_CONTRACTS)}")
            logger.debug(f"   累计资金费: {self.cache.get(FIELD_FUNDING_TOTAL)}")
            
            return True

        except Exception as e:
            logger.error(f"❌【欧易持仓缺失修复区】 第1步：读取MongoDB失败: {e}", exc_info=True)
            return False
        finally:
            if client:
                await loop.run_in_executor(None, client.close)
                logger.debug("🔌【欧易持仓缺失修复区】 MongoDB连接已关闭")

    async def _step2_check_funding(self) -> Optional[str]:
        """
        第2步：检测资金费状态
        ==================================================
        从门外存储区快照获取最新的欧意数据，然后检测：
            1. 有无历史：缓存累计资金费是否等于0
            2. 有无新结算：存储区累计资金费是否等于缓存累计资金费

        4种情况：
            A. 无历史 + 无新结算 → 返回 'skip_to_step4'
            B. 无历史 + 有新结算 → 更新4个资金费字段 → 返回 'skip_to_step4'
            C. 有历史 + 无新结算 → 返回 'skip_to_step4'
            D. 有历史 + 有新结算 → 返回 'do_fusion'
        ==================================================
        """
        logger.debug("【欧易持仓缺失修复区】第2步：检测资金费状态")

        snapshot_data = self._get_okx_from_snapshot()
        if not snapshot_data:
            logger.error("❌【欧易持仓缺失修复区】 门外存储区中没有欧意数据")
            return None

        cache_total = self.cache.get(FIELD_FUNDING_TOTAL, 0)
        if cache_total is None:
            cache_total = 0
        has_history = (cache_total != 0)

        snapshot_total = snapshot_data.get(FIELD_FUNDING_TOTAL, 0)
        if snapshot_total is None:
            snapshot_total = 0
        has_new = (snapshot_total != cache_total)

        logger.debug(f" 【欧易持仓缺失修复区】  缓存累计资金费: {cache_total}, 门外存储区累计资金费: {snapshot_total}")
        logger.debug(f" 【欧易持仓缺失修复区】  有无历史: {has_history}, 有无新结算: {has_new}")

        self._snapshot_data = snapshot_data

        if not has_history and not has_new:
            logger.debug(" 【欧易持仓缺失修复区】  情况A：无历史 + 无新结算，直接跳到第4步")
            return 'skip_to_step4'
        elif not has_history and has_new:
            logger.debug(" 【欧易持仓缺失修复区】  情况B：无历史 + 有新结算，更新4个资金费字段后跳到第4步")
            self._update_funding_fields(snapshot_data)
            return 'skip_to_step4'
        elif has_history and not has_new:
            logger.debug(" 【欧易持仓缺失修复区】  情况C：有历史 + 无新结算，直接跳到第4步")
            return 'skip_to_step4'
        else:
            logger.debug(" 【欧易持仓缺失修复区】  情况D：有历史 + 有新结算，进入第3步资金费融合")
            return 'do_fusion'

    async def _step3_funding_fusion(self):
        """
        第3步：资金费融合流程
        ==================================================
        只有情况D（有历史+有新结算）才会执行这一步。

        融合规则：
            1. 累计资金费、本次结算时间 → 直接从门外存储区覆盖（允许部分更新）
            2. 本次资金费 = 门外存储区累计资金费 - 缓存累计资金费
            3. 资金费结算次数 = 缓存次数 + 1

        【重要】允许部分更新：即使本次资金费计算失败，累计资金费和结算时间也会更新
        ==================================================
        """
        logger.debug("【欧易持仓缺失修复区】第3步：执行资金费融合")

        snapshot = self._snapshot_data
        cache = self.cache
        
        try:
            snapshot_total = self._safe_float(snapshot.get(FIELD_FUNDING_TOTAL))
            cache_total = self._safe_float(cache.get(FIELD_FUNDING_TOTAL))
            cache[FIELD_FUNDING_THIS] = snapshot_total - cache_total
            logger.debug(f" ✅ 本次资金费计算: {snapshot_total} - {cache_total} = {cache[FIELD_FUNDING_THIS]}")
        except Exception as e:
            logger.error(f" ❌ 本次资金费计算失败: {e}，保持原值 {cache.get(FIELD_FUNDING_THIS)}")

        try:
            cache_count = self._safe_float(cache.get(FIELD_FUNDING_COUNT))
            cache[FIELD_FUNDING_COUNT] = cache_count + 1
            logger.debug(f" ✅ 结算次数计算: {cache_count} + 1 = {cache[FIELD_FUNDING_COUNT]}")
        except Exception as e:
            logger.error(f" ❌ 结算次数计算失败: {e}，保持原值 {cache.get(FIELD_FUNDING_COUNT)}")

        if FIELD_FUNDING_TOTAL in snapshot:
            cache[FIELD_FUNDING_TOTAL] = snapshot[FIELD_FUNDING_TOTAL]
            logger.debug(f" ✅ 累计资金费已覆盖: {cache[FIELD_FUNDING_TOTAL]}")
        
        if FIELD_FUNDING_TIME in snapshot and snapshot[FIELD_FUNDING_TIME] is not None:
            cache[FIELD_FUNDING_TIME] = snapshot[FIELD_FUNDING_TIME]
            logger.debug(f" ✅ 本次结算时间已覆盖: {cache[FIELD_FUNDING_TIME]}")

        logger.debug(f" 【欧易持仓缺失修复区】  融合后 - 累计资金费: {cache.get(FIELD_FUNDING_TOTAL)}, "
                   f"本次资金费: {cache.get(FIELD_FUNDING_THIS)}, "
                   f"结算次数: {cache.get(FIELD_FUNDING_COUNT)}")
                   
    async def _step4_update_from_snapshot(self):
        """
        第4步：从门外存储区快照覆盖更新缓存
        ==================================================
        规则：
            1. 从门外存储区快照获取最新的欧意数据
            2. 把有值的字段（不为None/空）覆盖到缓存
            3. 保护4个资金费字段不被覆盖
        ==================================================
        """
        logger.debug("【欧易持仓缺失修复区】第4步：从门外存储区覆盖更新缓存")

        snapshot_data = self._get_okx_from_snapshot()
        if not snapshot_data:
            logger.warning("⚠️【欧易持仓缺失修复区】 门外存储区中没有欧意数据，跳过第4步")
            return

        protected_fields = [
            FIELD_FUNDING_THIS,
            FIELD_FUNDING_TOTAL,
            FIELD_FUNDING_COUNT,
            FIELD_FUNDING_TIME
        ]

        update_count = 0
        skip_count = 0

        for key, value in snapshot_data.items():
            await asyncio.sleep(0)
            if key in protected_fields:
                skip_count += 1
                continue

            if value is not None and value != '':
                self.cache[key] = value
                update_count += 1

        logger.debug(f" 【欧易持仓缺失修复区】  已覆盖 {update_count} 个字段，跳过 {skip_count} 个保护字段")

    async def _step5_calc_fixed_fields(self):
        """
        第5步：计算固定字段
        ==================================================
        【严格按照原始方案，独立计算，不复用任何中间结果】

        需要计算的字段（共10个）：
            原有6个字段：
                1. 标记价涨跌盈亏幅
                2. 标记价仓位价值
                3. 最新价涨跌盈亏幅
                4. 最新价保证金
                5. 最新价仓位价值
                6. 平均资金费率
            
            新增4个平仓相关字段（仅在平仓价不为空时计算）：
                7. 平仓价仓位价值
                8. 平仓价涨跌盈亏幅
                9. 平仓收益
                10. 平仓收益率

        计算公式（严格按照你的文档，每个字段独立计算）：

        当开仓方向为 "LONG" 时：
            标记价涨跌盈亏幅 = (标记价 - 开仓价) * 100 / 开仓价
            最新价涨跌盈亏幅 = (最新价 - 开仓价) * 100 / 开仓价
            标记价仓位价值 = 标记价 * 合约面值 * 持仓张数
            最新价仓位价值 = 最新价 * 合约面值 * 持仓张数
            最新价保证金 = 最新价 * 合约面值 * 持仓张数 ÷ 杠杆
            平均资金费率 = 累计资金费 * 100 / 开仓价仓位价值

            平仓价仓位价值 = 平仓价 * 持仓币数
            平仓价涨跌盈亏幅 = (平仓价 - 开仓价) * 100 / 开仓价
            平仓收益 = (平仓价 - 开仓价) * 持仓币数
            平仓收益率 = (平仓价 - 开仓价) * |标记价仓位价值| * 100 / (开仓价 * 标记价保证金)

        当开仓方向为 "SHORT" 时：
            标记价涨跌盈亏幅 = (开仓价 - 标记价) * 100 / 开仓价
            最新价涨跌盈亏幅 = (开仓价 - 最新价) * 100 / 开仓价
            标记价仓位价值 = 标记价 * 合约面值 * 持仓张数
            最新价仓位价值 = 最新价 * 合约面值 * 持仓张数
            最新价保证金 = 最新价 * 合约面值 * 持仓张数 ÷ 杠杆
            平均资金费率 = 累计资金费 * 100 / 开仓价仓位价值

            平仓价仓位价值 = 平仓价 * 持仓币数
            平仓价涨跌盈亏幅 = (开仓价 - 平仓价) * 100 / 开仓价
            平仓收益 = (开仓价 - 平仓价) * 持仓币数
            平仓收益率 = (开仓价 - 平仓价) * |标记价仓位价值| * 100 / (开仓价 * 标记价保证金)

        注意：
            - 欧易数据本身已经包含标记价和最新价，直接从缓存中获取
            - 平仓价字段值为空时，跳过平仓相关字段的计算
        ==================================================
        """
        logger.debug("【欧易持仓缺失修复区】第5步：计算固定字段（严格按照原始方案，独立计算）")

        cache = self.cache

        mark_price = self._safe_float(cache.get(FIELD_MARK_PRICE))
        latest_price = self._safe_float(cache.get(FIELD_LATEST_PRICE))
        close_price = cache.get(FIELD_CLOSE_PRICE)
        contract_value = self._safe_float(cache.get(FIELD_CONTRACT_VALUE))
        contracts = self._safe_float(cache.get(FIELD_POSITION_CONTRACTS))
        position_size = self._safe_float(cache.get(FIELD_POSITION_SIZE))
        leverage = self._safe_float(cache.get(FIELD_LEVERAGE), 1.0)
        open_price = self._safe_float(cache.get(FIELD_OPEN_PRICE))
        open_position_value = self._safe_float(cache.get(FIELD_OPEN_POSITION_VALUE))
        direction = cache.get(FIELD_OPEN_DIRECTION)
        total_funding = self._safe_float(cache.get(FIELD_FUNDING_TOTAL))
        mark_position_value = self._safe_float(cache.get(FIELD_MARK_POSITION_VALUE))
        mark_margin = self._safe_float(cache.get(FIELD_MARK_MARGIN))

        if direction == "LONG":
            mark_pnl_percent = (mark_price - open_price) * 100 / open_price if open_price else 0
            mark_pnl_percent = round(mark_pnl_percent, 4)

            latest_pnl_percent = (latest_price - open_price) * 100 / open_price if open_price else 0
            latest_pnl_percent = round(latest_pnl_percent, 4)

            mark_position_value_calc = mark_price * contract_value * contracts
            mark_position_value_calc = round(mark_position_value_calc, 4)

            latest_position_value = latest_price * contract_value * contracts
            latest_position_value = round(latest_position_value, 4)

            latest_margin = (latest_price * contract_value * contracts / leverage) if leverage else 0
            latest_margin = round(latest_margin, 4)

            avg_funding_rate = (total_funding * 100 / open_position_value) if open_position_value else 0
            avg_funding_rate = round(avg_funding_rate, 4)

            cache[FIELD_MARK_PNL_PERCENT] = mark_pnl_percent
            cache[FIELD_LATEST_PNL_PERCENT] = latest_pnl_percent
            cache[FIELD_MARK_POSITION_VALUE] = mark_position_value_calc
            cache[FIELD_LATEST_POSITION_VALUE] = latest_position_value
            cache[FIELD_LATEST_MARGIN] = latest_margin
            cache[FIELD_AVG_FUNDING_RATE] = avg_funding_rate

            if close_price is not None:
                close_price_float = self._safe_float(close_price)
                close_position_value = close_price_float * position_size
                close_position_value = round(close_position_value, 4)
                cache[FIELD_CLOSE_POSITION_VALUE] = close_position_value

                close_pnl_percent = (close_price_float - open_price) * 100 / open_price if open_price else 0
                close_pnl_percent = round(close_pnl_percent, 4)
                cache[FIELD_CLOSE_PNL_PERCENT] = close_pnl_percent

                close_pnl = (close_price_float - open_price) * position_size
                close_pnl = round(close_pnl, 4)
                cache[FIELD_CLOSE_PNL] = close_pnl

                mark_position_value_abs = abs(mark_position_value_calc)
                close_pnl_percent_of_margin = (close_price_float - open_price) * mark_position_value_abs * 100 / (open_price * mark_margin) if (open_price and mark_margin) else 0
                close_pnl_percent_of_margin = round(close_pnl_percent_of_margin, 4)
                cache[FIELD_CLOSE_PNL_PERCENT_OF_MARGIN] = close_pnl_percent_of_margin

                logger.debug(f"  【欧易持仓缺失修复区】 平仓计算完成 - 平仓价: {close_price_float}, "
                           f"平仓价仓位价值: {close_position_value:.2f}, "
                           f"平仓价涨跌盈亏幅: {close_pnl_percent:.2f}%, "
                           f"平仓收益: {close_pnl:.2f}, "
                           f"平仓收益率: {close_pnl_percent_of_margin:.2f}%")
            else:
                logger.debug("  【欧易持仓缺失修复区】 平仓价为空，跳过平仓相关字段计算")

        else:
            mark_pnl_percent = (open_price - mark_price) * 100 / open_price if open_price else 0
            mark_pnl_percent = round(mark_pnl_percent, 4)

            latest_pnl_percent = (open_price - latest_price) * 100 / open_price if open_price else 0
            latest_pnl_percent = round(latest_pnl_percent, 4)

            mark_position_value_calc = mark_price * contract_value * contracts
            mark_position_value_calc = round(mark_position_value_calc, 4)

            latest_position_value = latest_price * contract_value * contracts
            latest_position_value = round(latest_position_value, 4)

            latest_margin = (latest_price * contract_value * contracts / leverage) if leverage else 0
            latest_margin = round(latest_margin, 4)

            avg_funding_rate = (total_funding * 100 / open_position_value) if open_position_value else 0
            avg_funding_rate = round(avg_funding_rate, 4)

            cache[FIELD_MARK_PNL_PERCENT] = mark_pnl_percent
            cache[FIELD_LATEST_PNL_PERCENT] = latest_pnl_percent
            cache[FIELD_MARK_POSITION_VALUE] = mark_position_value_calc
            cache[FIELD_LATEST_POSITION_VALUE] = latest_position_value
            cache[FIELD_LATEST_MARGIN] = latest_margin
            cache[FIELD_AVG_FUNDING_RATE] = avg_funding_rate

            if close_price is not None:
                close_price_float = self._safe_float(close_price)
                close_position_value = close_price_float * position_size
                close_position_value = round(close_position_value, 4)
                cache[FIELD_CLOSE_POSITION_VALUE] = close_position_value

                close_pnl_percent = (open_price - close_price_float) * 100 / open_price if open_price else 0
                close_pnl_percent = round(close_pnl_percent, 4)
                cache[FIELD_CLOSE_PNL_PERCENT] = close_pnl_percent

                close_pnl = (open_price - close_price_float) * position_size
                close_pnl = round(close_pnl, 4)
                cache[FIELD_CLOSE_PNL] = close_pnl

                mark_position_value_abs = abs(mark_position_value_calc)
                close_pnl_percent_of_margin = (open_price - close_price_float) * mark_position_value_abs * 100 / (open_price * mark_margin) if (open_price and mark_margin) else 0
                close_pnl_percent_of_margin = round(close_pnl_percent_of_margin, 4)
                cache[FIELD_CLOSE_PNL_PERCENT_OF_MARGIN] = close_pnl_percent_of_margin

                logger.debug(f"  【欧易持仓缺失修复区】 平仓计算完成 - 平仓价: {close_price_float}, "
                           f"平仓价仓位价值: {close_position_value:.2f}, "
                           f"平仓价涨跌盈亏幅: {close_pnl_percent:.2f}%, "
                           f"平仓收益: {close_pnl:.2f}, "
                           f"平仓收益率: {close_pnl_percent_of_margin:.2f}%")
            else:
                logger.debug("  【欧易持仓缺失修复区】 平仓价为空，跳过平仓相关字段计算")

        logger.debug(f"  【欧易持仓缺失修复区】 计算完成 - 标记价: {mark_price}, 最新价: {latest_price}, "
                   f"标记价仓位价值: {mark_position_value_calc:.2f}, "
                   f"最新价仓位价值: {latest_position_value:.2f}, "
                   f"最新价保证金: {latest_margin:.2f}, "
                   f"最新价涨跌盈亏幅: {latest_pnl_percent:.2f}%, "
                   f"平均资金费率: {avg_funding_rate:.4f}%, "
                   f"开仓方向: {direction}")

    async def _step6_push_complete(self):
        """
        第6步：检测平仓价并打对应标签推送
        ==================================================
        做了三件事：
            1. 创建缓存数据的副本
            2. 检测平仓价字段：
               - 若平仓价为空，打标签"持仓完整"
               - 若平仓价不为空，打标签"平仓完整"
            3. 推送给调度器
        ==================================================
        """
        logger.debug("【欧易持仓缺失修复区】第6步：检测平仓价并打对应标签推送")

        data_copy = self.cache.copy()
        
        close_price = data_copy.get(FIELD_CLOSE_PRICE)
        close_time = data_copy.get(FIELD_CLOSE_TIME)
        
        if close_price is not None and close_price != '' and close_time is not None and close_time != '':
            tag = TAG_CLOSED_COMPLETE
            logger.debug(f"  【欧易持仓缺失修复区】 检测到平仓价有值，打标签: {tag}")
        else:
            tag = TAG_COMPLETE
            logger.debug(f"  【欧易持仓缺失修复区】 检测到平仓价为空，打标签: {tag}")

        await self.scheduler.handle({
            'tag': tag,
            'data': data_copy
        })

        exchange = data_copy.get(FIELD_EXCHANGE, 'unknown')
        contract = data_copy.get(FIELD_OPEN_CONTRACT, 'unknown')
        logger.debug(f"✅ 【欧易持仓缺失修复区】已推送{tag}数据: {exchange} - {contract}")

    # ==================== 辅助方法 ====================

    def _get_okx_from_snapshot(self) -> Optional[Dict]:
        """
        从门外存储区快照中获取最新的欧意数据
        ==================================================
        存储区快照格式：
            {
                'user_data': {
                    'okx_user': {
                        'exchange': 'okx',
                        'data': {...}
                    }
                }
            }
        ==================================================
        """
        if not self.latest_snapshot:
            logger.debug("【欧易持仓缺失修复区】门外还没有存储区数据")
            return None

        user_data = self.latest_snapshot.get('user_data', {})
        okx_key = f"{EXCHANGE_OKX}_user"
        okx_item = user_data.get(okx_key, {})

        return okx_item.get('data')

    def _update_funding_fields(self, snapshot_data: Dict):
        """
        更新4个资金费字段（用于情况B）
        ==================================================
        当无历史但有新结算时，直接把存储区的4个资金费字段
        覆盖到缓存。
        ==================================================
        """
        fields = [
            FIELD_FUNDING_THIS,
            FIELD_FUNDING_TOTAL,
            FIELD_FUNDING_COUNT,
            FIELD_FUNDING_TIME
        ]

        update_count = 0
        for field in fields:
            if field in snapshot_data and snapshot_data[field] is not None:
                self.cache[field] = snapshot_data[field]
                update_count += 1

        logger.debug(f" 【欧易持仓缺失修复区】  已更新 {update_count} 个资金费字段")