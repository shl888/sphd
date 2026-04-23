# frontend_relay/stats_handler.py
"""
交易数据统计处理器
======================================================================
【文件职责】
这个文件是一个独立的专门处理交易数据统计请求的处理器。

【数据流向】
qd_server 收到前端 get_stats 指令
        ↓
转发给 StatsHandler.handle(data)
        ↓
StatsHandler 自己：
    1. 解析时间范围参数
    2. 连接 MongoDB，查询 closed_positions 集合
    3. 按交易所分组 → 配对筛选（套利 vs 单边）
    4. 计算 15 个统计指标
    5. 主动调用 qd_server.broadcast_stats_result() 把结果推给前端

【依赖说明】
- 通过 qd_server -> brain -> data_manager 获取数据库连接字符串
- 不直接依赖环境变量
- 按需连接 MongoDB，用完即关
- 通过 qd_server 实例推送结果，自己不持有 WebSocket 连接

【可调参数】
- TIME_DIFF_THRESHOLD = 60  # 套利配对时间差阈值（秒）
- LEVERAGE = 20             # 固定杠杆倍数
- FEE_RATE = 0.001          # 手续费率 0.1%（开仓+平仓合计）
- DECIMAL_PLACES = 4        # 保留小数位数
======================================================================
"""

import os
import asyncio
import logging
import json
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient

logger = logging.getLogger(__name__)

# ==================== 可调参数 ====================
TIME_DIFF_THRESHOLD = 60  # 套利配对时间差阈值（秒）
LEVERAGE = 20             # 固定杠杆倍数
FEE_RATE = 0.001          # 手续费率 0.1%（开仓+平仓合计）
DECIMAL_PLACES = 4        # 保留小数位数

# ==================== 时区设置 ====================
BEIJING_TZ = timezone(timedelta(hours=8))  # 北京时间 UTC+8


class StatsHandler:
    """
    交易数据统计处理器 - 独立工作
    
    qd_server 调用方式：
        handler = StatsHandler(qd_server_instance)
        await handler.handle(data)
    
    处理器干完活后，主动调用 qd_server.broadcast_stats_result() 推送结果
    """
    
    def __init__(self, qd_server):
        """
        初始化处理器
        
        Args:
            qd_server: qd_server 实例引用，用于推送统计结果
        """
        # ========== 保存 qd_server 引用 ==========
        self.qd_server = qd_server
        logger.debug(f"✅ 【数据统计处理器】qd_server 引用已保存")
        # ========== qd_server 引用保存完毕 ==========
        
        logger.debug(f"✅ 【数据统计处理器】初始化完成")
        logger.debug(f"   - 时间差阈值: {TIME_DIFF_THRESHOLD} 秒")
        logger.debug(f"   - 杠杆倍数: {LEVERAGE}x")
        logger.debug(f"   - 手续费率: {FEE_RATE * 100}%")
        logger.debug(f"   - 小数位数: {DECIMAL_PLACES}")
        logger.debug(f"   - 时区: 北京时间 (UTC+8)")
    
    # ==================== 对外唯一入口 ====================
    
    async def handle(self, data: Dict):
        """
        处理统计请求（qd_server 调用的入口）
        
        Args:
            data: 前端发来的数据，包含 range 或 start/end
        
        数据格式示例：
            {"type": "get_stats", "params": {"range": "all"}}           # 全部历史
            {"type": "get_stats", "params": {"range": "today"}}         # 今日
            {"type": "get_stats", "params": {"range": "week"}}          # 本周
            {"type": "get_stats", "params": {"range": "month"}}         # 本月
            {"type": "get_stats", "params": {"start": "2026.01.01 00:00:00", "end": "2026.01.31 23:59:59"}}  # 自定义
        """
        
        logger.info(f"📊 【数据统计处理器】收到统计指令，开始处理")
        logger.info(f"📊 【数据统计处理器】指令参数: {data}")
        
        # ========== 1. 解析参数（从 params 里取） ==========
        params = data.get('params', {})
        range_param = params.get('range', 'all')
        start_time = params.get('start', '')
        end_time = params.get('end', '')
        
        logger.info(f"📊 【数据统计处理器】【步骤1】解析参数完成")
        logger.info(f"   - range: {range_param}")
        logger.info(f"   - start: {start_time if start_time else '未指定'}")
        logger.info(f"   - end: {end_time if end_time else '未指定'}")
        
        try:
            # ========== 2. 获取统计数据 ==========
            logger.info(f"📊 【数据统计处理器】【步骤2】开始获取数据库数据...")
            
            if start_time and end_time:
                logger.debug(f"   - 使用自定义时间范围: {start_time} ~ {end_time}")
                result = await self._get_summary_by_range(start_time, end_time)
            else:
                logger.debug(f"   - 使用预设时间范围: {range_param}")
                result = await self._get_summary(range_param)
            
            # ========== 3. 打印结果摘要 ==========
            logger.info(f"📊 【数据统计处理器】【步骤3】统计计算完成")
            logger.debug(f"   - 欧易交易笔数: {result.get('okx_trades', 0)}")
            logger.debug(f"   - 币安交易笔数: {result.get('binance_trades', 0)}")
            logger.debug(f"   - 总手续费: {result.get('net_fee', 0.0):.4f} U")
            logger.debug(f"   - 总资金费: {result.get('net_funding', 0.0):.4f} U")
            logger.debug(f"   - 总平仓收益: {result.get('net_profit', 0.0):.4f} U")
            logger.debug(f"   - 净盈亏: {result.get('net_pnl', 0.0):.4f} U")
            logger.debug(f"   - 净盈亏率: {result.get('net_pnl_rate', 0.0):.2f}%")
            
            # ========== 4. 调用 qd_server 推送结果 ==========
            logger.debug(f"📊 【数据统计处理器】【步骤4】调用 qd_server.broadcast_stats_result() 推送结果...")
            await self.qd_server.broadcast_stats_result(result)
            logger.info(f"✅ 【数据统计处理器】结果已推送给 qd_server")
            
        except Exception as e:
            logger.error(f"❌ 【数据统计处理器】处理失败: {e}", exc_info=True)
            
            # ========== 错误时返回空结果 ==========
            logger.info(f"📊 【数据统计处理器】推送空结果给前端...")
            await self.qd_server.broadcast_stats_result(self._empty_result())
            
        finally:
            logger.debug(f"📊 【数据统计处理器】========================================")
    
    # ==================== 时间范围解析 ====================
    
    async def _get_summary(self, range_param: str) -> Dict[str, Any]:
        """
        按预设范围查询
        
        Args:
            range_param: all / today / week / month
        
        Returns:
            统计结果字典
        """
        logger.info(f"📊 【数据统计处理器】解析预设时间范围: {range_param}")
        start_time, end_time = self._parse_time_range(range_param)
        
        if start_time and end_time:
            logger.info(f"   - 解析结果: {start_time} ~ {end_time}")
        else:
            logger.info(f"   - 解析结果: 全部历史（无时间限制）")
        
        return await self._get_summary_by_range(start_time, end_time)
    
    async def _get_summary_by_range(self, start_time: Optional[str], end_time: Optional[str]) -> Dict[str, Any]:
        """
        按自定义范围查询
        
        Args:
            start_time: 起始时间，格式 "YYYY.MM.DD HH:MM:SS"，None 表示不限
            end_time: 结束时间，格式 "YYYY.MM.DD HH:MM:SS"，None 表示不限
        
        Returns:
            统计结果字典
        """
        logger.debug(f"📊 【数据统计处理器】【数据库查询】开始...")
        
        # ========== 1. 从 MongoDB 查询数据 ==========
        records = await self._fetch_records(start_time, end_time)
        logger.info(f"📊 【数据统计处理器】【数据库查询】完成，读取 {len(records)} 条记录")
        
        if not records:
            logger.warning(f"⚠️ 【数据统计处理器】没有查询到任何记录，返回空结果")
            return self._empty_result()
        
        # ========== 2. 按交易所分组 ==========
        logger.info(f"📊 【数据统计处理器】【数据分组】按交易所分组...")
        okx_records, binance_records = self._group_by_exchange(records)
        logger.info(f"   - 欧易: {len(okx_records)} 条")
        logger.info(f"   - 币安: {len(binance_records)} 条")
        
        # ========== 3. 配对筛选 ==========
        logger.info(f"📊 【数据统计处理器】【配对筛选】开始配对...")
        okx_paired, binance_paired = self._match_pairs(okx_records, binance_records)
        logger.info(f"   - 配对成功: 欧易 {len(okx_paired)} 条, 币安 {len(binance_paired)} 条")
        
        if not okx_paired or not binance_paired:
            logger.warning(f"⚠️ 【数据统计处理器】配对后没有有效数据，返回空结果")
            return self._empty_result()
        
        # ========== 4. 计算指标 ==========
        logger.info(f"📊 【数据统计处理器】【指标计算】开始计算...")
        result = self._calculate(okx_paired, binance_paired)
        logger.info(f"📊 【数据统计处理器】【指标计算】完成")
        
        return result
    
    def _parse_time_range(self, range_param: str) -> Tuple[Optional[str], Optional[str]]:
        """
        解析时间范围参数（使用北京时间 UTC+8）
        
        Args:
            range_param: all / today / week / month
        
        Returns:
            (start_time, end_time) 元组，格式 "YYYY.MM.DD HH:MM:SS"
            all 时返回 (None, None)
        """
        logger.debug(f"📊 【数据统计处理器】解析参数: {range_param}")
        
        if range_param == 'all':
            logger.debug(f"   - 返回全部历史")
            return None, None
        
        # 使用北京时间
        now = datetime.now(BEIJING_TZ)
        
        if range_param == 'today':
            start_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
            end_time = now.replace(hour=23, minute=59, second=59, microsecond=999999)
            logger.debug(f"   - 今日: 从 {start_time} 到 {end_time}")
        elif range_param == 'week':
            start_time = now - timedelta(days=7)
            end_time = now.replace(hour=23, minute=59, second=59, microsecond=999999)
            logger.debug(f"   - 本周: 从 {start_time} 到 {end_time}")
        elif range_param == 'month':
            start_time = now - timedelta(days=30)
            end_time = now.replace(hour=23, minute=59, second=59, microsecond=999999)
            logger.debug(f"   - 本月: 从 {start_time} 到 {end_time}")
        else:
            logger.warning(f"⚠️ 【数据统计处理器】未知范围参数: {range_param}，使用全部历史")
            return None, None
        
        format_str = '%Y.%m.%d %H:%M:%S'
        start_str = start_time.strftime(format_str)
        end_str = end_time.strftime(format_str)
        
        logger.debug(f"   - 格式化后: {start_str} ~ {end_str}")
        return start_str, end_str
    
    # ==================== 数据库查询 ====================
    
    async def _fetch_records(self, start_time: Optional[str], end_time: Optional[str]) -> List[Dict]:
        """
        从 MongoDB 的 closed_positions 集合查询数据
        
        Args:
            start_time: 起始时间，None 表示不限
            end_time: 结束时间，None 表示不限
        
        Returns:
            平仓记录列表
        """
        # ===== 第一次被调用时才获取 MongoDB 连接字符串 =====
        if not hasattr(self, '_mongo_uri'):
            try:
                data_manager = self.qd_server.brain.data_manager
                self._mongo_uri = data_manager.get_database_config('mongodb_uri')
                if not self._mongo_uri:
                    logger.error("❌ 【数据统计处理器】MongoDB 连接信息为空")
                    return []
                else:
                    # 隐藏密码打印
                    masked_uri = self._mongo_uri
                    if '@' in masked_uri:
                        parts = masked_uri.split('@')
                        before_at = parts[0]
                        if ':' in before_at:
                            user_pass = before_at.split('://')[-1] if '://' in before_at else before_at
                            if ':' in user_pass:
                                user = user_pass.split(':')[0]
                                masked_uri = masked_uri.replace(user_pass, f"{user}:****")
                    logger.info(f"✅ 【数据统计处理器】MongoDB 连接信息已获取: {masked_uri}")
            except Exception as e:
                logger.error(f"❌ 【数据统计处理器】获取 MongoDB 连接失败: {e}")
                return []
        
        if not self._mongo_uri:
            return []
        
        logger.debug(f"📊 【数据统计处理器】连接 MongoDB...")
        
        loop = asyncio.get_event_loop()
        client = None
        
        try:
            # 创建 MongoDB 客户端
            client = await loop.run_in_executor(
                None,
                lambda: MongoClient(self._mongo_uri, serverSelectionTimeoutMS=5000)
            )
            
            db = client["trading_db"]
            collection = db["closed_positions"]
            
            # 构建查询条件
            query_filter = {}
            if start_time and end_time:
                query_filter["平仓时间"] = {"$gte": start_time, "$lte": end_time}
                logger.debug(f"📊 【数据统计处理器】查询条件: 平仓时间 between {start_time} and {end_time}")
            else:
                logger.debug(f"📊 【数据统计处理器】查询条件: 无条件（全部记录）")
            
            # 执行查询，按平仓时间降序
            cursor = await loop.run_in_executor(
                None,
                lambda: collection.find(query_filter).sort("平仓时间", -1)
            )
            
            records = await loop.run_in_executor(None, lambda: list(cursor))
            
            # 删除 MongoDB 的 _id 字段
            for record in records:
                if '_id' in record:
                    del record['_id']
            
            logger.debug(f"📊 【数据统计处理器】查询到 {len(records)} 条记录")
            return records
            
        except Exception as e:
            logger.error(f"❌ 【数据统计处理器】查询数据库失败: {e}", exc_info=True)
            return []
        finally:
            if client:
                await loop.run_in_executor(None, client.close)
                logger.debug(f"📊 【数据统计处理器】MongoDB 连接已关闭")
    
    # ==================== 数据分组 ====================
    
    def _group_by_exchange(self, records: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        按交易所分组
        
        Args:
            records: 平仓记录列表
        
        Returns:
            (okx_records, binance_records) 元组
        """
        okx_records = []
        binance_records = []
        
        for record in records:
            exchange = record.get('交易所', '').lower()
            if exchange == 'okx':
                okx_records.append(record)
            elif exchange == 'binance':
                binance_records.append(record)
            else:
                logger.debug(f"📊 【数据统计处理器】忽略未知交易所: {exchange}")
        
        logger.debug(f"📊 【数据统计处理器】分组结果: 欧易={len(okx_records)}, 币安={len(binance_records)}")
        return okx_records, binance_records
    
    # ==================== 配对算法 ====================
    
    def _match_pairs(self, okx_records: List[Dict], binance_records: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        配对筛选：时间差 ≤ 60秒 + 合约名相同
        
        算法说明：
            遍历欧易的每一条记录，在币安记录中找：
            1. 合约名相同
            2. 开仓时间差最小
            3. 时间差 ≤ TIME_DIFF_THRESHOLD（60秒）
            
            找到后配对成功，双方都从可用池中移除
        
        Args:
            okx_records: 欧易记录列表
            binance_records: 币安记录列表
        
        Returns:
            (okx_paired, binance_paired) 元组，已配对的记录
        """
        logger.debug(f"📊 【数据统计处理器】开始配对，欧易: {len(okx_records)} 条，币安: {len(binance_records)} 条")
        
        okx_paired = []
        binance_paired = []
        available_binance = list(binance_records)  # 可用的币安记录池
        
        for i, okx in enumerate(okx_records):
            okx_contract = okx.get('开仓合约名', '')
            okx_open_time = okx.get('开仓时间', '')
            
            if not okx_contract or not okx_open_time:
                logger.debug(f"📊 【数据统计处理器】欧易记录 {i} 缺少合约名或开仓时间，跳过")
                continue
            
            best_match = None
            best_diff = float('inf')
            best_index = -1
            
            # 在可用池中找最佳匹配
            for j, binance in enumerate(available_binance):
                binance_contract = binance.get('开仓合约名', '')
                binance_open_time = binance.get('开仓时间', '')
                
                if not binance_contract or not binance_open_time:
                    continue
                
                # 合约名必须相同
                if okx_contract != binance_contract:
                    continue
                
                # 计算时间差
                time_diff = self._time_diff_seconds(okx_open_time, binance_open_time)
                
                if time_diff < best_diff:
                    best_diff = time_diff
                    best_match = binance
                    best_index = j
            
            # 检查时间差是否在阈值内
            if best_match is not None and best_diff <= TIME_DIFF_THRESHOLD:
                okx_paired.append(okx)
                binance_paired.append(best_match)
                available_binance.pop(best_index)
                logger.debug(f"📊 【数据统计处理器】配对成功: 合约={okx_contract}, 时间差={best_diff:.2f}秒")
            else:
                if best_match is not None:
                    logger.debug(f"📊 【数据统计处理器】配对失败: 合约={okx_contract}, 时间差={best_diff:.2f}秒 > 阈值{TIME_DIFF_THRESHOLD}秒")
                else:
                    logger.debug(f"📊 【数据统计处理器】配对失败: 合约={okx_contract}, 未找到相同合约的币安记录")
        
        logger.info(f"📊 【数据统计处理器】配对完成: 成功 {len(okx_paired)} 对，欧易剩余 {len(okx_records) - len(okx_paired)} 条，币安剩余 {len(binance_records) - len(binance_paired)} 条")
        return okx_paired, binance_paired
    
    def _time_diff_seconds(self, time_str1: str, time_str2: str) -> float:
        """
        计算两个时间字符串的差值（秒）
        
        Args:
            time_str1: 时间字符串，格式 "YYYY.MM.DD HH:MM:SS"
            time_str2: 时间字符串，格式 "YYYY.MM.DD HH:MM:SS"
        
        Returns:
            时间差的绝对值（秒），解析失败返回无穷大
        """
        try:
            fmt = '%Y.%m.%d %H:%M:%S'
            dt1 = datetime.strptime(time_str1, fmt)
            dt2 = datetime.strptime(time_str2, fmt)
            diff = abs((dt1 - dt2).total_seconds())
            return diff
        except Exception as e:
            logger.warning(f"⚠️ 【数据统计处理器】时间解析失败: {time_str1} 或 {time_str2}, 错误: {e}")
            return float('inf')
    
    # ==================== 指标计算 ====================
    
    def _calculate(self, okx_records: List[Dict], binance_records: List[Dict]) -> Dict[str, Any]:
        """
        计算所有统计指标
        
        计算公式：
            - 手续费 = 平均保证金 × 杠杆 × 手续费率 × 交易次数（结果转为负数）
            - 净盈亏 = 净平仓收益 + 净资金费 + 净手续费
            - 净盈亏率 = (净盈亏 × 100) / 平均总保证金
        
        Args:
            okx_records: 已配对的欧易记录
            binance_records: 已配对的币安记录
        
        Returns:
            统计结果字典，包含 15 个字段
        """
        logger.debug(f"📊 【数据统计处理器】计算指标，欧易: {len(okx_records)} 条，币安: {len(binance_records)} 条")
        
        # ========== 欧易指标 ==========
        okx_trades = len(okx_records)
        okx_avg_margin = self._calc_avg(okx_records, '开仓保证金')
        okx_total_funding = self._calc_sum(okx_records, '累计资金费')
        okx_total_profit = self._calc_sum(okx_records, '平仓收益')
        okx_total_fee = okx_avg_margin * LEVERAGE * FEE_RATE * okx_trades
        # 手续费转为负数
        okx_total_fee = -okx_total_fee
        
        logger.debug(f"📊 【数据统计处理器】欧易: 交易={okx_trades}, 均保证金={okx_avg_margin:.4f}, 资金费={okx_total_funding:.4f}, 收益={okx_total_profit:.4f}, 手续费={okx_total_fee:.4f}")
        
        # ========== 币安指标 ==========
        binance_trades = len(binance_records)
        binance_avg_margin = self._calc_avg(binance_records, '开仓保证金')
        binance_total_funding = self._calc_sum(binance_records, '累计资金费')
        binance_total_profit = self._calc_sum(binance_records, '平仓收益')
        binance_total_fee = binance_avg_margin * LEVERAGE * FEE_RATE * binance_trades
        # 手续费转为负数
        binance_total_fee = -binance_total_fee
        
        logger.debug(f"📊 【数据统计处理器】币安: 交易={binance_trades}, 均保证金={binance_avg_margin:.4f}, 资金费={binance_total_funding:.4f}, 收益={binance_total_profit:.4f}, 手续费={binance_total_fee:.4f}")
        
        # ========== 套利汇总 ==========
        net_fee = okx_total_fee + binance_total_fee
        net_funding = okx_total_funding + binance_total_funding
        net_profit = okx_total_profit + binance_total_profit
        # 净盈亏 = 净平仓收益 + 净资金费 + 净手续费
        net_pnl = net_profit + net_funding + net_fee
        
        # 平均总保证金（双边）
        avg_total_margin = (okx_avg_margin + binance_avg_margin) / 2
        net_pnl_rate = (net_pnl * 100) / avg_total_margin if avg_total_margin != 0 else 0.0
        
        logger.debug(f"📊 【数据统计处理器】汇总: 总手续费={net_fee:.4f}, 总资金费={net_funding:.4f}, 总收益={net_profit:.4f}, 净盈亏={net_pnl:.4f}, 净盈亏率={net_pnl_rate:.2f}%")
        
        # ========== 构建返回结果 ==========
        result = {
            # 欧易
            'okx_trades': okx_trades,
            'okx_avg_margin': round(okx_avg_margin, DECIMAL_PLACES),
            'okx_total_fee': round(okx_total_fee, DECIMAL_PLACES),
            'okx_total_funding': round(okx_total_funding, DECIMAL_PLACES),
            'okx_total_profit': round(okx_total_profit, DECIMAL_PLACES),
            
            # 币安
            'binance_trades': binance_trades,
            'binance_avg_margin': round(binance_avg_margin, DECIMAL_PLACES),
            'binance_total_fee': round(binance_total_fee, DECIMAL_PLACES),
            'binance_total_funding': round(binance_total_funding, DECIMAL_PLACES),
            'binance_total_profit': round(binance_total_profit, DECIMAL_PLACES),
            
            # 汇总
            'net_fee': round(net_fee, DECIMAL_PLACES),
            'net_funding': round(net_funding, DECIMAL_PLACES),
            'net_profit': round(net_profit, DECIMAL_PLACES),
            'net_pnl': round(net_pnl, DECIMAL_PLACES),
            'net_pnl_rate': round(net_pnl_rate, DECIMAL_PLACES),
        }
        
        return result
    
    def _calc_sum(self, records: List[Dict], field: str) -> float:
        """
        计算指定字段的总和
        
        Args:
            records: 记录列表
            field: 字段名
        
        Returns:
            总和
        """
        total = 0.0
        for record in records:
            value = record.get(field)
            if value is not None:
                try:
                    total += float(value)
                except (TypeError, ValueError):
                    pass
        return total
    
    def _calc_avg(self, records: List[Dict], field: str) -> float:
        """
        计算指定字段的平均值
        
        Args:
            records: 记录列表
            field: 字段名
        
        Returns:
            平均值，无记录时返回 0.0
        """
        if not records:
            return 0.0
        return self._calc_sum(records, field) / len(records)
    
    def _empty_result(self) -> Dict[str, Any]:
        """
        返回全零的空结果
        
        Returns:
            所有字段都为 0 的统计结果
        """
        logger.debug(f"📊 【数据统计处理器】返回空结果")
        return {
            'okx_trades': 0,
            'okx_avg_margin': 0.0,
            'okx_total_fee': 0.0,
            'okx_total_funding': 0.0,
            'okx_total_profit': 0.0,
            'binance_trades': 0,
            'binance_avg_margin': 0.0,
            'binance_total_fee': 0.0,
            'binance_total_funding': 0.0,
            'binance_total_profit': 0.0,
            'net_fee': 0.0,
            'net_funding': 0.0,
            'net_profit': 0.0,
            'net_pnl': 0.0,
            'net_pnl_rate': 0.0,
        }