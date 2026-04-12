# frontend_relay/stats_handler.py
"""
交易数据统计处理器 -
======================================================================
【文件职责】
这个文件是一个独立的专门处理交易数据统计请求。

【数据流向】
qd_server 收到前端 get_stats 指令数据
        ↓
转发给 stats_handler.process_stats_command(data)
        ↓
StatsHandler 自己：
    1. 解析时间范围
    2. 连接 MongoDB，查询 closed_positions 集合
    3. 按交易所分组 → 配对筛选（套利 vs 单边）
    4. 计算 15 个统计指标
    5. 把结果交给 qd_server 推送给前端

【依赖说明】
- 只依赖环境变量 MONGODB_URI
- 不依赖大脑实例、不依赖存储区
- 按需连接 MongoDB，用完即关

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
from datetime import datetime, timedelta
from pymongo import MongoClient

logger = logging.getLogger(__name__)

# ==================== 可调参数 ====================
TIME_DIFF_THRESHOLD = 60  # 套利配对时间差阈值（秒）
LEVERAGE = 20             # 固定杠杆倍数
FEE_RATE = 0.001          # 手续费率 0.1%（开仓+平仓合计）
DECIMAL_PLACES = 4        # 保留小数位数


class StatsHandler:
    """
    交易数据统计处理器 - 独立工作
    
    qd_server 调用方式：
        handler = StatsHandler()
        await handler.handle(ws, data, client_id)
    """
    
    def __init__(self):
        """初始化处理器，从环境变量读取 MongoDB 连接信息"""
        self.mongo_uri = os.getenv('MONGODB_URI')
        if not self.mongo_uri:
            logger.error("❌ 【数据统计处理器】环境变量 MONGODB_URI 未设置")
        else:
            logger.info("✅ 【数据统计处理器】MongoDB 连接信息已读取")
    
    # ==================== 对外唯一入口 ====================
    
    async def handle(self, ws, data: Dict, client_id: str):
        """
        处理统计请求（qd_server 调用的入口）
        
        Args:
            ws: WebSocket 连接对象，用于推送结果
            data: 前端发来的数据，包含 params.range 或 params.start/end
            client_id: 客户端 ID
        """
        logger.info(f"📊 【数据统计处理器】收到请求，客户端: {client_id}")
        
        # 1. 从 params 里解析参数
        params = data.get('params', {})
        range_param = params.get('range', 'all')
        start_time = params.get('start', '')
        end_time = params.get('end', '')
        
        try:
            # 2. 获取统计数据
            if start_time and end_time:
                result = await self._get_summary_by_range(start_time, end_time)
            else:
                result = await self._get_summary(range_param)
            
            # 3. 推送结果给前端
            await ws.send_json({
                "type": "stats_result",
                "data": result,
                "client_id": client_id
            })
            logger.info(f"✅ 【数据统计处理器】结果已推送，净盈亏: {result['net_pnl']}")
            
        except Exception as e:
            logger.error(f"❌ 【数据统计处理器】处理失败: {e}", exc_info=True)
            await ws.send_json({
                "type": "stats_result",
                "data": self._empty_result(),
                "error": str(e),
                "client_id": client_id
            })
    
    async def _get_summary(self, range_param: str) -> Dict[str, Any]:
        """按预设范围查询"""
        start_time, end_time = self._parse_time_range(range_param)
        return await self._get_summary_by_range(start_time, end_time)
    
    async def _get_summary_by_range(self, start_time: Optional[str], end_time: Optional[str]) -> Dict[str, Any]:
        """按自定义范围查询"""
        # 1. 从 MongoDB 查询数据
        records = await self._fetch_records(start_time, end_time)
        logger.info(f"   从数据库读取 {len(records)} 条记录")
        
        if not records:
            return self._empty_result()
        
        # 2. 按交易所分组
        okx_records, binance_records = self._group_by_exchange(records)
        logger.debug(f"   欧易: {len(okx_records)} 条, 币安: {len(binance_records)} 条")
        
        # 3. 配对筛选
        okx_paired, binance_paired = self._match_pairs(okx_records, binance_records)
        logger.info(f"   配对成功: 欧易 {len(okx_paired)} 条, 币安 {len(binance_paired)} 条")
        
        if not okx_paired or not binance_paired:
            return self._empty_result()
        
        # 4. 计算指标
        return self._calculate(okx_paired, binance_paired)
    
    # ==================== 时间范围解析 ====================
    
    def _parse_time_range(self, range_param: str) -> Tuple[Optional[str], Optional[str]]:
        """解析时间范围参数"""
        if range_param == 'all':
            return None, None
        
        now = datetime.now()
        end_time = now
        
        if range_param == 'today':
            start_time = now.replace(hour=0, minute=0, second=0, microsecond=0)
        elif range_param == 'week':
            start_time = now - timedelta(days=7)
        elif range_param == 'month':
            start_time = now - timedelta(days=30)
        else:
            return None, None
        
        format_str = '%Y.%m.%d %H:%M:%S'
        return start_time.strftime(format_str), end_time.strftime(format_str)
    
    # ==================== 数据库查询 ====================
    
    async def _fetch_records(self, start_time: Optional[str], end_time: Optional[str]) -> List[Dict]:
        """从 MongoDB 的 closed_positions 集合查询数据"""
        if not self.mongo_uri:
            logger.error("❌ 【数据统计处理器】MONGODB_URI 未设置")
            return []
        
        loop = asyncio.get_event_loop()
        client = None
        
        try:
            client = await loop.run_in_executor(
                None,
                lambda: MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            )
            
            db = client["trading_db"]
            collection = db["closed_positions"]
            
            query_filter = {}
            if start_time and end_time:
                query_filter["平仓时间"] = {"$gte": start_time, "$lte": end_time}
            
            cursor = await loop.run_in_executor(
                None,
                lambda: collection.find(query_filter).sort("平仓时间", -1)
            )
            
            records = await loop.run_in_executor(None, lambda: list(cursor))
            
            for record in records:
                if '_id' in record:
                    del record['_id']
            
            return records
            
        except Exception as e:
            logger.error(f"❌ 【数据统计处理器】查询数据库失败: {e}")
            return []
        finally:
            if client:
                await loop.run_in_executor(None, client.close)
    
    # ==================== 数据分组 ====================
    
    def _group_by_exchange(self, records: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """按交易所分组"""
        okx_records = []
        binance_records = []
        
        for record in records:
            exchange = record.get('交易所', '').lower()
            if exchange == 'okx':
                okx_records.append(record)
            elif exchange == 'binance':
                binance_records.append(record)
        
        return okx_records, binance_records
    
    # ==================== 配对算法 ====================
    
    def _match_pairs(self, okx_records: List[Dict], binance_records: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """配对筛选：时间差 ≤ 60秒 + 合约名相同"""
        okx_paired = []
        binance_paired = []
        available_binance = list(binance_records)
        
        for okx in okx_records:
            okx_contract = okx.get('开仓合约名', '')
            okx_open_time = okx.get('开仓时间', '')
            
            if not okx_contract or not okx_open_time:
                continue
            
            best_match = None
            best_diff = float('inf')
            best_index = -1
            
            for i, binance in enumerate(available_binance):
                binance_contract = binance.get('开仓合约名', '')
                binance_open_time = binance.get('开仓时间', '')
                
                if not binance_contract or not binance_open_time:
                    continue
                
                if okx_contract != binance_contract:
                    continue
                
                time_diff = self._time_diff_seconds(okx_open_time, binance_open_time)
                
                if time_diff < best_diff:
                    best_diff = time_diff
                    best_match = binance
                    best_index = i
            
            if best_match is not None and best_diff <= TIME_DIFF_THRESHOLD:
                okx_paired.append(okx)
                binance_paired.append(best_match)
                available_binance.pop(best_index)
        
        return okx_paired, binance_paired
    
    def _time_diff_seconds(self, time_str1: str, time_str2: str) -> float:
        """计算两个时间字符串的差值（秒）"""
        try:
            fmt = '%Y.%m.%d %H:%M:%S'
            dt1 = datetime.strptime(time_str1, fmt)
            dt2 = datetime.strptime(time_str2, fmt)
            return abs((dt1 - dt2).total_seconds())
        except Exception:
            return float('inf')
    
    # ==================== 指标计算 ====================
    
    def _calculate(self, okx_records: List[Dict], binance_records: List[Dict]) -> Dict[str, Any]:
        """计算所有统计指标"""
        # 欧易
        okx_trades = len(okx_records)
        okx_avg_margin = self._calc_avg(okx_records, '开仓保证金')
        okx_total_funding = self._calc_sum(okx_records, '累计资金费')
        okx_total_profit = self._calc_sum(okx_records, '平仓收益')
        okx_total_fee = okx_avg_margin * LEVERAGE * FEE_RATE * okx_trades
        
        # 币安
        binance_trades = len(binance_records)
        binance_avg_margin = self._calc_avg(binance_records, '开仓保证金')
        binance_total_funding = self._calc_sum(binance_records, '累计资金费')
        binance_total_profit = self._calc_sum(binance_records, '平仓收益')
        binance_total_fee = binance_avg_margin * LEVERAGE * FEE_RATE * binance_trades
        
        # 套利结果
        net_fee = okx_total_fee + binance_total_fee
        net_funding = okx_total_funding + binance_total_funding
        net_profit = okx_total_profit + binance_total_profit
        net_pnl = net_funding + net_profit - net_fee
        
        avg_total_margin = (okx_avg_margin + binance_avg_margin) / 2
        net_pnl_rate = (net_pnl * 100) / avg_total_margin if avg_total_margin != 0 else 0.0
        
        return {
            'okx_trades': okx_trades,
            'okx_avg_margin': round(okx_avg_margin, DECIMAL_PLACES),
            'okx_total_fee': round(okx_total_fee, DECIMAL_PLACES),
            'okx_total_funding': round(okx_total_funding, DECIMAL_PLACES),
            'okx_total_profit': round(okx_total_profit, DECIMAL_PLACES),
            
            'binance_trades': binance_trades,
            'binance_avg_margin': round(binance_avg_margin, DECIMAL_PLACES),
            'binance_total_fee': round(binance_total_fee, DECIMAL_PLACES),
            'binance_total_funding': round(binance_total_funding, DECIMAL_PLACES),
            'binance_total_profit': round(binance_total_profit, DECIMAL_PLACES),
            
            'net_fee': round(net_fee, DECIMAL_PLACES),
            'net_funding': round(net_funding, DECIMAL_PLACES),
            'net_profit': round(net_profit, DECIMAL_PLACES),
            'net_pnl': round(net_pnl, DECIMAL_PLACES),
            'net_pnl_rate': round(net_pnl_rate, DECIMAL_PLACES),
        }
    
    def _calc_sum(self, records: List[Dict], field: str) -> float:
        """计算总和"""
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
        """计算平均值"""
        if not records:
            return 0.0
        return self._calc_sum(records, field) / len(records)
    
    def _empty_result(self) -> Dict[str, Any]:
        """返回全零结果"""
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


# ==================== 统计指令处理入口 ====================

def process_stats_command(data):
    """
    接收 qd_server 转发来的统计指令数据，开始干活
    """
    logger.info(f"📊 【数据统计处理器】收到统计指令数据")
    logger.info(f"   指令内容: {data}")
    
    handler = StatsHandler()
    asyncio.create_task(handler._execute_stats_task(data))


async def _execute_stats_task(self, data):
    """
    执行统计任务：解析数据、查询数据库、计算指标、交给 qd_server
    """
    try:
        # 解析参数
        params = data.get('params', {})
        range_param = params.get('range', 'all')
        start_time = params.get('start', '')
        end_time = params.get('end', '')
        
        logger.info(f"   解析参数: range={range_param}, start={start_time}, end={end_time}")
        
        # 查询并计算
        if start_time and end_time:
            result = await self._get_summary_by_range(start_time, end_time)
        else:
            result = await self._get_summary(range_param)
        
        logger.info(f"✅ 统计任务完成，净盈亏: {result['net_pnl']}")
        
        # 交给 qd_server
        from frontend_relay.qd_server import frontend_relay_instance
        frontend_relay_instance.receive_stats_result(result)
        
    except Exception as e:
        logger.error(f"❌ 统计任务执行失败: {e}", exc_info=True)