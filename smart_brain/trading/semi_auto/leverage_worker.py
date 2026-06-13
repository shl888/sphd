# trading/semi_auto/leverage_worker.py
"""
半自动杠杆工人 - 独立负责设置杠杆

工作流程：
1. 收到开仓指令 → 缓存
2. 拷贝欧易和币安的杠杆模板
3. 检查持仓（读私人数据的"开仓合约名"）
4. 检查保证金（读私人数据的"账户资产额"）
5. 填充杠杆参数（合约名、杠杆倍数）
6. 推送给下单工人（通过 trader.send_orders）
7. 清空缓存
"""

import copy
import asyncio
import logging
from typing import Dict, Any

from ..templates import SET_LEVERAGE_OKX, SET_LEVERAGE_BINANCE

logger = logging.getLogger(__name__)


class LeverageWorker:
    def __init__(self, brain):
        self.brain = brain
        self.data_manager = brain.data_manager
        
        # 缓存
        self.pending_command = None
        self.pending_params = None
        self.okx_cache = None
        self.binance_cache = None
        
        logger.info("🔧【半自动杠杆工人】初始化完成")
    
    def on_data(self, data: Dict[str, Any]):
        """接收大脑推送的数据"""
        if data.get("command") == "place_order":
            logger.info("📥【半自动杠杆工人】收到开仓指令")
            self.pending_command = data.get("command")
            self.pending_params = data.get("params", {})
            asyncio.create_task(self._execute())
    
    async def _execute(self):
        """执行杠杆设置流程"""
        if not self.pending_params:
            logger.error("❌【半自动杠杆工人】没有开仓参数")
            self._cleanup()
            return
        
        logger.info("🔧【半自动杠杆工人】开始执行")
        
        # 1. 拷贝模板
        self.okx_cache = copy.deepcopy(SET_LEVERAGE_OKX)
        self.binance_cache = copy.deepcopy(SET_LEVERAGE_BINANCE)
        logger.info("📦【半自动杠杆工人】模板已拷贝")
        
        # 2. 检查持仓
        if not await self._check_position():
            self._cleanup()
            return
        
        # 3. 检查保证金
        if not await self._check_margin():
            self._cleanup()
            return
        
        # 4. 填充参数
        self._fill_params()
        
        # 5. 推送给下单工人
        self._send_to_trader()
        
        # 6. 清理
        self._cleanup()
        
        logger.info("✅【半自动杠杆工人】完成")
    
    async def _check_position(self) -> bool:
        """检查持仓：开仓合约名字段为空才能继续"""
        try:
            result = await self.data_manager.get_private_user_data()
            user_data = result.get('data', {})
            
            okx_symbol = user_data.get('okx', {}).get('开仓合约名', '')
            binance_symbol = user_data.get('binance', {}).get('开仓合约名', '')
            
            if okx_symbol:
                logger.warning(f"⚠️【半自动杠杆工人】欧易已有持仓: {okx_symbol}，禁止开仓")
                return False
            if binance_symbol:
                logger.warning(f"⚠️【半自动杠杆工人】币安已有持仓: {binance_symbol}，禁止开仓")
                return False
            
            logger.info("✅【半自动杠杆工人】持仓检查通过，当前空仓")
            return True
            
        except Exception as e:
            logger.error(f"❌【半自动杠杆工人】检查持仓失败: {e}")
            return False
    
    async def _check_margin(self) -> bool:
        """检查保证金：开仓保证金 ≤ 账户资产额 × 70%"""
        try:
            margin = float(self.pending_params.get('margin', 0))
            
            result = await self.data_manager.get_private_user_data()
            user_data = result.get('data', {})
            
            okx_asset = float(user_data.get('okx', {}).get('账户资产额', 0))
            binance_asset = float(user_data.get('binance', {}).get('账户资产额', 0))
            
            okx_limit = okx_asset * 0.7
            binance_limit = binance_asset * 0.7
            
            if margin > okx_limit:
                logger.warning(f"⚠️【半自动杠杆工人】保证金 {margin} 超过欧易账户资产70% ({okx_limit:.2f})")
                return False
            if margin > binance_limit:
                logger.warning(f"⚠️【半自动杠杆工人】保证金 {margin} 超过币安账户资产70% ({binance_limit:.2f})")
                return False
            
            logger.info(f"✅【半自动杠杆工人】保证金检查通过: {margin} USDT")
            return True
            
        except Exception as e:
            logger.error(f"❌【半自动杠杆工人】检查保证金失败: {e}")
            return False
    
    def _fill_params(self):
        """填充杠杆参数"""
        symbol = self.pending_params.get('symbol', '')
        leverage = self.pending_params.get('leverage', 1)
        # 杠杆正常是20，如今测试改为1
        
        # 转换欧易合约名：BTCUSDT → BTC-USDT-SWAP
        okx_symbol = self._convert_okx_symbol(symbol)
        
        # 欧易参数
        self.okx_cache['params']['instId'] = okx_symbol
        self.okx_cache['params']['lever'] = str(leverage)
        
        # 币安参数
        self.binance_cache['params']['symbol'] = symbol
        self.binance_cache['params']['leverage'] = leverage
        
        logger.info(f"📝【半自动杠杆工人】参数已填充: 欧易={okx_symbol} x{leverage}, 币安={symbol} x{leverage}")
    
    def _convert_okx_symbol(self, symbol: str) -> str:
        """转换欧易合约名：BTCUSDT → BTC-USDT-SWAP"""
        if not symbol:
            return symbol
        if '-SWAP' in symbol:
            return symbol
        if symbol.endswith('USDT'):
            base = symbol[:-4]
            return f"{base}-USDT-SWAP"
        return symbol
    
    def _send_to_trader(self):
        """推送给下单工人（通过 trader.send_orders）"""
        orders = []
        if self.okx_cache:
            orders.append(self.okx_cache)
        if self.binance_cache:
            orders.append(self.binance_cache)
        
        if orders and self.brain.trader:
            self.brain.trader.send_orders(orders)
            logger.info(f"📤【半自动杠杆工人】已推送 {len(orders)} 个订单给下单工人")
    
    def _cleanup(self):
        """清空缓存"""
        self.pending_command = None
        self.pending_params = None
        self.okx_cache = None
        self.binance_cache = None
        logger.info("🧹【半自动杠杆工人】缓存已清空")
        