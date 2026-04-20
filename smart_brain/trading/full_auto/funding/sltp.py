# trading/full_auto/funding/sltp.py
"""
全自动止损止盈工人 - 被动接收数据，条件触发执行

工作流程：
1. 被动接收大脑推送的数据
2. 收到标签 {"info": "开启全自动"} → 缓存
3. 收到标签 {"info": "欧易开仓成功"} → 缓存
4. 收到标签 {"info": "币安开仓成功"} → 缓存
5. 每次收到数据后检查三个标签是否齐了，齐了就执行
6. 拷贝欧易和币安的止损止盈模板
7. 读取私人数据（开仓价、开仓方向、合约名）
8. 读取欧易面值数据（tickSz）
9. 读取币安精度数据（tickSize）
10. 计算止损价和止盈价（固定 |35%|）
11. 按精度取整并格式化
12. 填充参数
13. 推送给下单工人
14. 清理开仓成功标签，保留开启全自动标签
15. 收到标签 {"info": "结束全自动"} → 立刻重置所有状态，取消正在执行的任务
"""

import copy
import asyncio
import logging
from typing import Dict, Any

from ..templates import OCO_OKX, OCO_BINANCE

logger = logging.getLogger(__name__)


class FundingSlTp:
    def __init__(self, brain):
        self.brain = brain
        self.data_manager = brain.data_manager
        
        # 标签缓存
        self.auto_mode_active = False      # 开启全自动
        self.okx_open_ok = False           # 欧易开仓成功
        self.binance_open_ok = False       # 币安开仓成功
        
        # 参数缓存
        self.okx_cache = None
        self.binance_cache = None
        
        # 临时数据
        self.okx_symbol = None
        self.binance_symbol = None
        self.okx_open_price = 0.0
        self.okx_position_side = ""
        self.binance_open_price = 0.0
        self.binance_position_side = ""
        self.okx_tick_sz = 0.0
        self.binance_tick_size = 0.0
        
        # 计算结果
        self.okx_stop_price = ""
        self.okx_take_price = ""
        self.binance_stop_price = ""
        self.binance_take_price = ""
        
        # 防重入标志
        self._is_executing = False
        self._current_task = None
        
        logger.info("🛡️【全自动止损止盈】初始化完成")
    
    # ==================== 被动接收数据 ====================
    
    def on_data(self, data: Dict[str, Any]):
        """被动接收大脑推送的数据"""
        if "info" not in data:
            return
        
        info = data["info"]
        logger.info(f"📥【全自动止损止盈】收到标签: {info}")
        
        if info == "开启全自动":
            self.auto_mode_active = True
        elif info == "欧易开仓成功":
            self.okx_open_ok = True
        elif info == "币安开仓成功":
            self.binance_open_ok = True
        elif info == "结束全自动":
            self._deactivate()
            return
        
        self._check_and_execute()
    
    def _check_and_execute(self):
        """检查三个标签是否齐了，齐了就执行（防重入）"""
        if self._is_executing:
            return
        
        if self.auto_mode_active and self.okx_open_ok and self.binance_open_ok:
            logger.info("🎯【全自动止损止盈】三个标签已齐，开始执行")
            self._is_executing = True
            self._current_task = asyncio.create_task(self._execute())
    
    def _deactivate(self):
        """收到结束全自动标签，立刻重置所有状态"""
        logger.info("🛑【全自动止损止盈】收到结束全自动标签，立刻重置")
        
        if self._current_task and not self._current_task.done():
            self._current_task.cancel()
            logger.info("🛑【全自动止损止盈】已取消正在执行的任务")
        
        self._full_cleanup()
    
    # ==================== 执行流程 ====================
    
    async def _execute(self):
        """执行止损止盈设置流程"""
        try:
            logger.info("🔧【全自动止损止盈】开始执行")
            
            self._init_cache()
            
            if not await self._load_private_data():
                self._cleanup_work()
                return
            
            if not await self._load_okx_tick_sz():
                self._cleanup_work()
                return
            
            if not await self._load_binance_tick_size():
                self._cleanup_work()
                return
            
            self._calculate_prices()
            self._format_prices()
            self._fill_params()
            self._send_to_trader()
            
            logger.info("✅【全自动止损止盈】完成")
            
        except asyncio.CancelledError:
            logger.info("🛑【全自动止损止盈】任务被取消")
        except Exception as e:
            logger.error(f"❌【全自动止损止盈】执行异常: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self._cleanup_work()
            self._is_executing = False
            self._current_task = None
    
    def _init_cache(self):
        """拷贝模板到缓存"""
        self.okx_cache = copy.deepcopy(OCO_OKX)
        self.binance_cache = copy.deepcopy(OCO_BINANCE)
        logger.info("📦【全自动止损止盈】模板已拷贝")
    
    # ==================== 读取数据 ====================
    
    async def _load_private_data(self) -> bool:
        """读取私人数据，失败重试1次"""
        max_attempts = 2
        
        for attempt in range(max_attempts):
            try:
                result = await self.data_manager.get_private_user_data()
                user_data = result.get("data", {})
                
                okx_data = user_data.get("okx", {})
                binance_data = user_data.get("binance", {})
                
                self.okx_symbol = okx_data.get("开仓合约名", "")
                self.okx_open_price = float(okx_data.get("开仓价", 0))
                self.okx_position_side = okx_data.get("开仓方向", "").lower()
                
                self.binance_symbol = binance_data.get("开仓合约名", "")
                self.binance_open_price = float(binance_data.get("开仓价", 0))
                self.binance_position_side = binance_data.get("开仓方向", "").upper()
                
                if not self.okx_symbol or self.okx_open_price <= 0 or not self.okx_position_side:
                    logger.warning(f"⚠️【全自动止损止盈】欧易私人数据不完整")
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(1)
                        continue
                    return False
                
                if not self.binance_symbol or self.binance_open_price <= 0 or not self.binance_position_side:
                    logger.warning(f"⚠️【全自动止损止盈】币安私人数据不完整")
                    if attempt < max_attempts - 1:
                        await asyncio.sleep(1)
                        continue
                    return False
                
                logger.info(f"✅【全自动止损止盈】私人数据: 欧易={self.okx_symbol} 价格={self.okx_open_price} 方向={self.okx_position_side}")
                logger.info(f"✅【全自动止损止盈】私人数据: 币安={self.binance_symbol} 价格={self.binance_open_price} 方向={self.binance_position_side}")
                return True
                
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"❌【全自动止损止盈】读取私人数据失败 (尝试 {attempt+1}/{max_attempts}): {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(1)
        
        return False
    
    async def _load_okx_tick_sz(self) -> bool:
        """读取欧易价格精度 tickSz，失败重试1次"""
        max_attempts = 2
        okx_inst_id = self._convert_okx_symbol(self.okx_symbol)
        
        for attempt in range(max_attempts):
            try:
                result = await self.data_manager.get_okx_contracts_data()
                contracts = result.get("data", [])
                
                for contract in contracts:
                    if contract.get("instId") == okx_inst_id:
                        self.okx_tick_sz = float(contract.get("tickSz", 0))
                        
                        if self.okx_tick_sz <= 0:
                            logger.warning(f"⚠️【全自动止损止盈】欧易tickSz无效: {self.okx_tick_sz}")
                            if attempt < max_attempts - 1:
                                await asyncio.sleep(1)
                                continue
                            return False
                        
                        logger.info(f"✅【全自动止损止盈】欧易tickSz={self.okx_tick_sz}")
                        return True
                
                logger.warning(f"⚠️【全自动止损止盈】未找到欧易合约 {okx_inst_id} 的面值数据")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(1)
                    
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"❌【全自动止损止盈】读取欧易tickSz失败 (尝试 {attempt+1}/{max_attempts}): {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(1)
        
        return False
    
    async def _load_binance_tick_size(self) -> bool:
        """读取币安价格精度 tickSize，失败重试1次"""
        max_attempts = 2
        
        for attempt in range(max_attempts):
            try:
                result = await self.data_manager.get_binance_contracts_data()
                contracts = result.get("data", [])
                
                for contract in contracts:
                    if contract.get("symbol") == self.binance_symbol:
                        self.binance_tick_size = float(contract.get("tickSize", 0))
                        
                        if self.binance_tick_size <= 0:
                            logger.warning(f"⚠️【全自动止损止盈】币安tickSize无效: {self.binance_tick_size}")
                            if attempt < max_attempts - 1:
                                await asyncio.sleep(1)
                                continue
                            return False
                        
                        logger.info(f"✅【全自动止损止盈】币安tickSize={self.binance_tick_size}")
                        return True
                
                logger.warning(f"⚠️【全自动止损止盈】未找到币安合约 {self.binance_symbol} 的精度数据")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(1)
                    
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"❌【全自动止损止盈】读取币安tickSize失败 (尝试 {attempt+1}/{max_attempts}): {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(1)
        
        return False
    
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
    
    # ==================== 计算价格 ====================
    
    def _calculate_prices(self):
        """计算止损价和止盈价（固定 |35%|）"""
        sl_abs = 0.35
        tp = 0.35
        
        if self.okx_position_side == "long":
            self.okx_stop_price = self.okx_open_price * (1 - sl_abs)
            self.okx_take_price = self.okx_open_price * (1 + tp)
            logger.info(f"📊【全自动止损止盈】欧易做多: 止损={self.okx_stop_price}, 止盈={self.okx_take_price}")
        elif self.okx_position_side == "short":
            self.okx_stop_price = self.okx_open_price * (1 + sl_abs)
            self.okx_take_price = self.okx_open_price * (1 - tp)
            logger.info(f"📊【全自动止损止盈】欧易做空: 止损={self.okx_stop_price}, 止盈={self.okx_take_price}")
        
        if self.binance_position_side == "LONG":
            self.binance_stop_price = self.binance_open_price * (1 - sl_abs)
            self.binance_take_price = self.binance_open_price * (1 + tp)
            logger.info(f"📊【全自动止损止盈】币安做多: 止损={self.binance_stop_price}, 止盈={self.binance_take_price}")
        elif self.binance_position_side == "SHORT":
            self.binance_stop_price = self.binance_open_price * (1 + sl_abs)
            self.binance_take_price = self.binance_open_price * (1 - tp)
            logger.info(f"📊【全自动止损止盈】币安做空: 止损={self.binance_stop_price}, 止盈={self.binance_take_price}")
    
    def _format_prices(self):
        """按精度取整并格式化"""
        self.okx_stop_price = self._round_and_format(self.okx_stop_price, self.okx_tick_sz)
        self.okx_take_price = self._round_and_format(self.okx_take_price, self.okx_tick_sz)
        logger.info(f"🎯【全自动止损止盈】欧易精度化后: 止损={self.okx_stop_price}, 止盈={self.okx_take_price}")
        
        self.binance_stop_price = self._round_and_format(self.binance_stop_price, self.binance_tick_size)
        self.binance_take_price = self._round_and_format(self.binance_take_price, self.binance_tick_size)
        logger.info(f"🎯【全自动止损止盈】币安精度化后: 止损={self.binance_stop_price}, 止盈={self.binance_take_price}")
    
    def _round_and_format(self, price: float, tick: float) -> str:
        """按tick精度取整并格式化"""
        if tick <= 0:
            return str(price)
        
        rounded = round(price / tick) * tick
        
        if tick >= 1:
            decimal_places = 0
        else:
            tick_str = str(tick)
            if '.' in tick_str:
                decimal_places = len(tick_str.split('.')[1])
            else:
                decimal_places = 0
        
        return f"{rounded:.{decimal_places}f}"
    
    # ==================== 填充参数 ====================
    
    def _fill_params(self):
        """填充止损止盈参数"""
        okx_inst_id = self._convert_okx_symbol(self.okx_symbol)
        
        self.okx_cache["params"]["instId"] = okx_inst_id
        self.okx_cache["params"]["posSide"] = self.okx_position_side
        
        if self.okx_position_side == "long":
            self.okx_cache["params"]["side"] = "sell"
        else:
            self.okx_cache["params"]["side"] = "buy"
        
        self.okx_cache["params"]["slTriggerPx"] = self.okx_stop_price
        self.okx_cache["params"]["tpTriggerPx"] = self.okx_take_price
        
        logger.info(f"📝【全自动止损止盈】欧易参数已填充")
        
        self.binance_cache["orders"][0]["symbol"] = self.binance_symbol
        self.binance_cache["orders"][0]["positionSide"] = self.binance_position_side
        self.binance_cache["orders"][0]["triggerPrice"] = self.binance_stop_price
        
        self.binance_cache["orders"][1]["symbol"] = self.binance_symbol
        self.binance_cache["orders"][1]["positionSide"] = self.binance_position_side
        self.binance_cache["orders"][1]["triggerPrice"] = self.binance_take_price
        
        if self.binance_position_side == "LONG":
            self.binance_cache["orders"][0]["side"] = "SELL"
            self.binance_cache["orders"][1]["side"] = "SELL"
        else:
            self.binance_cache["orders"][0]["side"] = "BUY"
            self.binance_cache["orders"][1]["side"] = "BUY"
        
        logger.info(f"📝【全自动止损止盈】币安参数已填充")
    
    # ==================== 推送 ====================
    
    def _send_to_trader(self):
        """推送给下单工人"""
        orders = []
        if self.okx_cache:
            orders.append(self.okx_cache)
        if self.binance_cache:
            orders.append(self.binance_cache)
        
        if orders and self.brain.trader:
            self.brain.trader.send_orders(orders)
            logger.info(f"📤【全自动止损止盈】已推送 {len(orders)} 个订单给下单工人")
    
    # ==================== 清理 ====================
    
    def _cleanup_work(self):
        """清理本次工作缓存，保留 auto_mode_active"""
        self.okx_cache = None
        self.binance_cache = None
        
        self.okx_symbol = None
        self.binance_symbol = None
        self.okx_open_price = 0.0
        self.okx_position_side = ""
        self.binance_open_price = 0.0
        self.binance_position_side = ""
        self.okx_tick_sz = 0.0
        self.binance_tick_size = 0.0
        
        self.okx_stop_price = ""
        self.okx_take_price = ""
        self.binance_stop_price = ""
        self.binance_take_price = ""
        
        self.okx_open_ok = False
        self.binance_open_ok = False
        
        logger.info("🧹【全自动止损止盈】工作缓存已清空，全自动标签保留")
    
    def _full_cleanup(self):
        """完全重置"""
        self.auto_mode_active = False
        self.okx_open_ok = False
        self.binance_open_ok = False
        
        self.okx_cache = None
        self.binance_cache = None
        
        self.okx_symbol = None
        self.binance_symbol = None
        self.okx_open_price = 0.0
        self.okx_position_side = ""
        self.binance_open_price = 0.0
        self.binance_position_side = ""
        self.okx_tick_sz = 0.0
        self.binance_tick_size = 0.0
        
        self.okx_stop_price = ""
        self.okx_take_price = ""
        self.binance_stop_price = ""
        self.binance_take_price = ""
        
        self._is_executing = False
        self._current_task = None
        
        logger.info("🧹【全自动止损止盈】完全重置")
        