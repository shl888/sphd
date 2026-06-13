"""
半自动止损止盈工人 - 独立负责设置止损止盈

工作流程：
1. 收到止损止盈指令 → 缓存，开始工作
2. 拷贝欧易和币安的止损止盈模板
3. 填充合约名
4. 读取私人数据（开仓价、开仓方向）
5. 读取欧易面值数据（tickSz）
6. 读取币安精度数据（tickSize）
7. 计算止损价和止盈价
8. 按精度取整并格式化（严格对齐tick精度位数）
9. 填充参数
10. 推送给下单工人
11. 清空所有缓存
"""

import copy
import asyncio
import logging
import math
from typing import Dict, Any

from ..templates import OCO_OKX, OCO_BINANCE

logger = logging.getLogger(__name__)


class SlTpWorker:
    def __init__(self, brain):
        self.brain = brain
        self.data_manager = brain.data_manager
        
        # 缓存
        self.pending_command = None      # 原始指令
        self.pending_params = None       # 指令中的data部分
        
        self.okx_cache = None            # 欧易参数缓存
        self.binance_cache = None        # 币安参数缓存
        
        # 临时数据
        self.okx_symbol = None           # 欧易合约名
        self.binance_symbol = None       # 币安合约名
        self.stop_loss_percent = 0.0     # 止损幅度（负数，如 -8）
        self.take_profit_percent = 0.0   # 止盈幅度（正数，如 9）
        
        # 从私人数据提取
        self.okx_open_price = 0.0        # 欧易开仓价
        self.okx_position_side = ""      # 欧易开仓方向 long/short
        self.binance_open_price = 0.0    # 币安开仓价
        self.binance_position_side = ""  # 币安开仓方向 LONG/SHORT
        
        # 精度
        self.okx_tick_sz = 0.0           # 欧易价格精度
        self.binance_tick_size = 0.0     # 币安价格精度
        
        # 计算结果
        self.okx_stop_price = 0.0        # 欧易止损价
        self.okx_take_price = 0.0        # 欧易止盈价
        self.binance_stop_price = 0.0    # 币安止损价
        self.binance_take_price = 0.0    # 币安止盈价
        
        # 防重入标志
        self._is_executing = False
        
        logger.info("🔧【半自动止损止盈工人】初始化完成")
    
    def on_data(self, data: Dict[str, Any]):
        """接收大脑推送的数据"""
        if data.get("type") == "set_sl_tp":
            logger.info("📥【半自动止损止盈工人】收到止损止盈指令")
            # 新指令覆盖旧的
            self._cleanup()
            self.pending_command = data
            self.pending_params = data.get("data", {})
            self._start_work()
    
    def _start_work(self):
        """开始工作（只触发一次）"""
        if self._is_executing:
            return
        
        if self.pending_params:
            self._is_executing = True
            asyncio.create_task(self._execute())
    
    async def _execute(self):
        """执行止损止盈设置流程"""
        try:
            logger.info("🔧【半自动止损止盈工人】开始执行")
            
            # 1. 提取指令参数
            if not self._extract_command_params():
                self._cleanup()
                return
            
            # 2. 拷贝模板
            self._init_cache()
            
            # 3. 填充合约名
            self._fill_symbols()
            
            # 4. 读取私人数据（开仓价、开仓方向）
            # 【修改】支持单边持仓，不再强制要求双平台都有数据
            if not await self._load_private_data():
                self._cleanup()
                return
            
            # 5. 读取欧易面值数据（tickSz）- 仅当欧易需要处理时
            if self.okx_cache and not await self._load_okx_tick_sz():
                self._cleanup()
                return
            
            # 6. 读取币安精度数据（tickSize）- 仅当币安需要处理时
            if self.binance_cache and not await self._load_binance_tick_size():
                self._cleanup()
                return
            
            # 7. 计算止损止盈价 - 仅计算有缓存的交易所
            if not self._calculate_prices():
                self._cleanup()
                return
            
            # 8. 按精度取整并格式化 - 仅格式化有缓存的交易所
            self._format_prices()
            
            # 9. 填充参数 - 仅填充有缓存的交易所
            self._fill_params()
            
            # 10. 推送给下单工人
            self._send_to_trader()
            
            # 11. 清理
            self._cleanup()
            
            logger.info("✅【半自动止损止盈工人】完成")
            
        except Exception as e:
            logger.error(f"❌【半自动止损止盈工人】执行异常: {e}")
            self._cleanup()
        finally:
            self._is_executing = False
    
    def _extract_command_params(self) -> bool:
        """提取指令参数"""
        try:
            data = self.pending_params
            
            # 提取合约名
            okx_data = data.get("okx", {})
            binance_data = data.get("binance", {})
            
            self.okx_symbol = okx_data.get("symbol")
            self.binance_symbol = binance_data.get("symbol")
            
            # 【修改】至少需要一个合约名，不再强制要求两个都有
            if not self.okx_symbol and not self.binance_symbol:
                logger.error("❌【半自动止损止盈工人】至少需要一个合约名")
                return False
            
            # 提取幅度
            self.stop_loss_percent = float(data.get("stop_loss_percent", 0))
            self.take_profit_percent = float(data.get("take_profit_percent", 0))
            
            logger.info(f"📋【半自动止损止盈工人】指令参数: 欧易={self.okx_symbol}, 币安={self.binance_symbol}, 止损={self.stop_loss_percent}%, 止盈={self.take_profit_percent}%")
            return True
            
        except Exception as e:
            logger.error(f"❌【半自动止损止盈工人】提取指令参数失败: {e}")
            return False
    
    def _init_cache(self):
        """拷贝模板到缓存"""
        self.okx_cache = copy.deepcopy(OCO_OKX)
        self.binance_cache = copy.deepcopy(OCO_BINANCE)
        logger.info("📦【半自动止损止盈工人】模板已拷贝")
    
    def _fill_symbols(self):
        """填充合约名"""
        # 欧易
        if self.okx_symbol:
            self.okx_cache["params"]["instId"] = self.okx_symbol
        
        # 币安（索引0和索引1的symbol相同）
        if self.binance_symbol:
            self.binance_cache["orders"][0]["symbol"] = self.binance_symbol
            self.binance_cache["orders"][1]["symbol"] = self.binance_symbol
        
        logger.info(f"📝【半自动止损止盈工人】合约名已填充: 欧易={self.okx_symbol}, 币安={self.binance_symbol}")
    
    async def _load_private_data(self) -> bool:
        """
        读取私人数据，重试1次
        【修改】支持单边持仓：哪个交易所有数据就处理哪个，互不影响
        """
        max_attempts = 2
        
        for attempt in range(max_attempts):
            try:
                result = await self.data_manager.get_private_user_data()
                user_data = result.get("data", {})
                
                okx_data = user_data.get("okx", {})
                binance_data = user_data.get("binance", {})
                
                has_valid_data = False  # 标记是否至少有一个有效持仓
                
                # ========== 处理欧易数据 ==========
                if self.okx_symbol:
                    self.okx_open_price = float(okx_data.get("开仓价", 0))
                    self.okx_position_side = okx_data.get("开仓方向", "").lower()
                    
                    if self.okx_open_price > 0 and self.okx_position_side:
                        has_valid_data = True
                        logger.info(f"✅【半自动止损止盈工人】欧易数据有效: 开仓价={self.okx_open_price}, 方向={self.okx_position_side}")
                    else:
                        # 欧易无有效持仓数据，清空欧易缓存，跳过后续处理
                        self.okx_cache = None
                        logger.warning("⚠️【半自动止损止盈工人】欧易无有效持仓数据，跳过")
                
                # ========== 处理币安数据 ==========
                if self.binance_symbol:
                    self.binance_open_price = float(binance_data.get("开仓价", 0))
                    self.binance_position_side = binance_data.get("开仓方向", "").upper()
                    
                    if self.binance_open_price > 0 and self.binance_position_side:
                        has_valid_data = True
                        logger.info(f"✅【半自动止损止盈工人】币安数据有效: 开仓价={self.binance_open_price}, 方向={self.binance_position_side}")
                    else:
                        # 币安无有效持仓数据，清空币安缓存，跳过后续处理
                        self.binance_cache = None
                        logger.warning("⚠️【半自动止损止盈工人】币安无有效持仓数据，跳过")
                
                # 【修改】只要有一个交易所有效就继续，不再强制要求两个都有
                if has_valid_data:
                    return True
                else:
                    logger.warning(f"⚠️【半自动止损止盈工人】没有找到任何有效持仓数据 (尝试 {attempt+1}/{max_attempts})")
                    continue
                
            except Exception as e:
                logger.error(f"❌【半自动止损止盈工人】读取私人数据失败 (尝试 {attempt+1}/{max_attempts}): {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(1)
        
        logger.error("❌【半自动止损止盈工人】所有交易所均无有效持仓数据")
        return False
    
    async def _load_okx_tick_sz(self) -> bool:
        """
        读取欧易价格精度 tickSz，重试1次
        【修改】如果欧易不需要处理，直接返回成功
        """
        # 如果欧易不需要处理，直接跳过
        if not self.okx_symbol:
            logger.info("📭【半自动止损止盈工人】欧易无需处理，跳过精度读取")
            return True
        
        max_attempts = 2
        
        for attempt in range(max_attempts):
            try:
                result = await self.data_manager.get_okx_contracts_data()
                contracts = result.get("data", [])
                
                for contract in contracts:
                    if contract.get("instId") == self.okx_symbol:
                        self.okx_tick_sz = float(contract.get("tickSz", 0))
                        
                        if self.okx_tick_sz <= 0:
                            logger.warning(f"⚠️【半自动止损止盈工人】欧易tickSz无效: {self.okx_tick_sz}")
                            continue
                        
                        logger.info(f"✅【半自动止损止盈工人】欧易tickSz={self.okx_tick_sz}")
                        return True
                
                logger.warning(f"⚠️【半自动止损止盈工人】未找到欧易合约 {self.okx_symbol} 的面值数据")
                
            except Exception as e:
                logger.error(f"❌【半自动止损止盈工人】读取欧易tickSz失败 (尝试 {attempt+1}/{max_attempts}): {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(1)
        
        return False
    
    async def _load_binance_tick_size(self) -> bool:
        """
        读取币安价格精度 tickSize，重试1次
        【修改】如果币安不需要处理，直接返回成功
        """
        # 如果币安不需要处理，直接跳过
        if not self.binance_symbol:
            logger.info("📭【半自动止损止盈工人】币安无需处理，跳过精度读取")
            return True
        
        max_attempts = 2
        
        for attempt in range(max_attempts):
            try:
                result = await self.data_manager.get_binance_contracts_data()
                contracts = result.get("data", [])
                
                for contract in contracts:
                    if contract.get("symbol") == self.binance_symbol:
                        self.binance_tick_size = float(contract.get("tickSize", 0))
                        
                        if self.binance_tick_size <= 0:
                            logger.warning(f"⚠️【半自动止损止盈工人】币安tickSize无效: {self.binance_tick_size}")
                            continue
                        
                        logger.info(f"✅【半自动止损止盈工人】币安tickSize={self.binance_tick_size}")
                        return True
                
                logger.warning(f"⚠️【半自动止损止盈工人】未找到币安合约 {self.binance_symbol} 的精度数据")
                
            except Exception as e:
                logger.error(f"❌【半自动止损止盈工人】读取币安tickSize失败 (尝试 {attempt+1}/{max_attempts}): {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(1)
        
        return False
    
    def _calculate_prices(self) -> bool:
        """
        计算止损价和止盈价
        【修改】只计算有缓存的交易所（即有持仓的那个）
        """
        try:
            # 止损幅度取绝对值
            sl_abs = abs(self.stop_loss_percent) / 100
            tp = self.take_profit_percent / 100
            
            has_valid = False
            
            # ========== 计算欧易价格（如果欧易有缓存） ==========
            if self.okx_cache:
                if self.okx_position_side == "long":
                    # 做多：止损=开仓价×(1-止损%)，止盈=开仓价×(1+止盈%)
                    self.okx_stop_price = self.okx_open_price * (1 - sl_abs)
                    self.okx_take_price = self.okx_open_price * (1 + tp)
                    logger.info(f"📊【半自动止损止盈工人】欧易做多: 止损价={self.okx_stop_price}, 止盈价={self.okx_take_price}")
                    has_valid = True
                elif self.okx_position_side == "short":
                    # 做空：止损=开仓价×(1+止损%)，止盈=开仓价×(1-止盈%)
                    self.okx_stop_price = self.okx_open_price * (1 + sl_abs)
                    self.okx_take_price = self.okx_open_price * (1 - tp)
                    logger.info(f"📊【半自动止损止盈工人】欧易做空: 止损价={self.okx_stop_price}, 止盈价={self.okx_take_price}")
                    has_valid = True
                else:
                    logger.error(f"❌【半自动止损止盈工人】未知的欧易开仓方向: {self.okx_position_side}")
                    self.okx_cache = None  # 清空无效的欧易缓存
            
            # ========== 计算币安价格（如果币安有缓存） ==========
            if self.binance_cache:
                if self.binance_position_side == "LONG":
                    self.binance_stop_price = self.binance_open_price * (1 - sl_abs)
                    self.binance_take_price = self.binance_open_price * (1 + tp)
                    logger.info(f"📊【半自动止损止盈工人】币安做多: 止损价={self.binance_stop_price}, 止盈价={self.binance_take_price}")
                    has_valid = True
                elif self.binance_position_side == "SHORT":
                    self.binance_stop_price = self.binance_open_price * (1 + sl_abs)
                    self.binance_take_price = self.binance_open_price * (1 - tp)
                    logger.info(f"📊【半自动止损止盈工人】币安做空: 止损价={self.binance_stop_price}, 止盈价={self.binance_take_price}")
                    has_valid = True
                else:
                    logger.error(f"❌【半自动止损止盈工人】未知的币安开仓方向: {self.binance_position_side}")
                    self.binance_cache = None  # 清空无效的币安缓存
            
            if not has_valid:
                logger.error("❌【半自动止损止盈工人】没有任何有效价格可计算")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌【半自动止损止盈工人】计算价格失败: {e}")
            return False
    
    def _format_prices(self):
        """
        按精度取整并格式化（严格对齐tick精度位数）
        【修改】只格式化有缓存的交易所
        """
        # 欧易格式化
        if self.okx_cache:
            self.okx_stop_price = self._round_and_format(self.okx_stop_price, self.okx_tick_sz)
            self.okx_take_price = self._round_and_format(self.okx_take_price, self.okx_tick_sz)
            logger.info(f"🎯【半自动止损止盈工人】欧易精度化后: 止损={self.okx_stop_price}, 止盈={self.okx_take_price}")
        
        # 币安格式化
        if self.binance_cache:
            self.binance_stop_price = self._round_and_format(self.binance_stop_price, self.binance_tick_size)
            self.binance_take_price = self._round_and_format(self.binance_take_price, self.binance_tick_size)
            logger.info(f"🎯【半自动止损止盈工人】币安精度化后: 止损={self.binance_stop_price}, 止盈={self.binance_take_price}")
    
    def _round_and_format(self, price: float, tick: float) -> str:
        """
        按tick精度取整并格式化
        
        规则：
        - tick = 0.1 → 保留1位小数，整数也写成 123.0
        - tick = 0.01 → 保留2位小数
        - tick = 1 → 不保留小数
        """
        if tick <= 0:
            return str(price)
        
        # 先按tick取整
        rounded = round(price / tick) * tick
        
        # 计算需要保留的小数位数
        if tick >= 1:
            decimal_places = 0
        else:
            # tick = 0.1 → 1位, tick = 0.01 → 2位
            tick_str = str(tick)
            if '.' in tick_str:
                decimal_places = len(tick_str.split('.')[1])
            else:
                decimal_places = 0
        
        # 格式化
        return f"{rounded:.{decimal_places}f}"
    
    def _fill_params(self):
        """
        填充参数
        【修改】只填充有缓存的交易所
        """
        # ========== 填充欧易参数 ==========
        if self.okx_cache:
            # posSide：开仓方向小写
            self.okx_cache["params"]["posSide"] = self.okx_position_side
            
            # side：平仓方向（与开仓方向相反）
            if self.okx_position_side == "long":
                self.okx_cache["params"]["side"] = "sell"
            else:
                self.okx_cache["params"]["side"] = "buy"
            
            # 止损止盈价（已经是格式化后的字符串）
            self.okx_cache["params"]["slTriggerPx"] = self.okx_stop_price
            self.okx_cache["params"]["tpTriggerPx"] = self.okx_take_price
            
            logger.info(f"📝【半自动止损止盈工人】欧易参数已填充: side={self.okx_cache['params']['side']}, posSide={self.okx_cache['params']['posSide']}")
        
        # ========== 填充币安参数 ==========
        if self.binance_cache:
            # 止损单（索引0）
            self.binance_cache["orders"][0]["positionSide"] = self.binance_position_side
            self.binance_cache["orders"][0]["triggerPrice"] = self.binance_stop_price
            
            # 止盈单（索引1）
            self.binance_cache["orders"][1]["positionSide"] = self.binance_position_side
            self.binance_cache["orders"][1]["triggerPrice"] = self.binance_take_price
            
            # side：平仓方向（与开仓方向相反）
            if self.binance_position_side == "LONG":
                self.binance_cache["orders"][0]["side"] = "SELL"
                self.binance_cache["orders"][1]["side"] = "SELL"
            else:
                self.binance_cache["orders"][0]["side"] = "BUY"
                self.binance_cache["orders"][1]["side"] = "BUY"
            
            logger.info(f"📝【半自动止损止盈工人】币安参数已填充: 止损价={self.binance_stop_price}, 止盈价={self.binance_take_price}")
    
    def _send_to_trader(self):
        """推送给下单工人"""
        orders = []
        if self.okx_cache:
            orders.append(self.okx_cache)
        if self.binance_cache:
            orders.append(self.binance_cache)
        
        if orders and self.brain.trader:
            self.brain.trader.send_orders(orders)
            logger.info(f"📤【半自动止损止盈工人】已推送 {len(orders)} 个订单给下单工人")
        elif not orders:
            logger.warning("⚠️【半自动止损止盈工人】没有需要推送的订单")
    
    def _cleanup(self):
        """清空所有缓存"""
        self.pending_command = None
        self.pending_params = None
        self.okx_cache = None
        self.binance_cache = None
        
        self.okx_symbol = None
        self.binance_symbol = None
        self.stop_loss_percent = 0.0
        self.take_profit_percent = 0.0
        
        self.okx_open_price = 0.0
        self.okx_position_side = ""
        self.binance_open_price = 0.0
        self.binance_position_side = ""
        
        self.okx_tick_sz = 0.0
        self.binance_tick_size = 0.0
        
        self.okx_stop_price = 0.0
        self.okx_take_price = 0.0
        self.binance_stop_price = 0.0
        self.binance_take_price = 0.0
        
        logger.info("🧹【半自动止损止盈工人】缓存已清空")