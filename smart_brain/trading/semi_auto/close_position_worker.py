"""
半自动清仓工人 - 独立负责平仓

工作流程：
1. 收到平仓指令 → 缓存，开始工作
2. 拷贝欧易和币安的平仓模板
3. 填充合约名
4. 读取私人数据（开仓方向、币安持仓币数）
5. 填充方向字段和币安quantity
6. 推送给下单工人
7. 清空所有缓存
"""

import copy
import asyncio
import logging
from typing import Dict, Any

from ..templates import CLOSE_POSITION_OKX, CLOSE_POSITION_BINANCE

logger = logging.getLogger(__name__)


class ClosePositionWorker:
    def __init__(self, brain):
        self.brain = brain
        self.data_manager = brain.data_manager
        
        # 缓存
        self.pending_command = None      # 原始指令
        self.pending_params = None       # 指令中的data部分
        
        self.okx_cache = None            # 欧易平仓参数缓存
        self.binance_cache = None        # 币安平仓参数缓存
        
        # 临时数据
        self.okx_symbol = None           # 欧易合约名
        self.binance_symbol = None       # 币安合约名
        
        # 从私人数据提取
        self.okx_position_side = ""      # 欧易开仓方向
        self.binance_position_side = ""  # 币安开仓方向
        self.binance_quantity = 0.0      # 币安持仓币数
        
        # 防重入标志
        self._is_executing = False
        
        logger.info("🔧【半自动清仓工人】初始化完成")
    
    def on_data(self, data: Dict[str, Any]):
        """接收大脑推送的数据"""
        if data.get("type") == "close_position":
            logger.info("📥【半自动清仓工人】收到平仓指令")
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
        """执行平仓流程"""
        try:
            logger.info("🔧【半自动清仓工人】开始执行")
            
            # 1. 提取指令参数
            if not self._extract_command_params():
                self._cleanup()
                return
            
            # 2. 拷贝模板
            self._init_cache()
            
            # 3. 填充合约名
            self._fill_symbols()
            
            # 4. 读取私人数据（开仓方向、币安持仓币数）
            # 【修改】支持单边持仓，不再强制要求双平台都有数据
            if not await self._load_private_data():
                self._cleanup()
                return
            
            # 5. 填充方向 - 仅填充有缓存的交易所
            self._fill_direction()
            
            # 6. 推送给下单工人
            self._send_to_trader()
            
            # 7. 清理
            self._cleanup()
            
            logger.info("✅【半自动清仓工人】完成")
            
        except Exception as e:
            logger.error(f"❌【半自动清仓工人】执行异常: {e}")
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
                logger.error("❌【半自动清仓工人】至少需要一个合约名")
                return False
            
            logger.info(f"📋【半自动清仓工人】指令参数: 欧易={self.okx_symbol}, 币安={self.binance_symbol}")
            return True
            
        except Exception as e:
            logger.error(f"❌【半自动清仓工人】提取指令参数失败: {e}")
            return False
    
    def _init_cache(self):
        """拷贝模板到缓存"""
        self.okx_cache = copy.deepcopy(CLOSE_POSITION_OKX)
        self.binance_cache = copy.deepcopy(CLOSE_POSITION_BINANCE)
        logger.info("📦【半自动清仓工人】模板已拷贝")
    
    def _fill_symbols(self):
        """填充合约名"""
        # 欧易
        if self.okx_symbol:
            self.okx_cache["params"]["instId"] = self.okx_symbol
        
        # 币安
        if self.binance_symbol:
            self.binance_cache["params"]["symbol"] = self.binance_symbol
        
        logger.info(f"📝【半自动清仓工人】合约名已填充: 欧易={self.okx_symbol}, 币安={self.binance_symbol}")
    
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
                    self.okx_position_side = okx_data.get("开仓方向", "")
                    
                    if self.okx_position_side:
                        has_valid_data = True
                        logger.info(f"✅【半自动清仓工人】欧易数据有效: 方向={self.okx_position_side}")
                    else:
                        # 欧易无有效持仓数据，清空欧易缓存，跳过后续处理
                        self.okx_cache = None
                        logger.warning("⚠️【半自动清仓工人】欧易无有效持仓数据，跳过")
                
                # ========== 处理币安数据 ==========
                if self.binance_symbol:
                    self.binance_position_side = binance_data.get("开仓方向", "")
                    self.binance_quantity = float(binance_data.get("持仓币数", 0))
                    
                    if self.binance_position_side and self.binance_quantity > 0:
                        has_valid_data = True
                        logger.info(f"✅【半自动清仓工人】币安数据有效: 方向={self.binance_position_side}, 持仓币数={self.binance_quantity}")
                    else:
                        # 币安无有效持仓数据，清空币安缓存，跳过后续处理
                        self.binance_cache = None
                        logger.warning("⚠️【半自动清仓工人】币安无有效持仓数据，跳过")
                
                # 【修改】只要有一个交易所有效就继续，不再强制要求两个都有
                if has_valid_data:
                    return True
                else:
                    logger.warning(f"⚠️【半自动清仓工人】没有找到任何有效持仓数据 (尝试 {attempt+1}/{max_attempts})")
                    continue
                
            except Exception as e:
                logger.error(f"❌【半自动清仓工人】读取私人数据失败 (尝试 {attempt+1}/{max_attempts}): {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(1)
        
        logger.error("❌【半自动清仓工人】所有交易所均无有效持仓数据")
        return False
    
    def _fill_direction(self):
        """
        填充方向字段
        【修改】只填充有缓存的交易所
        """
        # ========== 填充欧易参数 ==========
        if self.okx_cache:
            # 欧易：小写
            self.okx_cache["params"]["posSide"] = self.okx_position_side.lower()
            logger.info(f"📝【半自动清仓工人】欧易方向已填充: posSide={self.okx_cache['params']['posSide']}")
        
        # ========== 填充币安参数 ==========
        if self.binance_cache:
            # positionSide：开仓方向直接使用（LONG/SHORT）
            self.binance_cache["params"]["positionSide"] = self.binance_position_side
            
            # side：平仓方向（与开仓方向相反）
            if self.binance_position_side == "LONG":
                self.binance_cache["params"]["side"] = "SELL"
            else:
                self.binance_cache["params"]["side"] = "BUY"
            
            # quantity：持仓币数，格式化
            qty_formatted = f"{self.binance_quantity:.8f}".rstrip('0').rstrip('.')
            self.binance_cache["params"]["quantity"] = qty_formatted
            
            logger.info(f"📝【半自动清仓工人】币安方向已填充: side={self.binance_cache['params']['side']}, positionSide={self.binance_cache['params']['positionSide']}, quantity={qty_formatted}")
    
    def _send_to_trader(self):
        """推送给下单工人"""
        orders = []
        if self.okx_cache:
            orders.append(self.okx_cache)
        if self.binance_cache:
            orders.append(self.binance_cache)
        
        if orders and self.brain.trader:
            self.brain.trader.send_orders(orders)
            logger.info(f"📤【半自动清仓工人】已推送 {len(orders)} 个订单给下单工人")
        elif not orders:
            logger.warning("⚠️【半自动清仓工人】没有需要推送的订单")
    
    def _cleanup(self):
        """清空所有缓存"""
        self.pending_command = None
        self.pending_params = None
        self.okx_cache = None
        self.binance_cache = None
        
        self.okx_symbol = None
        self.binance_symbol = None
        self.okx_position_side = ""
        self.binance_position_side = ""
        self.binance_quantity = 0.0
        
        logger.info("🧹【半自动清仓工人】缓存已清空")