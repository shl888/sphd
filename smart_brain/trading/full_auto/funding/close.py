# trading/full_auto/funding/close.py
"""
资金费清仓工人 - 持续监控，条件触发清仓

架构：
- 步骤1-3（准备阶段）：拷贝模板、读数据、填充参数创建副本，只执行一次
- 步骤4-5（监控阶段）：循环检测清仓条件，触发后清仓并回到准备阶段

工作流程：
1. 收到标签 {"info": "开启全自动"} → 立刻启动
2. 执行准备阶段（步骤1-3），创建平仓参数副本
3. 进入监控阶段（步骤4-5），循环检测清仓条件
4. 任一条件触发 → 发送副本 → 清空缓存 → 回到准备阶段
5. 收到标签 {"info": "结束全自动"} → 立刻停止，完全重置

清仓条件：
1. 孤儿单：只有一个交易所有持仓
2. 不是套利单：合约名不同 或 方向相同 或 仓位价值差 > 100
3. 危险仓位：|标记价涨跌盈亏幅| ≥ 36 或 |最新价涨跌盈亏幅| ≥ 36
4. 费率差缩小：rate_diff ≤ 0.3
5/6. 资金费后公式：60秒倒计时结束后，综合盈亏 ≥ 0.5 或 ≤ -0.2
7. 本小时不结算：结算时间更新后检测到倒计时 > 3610，安排第55分钟强制平仓（最后闸门）
"""

import asyncio
import logging
import copy
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from ...templates import CLOSE_POSITION_OKX, CLOSE_POSITION_BINANCE

logger = logging.getLogger(__name__)


class FundingClose:
    def __init__(self, brain):
        self.brain = brain
        self.data_manager = brain.data_manager
        
        # 工作状态
        self.is_active = False                  # 是否激活
        self.monitor_task = None                # 监控循环任务
        self.funding_timer_task = None          # 60秒倒计时任务
        self.delayed_close_task = None          # 延迟平仓任务（第55分钟）
        
        # 平仓参数缓存（填充时使用）
        self.okx_close_cache = None
        self.binance_close_cache = None
        
        # 平仓参数副本（填充完成后立即创建，触发清仓时直接发送）
        self.okx_close_copy = None
        self.binance_close_copy = None
        
        # 本次资金费结算时间缓存（用于检测是否更新）
        self.cached_okx_settle_time = None
        self.cached_binance_settle_time = None
        
        # 资金费公式检测标志（60秒倒计时结束后变为True）
        self.funding_check_active = False
        
        # 当前持仓合约名（用于检测费率差）
        self.current_symbol = None
        
        # 防重复触发（记录各条件是否已触发）
        self.last_orphan_type = None            # 孤儿单类型: 'okx' 或 'binance'
        self.last_not_arbitrage_key = None      # 不是套利单的标识
        self.last_dangerous_key = None          # 危险仓位的标识
        self.last_rate_diff_triggered = False   # 费率差是否已触发
        self.last_formula_triggered = False     # 公式是否已触发
        
        # 防重入标志
        self._is_closing = False
        
        logger.info("🔚【资金费清仓工人】初始化完成")
    
    # ==================== 标签控制 ====================
    
    def on_data(self, data: Dict[str, Any]):
        """被动接收大脑推送的数据"""
        if "info" not in data:
            return
        
        info = data["info"]
        logger.debug(f"📥【资金费清仓工人】收到标签: {info}")
        
        if info == "开启全自动":
            self._activate()
        elif info == "结束全自动":
            self._deactivate()
    
    def _activate(self):
        """激活，立刻开始监控"""
        if self.is_active:
            return
        
        self.is_active = True
        self._stop_monitor_task()
        self.monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info("✅【资金费清仓工人】已激活，开始持续监控")
    
    def _deactivate(self):
        """立刻停止所有工作，完全重置"""
        logger.info("🛑【资金费清仓工人】收到结束全自动标签，立刻重置")
        
        self.is_active = False
        self.funding_check_active = False
        
        self._stop_monitor_task()
        self._cancel_funding_timer()
        self._cancel_delayed_close_task()
        
        self._full_cleanup()
        logger.info("🛑【资金费清仓工人】已停用，状态完全重置")
    
    def _stop_monitor_task(self):
        """停止监控任务"""
        if self.monitor_task and not self.monitor_task.done():
            self.monitor_task.cancel()
            self.monitor_task = None
    
    def _cancel_funding_timer(self):
        """取消资金费倒计时任务"""
        if self.funding_timer_task and not self.funding_timer_task.done():
            self.funding_timer_task.cancel()
            self.funding_timer_task = None
    
    def _cancel_delayed_close_task(self):
        """取消延迟平仓任务"""
        if self.delayed_close_task and not self.delayed_close_task.done():
            self.delayed_close_task.cancel()
            self.delayed_close_task = None
    
    # ==================== 主监控循环 ====================
    
    async def _monitor_loop(self):
        """持续监控循环"""
        logger.info("🔄【资金费清仓工人】监控循环启动")
        
        while self.is_active:
            try:
                # ========== 准备阶段：步骤1-3（执行一次，直到成功创建副本） ==========
                
                # 步骤1：拷贝平仓模板
                self._init_close_cache()
                
                # 步骤2-3：循环读取数据，直到成功填充参数创建副本
                while self.is_active:
                    # 步骤2：读取数据
                    market_data, user_data = await self._fetch_data()
                    if market_data is None or user_data is None:
                        await asyncio.sleep(1)
                        continue
                    
                    # 步骤3：填充平仓参数，创建副本
                    has_position = self._fill_close_params(user_data)
                    if has_position:
                        logger.info("📦【资金费清仓工人】准备阶段完成，副本已创建，进入监控阶段")
                        break
                    
                    await asyncio.sleep(1)
                
                if not self.is_active:
                    break
                
                # ========== 监控阶段：步骤4-5（循环检测，直到触发清仓） ==========
                
                # 重置防重复状态
                self._reset_trigger_state()
                
                while self.is_active:
                    # 更新数据
                    market_data, user_data = await self._fetch_data()
                    if market_data is None or user_data is None:
                        await asyncio.sleep(1)
                        continue
                    
                    # 步骤4：缓存资金费结算时间，检测变化
                    self._update_settle_time_cache(user_data, market_data)
                    
                    # 步骤5：检测清仓条件
                    triggered = await self._check_close_conditions(market_data, user_data)
                    
                    if triggered:
                        # 触发清仓，退出监控阶段，回到外层重新从准备阶段开始
                        logger.info("🔄【资金费清仓工人】清仓已触发，返回准备阶段")
                        await asyncio.sleep(10)
                        break
                    
                    await asyncio.sleep(1)
                
            except asyncio.CancelledError:
                logger.info("🛑【资金费清仓工人】监控循环被取消")
                break
            except Exception as e:
                logger.error(f"❌【资金费清仓工人】监控循环异常: {e}")
                import traceback
                logger.error(traceback.format_exc())
                await asyncio.sleep(1)
        
        logger.info("🛑【资金费清仓工人】监控循环结束")
    
    def _reset_trigger_state(self):
        """重置防重复触发状态"""
        self.last_orphan_type = None
        self.last_not_arbitrage_key = None
        self.last_dangerous_key = None
        self.last_rate_diff_triggered = False
        self.last_formula_triggered = False
    
    # ==================== 步骤1：拷贝模板 ====================
    
    def _init_close_cache(self):
        """拷贝平仓模板到缓存"""
        self.okx_close_cache = copy.deepcopy(CLOSE_POSITION_OKX)
        self.binance_close_cache = copy.deepcopy(CLOSE_POSITION_BINANCE)
    
    # ==================== 步骤2：读取数据 ====================
    
    async def _fetch_data(self):
        """并行读取行情数据和私人数据"""
        try:
            market_task = asyncio.create_task(self.data_manager.get_public_market_data())
            user_task = asyncio.create_task(self.data_manager.get_private_user_data())
            
            market_result, user_result = await asyncio.gather(market_task, user_task)
            
            market_data = market_result.get('data', {}) if market_result else {}
            user_data = user_result.get('data', {}) if user_result else {}
            
            return market_data, user_data
            
        except Exception as e:
            logger.error(f"❌【资金费清仓工人】读取数据失败: {e}")
            return None, None
    
    # ==================== 步骤3：填充平仓参数 ====================
    
    def _fill_close_params(self, user_data: Dict) -> bool:
        """
        填充平仓参数，有值就填充，填充完立即创建副本
        
        返回: True 表示至少有一个交易所有持仓，副本创建成功
              False 表示两个交易所都没有持仓，或数据未到
        """
        okx_data = user_data.get('okx', {})
        binance_data = user_data.get('binance', {})
        
        # 提取开仓合约名（None 或空字符串都视为无持仓）
        okx_symbol = okx_data.get('开仓合约名') or ''
        binance_symbol = binance_data.get('开仓合约名') or ''
        
        # 记录当前持仓合约名（用于检测费率差）
        self.current_symbol = binance_symbol if binance_symbol else okx_symbol
        
        has_okx = bool(okx_symbol)
        has_binance = bool(binance_symbol)
        
        # 两个都没有持仓
        if not has_okx and not has_binance:
            self.okx_close_copy = None
            self.binance_close_copy = None
            return False
        
        # ========== 欧易有持仓，填充欧易参数，创建副本 ==========
        if has_okx:
            # 开仓方向（如果为None则跳过，数据未到）
            okx_position_side = okx_data.get('开仓方向')
            if okx_position_side is None:
                self.okx_close_copy = None
                return False
            okx_position_side = okx_position_side.lower()
            
            # 转换合约名格式：BTCUSDT → BTC-USDT-SWAP
            okx_inst_id = self._convert_okx_symbol(okx_symbol)
            
            self.okx_close_cache['params']['instId'] = okx_inst_id
            self.okx_close_cache['params']['posSide'] = okx_position_side
            
            # 创建副本
            self.okx_close_copy = copy.deepcopy(self.okx_close_cache)
            logger.debug(f"📝【资金费清仓工人】欧易平仓参数已填充: {okx_inst_id}")
        else:
            self.okx_close_copy = None
        
        # ========== 币安有持仓，填充币安参数，创建副本 ==========
        if has_binance:
            # 开仓方向（如果为None则跳过，数据未到）
            binance_position_side = binance_data.get('开仓方向')
            if binance_position_side is None:
                self.binance_close_copy = None
                return False
            binance_position_side = binance_position_side.upper()
            
            # 持仓币数（如果为None则跳过，数据未到）
            binance_quantity = binance_data.get('持仓币数')
            if binance_quantity is None:
                self.binance_close_copy = None
                return False
            
            self.binance_close_cache['params']['symbol'] = binance_symbol
            self.binance_close_cache['params']['positionSide'] = binance_position_side
            
            # quantity 格式化
            qty_str = f"{float(binance_quantity):.8f}".rstrip('0').rstrip('.')
            self.binance_close_cache['params']['quantity'] = qty_str
            
            # side：平仓方向与开仓方向相反
            if binance_position_side == 'LONG':
                self.binance_close_cache['params']['side'] = 'SELL'
            else:
                self.binance_close_cache['params']['side'] = 'BUY'
            
            # 创建副本
            self.binance_close_copy = copy.deepcopy(self.binance_close_cache)
            logger.debug(f"📝【资金费清仓工人】币安平仓参数已填充: {binance_symbol}")
        else:
            self.binance_close_copy = None
        
        return True
    
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
    
    # ==================== 步骤4：缓存资金费结算时间 ====================
    
    def _update_settle_time_cache(self, user_data: Dict, market_data: Dict = None):
        """缓存本次资金费结算时间，检测变化并启动倒计时"""
        okx_data = user_data.get('okx', {})
        binance_data = user_data.get('binance', {})
        
        # 读取当前值
        current_okx_time = okx_data.get('本次资金费结算时间')
        current_binance_time = binance_data.get('本次资金费结算时间')
        
        # 检测是否变化（只有缓存有值时才比较，首次运行不触发）
        settle_changed = False
        
        if self.cached_okx_settle_time is not None and current_okx_time != self.cached_okx_settle_time:
            settle_changed = True
            logger.info(f"📅【资金费清仓工人】欧易资金费结算时间更新")
        
        if self.cached_binance_settle_time is not None and current_binance_time != self.cached_binance_settle_time:
            settle_changed = True
            logger.info(f"📅【资金费清仓工人】币安资金费结算时间更新")
        
        # 更新缓存
        self.cached_okx_settle_time = current_okx_time
        self.cached_binance_settle_time = current_binance_time
        
        # 任意一个变了，执行后续逻辑
        if settle_changed:
            self.last_formula_triggered = False
            self._start_funding_delay_timer()
            
            # 新增：检测本小时是否结算
            if market_data is not None:
                self._check_countdown_for_delayed_settle(market_data)
    
    def _start_funding_delay_timer(self):
        """启动60秒倒计时任务"""
        self._cancel_funding_timer()
        self.funding_check_active = False
        self.funding_timer_task = asyncio.create_task(self._funding_delay_timer())
        logger.info("⏰【资金费清仓工人】启动60秒倒计时，结束后开始公式检测")
    
    async def _funding_delay_timer(self):
        """60秒倒计时，结束后开启公式检测"""
        try:
            await asyncio.sleep(60)
            if self.is_active:
                self.funding_check_active = True
                logger.info("✅【资金费清仓工人】60秒倒计时结束，开始公式检测")
        except asyncio.CancelledError:
            logger.info("🛑【资金费清仓工人】倒计时被取消")
    
    # ==================== 新增：检测本小时是否结算 ====================
    
    def _check_countdown_for_delayed_settle(self, market_data: Dict):
        """
        检测本小时是否结算，如果不结算，设置第55分钟强制平仓（最后一道闸门）
        
        逻辑说明：
        - 结算时间更新后立即调用一次
        - 如果倒计时 > 3610秒，说明本小时不结算
        - 启动定时任务，等待到第55分钟
        - 在这55分钟内，其他清仓条件仍正常监控
        - 第55分钟时若仍持仓，强制平仓，让位给第57分钟的侦察兵
        """
        if not self.current_symbol:
            logger.debug("⏭️【资金费清仓工人】无持仓合约，跳过倒计时检测")
            return
        
        symbol_data = market_data.get(self.current_symbol)
        if not symbol_data:
            logger.warning(f"⚠️【资金费清仓工人】未找到合约 {self.current_symbol} 的行情数据")
            return
        
        try:
            okx_countdown = int(symbol_data.get('okx_countdown_seconds') or 0)
            binance_countdown = int(symbol_data.get('binance_countdown_seconds') or 0)
            
            logger.info(f"🔍【资金费清仓工人】结算时间已更新，检测下次结算: 欧易倒计时={okx_countdown}秒, 币安倒计时={binance_countdown}秒")
            
            # 任一倒计时 > 3610（约1小时），说明本小时不结算
            if okx_countdown > 3610 or binance_countdown > 3610:
                logger.info(f"📅【资金费清仓工人】本小时不结算（倒计时 > 1小时），安排第55分钟平仓（最后闸门）")
                self._schedule_close_at_minute_55()
            else:
                logger.info(f"✅【资金费清仓工人】本小时会结算（倒计时 < 1小时），继续正常监控")
                
        except Exception as e:
            logger.error(f"❌【资金费清仓工人】检测结算倒计时异常: {e}")
    
    def _schedule_close_at_minute_55(self):
        """安排在本小时第55分钟执行平仓"""
        self._cancel_delayed_close_task()
        self.delayed_close_task = asyncio.create_task(self._delayed_close_worker())
    
    async def _delayed_close_worker(self):
        """等待到本小时第55分钟，然后执行平仓"""
        try:
            now = datetime.now()
            
            # 计算本小时第55分钟的时间
            target_time = now.replace(minute=55, second=0, microsecond=0)
            
            # 如果当前时间已经过了第55分钟，取下一个小时
            if now >= target_time:
                target_time = target_time + timedelta(hours=1)
            
            wait_seconds = (target_time - now).total_seconds()
            
            logger.info(f"⏰【资金费清仓工人】安排延迟平仓: 目标时间={target_time.strftime('%Y-%m-%d %H:%M:%S')}, 等待 {wait_seconds:.0f} 秒")
            
            await asyncio.sleep(wait_seconds)
            
            if not self.is_active:
                logger.info("🛑【资金费清仓工人】已停用，取消延迟平仓")
                return
            
            # 检查是否仍有有效的平仓副本
            if self.okx_close_copy or self.binance_close_copy:
                logger.warning(f"🔚【资金费清仓工人】执行延迟平仓（本小时第55分钟）- 最后闸门")
                await self._execute_close("本小时不结算，第55分钟强制平仓")
            else:
                logger.info("📭【资金费清仓工人】无持仓，取消延迟平仓")
                
        except asyncio.CancelledError:
            logger.info("🛑【资金费清仓工人】延迟平仓任务被取消")
        except Exception as e:
            logger.error(f"❌【资金费清仓工人】延迟平仓任务异常: {e}")
    
    # ==================== 步骤5：检测清仓条件 ====================
    
    async def _check_close_conditions(self, market_data: Dict, user_data: Dict) -> bool:
        """
        检测所有清仓条件，任一触发即平仓
        
        返回: True 表示触发了清仓
              False 表示没有触发
        """
        if self._is_closing:
            return False
        
        okx_data = user_data.get('okx', {})
        binance_data = user_data.get('binance', {})
        
        # 条件1：孤儿单（只有一个交易所有持仓）
        orphan_type = self._check_orphan(okx_data, binance_data)
        if orphan_type:
            await self._execute_close(f"{orphan_type}孤儿单")
            return True
        
        # 两个都有持仓，继续检测其他条件
        if not self.okx_close_copy or not self.binance_close_copy:
            return False
        
        # 条件2：不是套利单
        if self._check_not_arbitrage(okx_data, binance_data):
            await self._execute_close("不是套利单")
            return True
        
        # 条件3：危险仓位
        if self._check_dangerous(okx_data, binance_data):
            await self._execute_close("危险仓位")
            return True
        
        # 条件4：费率差缩小
        if self._check_rate_diff_small(market_data):
            await self._execute_close("费率差缩小")
            return True
        
        # 条件5/6：资金费后公式
        if self.funding_check_active and not self.last_formula_triggered:
            formula_result = self._check_funding_formula(okx_data, binance_data)
            if formula_result == "profit":
                self.last_formula_triggered = True
                self.funding_check_active = False
                await self._execute_close("资金费后公式触发（≥0.5）")
                return True
            elif formula_result == "loss":
                self.last_formula_triggered = True
                self.funding_check_active = False
                await self._execute_close("资金费后公式触发（≤-0.2）")
                return True
        
        return False
    
    # -------------------- 条件1：孤儿单 --------------------
    
    def _check_orphan(self, okx_data: Dict, binance_data: Dict) -> Optional[str]:
        """
        检查是否孤儿单（只有一个交易所有持仓）
        
        返回: '欧易' 表示欧易孤儿单
              '币安' 表示币安孤儿单
              None 表示不是孤儿单或已触发过
        """
        okx_symbol = okx_data.get('开仓合约名') or ''
        binance_symbol = binance_data.get('开仓合约名') or ''
        
        has_okx = bool(okx_symbol)
        has_binance = bool(binance_symbol)
        
        # 欧易孤儿单
        if has_okx and not has_binance:
            if self.last_orphan_type == 'okx':
                return None  # 已触发过，不重复
            self.last_orphan_type = 'okx'
            logger.warning(f"⚠️【资金费清仓工人】检测到欧易孤儿单")
            return '欧易'
        
        # 币安孤儿单
        if has_binance and not has_okx:
            if self.last_orphan_type == 'binance':
                return None
            self.last_orphan_type = 'binance'
            logger.warning(f"⚠️【资金费清仓工人】检测到币安孤儿单")
            return '币安'
        
        # 不是孤儿单，重置状态
        self.last_orphan_type = None
        return None
    
    # -------------------- 条件2：不是套利单 --------------------
    
    def _check_not_arbitrage(self, okx_data: Dict, binance_data: Dict) -> bool:
        """检查是否不是套利单"""
        okx_symbol = okx_data.get('开仓合约名') or ''
        binance_symbol = binance_data.get('开仓合约名') or ''
        
        # 开仓方向（如果为None则跳过，数据未到）
        okx_side = okx_data.get('开仓方向')
        binance_side = binance_data.get('开仓方向')
        if okx_side is None or binance_side is None:
            return False
        okx_side = okx_side.lower()
        binance_side = binance_side.lower()
        
        # 开仓价仓位价值（如果为None则跳过，数据未到）
        okx_value = okx_data.get('开仓价仓位价值')
        binance_value = binance_data.get('开仓价仓位价值')
        if okx_value is None or binance_value is None:
            return False
        okx_value = float(okx_value)
        binance_value = float(binance_value)
        
        # 生成当前状态标识
        current_key = f"{okx_symbol}_{binance_symbol}_{okx_side}_{binance_side}_{okx_value:.2f}_{binance_value:.2f}"
        
        # 合约名不同
        if okx_symbol != binance_symbol:
            if current_key == self.last_not_arbitrage_key:
                return False
            self.last_not_arbitrage_key = current_key
            logger.warning(f"⚠️【全自動清仓工人】合约名不同: 欧易={okx_symbol}, 币安={binance_symbol}")
            return True
        
        # 方向相同
        if okx_side == binance_side:
            if current_key == self.last_not_arbitrage_key:
                return False
            self.last_not_arbitrage_key = current_key
            logger.warning(f"⚠️【资金费清仓工人】方向相同: 欧易={okx_side}, 币安={binance_side}")
            return True
        
        # 仓位价值差 > 100
        value_diff = abs(okx_value - binance_value)
        if value_diff > 100:
            if current_key == self.last_not_arbitrage_key:
                return False
            self.last_not_arbitrage_key = current_key
            logger.warning(f"⚠️【资金费清仓工人】仓位价值差 > 100: {value_diff:.2f}")
            return True
        
        # 条件不满足，重置标识
        self.last_not_arbitrage_key = None
        return False
    
    # -------------------- 条件3：危险仓位 --------------------
    
    def _check_dangerous(self, okx_data: Dict, binance_data: Dict) -> bool:
        """检查是否危险仓位（涨跌幅绝对值 ≥ 36）"""
        # 生成当前状态标识
        okx_mark_val = okx_data.get('标记价涨跌盈亏幅')
        okx_last_val = okx_data.get('最新价涨跌盈亏幅')
        binance_mark_val = binance_data.get('标记价涨跌盈亏幅')
        binance_last_val = binance_data.get('最新价涨跌盈亏幅')
        
        current_key = f"{okx_mark_val}_{okx_last_val}_{binance_mark_val}_{binance_last_val}"
        
        # 欧易
        if okx_mark_val is not None:
            okx_mark = abs(float(okx_mark_val))
            if okx_mark >= 36:
                if current_key == self.last_dangerous_key:
                    return False
                self.last_dangerous_key = current_key
                logger.warning(f"⚠️【资金费清仓工人】欧易标记价涨跌盈亏幅 ≥ 36: {okx_mark:.2f}")
                return True
        
        if okx_last_val is not None:
            okx_last = abs(float(okx_last_val))
            if okx_last >= 36:
                if current_key == self.last_dangerous_key:
                    return False
                self.last_dangerous_key = current_key
                logger.warning(f"⚠️【资金费清仓工人】欧易最新价涨跌盈亏幅 ≥ 36: {okx_last:.2f}")
                return True
        
        # 币安
        if binance_mark_val is not None:
            binance_mark = abs(float(binance_mark_val))
            if binance_mark >= 36:
                if current_key == self.last_dangerous_key:
                    return False
                self.last_dangerous_key = current_key
                logger.warning(f"⚠️【资金费清仓工人】币安标记价涨跌盈亏幅 ≥ 36: {binance_mark:.2f}")
                return True
        
        if binance_last_val is not None:
            binance_last = abs(float(binance_last_val))
            if binance_last >= 36:
                if current_key == self.last_dangerous_key:
                    return False
                self.last_dangerous_key = current_key
                logger.warning(f"⚠️【资金费清仓工人】币安最新价涨跌盈亏幅 ≥ 36: {binance_last:.2f}")
                return True
        
        # 条件不满足，重置标识
        self.last_dangerous_key = None
        return False
    
    # -------------------- 条件4：费率差缩小 --------------------
    
    def _check_rate_diff_small(self, market_data: Dict) -> bool:
        """检查费率差是否 ≤ 0.3"""
        if not self.current_symbol:
            return False
        
        symbol_data = market_data.get(self.current_symbol)
        if not symbol_data:
            return False
        
        rate_diff = symbol_data.get('rate_diff')
        if rate_diff is None:
            return False
        # 这里的0，只是测试用，其实是0.3
        try:
            rate_diff = float(rate_diff)
            if rate_diff <= 0:
                if self.last_rate_diff_triggered:
                    return False
                self.last_rate_diff_triggered = True
                logger.warning(f"⚠️【资金费清仓工人】费率差缩小: {self.current_symbol} rate_diff={rate_diff}")
                return True
        except Exception as e:
            logger.error(f"❌【资金费清仓工人】检查费率差异常: {e}")
        
        return False
    
    # -------------------- 条件5/6：资金费后公式 --------------------
    
    def _check_funding_formula(self, okx_data: Dict, binance_data: Dict) -> Optional[str]:
        """
        计算综合盈亏公式
        
        结果 = (欧易最新价浮盈 + 币安最新价浮盈) × 100 / 分母
        分母 = |最新价浮盈| 较大一方的 开仓价仓位价值
        
        返回: "profit" 表示 ≥ 0.5
              "loss" 表示 ≤ -0.2
              None 表示不满足或数据未到
        """
        # 获取所需字段，任一为None则跳过
        okx_pnl = okx_data.get('最新价浮盈')
        binance_pnl = binance_data.get('最新价浮盈')
        okx_value = okx_data.get('开仓价仓位价值')
        binance_value = binance_data.get('开仓价仓位价值')
        
        if okx_pnl is None or binance_pnl is None or okx_value is None or binance_value is None:
            return None
        
        try:
            okx_pnl = float(okx_pnl)
            binance_pnl = float(binance_pnl)
            okx_value = float(okx_value)
            binance_value = float(binance_value)
            
            # 分母 = |最新价浮盈| 较大一方的开仓价仓位价值
            if abs(okx_pnl) >= abs(binance_pnl):
                denominator = okx_value
            else:
                denominator = binance_value
            
            if denominator == 0:
                return None
            
            result = (okx_pnl + binance_pnl) * 100 / denominator
            
            logger.debug(f"📊【资金费清仓工人】公式: 欧易浮盈={okx_pnl:.2f}, 币安浮盈={binance_pnl:.2f}, 分母={denominator:.2f}, 结果={result:.4f}")
            
            if result >= 0.5:
                logger.warning(f"⚠️【资金费清仓工人】公式结果 ≥ 0.5: {result:.4f}")
                return "profit"
            
            if result <= -0.2:
                logger.warning(f"⚠️【资金费清仓工人】公式结果 ≤ -0.2: {result:.4f}")
                return "loss"
            
            return None
            
        except Exception as e:
            logger.error(f"❌【资金费清仓工人】计算公式异常: {e}")
            return None
    
    # ==================== 执行清仓 ====================
    
    async def _execute_close(self, reason: str):
        """执行清仓"""
        if self._is_closing:
            return
        
        self._is_closing = True
        
        try:
            logger.info("=" * 50)
            logger.info(f"🔚【资金费清仓工人】触发清仓！原因: {reason}")
            
            # 取消延迟平仓任务（因为已经要平仓了）
            self._cancel_delayed_close_task()
            
            # 发送准备好的副本
            orders = []
            if self.okx_close_copy:
                orders.append(self.okx_close_copy)
            if self.binance_close_copy:
                orders.append(self.binance_close_copy)
            
            if orders and self.brain.trader:
                self.brain.trader.send_orders(orders)
                logger.info(f"📤【资金费清仓工人】已推送 {len(orders)} 个平仓订单给下单工人")
            
            logger.info("=" * 50)
            
            # 清理工作缓存，保留 is_active
            self._cleanup_work()
            
        except Exception as e:
            logger.error(f"❌【资金费清仓工人】执行清仓异常: {e}")
        finally:
            self._is_closing = False
    
    # ==================== 清理 ====================
    
    def _cleanup_work(self):
        """清理本次工作缓存，保留 is_active 和结算时间缓存"""
        self.okx_close_cache = None
        self.binance_close_cache = None
        self.okx_close_copy = None
        self.binance_close_copy = None
        self.current_symbol = None
        self.funding_check_active = False
        self._cancel_funding_timer()
        self._cancel_delayed_close_task()
        logger.debug("🧹【资金费清仓工人】工作缓存已清空")
    
    def _full_cleanup(self):
        """完全重置"""
        self.okx_close_cache = None
        self.binance_close_cache = None
        self.okx_close_copy = None
        self.binance_close_copy = None
        self.cached_okx_settle_time = None
        self.cached_binance_settle_time = None
        self.funding_check_active = False
        self.current_symbol = None
        self._is_closing = False
        self._cancel_funding_timer()
        self._cancel_delayed_close_task()
        self._reset_trigger_state()
        logger.info("🧹【资金费清仓工人】完全重置")