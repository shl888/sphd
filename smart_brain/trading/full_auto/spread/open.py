"""
价差套利开仓模块 - 四步筛选法工作流

工作流程（一次完整的、单向的工作流）：
─────────────────────────────────────────────────────────────
每小时第 2-10 分钟为筛选窗口，只有窗口内才启动工作流。

前置检查（带重试）：
- 读取行情数据 + 私人数据（最多重试2次）
- 空仓检查（最多重试2次）
- 资产检查（两边 ≥ 50 USDT）

步骤1：筛选（最长 10 分钟）
- 每 30 秒扫描一次，寻找跨平台价差 ≥ 5% 的合约
- 发现后立即进入步骤2
- 10 分钟内未发现则结束本小时工作

步骤2：持续监测（最长 20 分钟）
- 每 30 秒检查一次，等待价差降至 ≤ 3%
- 记录峰值价差（用于后续计算回归力度）
- 触发后进入步骤3
- 20 分钟内未触发则结束本小时工作

步骤3：蹲守期（固定 9 秒）
- 每 3 秒检测一次，需要连续 3 次价差 < 4.5%
- 任意一次 ≥ 4.5% 则重置计数
- 通过后进入步骤4

步骤4：健康检查 + 精选
- 必须通过双边健康检查（成交-标记价差 ≤ 1.5%）
- 如果有多个候选，选回归力度最大的
- 如果只有 1 个，直接选中

步骤5：生成开仓指令
- 计算保证金（较小资产 × 10%）
- 决定方向（空高价所，多低价所）
- 发送开仓指令给大脑
- 发送策略标签给标签调度器
─────────────────────────────────────────────────────────────
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple

logger = logging.getLogger(__name__)


class SpreadOpenConfig:
    """价差套利配置参数"""
    
    # 时间窗口
    SCREENING_WINDOW_START = 2        # 筛选窗口开始分钟数
    SCREENING_WINDOW_END = 10         # 筛选窗口结束分钟数
    
    # 步骤1：筛选
    STEP1_MAX_DURATION = 600          # 最长 10 分钟
    STEP1_SCAN_INTERVAL = 30          # 30秒扫描一次
    ENTRY_THRESHOLD = 5.0             # 进入观察的价差阈值（%）
    
    # 步骤2：持续监测
    STEP2_MAX_DURATION = 1200         # 最长 20 分钟
    STEP2_SCAN_INTERVAL = 30          # 30秒检查一次
    TRIGGER_THRESHOLD = 3.0           # 触发蹲守的价差阈值（%）
    
    # 步骤3：蹲守期
    WAITING_CHECK_INTERVAL = 3       # 3秒检测一次
    WAITING_SAFE_COUNT = 3            # 需要连续 3 次安全
    WAITING_REBOUND_THRESHOLD = 4.5   # 反弹淘汰阈值（%）
    # 总蹲守时长 = 3秒 × 3次 = 9秒
    
    # 步骤4：健康检查
    HEALTH_THRESHOLD = 1.5            # 成交-标记价差阈值（%）
    
    # 保证金
    MARGIN_RATIO = 0.05                # 保证金比例（较小资产的 10%）
    # 比例正常是0.1，如今测试改为0.05
    MIN_ASSET = 5                    # 最小资产要求（USDT）
    # 最小资产要求正常是50，如今测试改为5
    
    # 杠杆
    LEVERAGE = 1
    # 杠杆正常是20，如今测试改为1


class SpreadOpen:
    """价差开仓工人"""
    
    def __init__(self, brain):
        self.brain = brain
        self.data_manager = brain.data_manager
        
        # 工作流状态
        self.is_active = False
        self.workflow_task = None
        self.current_workflow = None
        
        # 配置
        self.config = SpreadOpenConfig()
        
        # 缓存
        self._peak_spreads = {}        # 记录每个候选的峰值价差
        self._cached_margin = 0.0      # 缓存的保证金
        
        logger.info("🔍【价差开仓工人】初始化完成")
    
    # ==================== 标签控制 ====================
    
    def on_data(self, data: Dict[str, Any]):
        """接收大脑推送的数据"""
        if "info" not in data:
            return
        
        info = data["info"]
        
        if info == "开启全自动":
            logger.info("🏷️【价差开仓工人】收到标签：开启全自动")
            self._activate()
        elif info == "结束全自动":
            logger.info("🏷️【价差开仓工人】收到标签：结束全自动")
            self._deactivate()
    
    def _activate(self):
        """激活"""
        if self.is_active:
            return
        self.is_active = True
        self._start_workflow_loop()
        logger.info("✅【价差开仓工人】已激活")
    
    def _deactivate(self):
        """停用"""
        self.is_active = False
        
        if self.workflow_task and not self.workflow_task.done():
            self.workflow_task.cancel()
            self.workflow_task = None
        
        if self.current_workflow and not self.current_workflow.done():
            self.current_workflow.cancel()
            logger.info("🛑【价差开仓工人】已取消正在执行的工作流")
        
        self._cleanup()
        logger.info("🛑【价差开仓工人】已停用")
    
    # ==================== 工作流循环 ====================
    
    def _start_workflow_loop(self):
        """启动工作流循环"""
        if self.workflow_task and not self.workflow_task.done():
            self.workflow_task.cancel()
        self.workflow_task = asyncio.create_task(self._workflow_loop())
    
    async def _workflow_loop(self):
        """工作流循环"""
        while self.is_active:
            try:
                # 检查是否在筛选窗口内
                if not self._is_within_screening_window():
                    await self._wait_until_next_window()
                    continue
                
                logger.info(f"🪟【价差开仓工人】当前在筛选窗口内（{datetime.now().minute}分），启动工作流")
                
                self.current_workflow = asyncio.create_task(self._run_workflow_once())
                result = await self.current_workflow
                self.current_workflow = None
                
                logger.info(f"📭【价差开仓工人】工作流结束，结果: {result}")
                
                # 本小时不再执行，等待下一个窗口
                await self._wait_until_next_window()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌【价差开仓工人】工作流循环异常: {e}")
                await asyncio.sleep(60)
    
    def _is_within_screening_window(self) -> bool:
        """检查是否在筛选窗口内（每小时第 2-10 分钟）"""
        minute = datetime.now().minute
        return self.config.SCREENING_WINDOW_START <= minute < self.config.SCREENING_WINDOW_END
    
    async def _wait_until_next_window(self):
        """等到下一个筛选窗口"""
        now = datetime.now()
        next_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        next_window = next_hour + timedelta(minutes=self.config.SCREENING_WINDOW_START)
        
        wait_seconds = (next_window - now).total_seconds()
        if wait_seconds > 0:
            logger.info(f"⏰【价差开仓工人】等待 {wait_seconds:.0f} 秒至下一个筛选窗口 ({next_window.strftime('%H:%M:%S')})")
            await asyncio.sleep(wait_seconds)
    
    # ==================== 工作流主流程 ====================
    
    async def _run_workflow_once(self) -> str:
        """执行一次完整的工作流"""
        workflow_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        logger.info("=" * 60)
        logger.info(f"🚀【价差开仓工人】开始工作流 {workflow_id}")
        logger.info("=" * 60)
        
        try:
            # ===== 前置检查：读取数据 =====
            market_data, user_data = await self._fetch_both_data_with_retry()
            if market_data is None or user_data is None:
                return 'no_data'
            
            # ===== 前置检查：空仓 =====
            if not await self._check_position_with_retry():
                return 'has_position'
            
            # ===== 前置检查：资产 =====
            if not self._check_asset(user_data):
                return 'asset_insufficient'
            
            # 计算保证金
            self._cached_margin = self._calculate_margin(user_data)
            logger.info(f"💰【价差开仓工人】保证金: {self._cached_margin:.2f} USDT")
            
            # ===== 步骤1：筛选 =====
            candidates = await self._step1_screen(market_data)
            if not candidates:
                logger.info(f"📭【价差开仓工人】步骤1：未筛选出 ≥{self.config.ENTRY_THRESHOLD}% 的合约")
                return 'no_candidate'
            logger.info(f"✅【价差开仓工人】步骤1：筛选出 {len(candidates)} 个合约: {candidates}")
            
            # ===== 步骤2：持续监测 =====
            triggered = await self._step2_monitor(candidates)
            if not triggered:
                logger.info(f"📭【价差开仓工人】步骤2：无合约在 20 分钟内降至 ≤{self.config.TRIGGER_THRESHOLD}%")
                return 'no_trigger'
            logger.info(f"✅【价差开仓工人】步骤2：触发蹲守的合约: {list(triggered.keys())}")
            
            # ===== 步骤3：蹲守期 =====
            passed = await self._step3_waiting(triggered)
            if not passed:
                logger.info(f"📭【价差开仓工人】步骤3：无合约通过蹲守期")
                return 'no_pass'
            logger.info(f"✅【价差开仓工人】步骤3：{passed} 通过蹲守期")
            
            # ===== 步骤4：健康检查 + 精选 =====
            best = await self._step4_health_check_and_select([passed])
            if not best:
                logger.info(f"📭【价差开仓工人】步骤4：健康检查不通过")
                return 'unhealthy'
            logger.info(f"🎯【价差开仓工人】步骤4：最终选中 {best}")
            
            # ===== 步骤5：生成开仓指令 =====
            success = await self._step5_execute(best)
            if not success:
                return 'execute_failed'
            
            logger.info(f"🎉【价差开仓工人】工作流 {workflow_id} 成功完成")
            return 'success'
            
        except asyncio.CancelledError:
            logger.info(f"🛑【价差开仓工人】工作流 {workflow_id} 被取消")
            raise
        except Exception as e:
            logger.error(f"❌【价差开仓工人】工作流 {workflow_id} 异常: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return 'failed'
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """清理缓存"""
        self._peak_spreads.clear()
        self._cached_margin = 0.0
    
    # ==================== 前置检查 ====================
    
    async def _fetch_both_data_with_retry(self) -> Tuple[Optional[Dict], Optional[Dict]]:
        """读取行情数据和私人数据，最多重试 2 次"""
        max_retries = 2
        retry_delay = 10
        
        for attempt in range(max_retries + 1):
            if attempt > 0:
                logger.info(f"🔄【价差开仓工人】第 {attempt} 次重试读取数据...")
                await asyncio.sleep(retry_delay)
            
            market_data, user_data = await self._fetch_both_data()
            
            market_ok = market_data is not None and len(market_data) > 0
            user_ok = user_data is not None and 'okx' in user_data and 'binance' in user_data
            
            if market_ok and user_ok:
                logger.info(f"✅【价差开仓工人】数据读取成功（行情{len(market_data)}条，私人数据正常）")
                return market_data, user_data
            
            if not market_ok:
                logger.warning(f"⚠️【价差开仓工人】行情数据读取不到或为空")
            if not user_ok:
                logger.warning(f"⚠️【价差开仓工人】私人数据读取不到或不完整")
        
        logger.error(f"❌【价差开仓工人】数据读取失败，已达最大重试次数")
        return None, None
    
    async def _fetch_both_data(self) -> Tuple[Optional[Dict], Optional[Dict]]:
        """并行读取行情数据和私人数据（单次）"""
        try:
            market_task = asyncio.create_task(self.data_manager.get_public_market_data())
            user_task = asyncio.create_task(self.data_manager.get_private_user_data())
            
            market_result, user_result = await asyncio.gather(market_task, user_task)
            
            market_data = market_result.get('data', {}) if market_result else None
            user_data = user_result.get('data', {}) if user_result else None
            
            return market_data, user_data
            
        except Exception as e:
            logger.error(f"❌【价差开仓工人】读取数据异常: {e}")
            return None, None
    
    async def _check_position_with_retry(self) -> bool:
        """检查是否空仓，最多重试 2 次"""
        max_retries = 2
        retry_delay = 10
        
        for attempt in range(max_retries + 1):
            if attempt > 0:
                logger.info(f"🔄【价差开仓工人】第 {attempt} 次重试检查持仓...")
                await asyncio.sleep(retry_delay)
            
            _, user_data = await self._fetch_both_data()
            
            if user_data is None:
                logger.warning(f"⚠️【价差开仓工人】无法获取私人数据检查持仓")
                continue
            
            okx_data = user_data.get('okx', {})
            binance_data = user_data.get('binance', {})
            
            okx_symbol = okx_data.get('开仓合约名', '')
            binance_symbol = binance_data.get('开仓合约名', '')
            
            if not okx_symbol and not binance_symbol:
                logger.info(f"✅【价差开仓工人】空仓检查通过")
                return True
            
            logger.warning(f"⚠️【价差开仓工人】已有持仓（欧易:{okx_symbol or '无'}, 币安:{binance_symbol or '无'}）")
        
        logger.error(f"❌【价差开仓工人】已有持仓，禁止开新仓")
        return False
    
    def _check_asset(self, user_data: Dict) -> bool:
        """检查两边资产是否都 ≥ 50"""
        try:
            okx_data = user_data.get('okx', {})
            binance_data = user_data.get('binance', {})
            
            okx_asset = float(okx_data.get('账户资产额', 0))
            binance_asset = float(binance_data.get('账户资产额', 0))
            
            if okx_asset < self.config.MIN_ASSET:
                logger.warning(f"⚠️【价差开仓工人】欧易资产不足{self.config.MIN_ASSET}: {okx_asset:.2f}")
                return False
            if binance_asset < self.config.MIN_ASSET:
                logger.warning(f"⚠️【价差开仓工人】币安资产不足{self.config.MIN_ASSET}: {binance_asset:.2f}")
                return False
            
            logger.info(f"✅【价差开仓工人】资产检查通过: 欧易={okx_asset:.2f}, 币安={binance_asset:.2f}")
            return True
            
        except Exception as e:
            logger.error(f"❌【价差开仓工人】资产检查异常: {e}")
            return False
    
    def _calculate_margin(self, user_data: Dict) -> float:
        """计算保证金 = 较小资产 × 10%"""
        try:
            okx_asset = float(user_data.get('okx', {}).get('账户资产额', 0))
            binance_asset = float(user_data.get('binance', {}).get('账户资产额', 0))
            smaller = min(okx_asset, binance_asset)
            return smaller * self.config.MARGIN_RATIO
        except Exception as e:
            logger.error(f"❌【价差开仓工人】计算保证金异常: {e}")
            return 10.0
    
    # ==================== 步骤1：筛选 ====================
    
    async def _step1_screen(self, market_data: Dict) -> List[str]:
        """步骤1：持续扫描 10 分钟，寻找 ≥5% 的合约"""
        step1_start = datetime.now()
        
        while True:
            elapsed = (datetime.now() - step1_start).total_seconds()
            if elapsed > self.config.STEP1_MAX_DURATION:
                logger.info(f"⏰【价差开仓工人】步骤1超时（{self.config.STEP1_MAX_DURATION}秒）")
                return []
            
            candidates = []
            for symbol, data in market_data.items():
                spread_pct = self._safe_float(data.get('trade_price_diff_percent'))
                if spread_pct is not None and spread_pct >= self.config.ENTRY_THRESHOLD:
                    candidates.append(symbol)
            
            if candidates:
                logger.info(f"✅【价差开仓工人】步骤1发现 {len(candidates)} 个候选: {candidates}")
                return candidates
            
            # 刷新行情数据
            await asyncio.sleep(self.config.STEP1_SCAN_INTERVAL)
            new_data = await self._fetch_market_data()
            if new_data:
                market_data = new_data
    
    async def _fetch_market_data(self) -> Optional[Dict]:
        """获取行情数据"""
        result = await self.data_manager.get_public_market_data()
        return result.get('data', {}) if result else None
    
    # ==================== 步骤2：持续监测 ====================
    
    async def _step2_monitor(self, candidates: List[str]) -> Dict[str, float]:
        """
        步骤2：持续监测 20 分钟，等待价差降至 ≤3%
        返回: {symbol: peak_spread}
        """
        step2_start = datetime.now()
        
        # 记录峰值
        for symbol in candidates:
            spread = await self._get_spread(symbol)
            if spread is not None:
                self._peak_spreads[symbol] = spread
        
        triggered = {}
        
        while True:
            elapsed = (datetime.now() - step2_start).total_seconds()
            if elapsed > self.config.STEP2_MAX_DURATION:
                logger.info(f"⏰【价差开仓工人】步骤2超时（{self.config.STEP2_MAX_DURATION}秒）")
                return {}
            
            for symbol in candidates:
                if symbol in triggered:
                    continue
                
                spread = await self._get_spread(symbol)
                if spread is None:
                    continue
                
                # 更新峰值
                if spread > self._peak_spreads.get(symbol, 0):
                    self._peak_spreads[symbol] = spread
                
                # 检查触发
                if spread <= self.config.TRIGGER_THRESHOLD and self._peak_spreads[symbol] >= self.config.ENTRY_THRESHOLD:
                    triggered[symbol] = self._peak_spreads[symbol]
                    logger.info(f"   🎯 {symbol}: 峰值 {self._peak_spreads[symbol]:.2f}% → 当前 {spread:.2f}%，触发蹲守")
            
            if triggered:
                return triggered
            
            await asyncio.sleep(self.config.STEP2_SCAN_INTERVAL)
    
    async def _get_spread(self, symbol: str) -> Optional[float]:
        """获取指定合约的价差百分比"""
        market_data = await self._fetch_market_data()
        if not market_data:
            return None
        data = market_data.get(symbol)
        if not data:
            return None
        return self._safe_float(data.get('trade_price_diff_percent'))
    
    # ==================== 步骤3：蹲守期 ====================
    
    async def _step3_waiting(self, triggered: Dict[str, float]) -> Optional[str]:
        """
        步骤3：蹲守期
        每 3 秒检测一次，连续 3 次 < 4.5% 即通过
        """
        for symbol, peak in triggered.items():
            safe_count = 0
            logger.info(f"⏳【价差开仓工人】{symbol} 进入蹲守期，需要连续 {self.config.WAITING_SAFE_COUNT} 次 < {self.config.WAITING_REBOUND_THRESHOLD}%")
            
            max_attempts = self.config.WAITING_SAFE_COUNT * 3
            
            for attempt in range(max_attempts):
                await asyncio.sleep(self.config.WAITING_CHECK_INTERVAL)
                
                spread = await self._get_spread(symbol)
                if spread is None:
                    continue
                
                if spread >= self.config.WAITING_REBOUND_THRESHOLD:
                    safe_count = 0
                    logger.debug(f"   ⚠️ {symbol} 第{attempt+1}次: {spread:.2f}% ≥ {self.config.WAITING_REBOUND_THRESHOLD}%，重置")
                else:
                    safe_count += 1
                    logger.debug(f"   ✅ {symbol} 第{attempt+1}次: {spread:.2f}%，计数={safe_count}")
                    
                    if safe_count >= self.config.WAITING_SAFE_COUNT:
                        logger.info(f"   🎉 {symbol} 通过蹲守期！")
                        return symbol
        
        return None
    
    # ==================== 步骤4：健康检查 + 精选 ====================
    
    async def _step4_health_check_and_select(self, passed: List[str]) -> Optional[str]:
        """步骤4：健康检查 + 精选"""
        market_data = await self._fetch_market_data()
        if not market_data:
            return None
        
        healthy = []
        for symbol in passed:
            data = market_data.get(symbol)
            if not data:
                continue
            
            if self._check_bilateral_health(data):
                healthy.append({
                    'symbol': symbol,
                    'data': data,
                    'peak': self._peak_spreads.get(symbol, 0),
                    'current': self._safe_float(data.get('trade_price_diff_percent', 0))
                })
            else:
                logger.warning(f"⚠️【价差开仓工人】{symbol} 健康检查不通过")
        
        if not healthy:
            return None
        
        if len(healthy) == 1:
            return healthy[0]['symbol']
        
        # 多个候选，选回归力度最大的
        best = None
        best_power = -1.0
        
        for c in healthy:
            power = self._calc_regression_power(c['peak'], c['current'])
            logger.info(f"   📊 {c['symbol']}: 峰值={c['peak']:.2f}%, 当前={c['current']:.2f}%, 回归力度={power:.2%}")
            
            if power > best_power:
                best_power = power
                best = c['symbol']
        
        logger.info(f"   🏆 选中回归力度最大的: {best} ({best_power:.2%})")
        return best
    
    def _check_bilateral_health(self, data: Dict) -> bool:
        """检查双边健康度"""
        okx_dev = self._safe_float(data.get('okx_price_to_mark_diff_percent'))
        binance_dev = self._safe_float(data.get('binance_price_to_mark_diff_percent'))
        
        if okx_dev is None or binance_dev is None:
            return False
        
        if okx_dev > self.config.HEALTH_THRESHOLD:
            logger.debug(f"   ❌ 欧易偏离 {okx_dev:.4f}% > {self.config.HEALTH_THRESHOLD}%")
            return False
        
        if binance_dev > self.config.HEALTH_THRESHOLD:
            logger.debug(f"   ❌ 币安偏离 {binance_dev:.4f}% > {self.config.HEALTH_THRESHOLD}%")
            return False
        
        return True
    
    def _calc_regression_power(self, peak: float, current: float) -> float:
        """计算回归力度"""
        if peak <= 0:
            return 0.0
        return (peak - current) / peak
    
    # ==================== 步骤5：执行开仓 ====================
    
    async def _step5_execute(self, symbol: str) -> bool:
        """步骤5：生成开仓指令并发送给大脑，同时发送策略标签给标签调度器"""
        try:
            market_data = await self._fetch_market_data()
            if not market_data:
                return False
            
            data = market_data.get(symbol)
            if not data:
                logger.error(f"❌【价差开仓工人】无法获取 {symbol} 的数据")
                return False
            
            okx_price = self._safe_float(data.get('okx_trade_price'))
            binance_price = self._safe_float(data.get('binance_trade_price'))
            
            if okx_price is None or binance_price is None:
                logger.error(f"❌【价差开仓工人】{symbol} 价格数据缺失")
                return False
            
            # 决定方向：空高价所，多低价所
            if okx_price > binance_price:
                direction = "short_okx_long_binance"
                logger.info(f"📈【价差开仓工人】方向: 欧易做空({okx_price})，币安做多({binance_price})")
            else:
                direction = "long_okx_short_binance"
                logger.info(f"📉【价差开仓工人】方向: 欧易做多({okx_price})，币安做空({binance_price})")
            
            # 构建开仓指令
            instruction = {
                "command": "place_order",
                "params": {
                    "strategy": "spread_arbitrage",
                    "position_mode": "cross",
                    "order_type": "market",
                    "symbol": symbol,
                    "margin": self._cached_margin,
                    "leverage": self.config.LEVERAGE,
                    "direction": direction
                }
            }
            
            # 发送开仓指令给大脑
            await self.brain.handle_frontend_command(instruction)
            logger.info(f"📤【价差开仓工人】开仓指令已发送: {symbol}, 方向={direction}, 保证金={self._cached_margin:.2f}")
            
            # 🆕 发送策略标签给标签调度器
            if hasattr(self.brain, 'tag_dispatcher') and self.brain.tag_dispatcher:
                await self.brain.tag_dispatcher.receive({"info": "当前策略:价差套利"})
                logger.info("🏷️【价差开仓工人】策略标签已发送: 当前策略:价差套利")
            
            return True
            
        except Exception as e:
            logger.error(f"❌【价差开仓工人】执行开仓异常: {e}")
            return False
    
    # ==================== 工具方法 ====================
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """安全转换为 float"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None