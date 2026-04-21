# trading/full_auto/funding/open.py
"""
资金费开仓工人 - 负责检测开仓条件，生成开仓指令

工作流程：
1. 收到标签 {"info": "开启全自动"} → 激活工作状态
2. 每小时第57分钟执行一次检测流程
3. 读取行情数据和私人数据（最多重试2次，共3次机会）
4. 检测开仓条件，选出交易标的
5. 生成开仓指令，发给大脑
6. 收到标签 {"info": "结束全自动"} → 立刻重置所有状态，取消正在执行的任务

重试规则：
- 任何步骤失败（除资产检查外），10秒后从第一步（读取数据）重新开始
- 最多重试2次（即总共执行3次完整流程）
- 资产检查失败直接结束，不重试
- 发给大脑不参与重试

合约筛选条件（必须同时满足）：
- 费率差 >= 0.8
- 价差 <= 3%
- 欧易结算倒计时 < 200秒
- 币安结算倒计时 < 200秒
- 方向一致性：费率低的交易所，价格也必须低

精选规则：
- 1个合约：直接选中
- 多个合约：选费率差最大的
"""

import asyncio
import logging
import copy
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, Tuple, List

logger = logging.getLogger(__name__)


class FundingOpen:
    def __init__(self, brain):
        self.brain = brain
        self.data_manager = brain.data_manager
        
        # 工作状态
        self.is_active = False          # 是否激活（收到开启全自动标签）
        self.scout_task = None          # 定时任务
        self.current_task = None        # 当前执行的侦察任务
        
        # 开仓指令模板
        self.order_template = {
            "command": "place_order",
            "params": {
                "position_mode": "cross",
                "order_type": "market",
                "symbol": None,
                "margin": None,
                "leverage": 20,
                "direction": None
            }
        }
        
        # 缓存
        self.cached_template = None
        
        logger.info("🔍【资金费开仓工人】初始化完成")
    
    # ==================== 标签控制 ====================
    
    def on_data(self, data: Dict[str, Any]):
        """接收大脑推送的数据（信息标签）"""
        if "info" not in data:
            return
        
        info = data["info"]
        
        if info == "开启全自动":
            logger.info("🏷️【资金费开仓工人】收到标签：开启全自动")
            self._activate()
        elif info == "结束全自动":
            logger.info("🏷️【资金费开仓工人】收到标签：结束全自动")
            self._deactivate()
    
    def _activate(self):
        """激活工作状态"""
        if self.is_active:
            return
        self.is_active = True
        self._start_scout_task()
        logger.info("✅【资金费开仓工人】已激活，等待每小时第57分钟执行")
    
    def _deactivate(self):
        """停用，立刻重置所有状态，取消正在执行的任务"""
        self.is_active = False
        
        # 取消定时任务
        self._stop_scout_task()
        
        # 取消正在执行的侦察任务
        if self.current_task and not self.current_task.done():
            self.current_task.cancel()
            logger.info("🛑【资金费开仓工人】已取消正在执行的侦察任务")
        
        self._cleanup()
        logger.info("🛑【资金费开仓工人】已停用，状态已重置")
    
    # ==================== 定时任务 ====================
    
    def _start_scout_task(self):
        """启动定时侦察任务"""
        if self.scout_task and not self.scout_task.done():
            self.scout_task.cancel()
        self.scout_task = asyncio.create_task(self._scout_loop())
    
    def _stop_scout_task(self):
        """停止定时侦察任务"""
        if self.scout_task and not self.scout_task.done():
            self.scout_task.cancel()
            self.scout_task = None
    
    async def _scout_loop(self):
        """定时侦察循环 - 每小时第57分钟执行"""
        while self.is_active:
            try:
                now = datetime.now()
                next_run = self._calculate_next_run(now)
                wait_seconds = (next_run - now).total_seconds()
                
                logger.info(f"⏰【资金费开仓工人】下次执行时间: {next_run.strftime('%Y-%m-%d %H:%M:%S')}，等待 {wait_seconds:.0f} 秒")
                
                await asyncio.sleep(wait_seconds)
                
                if not self.is_active:
                    break
                
                # 执行侦察（带重试）
                self.current_task = asyncio.create_task(self._execute_with_retry())
                try:
                    await self.current_task
                except asyncio.CancelledError:
                    logger.info("🛑【资金费开仓工人】侦察任务被取消")
                finally:
                    self.current_task = None
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌【资金费开仓工人】侦察循环异常: {e}")
                await asyncio.sleep(60)
    
    def _calculate_next_run(self, now: datetime) -> datetime:
        """计算下一个第57分钟的时间，正确处理跨小时和跨天"""
        current_hour_57 = now.replace(minute=57, second=0, microsecond=0)
        
        if now < current_hour_57:
            return current_hour_57
        else:
            next_hour = now + timedelta(hours=1)
            return next_hour.replace(minute=57, second=0, microsecond=0)
    
    # ==================== 重试控制 ====================
    
    async def _execute_with_retry(self):
        """
        执行侦察流程，支持重试
        
        重试规则：
        - 最多执行3次完整流程
        - 每次失败后等待10秒
        - 从第一步（读取数据）重新开始
        - 资产检查失败不重试，直接结束
        """
        max_attempts = 3
        retry_delay = 10
        
        for attempt in range(max_attempts):
            if not self.is_active:
                logger.info("🛑【资金费开仓工人】收到结束标签，停止侦察")
                return
            
            if attempt > 0:
                logger.info(f"🔄【资金费开仓工人】第 {attempt + 1} 次尝试，{retry_delay} 秒后重新开始...")
                await asyncio.sleep(retry_delay)
            
            logger.info("=" * 50)
            logger.info(f"🔍【资金费开仓工人】开始侦察任务 (第 {attempt + 1}/{max_attempts} 次)")
            
            success, should_retry = await self._execute_scout_once()
            
            if success:
                logger.info(f"✅【资金费开仓工人】侦察任务成功完成")
                self._cleanup()
                return
            
            if not should_retry:
                logger.warning(f"📭【资金费开仓工人】侦察任务终止，不重试")
                self._cleanup()
                return
            
            logger.warning(f"⚠️【资金费开仓工人】第 {attempt + 1} 次侦察失败，将进行重试")
        
        logger.warning(f"📭【资金费开仓工人】已达最大重试次数 ({max_attempts})，结束本次侦察")
        self._cleanup()
    
    async def _execute_scout_once(self) -> Tuple[bool, bool]:
        """
        执行一次完整的侦察流程（不包含重试）
        
        返回: (success, should_retry)
              success: True 表示成功
              should_retry: True 表示失败后需要重试
        """
        try:
            # 拷贝模板
            self.cached_template = copy.deepcopy(self.order_template)
            
            # 步骤1：读取数据
            market_data, user_data = await self._fetch_both_data()
            if market_data is None or user_data is None:
                logger.warning("📭【资金费开仓工人】数据读取失败")
                return False, True
            
            # 步骤2：检查持仓
            if not self._check_position(user_data):
                logger.warning("📭【资金费开仓工人】已有持仓，禁止开新仓")
                return False, True
            
            # 步骤3：检查资产（不重试，直接结束）
            if not self._check_asset(user_data):
                logger.warning("📭【资金费开仓工人】资产检查不通过")
                return False, False
            
            # 步骤4：筛选交易标的
            selected_symbol = self._select_best_symbol(market_data)
            if selected_symbol is None:
                logger.warning("📭【资金费开仓工人】无合适交易标的")
                return False, True
            
            # 步骤5：获取选中合约的数据
            symbol_data = market_data.get(selected_symbol)
            if symbol_data is None:
                logger.error(f"❌【资金费开仓工人】选中合约 {selected_symbol} 数据为空")
                return False, True
            
            # 步骤6：决定方向
            direction = self._determine_direction(symbol_data)
            
            # 步骤7：计算保证金
            margin = self._calculate_margin(user_data)
            
            # 步骤8：填充模板
            self.cached_template["params"]["symbol"] = selected_symbol
            self.cached_template["params"]["margin"] = margin
            self.cached_template["params"]["direction"] = direction
            
            logger.info(f"📋【资金费开仓工人】开仓指令已生成: symbol={selected_symbol}, margin={margin:.2f}, direction={direction}")
            
        except asyncio.CancelledError:
            logger.info("🛑【资金费开仓工人】侦察任务被取消")
            raise
        except Exception as e:
            logger.error(f"❌【资金费开仓工人】侦察流程异常: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False, True
        
        # 步骤9：发给大脑（不参与重试）
        await self._send_to_brain()
        
        return True, False
    
    # ==================== 数据读取 ====================
    
    async def _fetch_both_data(self) -> Tuple[Optional[Dict], Optional[Dict]]:
        """并行读取行情数据和私人数据"""
        try:
            market_task = asyncio.create_task(self.data_manager.get_public_market_data())
            user_task = asyncio.create_task(self.data_manager.get_private_user_data())
            
            market_result, user_result = await asyncio.gather(market_task, user_task)
            
            market_data = market_result.get('data', {}) if market_result else {}
            user_data = user_result.get('data', {}) if user_result else {}
            
            market_ok = market_data and len(market_data) > 0
            user_ok = user_data and 'okx' in user_data and 'binance' in user_data
            
            if market_ok and user_ok:
                logger.info(f"✅【资金费开仓工人】数据读取成功: 行情{len(market_data)}条, 私人数据正常")
                return market_data, user_data
            
            if not market_ok:
                logger.warning(f"⚠️【资金费开仓工人】行情数据读取不到或为空")
            if not user_ok:
                logger.warning(f"⚠️【资金费开仓工人】私人数据读取不到或不完整")
            
            return None, None
            
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"❌【资金费开仓工人】读取数据异常: {e}")
            return None, None
    
    # ==================== 条件检测 ====================
    
    def _check_position(self, user_data: Dict) -> bool:
        """检查是否有持仓"""
        okx_data = user_data.get('okx', {})
        binance_data = user_data.get('binance', {})
        
        okx_symbol = okx_data.get('开仓合约名', '')
        binance_symbol = binance_data.get('开仓合约名', '')
        
        if okx_symbol:
            logger.warning(f"⚠️【资金费开仓工人】欧易已有持仓: {okx_symbol}")
            return False
        if binance_symbol:
            logger.warning(f"⚠️【资金费开仓工人】币安已有持仓: {binance_symbol}")
            return False
        
        logger.info("✅【资金费开仓工人】持仓检查通过，当前空仓")
        return True
    
    def _check_asset(self, user_data: Dict) -> bool:
        """检查资产是否满足条件（欧易和币安资产都必须 >= 50）"""
        try:
            okx_data = user_data.get('okx', {})
            binance_data = user_data.get('binance', {})
            
            okx_asset = float(okx_data.get('账户资产额', 0))
            binance_asset = float(binance_data.get('账户资产额', 0))
            
            if okx_asset < 50:
                logger.warning(f"⚠️【资金费开仓工人】欧易资产不足50: {okx_asset:.2f}")
                return False
            if binance_asset < 50:
                logger.warning(f"⚠️【资金费开仓工人】币安资产不足50: {binance_asset:.2f}")
                return False
            
            logger.info(f"✅【资金费开仓工人】资产检查通过: 欧易={okx_asset:.2f}, 币安={binance_asset:.2f}")
            return True
            
        except Exception as e:
            logger.error(f"❌【资金费开仓工人】检查资产异常: {e}")
            return False
    
    # ==================== 费率与价格比较（复用逻辑）====================
    
    def _get_rate_and_price_comparison(self, symbol_data: Dict) -> Tuple[bool, bool]:
        """
        获取费率比较和价格比较的结果
        
        返回:
            okx_rate_lower: 欧易费率是否低于币安
            okx_price_lower: 欧易价格是否低于币安
        """
        try:
            okx_rate = float(symbol_data.get('okx_funding_rate') or 0)
            binance_rate = float(symbol_data.get('binance_funding_rate') or 0)
            okx_price = float(symbol_data.get('okx_trade_price') or 0)
            binance_price = float(symbol_data.get('binance_trade_price') or 0)
            
            return okx_rate < binance_rate, okx_price < binance_price
        except Exception as e:
            logger.error(f"⚠️【资金费开仓工人】获取费率与价格比较异常: {e}")
            return False, False
    
    def _check_direction_consistency(self, symbol_data: Dict) -> bool:
        """
        检查方向一致性：费率低的交易所，价格是否也低
        
        返回 True 表示方向一致（情况一），False 表示方向冲突（情况二）
        """
        try:
            okx_rate_lower, okx_price_lower = self._get_rate_and_price_comparison(symbol_data)
            
            # 检查价格是否有效
            okx_price = float(symbol_data.get('okx_trade_price') or 0)
            binance_price = float(symbol_data.get('binance_trade_price') or 0)
            
            if okx_price == 0 or binance_price == 0:
                logger.warning(f"⚠️【资金费开仓工人】价格数据缺失，无法判断方向一致性")
                return False
            
            # 方向一致 = 费率低的那边价格也低
            is_consistent = (okx_rate_lower == okx_price_lower)
            
            if is_consistent:
                logger.info(f"✅ 【资金费开仓工人】方向一致: 欧易费率{'低' if okx_rate_lower else '高'}, 欧易价格{'低' if okx_price_lower else '高'}")
            else:
                logger.info(f"❌ 【资金费开仓工人】方向冲突: 欧易费率{'低' if okx_rate_lower else '高'}, 欧易价格{'低' if okx_price_lower else '高'}")
            
            return is_consistent
            
        except Exception as e:
            logger.error(f"⚠️【资金费开仓工人】检查方向一致性异常: {e}")
            return False
    
    def _select_best_symbol(self, market_data: Dict) -> Optional[str]:
        """
        筛选最佳交易标的
        
        条件（必须同时满足）：
        1. 费率差 >= 0.8
        2. 价差 <= 3%
        3. 欧易结算倒计时 < 200秒
        4. 币安结算倒计时 < 200秒
        5. 方向一致性：费率低的交易所，价格也必须低
        
        精选规则：
        - 1个合约：直接选中
        - 多个合约：选费率差最大的
        """
        candidates: List[Dict] = []
        
        for symbol, data in market_data.items():
            if not isinstance(data, dict):
                continue
            
            try:
                rate_diff = float(data.get('rate_diff') or 0)
                price_diff_percent = float(data.get('trade_price_diff_percent') or 0)
                okx_countdown = int(data.get('okx_countdown_seconds') or 0)
                binance_countdown = int(data.get('binance_countdown_seconds') or 0)
                # 这里的0.3只是测试用，实战时改回0.8
                
                # 条件1：费率差检查
                if rate_diff < 0.3:
                    logger.debug(f"⏭️【资金费开仓工人】{symbol} 费率差不足: {rate_diff} < 0.3")
                    continue
                
                # 条件2：价差检查（新增）
                if price_diff_percent > 3:
                    logger.debug(f"⏭️【资金费开仓工人】{symbol} 价差过大: {price_diff_percent}% > 3%")
                    continue
                
                # 条件3：欧易倒计时检查
                if okx_countdown >= 200:
                    logger.debug(f"⏭️【资金费开仓工人】{symbol} 欧易倒计时过长: {okx_countdown} >= 200")
                    continue
                
                # 条件4：币安倒计时检查
                if binance_countdown >= 200:
                    logger.debug(f"⏭️【资金费开仓工人】{symbol} 币安倒计时过长: {binance_countdown} >= 200")
                    continue
                
                # 条件5：方向一致性检查
                if not self._check_direction_consistency(data):
                    logger.debug(f"⏭️【资金费开仓工人】{symbol} 方向不一致，跳过")
                    continue
                
                # 满足所有条件的合约
                candidates.append({
                    'symbol': symbol,
                    'rate_diff': rate_diff,
                    'okx_countdown': okx_countdown,
                    'binance_countdown': binance_countdown
                })
                
                logger.info(f"✅【资金费开仓工人】符合条件的合约: {symbol}, rate_diff={rate_diff}, price_diff={price_diff_percent}%, 欧易倒计时={okx_countdown}秒, 币安倒计时={binance_countdown}秒")
                
            except Exception as e:
                logger.debug(f"⚠️【资金费开仓工人】解析合约 {symbol} 数据异常: {e}")
                continue
        
        if not candidates:
            logger.warning("⚠️【资金费开仓工人】未找到同时满足五个条件的合约")
            return None
        
        # 精选规则
        if len(candidates) == 1:
            best = candidates[0]
            logger.info(f"🎯【资金费开仓工人】仅1个符合条件的合约，直接选中: {best['symbol']}")
        else:
            # 选择费率差最大的
            best = max(candidates, key=lambda x: x['rate_diff'])
            logger.info(f"🎯【资金费开仓工人】共{len(candidates)}个合约符合条件，选费率差最大的: {best['symbol']} (rate_diff={best['rate_diff']})")
        
        return best['symbol']
    
    # ==================== 参数计算 ====================
    
    def _determine_direction(self, symbol_data: Dict) -> str:
        """根据资金费率决定方向"""
        try:
            okx_rate_lower, _ = self._get_rate_and_price_comparison(symbol_data)
            
            okx_rate = float(symbol_data.get('okx_funding_rate') or 0)
            binance_rate = float(symbol_data.get('binance_funding_rate') or 0)
            
            logger.info(f"📊【资金费开仓工人】资金费率: 欧易={okx_rate}, 币安={binance_rate}")
            
            if okx_rate_lower:
                logger.info(f"📈【资金费开仓工人】方向: 欧易做多，币安做空")
                return "long_okx_short_binance"
            else:
                logger.info(f"📉【资金费开仓工人】方向: 币安做多，欧易做空")
                return "long_binance_short_okx"
                
        except Exception as e:
            logger.error(f"❌【资金费开仓工人】决定方向异常: {e}，默认使用 long_okx_short_binance")
            return "long_okx_short_binance"
    
    def _calculate_margin(self, user_data: Dict) -> float:
        """计算保证金 = 较小账户资产 × 10%"""
        try:
            okx_data = user_data.get('okx', {})
            binance_data = user_data.get('binance', {})
            
            okx_asset = float(okx_data.get('账户资产额', 0))
            binance_asset = float(binance_data.get('账户资产额', 0))
            
            smaller = min(okx_asset, binance_asset)
            margin = smaller * 0.1
            
            logger.info(f"💰【资金费开仓工人】保证金计算: 较小资产={smaller:.2f}, margin={margin:.2f}")
            return margin
            
        except Exception as e:
            logger.error(f"❌【资金费开仓工人】计算保证金异常: {e}")
            return 10.0
    
    # ==================== 发送指令 ====================
    
    async def _send_to_brain(self):
        """把开仓指令发给大脑"""
        if self.cached_template is None:
            logger.error("❌【资金费开仓工人】没有缓存的指令")
            return
        
        instruction = copy.deepcopy(self.cached_template)
        
        try:
            await self.brain.handle_frontend_command(instruction)
            logger.info(f"📤【资金费开仓工人】开仓指令已发送给大脑")
        except Exception as e:
            logger.error(f"❌【资金费开仓工人】发送指令给大脑失败: {e}")
    
    # ==================== 清理 ====================
    
    def _cleanup(self):
        """清理缓存"""
        self.cached_template = None
        logger.debug("🧹【资金费开仓工人】缓存已清空")