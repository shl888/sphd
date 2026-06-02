"""
私人HTTP数据获取器 - 冷酷重启版
支持两种恢复机制：
1. 平仓重置（_rebuild_session）- 预防性清理，防止急刹车
2. 服务重启（_handle_restart）- 卡死/异常时完全重启
"""

import asyncio
import logging
import time
import hmac
import hashlib
import urllib.parse
from datetime import datetime
from typing import Dict, Any, Optional
import aiohttp

logger = logging.getLogger(__name__)


class PrivateHTTPFetcher:
    """
    私人HTTP数据获取器
    冷酷版：force_close=True + 真正重启 + 超时保护
    
    两种恢复机制：
    ┌─────────────────────────────────────────────────────────────┐
    │ 1. 平仓重置（_rebuild_session）                              │
    │    - 触发：检测到平仓（持仓从有变无）                         │
    │    - 作用：重建 HTTP 连接，清理僵尸，防止急刹车               │
    │    - 恢复时间：毫秒级                                        │
    │    - 调度器：继续运行，不重启                                │
    ├─────────────────────────────────────────────────────────────┤
    │ 2. 服务重启（_handle_restart）                               │
    │    - 触发：账户获取失败/418/401/调度器异常/任务异常           │
    │    - 作用：取消所有任务 → 重建 session → 重新创建调度器       │
    │    - 恢复时间：2分钟 + 账户获取时间                          │
    │    - 调度器：完全重启，从头开始                              │
    └─────────────────────────────────────────────────────────────┘
    """

    def __init__(self):
        # ==================== 基础组件 ====================
        self.brain_store = None  # DataManager实例
        self.running = False

        # ========== 密钥就绪标志 ==========
        self._keys_ready = False
        self._pending_work = False

        # API凭证（启动时获取一次）
        self.api_key = None
        self.api_secret = None
        self.listen_key = None

        # 任务管理
        self.scheduler_task = None      # 主调度器任务
        self.fetch_tasks = []           # 子任务列表

        # HTTP 会话（冷酷版：使用 connector）
        self.session = None
        self.connector = None

        # ==================== 状态标志 ====================
        self.account_fetched = False
        self.account_fetch_success = False

        # ==================== 重试策略 ====================
        self.account_retry_delays = [10, 20, 40, 60]  # 指数退避延迟
        self.max_account_retries = 4                   # 最多重试4次

        # ==================== 自适应频率控制 ====================
        self.account_check_interval = 1   # 当前检查间隔（秒）
        self.account_high_freq = 1        # 有持仓：1秒高频
        self.account_low_freq = 60        # 无持仓：60秒低频
        self.has_position = False         # 当前是否有持仓
        self.last_log_time = 0            # 上次日志时间
        self.log_interval = 60            # 日志间隔（秒）

        # ==================== 重启机制 ====================
        self.restart_attempts = 0         # 重启尝试次数
        self.in_restart_cooldown = False  # 是否在重启冷却中

        # ==================== 质量统计 ====================
        self.quality_stats = {
            'account_fetch': {
                'total_attempts': 0,
                'success_attempts': 0,
                'last_success': None,
                'last_error': None,
                'success_rate': 100.0,
                'retry_count': 0,
                'restart_count': 0,
                'last_restart': None
            }
        }

        # ==================== API 配置 ====================
        # 模拟交易端点（Testnet，模拟环境启用）
#        self.BASE_URL = "https://testnet.binancefuture.com"

        # 真实交易端点（正式环境启用）
        self.BASE_URL = "https://fapi.binance.com"

        self.ACCOUNT_ENDPOINT = "/fapi/v3/account"
        self.RECV_WINDOW = 5000  # 5秒接收窗口

        self.environment = "testnet" if "testnet" in self.BASE_URL else "live"
        logger.info(f"🔗 [HTTP获取器] 冷酷重启版初始化完成（环境: {self.environment}）")

    # ==================== 标签接收 ====================

    def on_keys_ready(self):
        """
        接收「密钥已就绪」标签
        由 TagDispatcher 调用
        """
        self._keys_ready = True
        logger.info("🔑【HTTP获取器】密钥已就绪，获得工作权限")
        
        if self._pending_work:
            logger.info("🚀【HTTP获取器】开始执行待处理的工作")
            asyncio.create_task(self._start_work())
            self._pending_work = False

    # ==================== 启动方法 ====================

    async def start(self, brain_store):
        """
        启动获取器 - 冷酷版
        
        Args:
            brain_store: DataManager实例
        """
        self.brain_store = brain_store
        
        if self._keys_ready:
            # 密钥已就绪，直接开始工作
            await self._start_work()
        else:
            # 密钥未就绪，标记待执行，等待标签
            logger.info("⏳【HTTP获取器】密钥未就绪，等待标签...")
            self._pending_work = True
        
        return True

    async def _start_work(self):
        """实际执行启动逻辑"""
        logger.info(f"🚀 [HTTP获取器] 冷酷重启版启动（环境: {self.environment}）")
        
        self.running = True
        
        # 🔴 冷酷核心：force_close=True，用完就关，不留回味
        timeout = aiohttp.ClientTimeout(total=30)
        self.connector = aiohttp.TCPConnector(
            force_close=True,
            enable_cleanup_closed=True,
            limit=20,
            limit_per_host=10
        )
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=self.connector
        )
        
        # 创建主调度器任务
        self.scheduler_task = asyncio.create_task(self._controlled_scheduler())
        logger.info("✅ [HTTP获取器] 调度器已启动，force_close=True 生效")

    # ==================== 机制1：平仓重置（预防性清理）====================

    async def _rebuild_session(self, reason: str = ""):
        """
        【机制1：平仓重置】
        重建 HTTP session - 平仓急刹车时主动调用
        
        触发时机：检测到平仓（持仓从有变无）
        作用：清理僵尸连接，防止 session.close() 卡死
        特点：只重建连接，不重启调度器，毫秒级恢复
        
        Args:
            reason: 重建原因（用于日志）
        """
        try:
            logger.info(f"🔄 [HTTP获取器] 重建session (原因: {reason})")
            
            # 1. 关闭旧 session（加超时保护）
            if self.session and not self.session.closed:
                try:
                    await asyncio.wait_for(self.session.close(), timeout=3.0)
                    logger.debug("✅ 旧session关闭成功")
                except asyncio.TimeoutError:
                    # 超时也不怕，强制跳过，防止死锁
                    logger.warning("⚠️ 旧session关闭超时，强制跳过")
                except Exception as e:
                    logger.warning(f"⚠️ 关闭旧session异常: {e}")
            
            # 2. 重新创建 session（保持 force_close=True）
            timeout = aiohttp.ClientTimeout(total=30)
            self.connector = aiohttp.TCPConnector(
                force_close=True,
                enable_cleanup_closed=True,
                limit=20,
                limit_per_host=10
            )
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                connector=self.connector
            )
            logger.info(f"✅ [HTTP获取器] session重建完成 {reason}")
            
        except Exception as e:
            logger.error(f"❌ [HTTP获取器] session重建失败: {e}")
            # 兜底：确保 session 不为 None
            if not self.session or self.session.closed:
                timeout = aiohttp.ClientTimeout(total=30)
                self.connector = aiohttp.TCPConnector(
                    force_close=True,
                    enable_cleanup_closed=True,
                    limit=20
                )
                self.session = aiohttp.ClientSession(
                    timeout=timeout,
                    connector=self.connector
                )

    # ==================== 机制2：服务重启（完全恢复）====================

    async def _handle_restart(self, reason: str):
        """
        【机制2：服务重启】
        处理重启逻辑 - 所有严重错误都立即重启，并自动恢复运行
        
        触发时机：
        - 账户获取失败（5次重试都失败）
        - 调度器异常
        - 418/401 严重错误
        - 账户任务异常退出
        
        作用：
        1. 取消所有任务（fetch_tasks + scheduler_task）
        2. 关闭并重建 session
        3. 重置状态标志
        4. 🔴 重新创建调度器任务（从头开始执行完整流程）
        
        特点：完全重启，恢复时间约 2分钟 + 账户获取时间
        """
        if self.in_restart_cooldown:
            return
            
        self.in_restart_cooldown = True
        self.restart_attempts += 1
        
        logger.warning(f"🔄 [HTTP获取器] 服务重启（原因: {reason} | 第{self.restart_attempts}次重启）")
        
        # ----- 步骤1：取消所有 fetch 任务（加超时保护）-----
        for task in self.fetch_tasks:
            if not task.done():
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=3.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
        self.fetch_tasks.clear()
        
        # ----- 步骤2：取消调度器任务（加超时保护）-----
        if self.scheduler_task and not self.scheduler_task.done():
            old_task = self.scheduler_task
            self.scheduler_task = None
            old_task.cancel()
            try:
                await asyncio.wait_for(old_task, timeout=3.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        
        # ----- 步骤3：关闭旧 session（加超时保护）-----
        if self.session and not self.session.closed:
            try:
                await asyncio.wait_for(self.session.close(), timeout=3.0)
                logger.info("✅ 旧session关闭成功")
            except asyncio.TimeoutError:
                logger.error("❌ session.close()超时！强制跳过（防止卡死）")
            except Exception as e:
                logger.warning(f"⚠️ 关闭session异常: {e}")
        
        if not self.running:
            return
        
        # ----- 步骤4：重新创建 session（保持 force_close=True）-----
        timeout = aiohttp.ClientTimeout(total=30)
        self.connector = aiohttp.TCPConnector(
            force_close=True,
            enable_cleanup_closed=True,
            limit=20,
            limit_per_host=10
        )
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=self.connector
        )
        
        # ----- 步骤5：重置状态标志 -----
        self.account_fetched = False
        self.account_fetch_success = False
        self.has_position = False
        self.account_check_interval = 1
        
        # ----- 步骤6：退出冷却标志 -----
        self.in_restart_cooldown = False
        
        # ----- 步骤7：🔴 关键！重新创建调度器任务（从头开始）-----
        self.scheduler_task = asyncio.create_task(self._controlled_scheduler())
        
        # 更新统计
        self.quality_stats['account_fetch']['restart_count'] = self.restart_attempts
        self.quality_stats['account_fetch']['last_restart'] = datetime.now().isoformat()
        
        logger.info(f"✅ [HTTP获取器] 服务重启完成，调度器已重新启动（将从头开始执行完整流程）")

    # ==================== 主调度器 ====================

    async def _controlled_scheduler(self):
        """
        受控调度器 - 支持循环自愈
        
        执行流程：
        1. 等待2分钟（让其他模块先运行）
        2. 尝试获取账户资产（5次指数退避重试）
        3. 成功后启动自适应频率任务
        4. 监控任务状态，异常时触发重启
        """
        while self.running:
            try:
                self.in_restart_cooldown = False
                
                # ========== 第一阶段：等待2分钟 ==========
                logger.info("⏳ [HTTP获取器] 第一阶段：等待2分钟，让其他模块先运行...")
                for i in range(120):
                    await asyncio.sleep(0)
                    if not self.running or self.in_restart_cooldown:
                        return
                    if i % 60 == 0:
                        remaining = 120 - i
                        logger.info(f"⏳ [HTTP获取器] 等待中...剩余{remaining}秒")
                    await asyncio.sleep(1)

                logger.info("✅ [HTTP获取器] 2分钟等待完成，开始账户获取（5次尝试）")

                # ========== 第二阶段：获取账户资产 ==========
                self.account_fetch_success = await self._fetch_account_with_retry()

                if self.account_fetch_success:
                    logger.info("✅ [HTTP获取器] 账户获取成功，启动自适应频率任务")

                    # 冷却30秒
                    for i in range(30):
                        await asyncio.sleep(0)
                        if not self.running or self.in_restart_cooldown:
                            break
                        await asyncio.sleep(1)

                    if self.running and not self.in_restart_cooldown:
                        # 启动自适应频率任务
                        account_task = asyncio.create_task(
                            self._fetch_account_adaptive_freq())
                        self.fetch_tasks.append(account_task)
                        logger.info("✅ [HTTP获取器] 自适应频率任务已启动")
                        
                        # 监控模式：不阻塞，每秒检查任务状态
                        while self.running and not self.in_restart_cooldown:
                            if account_task.done():
                                try:
                                    account_task.result()
                                except asyncio.CancelledError:
                                    logger.info("📢 [HTTP获取器] 账户任务被取消")
                                    break
                                except Exception as e:
                                    logger.error(f"❌ [HTTP获取器] 账户任务异常: {e}")
                                    await self._handle_restart(f"账户任务异常: {e}")
                                    break
                            await asyncio.sleep(1)
                else:
                    logger.warning("⚠️ [HTTP获取器] 账户获取失败，触发重启")
                    if self.running:
                        await self._handle_restart("账户获取失败")

            except asyncio.CancelledError:
                logger.info("🛑 [HTTP获取器] 调度器被取消")
                break
            except Exception as e:
                logger.error(f"❌ [HTTP获取器] 调度器异常: {e}")
                if self.running and not self.in_restart_cooldown:
                    await self._handle_restart(f"调度器异常: {e}")

    # ==================== 账户获取（带重试）====================

    async def _fetch_account_with_retry(self):
        """
        获取账户资产 - 5次指数退避重试
        第1次尝试 + 4次重试（10秒, 20秒, 40秒, 60秒后）
        """
        retry_count = 0
        total_attempts = 0

        # 第1次尝试
        logger.info(f"💰 [HTTP获取器] 账户获取第1次尝试...")
        result = await self._fetch_account_single()
        total_attempts += 1

        if result == 'NEED_RESTART':
            logger.warning("⚠️ [HTTP获取器] 遇到需要重启的错误")
            self.quality_stats['account_fetch']['retry_count'] = 0
            return False

        if result == True:
            self.quality_stats['account_fetch']['retry_count'] = 0
            return True

        # 4次重试
        while retry_count < self.max_account_retries and self.running and not self.in_restart_cooldown:
            await asyncio.sleep(0)
            delay = self.account_retry_delays[retry_count]
            logger.info(f"⏳ [HTTP获取器] {delay}秒后重试（第{retry_count + 2}次）")
            await asyncio.sleep(delay)

            logger.info(f"💰 [HTTP获取器] 账户获取第{retry_count + 2}次尝试...")
            result = await self._fetch_account_single()
            total_attempts += 1
            retry_count += 1

            if result == 'NEED_RESTART':
                logger.warning(f"⚠️ [HTTP获取器] 第{retry_count}次尝试需要重启")
                self.quality_stats['account_fetch']['retry_count'] = retry_count
                return False

            if result == True:
                self.quality_stats['account_fetch']['retry_count'] = retry_count
                return True

        logger.error(f"❌ [HTTP获取器] 账户获取{total_attempts}次尝试全部失败")
        return False

    async def _fetch_account_single(self):
        """
        单次获取账户资产
        
        Returns:
            True: 成功
            False: 失败，可重试
            'NEED_RESTART': 遇到需要重启的错误（418/401），触发服务重启
        """
        try:
            self.quality_stats['account_fetch']['total_attempts'] += 1

            api_key, api_secret = await self._get_fresh_credentials()
            if not api_key or not api_secret:
                logger.warning("⚠️ [HTTP获取器] 凭证读取失败")
                self.quality_stats['account_fetch']['last_error'] = "凭证读取失败"
                return False

            params = {
                'timestamp': int(time.time() * 1000),
                'recvWindow': self.RECV_WINDOW
            }
            signed_params = self._sign_params(params, api_secret)
            url = f"{self.BASE_URL}{self.ACCOUNT_ENDPOINT}"
            headers = {'X-MBX-APIKEY': api_key}

            async with self.session.get(url, params=signed_params, headers=headers) as resp:

                if resp.status == 200:
                    data = await resp.json()
                    await self._push_data('http_account', data)

                    self.quality_stats['account_fetch']['success_attempts'] += 1
                    self.quality_stats['account_fetch']['last_success'] = datetime.now().isoformat()
                    self.quality_stats['account_fetch']['last_error'] = None
                    self.quality_stats['account_fetch']['success_rate'] = (
                        self.quality_stats['account_fetch']['success_attempts'] /
                        self.quality_stats['account_fetch']['total_attempts'] * 100
                    )

                    logger.info("✅ [HTTP获取器] 账户资产获取成功")
                    self.account_fetched = True
                    return True

                else:
                    error_text = await resp.text()
                    error_msg = f"HTTP {resp.status}: {error_text[:100]}"
                    self.quality_stats['account_fetch']['last_error'] = error_msg

                    # 418/401 触发服务重启
                    if resp.status in [418, 401]:
                        logger.warning(f"⚠️ [HTTP获取器] 严重错误({resp.status})，触发重启")
                        return 'NEED_RESTART'

                    # 429 频率限制 - 等待后重试
                    if resp.status == 429:
                        wait_time = await self._get_retry_after_time(resp)
                        logger.warning(f"⚠️ [HTTP获取器] 频率限制(429)，等待{wait_time}秒")
                        await asyncio.sleep(wait_time)
                        return False

                    logger.error(f"❌ [HTTP获取器] 账户请求失败 {error_msg}")
                    return False

        except asyncio.TimeoutError:
            logger.error(f"⏱️ [HTTP获取器] 账户请求超时")
            self.quality_stats['account_fetch']['last_error'] = "请求超时"
            return False
        except Exception as e:
            logger.error(f"❌ [HTTP获取器] 获取账户异常: {e}")
            self.quality_stats['account_fetch']['last_error'] = str(e)
            return False

    # ==================== 自适应频率任务（核心监控）====================

    async def _fetch_account_adaptive_freq(self):
        """
        自适应频率获取账户数据
        
        核心逻辑：
        - 有持仓 → 1秒高频（快速响应）
        - 无持仓 → 60秒低频（节省资源）
        - 🔴 检测到平仓时主动调用 _rebuild_session（机制1：平仓重置）
        """
        request_count = 0
        last_position_state = False  # 记录上次持仓状态，用于检测变化

        await asyncio.sleep(30)

        while self.running and not self.in_restart_cooldown:
            await asyncio.sleep(0)
            try:
                request_count += 1

                api_key, api_secret = await self._get_fresh_credentials()
                if not api_key or not api_secret:
                    logger.warning("⚠️ [HTTP获取器] 账户请求-凭证读取失败")
                    await asyncio.sleep(self.account_check_interval)
                    continue

                params = {
                    'timestamp': int(time.time() * 1000),
                    'recvWindow': self.RECV_WINDOW
                }
                signed_params = self._sign_params(params, api_secret)
                url = f"{self.BASE_URL}{self.ACCOUNT_ENDPOINT}"
                headers = {'X-MBX-APIKEY': api_key}

                async with self.session.get(url, params=signed_params, headers=headers) as resp:

                    if resp.status == 200:
                        data = await resp.json()

                        # 检查持仓
                        positions = data.get('positions', [])
                        has_position_now = False
                        for pos in positions:
                            if float(pos.get('positionAmt', '0')) != 0:
                                has_position_now = True
                                break

                        # 🔴 机制1触发：检测到平仓（从有持仓变为无持仓）
                        if last_position_state and not has_position_now:
                            logger.warning("🚨 [HTTP获取器] 检测到平仓！主动重建session防止急刹车")
                            await self._rebuild_session(reason="平仓急刹车")

                        # 自适应频率调整
                        if has_position_now:
                            if not self.has_position:
                                logger.info("🚀 [HTTP获取器] 开仓检测，切换到高频模式(1秒)")
                            self.account_check_interval = self.account_high_freq
                        else:
                            if self.has_position:
                                logger.info("💤 [HTTP获取器] 平仓检测，切换到低频模式(60秒)")
                            self.account_check_interval = self.account_low_freq

                        self.has_position = has_position_now
                        last_position_state = has_position_now

                        # 日志控制
                        current_time = time.time()
                        if current_time - self.last_log_time >= self.log_interval:
                            if has_position_now:
                                positions_count = len([p for p in positions if float(p.get('positionAmt', '0')) != 0])
                                logger.info(f"📊 [HTTP获取器] 当前持仓{positions_count}个 | 高频模式 | 请求:{request_count}")
                            else:
                                logger.info(f"📊 [HTTP获取器] 当前无持仓 | 低频模式 | 请求:{request_count}")
                            self.last_log_time = current_time

                        await self._push_data('http_account', data)

                        self.quality_stats['account_fetch']['success_attempts'] += 1
                        self.quality_stats['account_fetch']['total_attempts'] += 1
                        self.quality_stats['account_fetch']['last_success'] = datetime.now().isoformat()
                        self.quality_stats['account_fetch']['last_error'] = None

                        await asyncio.sleep(self.account_check_interval)

                    else:
                        error_text = await resp.text()
                        error_msg = f"HTTP {resp.status}: {error_text[:100]}"
                        self.quality_stats['account_fetch']['last_error'] = error_msg

                        # 418/401 触发服务重启
                        if resp.status in [418, 401]:
                            logger.warning(f"⚠️ [HTTP获取器] 严重错误({resp.status})，触发重启")
                            await self._handle_restart(f"HTTP {resp.status}错误")
                            return

                        # 429 频率限制
                        elif resp.status == 429:
                            wait_time = await self._get_retry_after_time(resp)
                            logger.warning(f"⚠️ [HTTP获取器] 频率限制(429)，等待{wait_time}秒")
                            await asyncio.sleep(wait_time)
                            continue

                        else:
                            logger.error(f"❌ [HTTP获取器] 账户请求失败 {error_msg}")
                            await asyncio.sleep(self.account_check_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ [HTTP获取器] 账户循环异常: {e}")
                await asyncio.sleep(self.account_check_interval)

    # ==================== 工具方法 ====================

    async def _get_retry_after_time(self, resp) -> int:
        """从429响应获取等待时间"""
        for header in ['Retry-After', 'retry-after']:
            if header in resp.headers:
                try:
                    wait_time = int(resp.headers[header])
                    return max(10, min(wait_time, 300))
                except (ValueError, TypeError):
                    continue
        return 60

    async def on_listen_key_updated(self, exchange: str, listen_key: str):
        """接收listenKey更新"""
        if exchange == 'binance':
            logger.debug(f"📢 [HTTP获取器] 收到{exchange} listenKey更新")

    async def _get_fresh_credentials(self):
        """从大脑读取新鲜凭证"""
        try:
            if not self.brain_store:
                return None, None
            creds = await self.brain_store.get_api_credentials('binance')
            if creds and creds.get('api_key') and creds.get('api_secret'):
                return creds['api_key'], creds['api_secret']
        except Exception as e:
            logger.error(f"❌ [HTTP获取器] 读取凭证失败: {e}")
        return None, None

    def _sign_params(self, params: Dict, api_secret: str) -> Dict:
        """生成币安API签名"""
        query = urllib.parse.urlencode(params)
        signature = hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        params['signature'] = signature
        return params

    async def _push_data(self, data_type: str, raw_data: Dict):
        """推送原始数据到处理模块"""
        try:
            from private_data_processing.manager import receive_private_data
            asyncio.create_task(receive_private_data({
                'exchange': 'binance',
                'data_type': data_type,
                'data': raw_data,
                'timestamp': datetime.now().isoformat(),
                'source': 'http_fetcher'
            }))
        except ImportError as e:
            logger.error(f"❌ [HTTP获取器] 无法导入私人数据处理模块: {e}")
        except Exception as e:
            logger.error(f"❌ [HTTP获取器] 推送数据失败: {e}")

    async def shutdown(self):
        """关闭获取器"""
        logger.info("🛑 [HTTP获取器] 正在关闭...")
        self.running = False
        self.in_restart_cooldown = True

        if self.scheduler_task:
            self.scheduler_task.cancel()
            try:
                await self.scheduler_task
            except asyncio.CancelledError:
                pass

        for task in self.fetch_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        if self.session:
            try:
                await asyncio.wait_for(self.session.close(), timeout=3.0)
            except:
                pass

        if self.connector:
            try:
                await asyncio.wait_for(self.connector.close(), timeout=3.0)
            except:
                pass

        logger.info("✅ [HTTP获取器] 已关闭")

    def get_status(self) -> Dict[str, Any]:
        """获取状态信息"""
        return {
            'timestamp': datetime.now().isoformat(),
            'running': self.running,
            'keys_ready': self._keys_ready,
            'pending_work': self._pending_work,
            'account_fetched': self.account_fetched,
            'account_fetch_success': self.account_fetch_success,
            'environment': self.environment,
            'connection_mode': 'force_close=True (冷酷版，用完即关)',
            'adaptive_frequency': {
                'current_interval': self.account_check_interval,
                'has_position': self.has_position,
                'high_freq': self.account_high_freq,
                'low_freq': self.account_low_freq
            },
            'restart_info': {
                'restart_attempts': self.restart_attempts,
                'in_restart_cooldown': self.in_restart_cooldown,
                'last_restart': self.quality_stats['account_fetch'].get('last_restart')
            },
            'recovery_mechanisms': {
                '平仓重置': '_rebuild_session - 检测到平仓时重建连接，毫秒级恢复',
                '服务重启': '_handle_restart - 账户失败/异常时完全重启，2分钟+恢复'
            },
            'quality_stats': self.quality_stats,
            'api_config': {
                'recvWindow': self.RECV_WINDOW,
                'force_close': True,
                'session_reuse': False
            }
        }