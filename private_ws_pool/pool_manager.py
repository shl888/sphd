"""
私人WebSocket连接池管理器 - 简化版，直接传递原始数据
"""
import asyncio
import logging
import traceback
from datetime import datetime
from typing import Dict, Any, Optional, Callable

from .connection import BinancePrivateConnection, OKXPrivateConnection

logger = logging.getLogger(__name__)

class PrivateWebSocketPool:
    """私人连接池 - 简化版，直接传递原始数据"""
    
    def __init__(self):
        """🔴 【修改点】删除data_callback参数"""
        self.data_callback = None  # 设为None保持兼容
        
        # 组件初始化
        # 🔴 【修改点】删除raw_data_cache初始化
        # self.raw_data_cache = RawDataCache()  # 删除这行
        
        # 连接存储
        self.connections = {
            'binance': None,
            'okx': None
        }
        
        # 状态管理
        self.running = False
        self.brain_store = None
        self.start_time = None
        self.reconnect_tasks = {}
        
        # ========== 密钥就绪标志 ==========
        self._keys_ready = False
        self._pending_connect = False  # 是否有待执行的连接
        
        # 连接质量统计
        self.quality_stats = {
            'binance': {
                'total_attempts': 0,
                'success_attempts': 0,
                'consecutive_failures': 0,
                'last_success': None,
                'success_rate': 100.0,
                'last_error': None,
                'mode': 'active_probe'
            },
            'okx': {
                'total_attempts': 0,
                'success_attempts': 0,
                'consecutive_failures': 0,
                'last_success': None,
                'success_rate': 100.0,
                'last_error': None,
                'mode': 'heartbeat'
            }
        }
        
        logger.info("🔗 [私人连接池] 初始化完成 (直接推送模式)")
    
    # ==================== 标签接收 ====================
    
    def on_keys_ready(self):
        """
        接收「密钥已就绪」标签
        由 TagDispatcher 调用
        """
        self._keys_ready = True
        logger.info("🔑【私人连接池】密钥已就绪，获得工作权限")
        
        # 如果有待执行的连接，立即启动
        if self._pending_connect:
            logger.info("🚀【私人连接池】开始执行待处理的连接任务")
            asyncio.create_task(self._staggered_connect_all())
            self._pending_connect = False
    
    # ==================== 启动与连接 ====================
    
    async def start(self, brain_store):
        """启动连接池"""
        logger.info("🚀 [私人连接池] 正在稳健启动...")
        
        self.brain_store = brain_store
        self.running = True
        self.start_time = datetime.now()
        
        # 启动连接监控循环（只做基础状态检查）
        asyncio.create_task(self._connection_monitor_loop())
        
        # 检查密钥是否已就绪
        if self._keys_ready:
            # 密钥已就绪，直接启动连接
            logger.info("🔑【私人连接池】密钥已就绪，直接启动连接")
            asyncio.create_task(self._staggered_connect_all())
        else:
            # 密钥未就绪，标记待执行，等待标签
            logger.info("⏳【私人连接池】密钥未就绪，等待标签...")
            self._pending_connect = True
        
        logger.info("✅ [私人连接池] 已启动")
        return True
    
    async def _staggered_connect_all(self):
        """分批连接所有交易所"""
        # 先连接币安（主动探测模式）
        logger.info("🔗 [私人连接池] 第一阶段：连接币安（主动探测模式）")
        binance_success = await self._setup_binance_connection()
        
        # 等待3秒再连接欧意
        await asyncio.sleep(3)
        
        logger.info("🔗 [私人连接池] 第二阶段：连接欧意（协议层心跳模式）")
        okx_success = await self._setup_okx_connection()
        
        success_count = sum([binance_success, okx_success])
        logger.info(f"🎯 [私人连接池] 连接尝试完成: {success_count}/2 成功")
        
        # 失败的安排重连
        if not binance_success:
            logger.error("🔁 [私人连接池] 币安连接失败，10秒后重试")
            await self._schedule_reconnect('binance', 10)
        
        if not okx_success:
            logger.error("🔁 [私人连接池] 欧意连接失败，10秒后重试")
            await self._schedule_reconnect('okx', 10)
    
    async def _connection_monitor_loop(self):
        """基础连接监控循环 - 增强版调试日志"""
        while self.running:
            try:
                # 记录当前时间，方便追踪
                check_time = datetime.now().strftime('%H:%M:%S')
                logger.debug(f"[私人连接池] 🔍 监控检查开始 at {check_time}")
                
                for exchange in ['binance', 'okx']:
                    await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU，避免监控循环阻塞
                    connection = self.connections[exchange]
                    
                    if connection:
                        # 🔴 添加详细的状态日志 - INFO级别
                        last_msg = connection.last_message_time.strftime('%H:%M:%S') if connection.last_message_time else '无'
                        logger.info(f"[私人连接池] 📊 {exchange}状态: connected={connection.connected}, 最后消息={last_msg}, 消息计数={connection.message_counter}, 连续失败={connection.continuous_failure_count}")
                        
                        # 如果是欧意，额外检查认证状态
                        if exchange == 'okx' and hasattr(connection, 'authenticated'):
                            logger.info(f"[私人连接池] 🔐 {exchange}认证状态: {connection.authenticated}")
                        
                        # 检查是否有last_error属性
                        if hasattr(connection, 'last_error') and connection.last_error:
                            logger.error(f"[私人连接池] ⚠️ {exchange}最后错误: {connection.last_error}")
                        
                        if not connection.connected:
                            # 连接已断开，触发重连
                            self.quality_stats[exchange]['consecutive_failures'] += 1
                            logger.warning(f"[私人连接池] 🔁 {exchange}连接断开，触发重连")
                            logger.warning(f"[私人连接池] 📋 {exchange}断开详情: 连续失败={connection.continuous_failure_count}, 总消息={connection.message_counter}, 最后消息时间={last_msg}")
                            await self._smart_reconnect(exchange)
                    else:
                        logger.error(f"[私人连接池] ⚪ {exchange}连接对象不存在")
                
                logger.info(f"[私人连接池] 💤 监控检查完成，等待10秒")
                await asyncio.sleep(10)  # 10秒检查一次
                
            except asyncio.CancelledError:
                logger.error("[私人连接池] 🛑 监控循环被取消")
                break
            except Exception as e:
                logger.error(f"[私人连接池] ❌ 监控循环异常: {e}")
                logger.error(f"[私人连接池] ❌ 异常详情: {traceback.format_exc()}")
                await asyncio.sleep(30)
    
    async def _smart_reconnect(self, exchange: str):
        """智能重连算法 - 增强日志"""
        connection = self.connections[exchange]
        if not connection:
            logger.error(f"[私人连接池] ⚠️ {exchange}重连时连接对象不存在")
            return
        
        consecutive_failures = connection.continuous_failure_count
        logger.error(f"[私人连接池] 📊 {exchange}重连决策: 当前连续失败={consecutive_failures}")
        
        # 根据连续失败次数计算延迟
        if consecutive_failures == 0:
            delay = 5
            logger.error(f"[私人连接池] 📝 {exchange}首次失败，采用5秒延迟")
        elif consecutive_failures == 1:
            delay = 10
            logger.error(f"[私人连接池] 📝 {exchange}连续2次失败，采用10秒延迟")
        elif consecutive_failures == 2:
            delay = 20
            logger.error(f"[私人连接池] 📝 {exchange}连续3次失败，采用20秒延迟")
        elif consecutive_failures == 3:
            delay = 40
            logger.error(f"[私人连接池] 📝 {exchange}连续4次失败，采用40秒延迟")
        else:
            delay = 60
            logger.error(f"[私人连接池] 📝 {exchange}连续{consecutive_failures}次失败，采用60秒最大延迟")
        
        logger.error(f"[私人连接池] 🔁 {exchange} {delay}秒后重连 (连续失败{consecutive_failures}次)")
        await self._schedule_reconnect(exchange, delay)
    
    async def _schedule_reconnect(self, exchange: str, delay: int = 5):
        """安排重连 - 增强日志"""
        if exchange in self.reconnect_tasks:
            logger.error(f"[私人连接池] 🚫 取消{exchange}现有重连任务")
            try:
                self.reconnect_tasks[exchange].cancel()
            except:
                pass
        
        async def reconnect_task():
            try:
                logger.error(f"[私人连接池] ⏰ {exchange}等待{delay}秒后重连...")
                await asyncio.sleep(delay)
                if self.running:
                    logger.error(f"[私人连接池] 🔁 执行{exchange}重连...")
                    success = False
                    
                    if exchange == 'binance':
                        logger.error("[私人连接池] 📡 开始币安重连流程")
                        success = await self._setup_binance_connection()
                    elif exchange == 'okx':
                        logger.error("[私人连接池] 📡 开始欧意重连流程")
                        success = await self._setup_okx_connection()
                    
                    logger.info(f"[私人连接池] 📊 {exchange}重连结果: {'✅成功' if success else '❌失败'}")
                    self._update_quality_stats(exchange, success)
                    
                    if not success:
                        next_delay = min(delay * 2, 120)
                        logger.error(f"[私人连接池] 🔄 {exchange}重连失败，{next_delay}秒后再次尝试")
                        await self._schedule_reconnect(exchange, next_delay)
            except asyncio.CancelledError:
                logger.error(f"[私人连接池] 🚫 {exchange}重连任务被取消")
                pass
            except Exception as e:
                logger.error(f"[私人连接池] ❌ 重连任务异常: {e}")
                logger.error(f"[私人连接池] ❌ 异常详情: {traceback.format_exc()}")
        
        self.reconnect_tasks[exchange] = asyncio.create_task(reconnect_task())
        logger.info(f"[私人连接池] ✅ {exchange}重连任务已创建，延迟{delay}秒")
    
    def _update_quality_stats(self, exchange: str, success: bool):
        """更新连接质量统计"""
        stats = self.quality_stats[exchange]
        stats['total_attempts'] += 1
        
        if success:
            stats['success_attempts'] += 1
            stats['consecutive_failures'] = 0
            stats['last_success'] = datetime.now()
            stats['last_error'] = None
            logger.debug(f"[私人连接池] 📈 {exchange}质量统计: 成功，总尝试={stats['total_attempts']}, 成功率={stats['success_rate']:.1f}%")
        else:
            stats['consecutive_failures'] += 1
            logger.debug(f"[私人连接池] 📉 {exchange}质量统计: 失败，连续失败={stats['consecutive_failures']}, 总尝试={stats['total_attempts']}")
        
        if stats['total_attempts'] > 0:
            stats['success_rate'] = (stats['success_attempts'] / stats['total_attempts']) * 100
        
        # 成功率警告
        if stats['total_attempts'] >= 5 and stats['success_rate'] < 70.0:
            logger.warning(f"[私人连接池] ⚠️ {exchange} 连接成功率低: {stats['success_rate']:.1f}%")
    
    async def _setup_binance_connection(self) -> bool:
        """设置币安连接（主动探测模式）"""
        try:
            if not self.brain_store:
                logger.error("[私人连接池] ❌ 未设置大脑存储接口")
                return False
            
            # 获取listenKey
            logger.info("[私人连接池] 🔑 获取币安listenKey...")
            listen_key = await self.brain_store.get_listen_key('binance')
            if not listen_key:
                logger.warning("[私人连接池] ⚠️ 币安listenKey不存在，等待中...")
                return False
            
            # 获取API凭证
            logger.info("[私人连接池] 🔑 获取币安API凭证...")
            api_creds = await self.brain_store.get_api_credentials('binance')
            if not api_creds:
                logger.error("[私人连接池] ❌ 币安API凭证不存在")
                return False
            
            # 创建连接实例
            logger.info("[私人连接池] 🏗️ 创建币安连接实例...")
            connection = BinancePrivateConnection(
                listen_key=listen_key,
                status_callback=self._handle_connection_status,
                data_callback=self._process_and_forward_data,
                raw_data_cache=None
            )
            
            # 建立连接
            logger.info("[私人连接池] 🔌 建立币安连接...")
            success = await connection.connect()
            if success:
                self.connections['binance'] = connection
                logger.info("[私人连接池] ✅ 币安连接成功（主动探测模式）")
            else:
                logger.error("[私人连接池] ❌ 币安连接失败")
                await self._schedule_reconnect('binance')
            
            return success
            
        except Exception as e:
            logger.error(f"[私人连接池] ❌ 设置币安连接异常: {e}")
            logger.error(f"[私人连接池] ❌ 异常详情: {traceback.format_exc()}")
            self.quality_stats['binance']['last_error'] = str(e)
            await self._schedule_reconnect('binance')
            return False
    
    async def _setup_okx_connection(self) -> bool:
        """设置欧意连接（协议层心跳模式）"""
        try:
            if not self.brain_store:
                logger.error("[私人连接池] ❌ 未设置大脑存储接口")
                return False
            
            # 获取API凭证
            logger.info("[私人连接池] 🔑 获取欧意API凭证...")
            api_creds = await self.brain_store.get_api_credentials('okx')
            if not api_creds:
                logger.warning("[私人连接池] ⚠️ 欧意API凭证不存在，等待中...")
                return False
            
            logger.info(f"[私人连接池] 📋 欧意API Key: {api_creds['api_key'][:8]}...")
            
            # 创建连接实例
            logger.info("[私人连接池] 🏗️ 创建欧意连接实例...")
            connection = OKXPrivateConnection(
                api_key=api_creds['api_key'],
                api_secret=api_creds['api_secret'],
                passphrase=api_creds.get('passphrase', ''),
                status_callback=self._handle_connection_status,
                data_callback=self._process_and_forward_data,
                raw_data_cache=None
            )
            
            # 建立连接
            logger.info("[私人连接池] 🔌 建立欧意连接...")
            success = await connection.connect()
            if success:
                self.connections['okx'] = connection
                logger.info("[私人连接池] ✅ 欧意连接成功（协议层心跳模式）")
            else:
                logger.error("[私人连接池] ❌ 欧意连接失败")
                await self._schedule_reconnect('okx')
            
            return success
            
        except Exception as e:
            logger.error(f"[私人连接池] ❌ 设置欧意连接异常: {e}")
            logger.error(f"[私人连接池] ❌ 异常详情: {traceback.format_exc()}")
            self.quality_stats['okx']['last_error'] = str(e)
            await self._schedule_reconnect('okx')
            return False
    
    async def on_listen_key_updated(self, exchange: str, listen_key: str):
        """监听listenKey更新事件"""
        try:
            logger.info(f"[私人连接池] 📢 收到{exchange} listenKey更新通知")
            
            if exchange == 'binance':
                logger.info(f"[私人连接池] 🔗 5秒后重建币安连接...")
                await self._schedule_reconnect('binance', 5)
            elif exchange == 'okx':
                logger.info(f"[私人连接池] 🔗 listenKey更新，但OKX使用API key连接，跳过")
            else:
                logger.warning(f"[私人连接池] ⚠️ 未知交易所: {exchange}")
                
        except Exception as e:
            logger.error(f"[私人连接池] ❌ 处理listenKey更新失败: {e}")
    
    async def _handle_connection_status(self, status_data: Dict[str, Any]):
        """处理连接状态事件"""
        try:
            exchange = status_data.get('exchange')
            event = status_data.get('event')
            
            logger.info(f"[私人连接池] 📡 {exchange}状态事件: {event}")
            
            if event in ['connection_closed', 'health_check_failed', 'listenkey_expired']:
                logger.warning(f"[私人连接池] ⚠️ {exchange}连接断开，事件: {event}")
                await self._smart_reconnect(exchange)
                
            elif event == 'connection_established':
                logger.info(f"[私人连接池] ✅ {exchange}私人连接已建立")
                
            elif event == 'connection_failed':
                error_msg = status_data.get('error', 'unknown')
                logger.error(f"[私人连接池] ❌ {exchange}连接失败: {error_msg}")
                self.quality_stats[exchange]['last_error'] = error_msg
                await self._smart_reconnect(exchange)
                
        except Exception as e:
            logger.error(f"[私人连接池] ❌ 处理状态事件失败: {e}")
    
    async def _process_and_forward_data(self, raw_data: Dict[str, Any]):
        """处理并转发数据 - 硬编码推送到新模块"""
        try:
            # 硬编码推送到私人数据处理模块
            try:
                from private_data_processing.manager import receive_private_data
                asyncio.create_task(receive_private_data(raw_data))
                logger.debug(f"[私人连接池] 📨 已推送到私人数据处理模块: {raw_data['exchange']}.{raw_data['data_type']}")
            except ImportError as e:
                logger.error(f"[私人连接池] ❌ 无法导入私人数据处理模块: {e}")
            except Exception as e:
                logger.error(f"[私人连接池] ❌ 推送数据失败: {e}")
                logger.error(f"[私人连接池] ❌ 异常详情: {traceback.format_exc()}")
            
        except Exception as e:
            logger.error(f"[私人连接池] ❌ 处理转发数据失败: {e}")
    
    async def shutdown(self):
        """关闭所有连接"""
        logger.info("[私人连接池] 🛑 正在关闭...")
        self.running = False
        
        # 取消重连任务
        for exchange, task in self.reconnect_tasks.items():
            if task:
                logger.error(f"[私人连接池] 🚫 取消{exchange}重连任务")
                task.cancel()
        
        # 关闭所有连接
        shutdown_tasks = []
        for exchange, connection in self.connections.items():
            if connection:
                logger.error(f"[私人连接池] 🔌 正在断开{exchange}连接...")
                shutdown_tasks.append(
                    asyncio.wait_for(connection.disconnect(), timeout=5)
                )
        
        if shutdown_tasks:
            try:
                await asyncio.gather(*shutdown_tasks, return_exceptions=True)
            except:
                pass
        
        self.connections = {'binance': None, 'okx': None}
        logger.error("[私人连接池] ✅ 已关闭")
    
    def get_status(self) -> Dict[str, Any]:
        """获取连接池状态"""
        status = {
            'timestamp': datetime.now().isoformat(),
            'running': self.running,
            'uptime_seconds': (datetime.now() - self.start_time).total_seconds() if self.start_time else 0,
            'keys_ready': self._keys_ready,  # 新增：密钥就绪状态
            'pending_connect': self._pending_connect,  # 新增：是否有待执行连接
            'connections': {},
            'quality_stats': self.quality_stats,
            'alerts': [],
            'exchange_modes': {
                'binance': '主动探测模式（30秒探测）',
                'okx': '协议层心跳模式（25秒协议层心跳 + 45秒被动检测）'
            },
            'data_destination': '私人数据处理模块（硬编码推送）'
        }
        
        for exchange in ['binance', 'okx']:
            connection = self.connections[exchange]
            
            if connection:
                conn_info = {
                    'connected': connection.connected,
                    'last_message_time': connection.last_message_time.isoformat() if connection.last_message_time else None,
                    'continuous_failure_count': connection.continuous_failure_count,
                    'connection_established_time': connection.connection_established_time.isoformat() if connection.connection_established_time else None,
                    'message_counter': connection.message_counter,
                    'first_message_received': connection.first_message_received,
                    'mode': '被动探测' if exchange == 'binance' else '心跳'
                }
                
                if not connection.connected:
                    conn_info['alert'] = 'disconnected'
                    status['alerts'].append(f"私人连接池{exchange}连接断开")
                
                if connection.continuous_failure_count > 3:
                    conn_info['alert'] = 'high_failure_rate'
                    status['alerts'].append(f"私人连接池{exchange}连续失败{connection.continuous_failure_count}次")
                
                # 币安探测状态
                if exchange == 'binance' and hasattr(connection, 'consecutive_probe_failures'):
                    conn_info['probe_failures'] = connection.consecutive_probe_failures
                    if connection.consecutive_probe_failures > 0:
                        conn_info['alert'] = 'probe_failing'
                
                # 欧意认证状态
                if exchange == 'okx' and hasattr(connection, 'authenticated'):
                    conn_info['authenticated'] = connection.authenticated
                
                status['connections'][exchange] = conn_info
            else:
                status['connections'][exchange] = {'connected': False, 'alert': 'not_initialized'}
                status['alerts'].append(f"私人连接池{exchange}未初始化")
        
        # 添加调试信息
        status['debug_info'] = {
            'reconnect_tasks': list(self.reconnect_tasks.keys()),
            'check_time': datetime.now().strftime('%H:%M:%S')
        }
        
        return status