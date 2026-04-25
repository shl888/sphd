"""
私人WebSocket连接实现 - 双模式稳定版
币安：主动探测模式 | 欧意：心跳+间隔模式
简化版：只保留原始数据，不添加额外包装
"""
import asyncio
import json
import logging
import time
import hmac
import hashlib
import base64
from datetime import datetime
from typing import Dict, Any, Set
import websockets
import ssl
import traceback

logger = logging.getLogger(__name__)

class PrivateWebSocketConnection:
    """私人WebSocket连接基类 - 双模式稳定版"""
    
    def __init__(self, exchange: str, connection_id: str,
                 status_callback, data_callback, raw_data_cache):
        self.exchange = exchange
        self.connection_id = connection_id
        self.status_callback = status_callback
        self.data_callback = data_callback
        self.raw_data_cache = raw_data_cache
        
        # 连接状态
        self.ws = None
        self.connected = False
        self.subscribed = False
        self.last_message_time = None
        self.reconnect_count = 0
        
        # 稳定性参数
        self.continuous_failure_count = 0
        self.last_connect_success = None
        self.message_counter = 0
        self.connection_established_time = None
        self.first_message_received = False
        
        # 任务
        self.receive_task = None
        self.health_check_task = None
        self.heartbeat_task = None
        
        # 保守重连策略
        self.quick_retry_delays = [2, 4, 8]
        self.slow_retry_delays = [15, 30, 60]
        
        logger.info(f"[私人连接池] {connection_id} 初始化完成")
    
    async def connect(self):
        """建立连接（由子类实现）"""
        raise NotImplementedError
    
    async def disconnect(self):
        """断开连接"""
        try:
            self.connected = False
            self.subscribed = False
            
            # 取消所有任务
            tasks = [self.health_check_task, self.heartbeat_task, self.receive_task]
            for task in tasks:
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU，避免任务取消过程阻塞
                if task:
                    task.cancel()
                    try:
                        await task
                    except (asyncio.CancelledError, Exception):
                        pass
            
            if self.ws:
                await self.ws.close()
                self.ws = None
            
            logger.error(f"[私人连接池] {self.connection_id} 已断开")
            
        except Exception as e:
            logger.error(f"[私人连接池] 断开连接失败: {e}")
    
    async def _report_status(self, event: str, extra_data: Dict[str, Any] = None):
        """上报状态给大脑"""
        try:
            status = {
                'exchange': self.exchange,
                'connection_id': self.connection_id,
                'event': event,
                'timestamp': datetime.now().isoformat(),
                'continuous_failures': self.continuous_failure_count
            }
            if extra_data:
                status.update(extra_data)
            
            await self.status_callback(status)
            
        except Exception as e:
            logger.error(f"[私人连接池] 上报状态失败: {e}")
    
    async def _connect_with_retry(self, connect_func, max_quick_retries=3, max_slow_retries=2):
        """通用带重试的连接方法"""
        # 快速重试
        for attempt in range(max_quick_retries):
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环开始让出CPU，避免重试循环阻塞
            try:
                logger.error(f"[私人连接池] {self.connection_id} 快速重试第{attempt + 1}次")
                await connect_func()
                return True
            except Exception as e:
                logger.warning(f"[私人连接池] {self.connection_id} 快速重试失败: {type(e).__name__}")
                if attempt == max_quick_retries - 1:
                    break
                wait_time = self.quick_retry_delays[attempt] if attempt < len(self.quick_retry_delays) else 8
                await asyncio.sleep(wait_time)
        
        # 慢速重试
        for attempt in range(max_slow_retries):
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环开始让出CPU，避免重试循环阻塞
            try:
                logger.error(f"[私人连接池] {self.connection_id} 慢速重试第{attempt + 1}次")
                await connect_func()
                return True
            except Exception as e:
                logger.warning(f"[私人连接池] {self.connection_id} 慢速重试失败: {type(e).__name__}")
                if attempt == max_slow_retries - 1:
                    break
                wait_time = self.slow_retry_delays[attempt] if attempt < len(self.slow_retry_delays) else 60
                await asyncio.sleep(wait_time)
        
        return False


class BinancePrivateConnection(PrivateWebSocketConnection):
    """币安私人连接 - 主动探测模式"""
    
    def __init__(self, listen_key: str, **kwargs):
        super().__init__('binance', 'binance_private', **kwargs)
        self.listen_key = listen_key
        
        # 主动探测参数
        self.probe_interval = 30  # 30秒探测一次（保守）
        self.probe_timeout = 10   # 10秒等待响应
        self.max_consecutive_failures = 3  # 连续3次失败断开
        
        # 探测状态
        self.probe_task = None
        self.probe_counter = 0
        self.probe_ids: Set[int] = set()  # 已发送的探测ID
        self.probe_response_received = True  # 初始为True
        self.consecutive_probe_failures = 0
        self.last_probe_sent = None
        self.waiting_for_probe = False
        
        # 服务器配置
        self.ws_url = f"wss://fstream.binancefuture.com/ws/{listen_key}"
        self.backup_servers = [
            f"wss://fstream.binancefuture.com/ws/{listen_key}",
            f"wss://fstream.binance.com/ws/{listen_key}",
        ]
        self.current_server_index = 0
        
        logger.info(f"[私人连接池] 币安私人 初始化完成（主动探测模式，间隔{self.probe_interval}秒）")
    
    async def connect(self):
        """建立连接并启动主动探测"""
        try:
            logger.info(f"[私人连接池] 币安私人 正在连接，listenKey: {self.listen_key[:8]}...")
            
            self.continuous_failure_count += 1
            success = await self._try_multiple_servers()
            
            if success:
                self.continuous_failure_count = 0
                self.last_connect_success = datetime.now()
                self.connection_established_time = datetime.now()
                self.first_message_received = False
                self.consecutive_probe_failures = 0
                self.probe_ids.clear()
                
                # 启动主动探测任务
                self.probe_task = asyncio.create_task(self._active_probe_loop())
                
                logger.info(f"[私人连接池] 币安私人 连接成功，主动探测已启动")
                return True
            else:
                logger.error(f"[私人连接池] 币安私人 所有服务器连接失败")
                return False
                
        except Exception as e:
            logger.error(f"[私人连接池] 币安私人 连接异常: {e}")
            await self._report_status('connection_failed', {'error': str(e)})
            return False
    
    async def _try_multiple_servers(self):
        """币安尝试多个服务器"""
        for server_index, server_url in enumerate(self.backup_servers):
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环开始让出CPU，避免服务器切换循环阻塞
            logger.info(f"[私人连接池] 币安私人 尝试服务器 {server_index + 1}/{len(self.backup_servers)}")
            self.ws_url = server_url
            
            success = await self._connect_with_retry(self._connect_single_server)
            
            if success:
                self.current_server_index = server_index
                return True
            else:
                logger.warning(f"[私人连接池] 币安私人 服务器{server_index + 1}连接失败")
                await asyncio.sleep(3)
        
        return False
    
    async def _connect_single_server(self):
        """币安连接到单个服务器"""
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        self.ws = await asyncio.wait_for(
            websockets.connect(
                self.ws_url,
                ssl=ssl_context,
                ping_interval=30,
                ping_timeout=15,
                close_timeout=8,
                max_size=5*1024*1024,
            ),
            timeout=20
        )
        
        self.connected = True
        self.last_message_time = datetime.now()
        self.first_message_received = False
        
        # 启动接收任务
        self.receive_task = asyncio.create_task(self._receive_messages())
        
        await self._report_status('connection_established')
        logger.info(f"[私人连接池] 币安私人 服务器连接成功")
    
    async def _active_probe_loop(self):
        """主动探测循环 - 核心检测逻辑"""
        while self.connected:
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环开始让出CPU，避免极端情况下循环饿死
            try:
                await asyncio.sleep(self.probe_interval)
                
                # 检查上次探测是否收到响应
                if self.waiting_for_probe:
                    self.consecutive_probe_failures += 1
                    logger.warning(f"[私人连接池] 币安探测 探测#{self.probe_counter}未响应，连续失败: {self.consecutive_probe_failures}")
                    
                    if self.consecutive_probe_failures >= self.max_consecutive_failures:
                        logger.error(f"[私人连接池] 币安探测 连续{self.consecutive_probe_failures}次探测失败，断开连接")
                        self.connected = False
                        break
                else:
                    # 重置连续失败计数
                    if self.consecutive_probe_failures > 0:
                        logger.info(f"[私人连接池] 币安探测 探测恢复，重置失败计数")
                        self.consecutive_probe_failures = 0
                
                # 发送探测消息
                self.probe_counter += 1
                probe_id = 99900 + (self.probe_counter % 100)
                
                # 使用LIST_SUBSCRIPTIONS（币安必响应）
                probe_msg = {
                    "method": "LIST_SUBSCRIPTIONS",
                    "id": probe_id
                }
                
                logger.debug(f"[私人连接池] 币安探测 发送探测#{self.probe_counter} (ID={probe_id})")
                self.last_probe_sent = datetime.now()
                self.waiting_for_probe = True
                self.probe_ids.add(probe_id)
                
                # 发送失败 = 连接已死
                await self.ws.send(json.dumps(probe_msg))
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                # 发送异常 = 连接已死
                logger.error(f"[私人连接池] 币安探测 发送失败: {e}")
                self.connected = False
                break
    
    async def _receive_messages(self):
        """接收消息 - 处理探测响应"""
        try:
            async for message in self.ws:
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 异步迭代循环内让出CPU，避免消息风暴时阻塞
                self.last_message_time = datetime.now()
                self.message_counter += 1
                
                if not self.first_message_received:
                    self.first_message_received = True
                    logger.info(f"[私人连接池] 币安私人 收到第一条消息")
                
                try:
                    data = json.loads(message)
                    
                    # 核心逻辑：只检查ID，不问内容
                    msg_id = data.get('id')
                    if msg_id and msg_id in self.probe_ids:
                        # 有回音 = 连接活（不管内容是什么）
                        self.waiting_for_probe = False
                        self.probe_ids.discard(msg_id)
                        logger.debug(f"[私人连接池] 币安探测 收到响应 ID={msg_id}")
                        continue  # 不转发探测响应
                    
                    # 正常业务消息
                    event_type = data.get('e', 'unknown')
                    formatted_data = {
                        'exchange': 'binance',
                        'data_type': self._map_binance_event_type(event_type),
                        'timestamp': datetime.now().isoformat(),
                        'data': data
                    }
                    
                    # 异步转发，不等待
                    asyncio.create_task(self.data_callback(formatted_data))
                    
                except json.JSONDecodeError:
                    logger.warning(f"[私人连接池] 币安私人 无法解析JSON消息: {message[:100]}")
                except Exception as e:
                    logger.error(f"[私人连接池] 币安私人 处理消息错误: {e}")
                    
        except websockets.ConnectionClosed as e:
            logger.warning(f"[私人连接池] 币安私人 连接关闭: code={e.code}, reason={e.reason}")
            await self._report_status('connection_closed', {
                'code': e.code,
                'reason': e.reason
            })
        except Exception as e:
            logger.error(f"[私人连接池] 币安私人 接收消息错误: {e}")
            await self._report_status('error', {'error': str(e)})
        finally:
            self.connected = False
            # 清理探测任务
            if self.probe_task:
                self.probe_task.cancel()
    
    def _map_binance_event_type(self, event_type: str) -> str:
        """映射币安事件类型到标准类型"""
        mapping = {
            'ORDER_TRADE_UPDATE': 'order_update',
            'ACCOUNT_UPDATE': 'account_update',
            'ACCOUNT_CONFIG_UPDATE': 'account_config_update',
            'MARGIN_CALL': 'risk_event',
            'listenKeyExpired': 'system_event',
            'balanceUpdate': 'balance_update',
            'outboundAccountPosition': 'account_update',
            'executionReport': 'order_update'
        }
        return mapping.get(event_type, event_type.lower())
    
    async def disconnect(self):
        """断开连接 - 清理探测任务"""
        if self.probe_task:
            self.probe_task.cancel()
            try:
                await self.probe_task
            except (asyncio.CancelledError, Exception):
                pass
        await super().disconnect()


class OKXPrivateConnection(PrivateWebSocketConnection):
    """欧意私人连接 - 极简版：收到就推，不做任何处理"""
    
    def __init__(self, api_key: str, api_secret: str, passphrase: str = '', **kwargs):
        super().__init__('okx', 'okx_private', **kwargs)
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        
        # 欧意模拟交易地址
        self.ws_url = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
        self.broker_id = "9999"
        self.backup_url = "wss://ws.okx.com:8443/ws/v5/private"
        
        # 主动模式参数
        self.authenticated = False
        self.last_heartbeat_time = None
        self.heartbeat_interval = 25
        self.no_message_threshold = 45
        
        # 统计
        self.message_count = 0
        self.last_stats_time = time.time()
        
        logger.info(f"[私人连接池] 欧意私人 初始化完成（极简版：收到就推）")
    
    async def connect(self):
        """建立欧意连接"""
        try:
            logger.info(f"[私人连接池] 欧意私人 正在连接")
            
            self.continuous_failure_count += 1
            success = await self._triple_connect_flow()
            
            if success:
                self.continuous_failure_count = 0
                self.last_connect_success = datetime.now()
                self.connection_established_time = datetime.now()
                self.first_message_received = False
                self.connected = True
                logger.info(f"[私人连接池] 欧意私人连接建立成功")
                return True
            else:
                logger.error(f"[私人连接池] 欧意私人 连接失败")
                return False
                
        except Exception as e:
            logger.error(f"[私人连接池] 欧意私人 连接异常: {e}")
            await self._report_status('connection_failed', {'error': str(e)})
            return False
    
    async def _triple_connect_flow(self):
        """三重保障连接流程"""
        # 1. 连接WebSocket
        connect_success = await self._connect_with_retry(self._connect_websocket)
        if not connect_success:
            return False
        
        await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 步骤间主动让出，避免连续操作阻塞
        
        # 2. 认证
        auth_success = await self._authenticate_with_fallback()
        if not auth_success:
            await self.disconnect()
            return False
        
        self.authenticated = True
        await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 步骤间主动让出
        
        # 3. 订阅
        subscribe_success = await self._smart_subscribe()
        if not subscribe_success:
            logger.warning("[私人连接池] 欧意私人 订阅部分失败，但连接已建立")
        
        await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 步骤间主动让出
        
        # 4. 启动接收任务
        self.receive_task = asyncio.create_task(self._receive_messages())
        
        return True
    
    async def _connect_websocket(self):
        """连接WebSocket"""
        logger.info("[私人连接池] 欧意私人 正在连接WebSocket...")
        
        try:
            self.ws = await asyncio.wait_for(
                websockets.connect(
                    self.ws_url,
                    ping_interval=None,
                    ping_timeout=None,
                    close_timeout=5,
                    max_size=10*1024*1024,
                ),
                timeout=15
            )
            logger.info("[私人连接池] 欧意私人 WebSocket连接成功")
        except Exception as e:
            logger.warning(f"[私人连接池] 欧意私人 主URL连接失败，尝试备用URL: {e}")
            try:
                self.ws = await asyncio.wait_for(
                    websockets.connect(
                        self.backup_url,
                        ping_interval=None,
                        ping_timeout=None,
                        close_timeout=5,
                        max_size=10*1024*1024,
                    ),
                    timeout=15
                )
                logger.info("[私人连接池] 欧意私人 备用URL连接成功")
            except Exception as e2:
                logger.error(f"[私人连接池] 欧意私人 备用URL连接失败: {e2}")
                raise
    
    async def _authenticate_with_fallback(self):
        """双重认证保障"""
        try:
            if await self._authenticate():
                return True
        except Exception as e:
            logger.warning(f"[私人连接池] 欧意私人 主认证失败: {e}")
        
        await asyncio.sleep(1)
        logger.info("[私人连接池] 欧意私人 尝试备认证方案")
        try:
            return await self._authenticate_with_new_timestamp()
        except Exception as e:
            logger.error(f"[私人连接池] 欧意私人 备认证失败: {e}")
            return False
    
    async def _authenticate(self) -> bool:
        timestamp = str(int(time.time()))
        return await self._authenticate_with_timestamp(timestamp)
    
    async def _authenticate_with_new_timestamp(self) -> bool:
        timestamp = str(int(time.time()) - 1)
        return await self._authenticate_with_timestamp(timestamp)
    
    async def _authenticate_with_timestamp(self, timestamp: str) -> bool:
        """使用指定时间戳认证"""
        try:
            message = timestamp + 'GET' + '/users/self/verify'
            
            signature = hmac.new(
                self.api_secret.encode('utf-8'),
                message.encode('utf-8'),
                hashlib.sha256
            ).digest()
            
            signature_base64 = base64.b64encode(signature).decode('utf-8')
            
            auth_msg = {
                "op": "login",
                "args": [
                    {
                        "apiKey": self.api_key,
                        "passphrase": self.passphrase,
                        "timestamp": timestamp,
                        "sign": signature_base64
                    }
                ]
            }
            
            logger.info(f"[私人连接池] 欧意私人 发送认证请求")
            await self.ws.send(json.dumps(auth_msg))
            
            response = await asyncio.wait_for(self.ws.recv(), timeout=10)
            response_data = json.loads(response)
            
            if response_data.get('event') == 'login' and response_data.get('code') == '0':
                logger.info("[私人连接池] 欧意私人 认证成功")
                return True
            else:
                logger.error(f"[私人连接池] 欧意私人 认证失败: {response_data}")
                return False
                
        except Exception as e:
            logger.error(f"[私人连接池] 欧意私人 认证异常: {e}")
            return False
    
    async def _smart_subscribe(self) -> bool:
        """智能订阅"""
        try:
            channels = [
                {"channel": "account", "brokerId": self.broker_id},
                {"channel": "orders", "instType": "SWAP", "brokerId": self.broker_id},
                {"channel": "positions", "instType": "SWAP", "brokerId": self.broker_id}
            ]
            
            await self.ws.send(json.dumps({
                "op": "subscribe",
                "args": channels
            }))
            
            logger.info("[私人连接池] 欧意私人 已发送订阅请求")
            
            try:
                response = await asyncio.wait_for(self.ws.recv(), timeout=3)
                resp_data = json.loads(response)
                if resp_data.get('event') == 'subscribe':
                    logger.info(f"[私人连接池] 欧意私人 订阅成功")
                elif resp_data.get('event') == 'error':
                    logger.error(f"[私人连接池] 欧意私人 订阅失败: {resp_data}")
            except asyncio.TimeoutError:
                logger.warning("[私人连接池] 欧意私人 订阅确认超时，继续运行")
            
            return True
            
        except Exception as e:
            logger.error(f"[私人连接池] 欧意私人 订阅失败: {e}")
            return False
    
    async def _receive_messages(self):
        """接收欧意私人消息 - 极简版：收到就推，不做任何处理"""
        try:
            async for message in self.ws:
                await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 异步迭代循环内让出CPU，避免消息风暴时阻塞
                # 只记录必要信息
                self.last_message_time = datetime.now()
                self.message_counter += 1
                
                if not self.first_message_received:
                    self.first_message_received = True
                    logger.info(f"[私人连接池] 欧意私人 收到第一条消息")
                
                try:
                    # 直接解析并推送，不做任何判断和处理
                    data = json.loads(message)
                    
                    # ===== 过滤系统事件 =====
                    event = data.get('event', '')
                    if event in ['channel-conn-count', 'login', 'subscribe', 'error', 'unsubscribe']:
                        logger.debug(f"[私人连接池] 欧意私人 过滤系统事件: {event}")
                        continue  # 跳过，不推送
                    
                    # 提取 channel 并映射到标准类型
                    arg = data.get('arg', {})
                    channel = arg.get('channel', 'unknown')
                    
                    channel_mapping = {
                        'account': 'account_update',
                        'orders': 'order_update',
                        'positions': 'position_update',
                        'balance_and_position': 'account_position_update'
                    }
                    data_type = channel_mapping.get(channel, 'unknown')
                    
                    # 使用create_task异步推送，不等待
                    asyncio.create_task(self.data_callback({
                        'exchange': 'okx',
                        'data_type': data_type,
                        'timestamp': datetime.now().isoformat(),
                        'data': data
                    }))
                    
                except json.JSONDecodeError:
                    logger.warning(f"[私人连接池] 欧意私人 无法解析JSON: {message[:100]}")
                except Exception as e:
                    # 任何错误只记录，继续收下一条
                    logger.error(f"[私人连接池] 欧意私人 处理消息错误: {e}")
                    continue  # 继续收下一条，不断开
                    
        except websockets.ConnectionClosed as e:
            logger.warning(f"[私人连接池] 欧意私人 连接关闭: code={e.code}, reason={e.reason}")
            self.connected = False
            self.authenticated = False
        except Exception as e:
            logger.error(f"[私人连接池] 欧意私人 接收消息错误: {e}")
            self.connected = False
            self.authenticated = False
    
    async def disconnect(self):
        """断开连接"""
        await super().disconnect()