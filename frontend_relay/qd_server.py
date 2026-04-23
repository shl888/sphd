# frontend_relay/qd_server.py
"""
前端中继服务器 - qd表示前端，避免与http_server/server.py冲突
功能：1.接收前端连接 2.推送数据 3.执行指令 4.转发统计指令
"""

import asyncio
import time
import logging
import json
import os
import base64
from typing import List, Dict, Any, Optional
from aiohttp import web
from Crypto.Cipher import AES

# ========== 处理器导入 ==========
from .stats_handler import StatsHandler
from .logs_handler import LogsHandler
# ========== 处理器导入结束 ==========

logger = logging.getLogger(__name__)


class FrontendRelayServer:
    """前端中继服务器 - 完整实现"""
    
    def __init__(self, brain_instance, port: int = 10001):
        """
        初始化前端中继服务器
        
        Args:
            brain_instance: 大脑实例引用（用于处理指令）
            port: 服务端口，默认10001（避免与现有服务冲突）
        """
        self.brain = brain_instance
        self.port = port
        
        # 从环境变量读取密文
        self._token_enc = os.getenv('FRONTEND_TOKEN')
        self.valid_token = None  # 解密后才会有值
        
        # 连接锁定
        self._connection_locked = False
        self.current_client_id = None
        
        # 失败次数限制
        self._failed_attempts: Dict[str, int] = {}
        self._max_failed_attempts = 3
        
        # WebSocket客户端管理（存储认证状态）
        self.ws_clients: List[Dict] = []  # 每个元素: {'ws': ws, 'authenticated': bool, 'client_id': str}
        
        # 基础统计
        self.stats = {
            "server_start": time.time(),
            "total_connections": 0,
            "current_connections": 0,
            "messages_broadcast": 0,
            "commands_processed": 0
        }
        
        # ========== 初始化统计处理器 ==========
        logger.info(f"📊【客户端】 正在初始化统计处理器...")
        self.stats_handler = StatsHandler(self)
        logger.info(f"✅【客户端】 统计处理器已初始化完成")
        
        # ========== 初始化日志处理器 ==========
        logger.info(f"📋【客户端】 正在初始化日志处理器...")
        self.logs_handler = LogsHandler()
        logger.info(f"✅【客户端】 日志处理器已初始化完成")
        
        # 创建aiohttp应用
        self.app = web.Application()
        self._setup_routes()
        
        # 服务器运行器
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None
        
        # ========== 密钥就绪标志 ==========
        self._keys_ready = False
        
        logger.info(f"🔄【客户端】 前端中继初始化完成，端口: {self.port}")
        logger.info(f"🔐【客户端】 等待前端连接验证...")
    
    def _setup_routes(self):
        """设置路由"""
        # WebSocket端点 - 前端数据流
        self.app.router.add_get('/ws', self._handle_websocket)
        
        # HTTP API端点 - 前端指令
        self.app.router.add_post('/api/cmd', self._handle_command)
        
        # 状态查询
        self.app.router.add_get('/status', self._handle_status)
        
        # 健康检查
        self.app.router.add_get('/health', self._handle_health)
        
        # ========== 日志接口（转发给日志处理器） ==========
        self.app.router.add_get('/api/logs/stream', self.logs_handler.stream)
        self.app.router.add_get('/api/logs/history', self.logs_handler.history)
    
    # ==================== 解密方法 ====================
    
    def _decrypt(self, ciphertext_b64: str, password: str) -> str:
        """
        用密码解密密文，返回明文
        使用 AES-256-GCM
        """
        if not ciphertext_b64:
            return None
        
        # 密码补齐到 32 字节
        key = password.encode('utf-8').ljust(32, b'\0')[:32]
        
        # 解码 Base64
        data = base64.b64decode(ciphertext_b64)
        
        # 拆分: nonce(12) + 密文 + tag(16)
        nonce = data[:12]
        tag = data[-16:]
        ciphertext = data[12:-16]
        
        # AES-256-GCM 解密
        cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
        plaintext = cipher.decrypt_and_verify(ciphertext, tag)
        
        return plaintext.decode('utf-8')
    
    # ==================== 踢掉其他连接 ====================
    
    async def _kick_all_other_clients(self, keep_client_id: str):
        """踢掉除指定客户端外的所有连接"""
        for client in self.ws_clients:
            if client['client_id'] != keep_client_id:
                try:
                    await client['ws'].close()
                except:
                    pass
    
    # ==================== 标签接收 ====================
    
    def on_keys_ready(self):
        """
        接收「密钥已就绪」标签
        由 TagDispatcher 调用
        """
        self._keys_ready = True
        logger.info("🔑【客户端】密钥已就绪，获得工作权限")
    
    # ======================================================================
    # 🏠 房间1：WebSocket 和 HTTP API 处理
    # ======================================================================
    
    async def _handle_websocket(self, request):
        """
        处理WebSocket连接
        先建立连接，等客户端发送auth消息验证
        """
        # 1. 建立连接（不验证token）
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        # 2. 记录临时连接（未认证）
        client_ip = request.remote
        client_id = f"qd_{client_ip}_{int(time.time())}"
        
        # ===== 检查是否已锁定 =====
        if self._connection_locked:
            logger.warning(f"🔒【客户端】已有活动连接，拒绝新连接: {client_id}")
            await ws.send_json({"type": "error", "error": "已有其他客户端连接"})
            await ws.close()
            return ws
        # ========================
        
        client_info = {
            'ws': ws,
            'authenticated': False,
            'client_id': client_id,
            'ip': client_ip,
            'password': None
        }
        self.ws_clients.append(client_info)
        self.stats["total_connections"] += 1
        self.stats["current_connections"] = len(self.ws_clients)
        
        logger.info(f"🔌【客户端】新连接建立，等待认证: {client_id} (当前连接数: {len(self.ws_clients)})")
        
        try:
            # 3. 等待客户端发送认证消息
            auth_timeout = 10  # 10秒内必须认证
            auth_received = False
            
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        
                        # 处理认证消息
                        if data.get('type') == 'auth':
                            password = data.get('token', '')
                            client_info['password'] = password
                            logger.info(f"🔐【客户端】收到认证请求，客户端: {client_id}")
                            
                            # ===== 检查失败次数 =====
                            failed_count = self._failed_attempts.get(client_ip, 0)
                            if failed_count >= self._max_failed_attempts:
                                logger.warning(f"🚫【客户端】IP {client_ip} 失败次数已达上限，拒绝连接")
                                await ws.send_json({"type": "error", "error": "尝试次数过多，请稍后再试"})
                                await ws.close()
                                return ws
                            # ========================
                            
                            # 检查是否有密文
                            if not self._token_enc:
                                logger.error("❌【客户端】FRONTEND_TOKEN 密文未设置")
                                await ws.send_json({"type": "auth_failed", "error": "服务器配置错误"})
                                await ws.close()
                                return ws
                            
                            # 用密码尝试解密
                            try:
                                decrypted_token = self._decrypt(self._token_enc, password)
                                
                                # 解密成功！
                                self.valid_token = decrypted_token
                                self._connection_locked = True
                                self.current_client_id = client_id
                                client_info['authenticated'] = True
                                auth_received = True
                                
                                # 清除该IP的失败记录
                                if client_ip in self._failed_attempts:
                                    del self._failed_attempts[client_ip]
                                
                                # 踢掉其他连接
                                await self._kick_all_other_clients(client_id)
                                
                                # 发送认证成功
                                await ws.send_json({
                                    "type": "auth_success",
                                    "client_id": client_id,
                                    "timestamp": time.time()
                                })
                                logger.info(f"✅【客户端】客户端认证成功: {client_id}")
                                
                                # 认证成功后进入正常消息循环
                                async for msg2 in ws:
                                    if msg2.type == web.WSMsgType.TEXT:
                                        try:
                                            data2 = json.loads(msg2.data)
                                            msg_type = data2.get('type')
                                            
                                            if msg_type == 'ping':
                                                logger.debug(f"💓【客户端】收到心跳 ping，客户端: {client_id}")
                                                await ws.send_json({
                                                    "type": "pong",
                                                    "timestamp": time.time()
                                                })
                                            
                                            elif msg_type == 'order':
                                                logger.debug(f"💰【客户端】收到开仓指令，准备转发给大脑")
                                                logger.debug(f"   参数: {data2.get('data', {})}")
                                                logger.debug(f"   客户端: {client_id}")
                                                
                                                await self.brain.handle_frontend_command({
                                                    "command": "place_order",
                                                    "params": data2.get('data', {}),
                                                    "client_id": client_id
                                                })
                                                
                                                self.stats["commands_processed"] += 1
                                            
                                            elif msg_type == 'set_sl_tp':
                                                logger.debug(f"⚙️【客户端】收到止损止盈指令，准备转发给大脑")
                                                logger.debug(f"   参数: {data2.get('data', {})}")
                                                logger.debug(f"   客户端: {client_id}")
                                                
                                                await self.brain.handle_frontend_command({
                                                    "command": "set_sl_tp",
                                                    "params": data2.get('data', {}),
                                                    "client_id": client_id
                                                })
                                                
                                                self.stats["commands_processed"] += 1
                                            
                                            elif msg_type == 'close_position':
                                                logger.debug(f"🔚【客户端】收到平仓指令，准备转发给大脑")
                                                logger.debug(f"   参数: {data2.get('data', {})}")
                                                logger.debug(f"   客户端: {client_id}")
                                                
                                                await self.brain.handle_frontend_command({
                                                    "command": "close_position",
                                                    "params": data2.get('data', {}),
                                                    "client_id": client_id
                                                })
                                                
                                                self.stats["commands_processed"] += 1
                                            
                                            elif msg_type == 'config':
                                                logger.info(f"💾【客户端】收到配置指令，转发给 配置处理器")
                                                logger.debug(f"   客户端: {client_id}")
                                                
                                                from smart_brain import get_config_handler
                                                config_handler = get_config_handler()
                                                if config_handler:
                                                    config_handler.set_config(data2.get('data', ''))
                                                else:
                                                    logger.error(f"❌【客户端】配置处理器 实例未初始化")
                                            
                                            elif msg_type == 'set_trade_mode':
                                                logger.debug(f"🎮【客户端】收到交易模式指令，转发给大脑")
                                                logger.debug(f"   模式: {data2.get('mode')}")
                                                logger.debug(f"   客户端: {client_id}")
                                                
                                                await self.brain.handle_frontend_command({
                                                    "command": "set_trade_mode",
                                                    "params": {"mode": data2.get('mode', '')},
                                                    "client_id": client_id
                                                })
                                            
                                            # ========== 统计指令处理 ==========
                                            elif msg_type == 'get_stats':
                                                logger.debug(f"📊【客户端】收到统计指令")
                                                logger.debug(f"   请求参数: {data2}")
                                                logger.debug(f"   客户端: {client_id}")
                                                
                                                # ===== 检查密钥是否就绪 =====
                                                if not self._keys_ready:
                                                    logger.warning("⏳【客户端】密钥未就绪，无法处理统计请求")
                                                    await ws.send_json({
                                                        "type": "stats_result",
                                                        "data": {
                                                            'okx_trades': 0, 'okx_avg_margin': 0.0, 'okx_total_fee': 0.0,
                                                            'okx_total_funding': 0.0, 'okx_total_profit': 0.0,
                                                            'binance_trades': 0, 'binance_avg_margin': 0.0, 'binance_total_fee': 0.0,
                                                            'binance_total_funding': 0.0, 'binance_total_profit': 0.0,
                                                            'net_fee': 0.0, 'net_funding': 0.0, 'net_profit': 0.0,
                                                            'net_pnl': 0.0, 'net_pnl_rate': 0.0,
                                                        },
                                                        "timestamp": time.time()
                                                    })
                                                else:
                                                    logger.debug(f"📤【客户端】转发统计指令给 StatsHandler 处理...")
                                                    await self.stats_handler.handle(data2)
                                                    logger.info(f"✅【客户端】统计指令已转发给 StatsHandler")
                                                # =================================
                                                
                                                continue
                                            # ========== 统计指令处理结束 ==========
                                            
                                            # ========== 信息标签处理 ==========
                                            elif 'info' in data2:
                                                logger.debug(f"🏷️【客户端】收到信息标签: {data2.get('info')}")
                                                if hasattr(self.brain, 'tag_dispatcher') and self.brain.tag_dispatcher:
                                                    await self.brain.tag_dispatcher.receive(data2)
                                                    logger.info(f"📤【客户端】信息标签已转发给标签调度器: {data2.get('info')}")
                                                else:
                                                    logger.warning(f"⚠️【客户端】标签调度器未初始化，标签丢弃: {data2.get('info')}")
                                            # ========== 信息标签处理结束 ==========
                                            
                                            else:
                                                logger.debug(f"📨【客户端】收到未知消息类型: {msg_type}，客户端: {client_id}")
                                                
                                        except Exception as e:
                                            logger.error(f"❌【客户端】处理消息异常，客户端: {client_id}, 错误: {e}", exc_info=True)
                                    
                                    elif msg2.type in (web.WSMsgType.CLOSE, web.WSMsgType.ERROR):
                                        logger.info(f"🔌【客户端】WebSocket 连接关闭或出错，客户端: {client_id}")
                                        break
                                break
                                
                            except Exception as e:
                                # 解密失败，密码错误
                                self._failed_attempts[client_ip] = failed_count + 1
                                logger.warning(f"❌【客户端】密码错误 ({self._failed_attempts[client_ip]}/{self._max_failed_attempts})，客户端: {client_id}")
                                await ws.send_json({
                                    "type": "auth_failed",
                                    "error": "Invalid token",
                                    "timestamp": time.time()
                                })
                                await ws.close()
                                return ws
                        else:
                            # 未认证前收到其他消息，要求先认证
                            logger.warning(f"⚠️【客户端】客户端未认证就发送其他消息，客户端: {client_id}")
                            await ws.send_json({
                                "type": "error",
                                "error": "Please authenticate first. Send: {'type':'auth', 'token':'your_token'}",
                                "timestamp": time.time()
                            })
                            
                    except json.JSONDecodeError:
                        logger.warning(f"⚠️【客户端】收到无效 JSON，客户端: {client_id}")
                        pass
                        
                elif msg.type in (web.WSMsgType.CLOSE, web.WSMsgType.ERROR):
                    logger.info(f"🔌【客户端】WebSocket 连接在认证前关闭或出错")
                    break
            
            # 认证超时处理
            if not auth_received and client_info in self.ws_clients:
                logger.warning(f"⏰【客户端】客户端认证超时（{auth_timeout}秒）: {client_id}")
                try:
                    await ws.send_json({
                        "type": "auth_timeout",
                        "error": "Authentication timeout",
                        "timestamp": time.time()
                    })
                except:
                    pass
                
        except Exception as e:
            logger.debug(f"WebSocket异常 {client_id}: {e}")
        
        finally:
            # 4. 清理连接
            if client_info in self.ws_clients:
                self.ws_clients.remove(client_info)
                self.stats["current_connections"] = len(self.ws_clients)
                
                # 如果是当前活动连接断开，解锁
                if client_id == self.current_client_id:
                    self._connection_locked = False
                    self.current_client_id = None
                    self.valid_token = None
                    logger.info("🔓【客户端】连接断开，已解锁，可接受新连接")
                
                logger.info(f"🔌【客户端】连接断开，已清理: {client_id} (剩余连接数: {len(self.ws_clients)})")
        
        return ws
    
    async def _handle_command(self, request):
        """处理前端HTTP指令"""
        try:
            # 1. 验证token（HTTP指令需要验证）
            token = self._get_token_from_request(request)
            if not self._validate_token(token):
                logger.warning(f"⚠️【客户端】HTTP 指令认证失败")
                return web.json_response({
                    "success": False,
                    "error": "认证失败"
                }, status=401)
            
            # 2. 解析请求
            data = await request.json()
            command = data.get('command', '')
            params = data.get('params', {})
            client_id = data.get('client_id', 'unknown')
            
            logger.info(f"📨【客户端】收到前端HTTP指令: {command}，来源: {client_id}")
            logger.debug(f"   参数: {params}")
            
            # 3. 调用大脑处理指令
            if not self.brain:
                logger.error(f"❌【客户端】大脑实例未连接，无法处理指令")
                return web.json_response({
                    "success": False,
                    "error": "大脑实例未连接"
                }, status=503)
            
            await self.brain.handle_frontend_command({
                "command": command,
                "params": params,
                "client_id": client_id
            })
            
            # 4. 更新统计
            self.stats["commands_processed"] += 1
            
            # 5. 返回结果
            logger.info(f"✅【客户端】HTTP 指令处理完成: {command}")
            return web.json_response({
                "success": True,
                "command": command,
                "timestamp": time.time()
            })
            
        except json.JSONDecodeError:
            logger.error(f"❌【客户端】HTTP 请求 JSON 解析失败")
            return web.json_response({
                "success": False,
                "error": "无效的JSON格式"
            }, status=400)
        except Exception as e:
            logger.error(f"❌【客户端】处理前端指令失败: {e}", exc_info=True)
            return web.json_response({
                "success": False,
                "error": str(e)
            }, status=500)
    
    async def _handle_status(self, request):
        """状态查询接口"""
        uptime = time.time() - self.stats["server_start"]
        
        # 统计已认证和未认证的客户端
        authenticated = len([c for c in self.ws_clients if c.get('authenticated', False)])
        unauthenticated = len(self.ws_clients) - authenticated
        
        logger.debug(f"📊【客户端】状态查询，已认证: {authenticated}，未认证: {unauthenticated}")
        
        return web.json_response({
            "service": "frontend_relay",
            "status": "running",
            "port": self.port,
            "uptime_seconds": uptime,
            "uptime_human": f"{int(uptime // 3600)}小时{int((uptime % 3600) // 60)}分钟",
            "stats": self.stats,
            "clients": {
                "total": len(self.ws_clients),
                "authenticated": authenticated,
                "unauthenticated": unauthenticated
            },
            "connection_locked": self._connection_locked,
            "current_client": self.current_client_id,
            "auth_enabled": True,
            "timestamp": time.time()
        })
    
    async def _handle_health(self, request):
        """健康检查（极简）"""
        return web.json_response({
            "status": "healthy",
            "service": "frontend_relay",
            "timestamp": time.time()
        })

    # ======================================================================
    # 🏠 房间2：数据广播方法
    # ======================================================================
    
    async def broadcast_market_data(self, market_data):
        """广播市场数据到所有前端"""
        logger.debug(f"📤【客户端】【市场数据推送】开始推送，客户端数: {len(self.ws_clients)}")
        
        if not self.ws_clients:
            logger.debug(f"⚠️【客户端】【市场数据推送】没有客户端连接，跳过推送")
            return
        
        message = {
            "type": "market_data",
            "data": market_data,
            "timestamp": time.time()
        }
        
        await self._safe_broadcast(message)
    
    async def broadcast_private_data(self, private_data):
        """广播私人数据到所有前端"""
        logger.debug(f"📤【客户端】【私人数据推送】开始推送，客户端数: {len(self.ws_clients)}")
        
        if not self.ws_clients:
            logger.debug(f"⚠️【客户端】【私人数据推送】没有客户端连接，跳过推送")
            return
        
        message = {
            "type": "private_data",
            "data": private_data,
            "timestamp": time.time()
        }
        
        await self._safe_broadcast(message)
    
    async def broadcast_reference_data(self, reference_data):
        """广播面值数据到所有前端"""
        logger.debug(f"📤【客户端】【面值数据推送】开始推送，客户端数: {len(self.ws_clients)}")
        
        if not self.ws_clients:
            logger.debug(f"⚠️【客户端】【面值数据推送】没有客户端连接，跳过推送")
            return
        
        message = {
            "type": "reference_data",
            "data": reference_data,
            "timestamp": time.time()
        }
        
        await self._safe_broadcast(message)
    
    async def broadcast_system_status(self, status_data):
        """广播系统状态到所有前端"""
        logger.debug(f"📤【客户端】【系统状态推送】开始推送，客户端数: {len(self.ws_clients)}")
        
        if not self.ws_clients:
            logger.debug(f"⚠️【客户端】【系统状态推送】没有客户端连接，跳过推送")
            return
        
        message = {
            "type": "system_status",
            "data": status_data,
            "timestamp": time.time()
        }
        
        await self._safe_broadcast(message)
    
    async def broadcast_execution_results(self, results):
        """广播订单执行结果到前端"""
        logger.debug(f"📥【客户端收到】results 数量: {len(results)}")
        for i, res in enumerate(results):
            logger.debug(f"📥【客户端收到】第{i+1}条: exchange={res.get('exchange')}, type={res.get('type')}, success={res.get('success')}")
        
        if not self.ws_clients:
            logger.debug(f"⚠️【客户端】【执行结果推送】没有客户端连接，跳过推送")
            return
        
        message = {
            "type": "execution_results",
            "data": results,
            "timestamp": time.time()
        }
        
        logger.debug(f"📤【客户端发送】准备广播: type={message['type']}, data数量={len(message['data'])}")
        for i, res in enumerate(message['data']):
            logger.debug(f"📤【客户端发送】第{i+1}条: exchange={res.get('exchange')}, type={res.get('type')}")
        
        await self._safe_broadcast(message)
    
    async def broadcast_binance_ticker_24hr(self, ticker_data: Dict):
        """广播币安24小时涨跌幅数据到所有前端"""
        logger.debug(f"📤【客户端】【涨跌幅数据推送】开始推送，客户端数: {len(self.ws_clients)}")
        
        if not self.ws_clients:
            logger.debug(f"⚠️【客户端】【涨跌幅数据推送】没有客户端连接，跳过推送")
            return
        
        message = {
            "type": "binance_ticker_24hr",
            "data": ticker_data,
            "timestamp": time.time()
        }
        
        await self._safe_broadcast(message)
    
    # ========== 统计结果推送方法 ==========
    async def broadcast_stats_result(self, stats_data: Dict):
        """
        推送统计结果到前端
        
        这个方法由 StatsHandler 调用，qd_server 只负责把数据推给前端
        qd_server 不关心统计数据是怎么算出来的，纯粹是个推送工具
        
        Args:
            stats_data: StatsHandler 计算好的统计结果，包含净盈亏、交易次数等
        """
        logger.debug(f"📊【客户端】【统计结果推送】StatsHandler 调用推送方法")
        logger.debug(f"📊【客户端】【统计结果推送】当前已认证客户端数: {len([c for c in self.ws_clients if c.get('authenticated', False)])}")
        
        if not self.ws_clients:
            logger.warning(f"⚠️【客户端】【统计结果推送】没有客户端连接，跳过推送")
            return
        
        # 打印推送的数据摘要
        net_pnl = stats_data.get('net_pnl', 0.0)
        net_pnl_rate = stats_data.get('net_pnl_rate', 0.0)
        okx_trades = stats_data.get('okx_trades', 0)
        binance_trades = stats_data.get('binance_trades', 0)
        logger.debug(f"📊【客户端】【统计结果推送】数据摘要: 净盈亏={net_pnl}, 净盈亏率={net_pnl_rate}%, 欧易交易={okx_trades}笔, 币安交易={binance_trades}笔")
        
        message = {
            "type": "stats_result",
            "data": stats_data,
            "timestamp": time.time()
        }
        
        logger.debug(f"📤【客户端】【统计结果推送】开始广播给前端...")
        await self._safe_broadcast(message)
        logger.debug(f"✅【客户端】【统计结果推送】广播完成")
    
    async def _safe_broadcast(self, message):
        """
        安全广播 - 只推送给已认证的客户端，带详细日志
        """
        # 过滤出已认证的客户端
        authenticated_clients = [c for c in self.ws_clients if c.get('authenticated', False)]
        
        if not authenticated_clients:
            logger.debug(f"⚠️【客户端】【广播】没有已认证的客户端，跳过")
            return
        
        message_type = message.get('type', 'unknown')
        logger.debug(f"🔥【客户端】【广播开始】类型: {message_type}, 已认证客户端数: {len(authenticated_clients)}")
        
        dead_clients = []
        message_json = json.dumps(message, default=str)
        
        for client in authenticated_clients:
            ws = client['ws']
            client_id = client.get('client_id', 'unknown')
            try:
                await ws.send_str(message_json)
                logger.debug(f"✅【客户端】【广播成功】类型: {message_type}, 客户端: {client_id}")
            except Exception as e:
                logger.error(f"❌【客户端】【广播失败】类型: {message_type}, 客户端: {client_id}, 错误: {e}")
                dead_clients.append(client)
        
        # 清理死连接
        if dead_clients:
            logger.info(f"🧹【客户端】【清理连接】清理 {len(dead_clients)} 个死连接")
            for client in dead_clients:
                if client in self.ws_clients:
                    self.ws_clients.remove(client)
            self.stats["current_connections"] = len(self.ws_clients)
        
        self.stats["messages_broadcast"] += len(authenticated_clients) - len(dead_clients)
        logger.debug(f"✅【客户端】【广播完成】类型: {message_type}, 成功发送到 {len(authenticated_clients) - len(dead_clients)} 个客户端")

    # ======================================================================
    # 🏠 房间3：辅助方法和服务器控制
    # ======================================================================
    
    def _validate_token(self, token: str) -> bool:
        """验证token"""
        if not token:
            logger.debug(f"🔐【token验证】token 为空")
            return False
        
        if not self.valid_token:
            logger.debug(f"🔐【token验证】valid_token 未设置")
            return False
        
        # 解密后的密钥
        is_valid = token == self.valid_token
        logger.debug(f"🔐【token验证】验证结果: {is_valid}")
        return is_valid
    
    def _get_token_from_request(self, request) -> str:
        """从HTTP请求获取token"""
        # 1. 检查Authorization头
        auth_header = request.headers.get('Authorization', '')
        if auth_header.startswith('Bearer '):
            token = auth_header[7:]
            logger.debug(f"🔐【获取token】从 Authorization 头获取")
            return token
        
        # 2. 检查查询参数
        token = request.query.get('token', '')
        if token:
            logger.debug(f"🔐【获取token】从查询参数获取")
            return token
        
        logger.debug(f"🔐【获取token】未找到 token")
        return ''
    
    async def start(self):
        """启动前端中继服务器"""
        try:
            logger.info(f"🚀【客户端】启动前端中继服务器，端口: {self.port}")
            
            # 创建运行器
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            
            # 启动TCP站点
            self.site = web.TCPSite(self.runner, '0.0.0.0', self.port)
            await self.site.start()
            
            logger.info(f"✅【客户端】前端中继服务器启动成功")
            logger.info(f"📡【客户端】WebSocket: ws://0.0.0.0:{self.port}/ws")
            logger.info(f"📨【客户端】HTTP API: http://0.0.0.0:{self.port}/api/cmd")
            logger.info(f"📊【客户端】状态查询: http://0.0.0.0:{self.port}/status")
            logger.info(f"❤️【客户端】健康检查: http://0.0.0.0:{self.port}/health")
            logger.info(f"📋【客户端】日志流: http://0.0.0.0:{self.port}/api/logs/stream")
            logger.info(f"📋【客户端】历史日志: http://0.0.0.0:{self.port}/api/logs/history")
            logger.info(f"📊【客户端】统计功能: 已启用（通过 WebSocket get_stats 指令）")
            logger.info(f"🔐【客户端】认证方式: 连接WebSocket后发送 {{'type':'auth', 'token':'YOUR_TOKEN'}}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌【客户端】启动前端中继服务器失败: {e}", exc_info=True)
            return False
    
    async def stop(self):
        """停止前端中继服务器"""
        logger.info("🛑【客户端】停止前端中继服务器...")
        
        # 关闭所有WebSocket连接
        for client in self.ws_clients:
            try:
                await client['ws'].close()
            except:
                pass
        self.ws_clients.clear()
        logger.info(f"🔌【客户端】已关闭所有 WebSocket 连接")
        
        # 停止HTTP服务器
        if self.runner:
            await self.runner.cleanup()
            self.runner = None
            self.site = None
        
        logger.info("✅【客户端】前端中继服务器已停止")
    
    def get_stats_summary(self) -> Dict[str, Any]:
        """获取统计摘要"""
        uptime = time.time() - self.stats["server_start"]
        
        authenticated = len([c for c in self.ws_clients if c.get('authenticated', False)])
        
        return {
            "running": self.runner is not None,
            "port": self.port,
            "clients_connected": len(self.ws_clients),
            "authenticated_clients": authenticated,
            "total_connections": self.stats["total_connections"],
            "messages_broadcast": self.stats["messages_broadcast"],
            "commands_processed": self.stats["commands_processed"],
            "connection_locked": self._connection_locked,
            "uptime_seconds": uptime,
            "auth_enabled": True
        }