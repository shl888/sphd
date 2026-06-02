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
    """前端中继服务器 - 完整实现（简化版）"""
    
    def __init__(self, brain_instance, port: int = 10001):
        """
        初始化前端中继服务器
        
        Args:
            brain_instance: 大脑实例引用（用于处理指令）
            port: 服务端口，默认10001
        """
        self.brain = brain_instance
        self.port = port
        
        # 从环境变量读取密文
        self._token_enc = os.getenv('FRONTEND_TOKEN')
        
        # 简化：只保留当前活动连接
        self.current_ws = None
        self.current_client_id = None
        
        # 失败次数限制（对外部攻击的基本防护）
        self._failed_attempts: Dict[str, int] = {}
        self._max_failed_attempts = 3
        
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
        
        # 启动健康检查任务
        self._health_task = None
        
        logger.info(f"🔄【客户端】 前端中继初始化完成，端口: {self.port}")
        logger.info(f"🔐【客户端】 等待前端连接验证...")
    
    def _setup_routes(self):
        """设置路由"""
        self.app.router.add_get('/ws', self._handle_websocket)
        self.app.router.add_post('/api/cmd', self._handle_command)
        self.app.router.add_get('/status', self._handle_status)
        self.app.router.add_get('/health', self._handle_health)
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
        
        key = password.encode('utf-8').ljust(32, b'\0')[:32]
        data = base64.b64decode(ciphertext_b64)
        nonce = data[:12]
        tag = data[-16:]
        ciphertext = data[12:-16]
        cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
        plaintext = cipher.decrypt_and_verify(ciphertext, tag)
        
        return plaintext.decode('utf-8')
    
    # ==================== 健康检查 ====================
    
    async def _health_check(self):
        """每30秒检查一次当前连接是否还活着"""
        while True:
            await asyncio.sleep(30)
            if self.current_ws is not None:
                try:
                    # 发送 ping 检测连接是否存活
                    await self.current_ws.send_json({"type": "ping"})
                    await asyncio.sleep(1)
                    if self.current_ws.closed:
                        logger.info("🧹【客户端】健康检查发现僵尸连接，清理")
                        self.current_ws = None
                        self.current_client_id = None
                except Exception as e:
                    logger.debug(f"健康检查异常: {e}")
                    self.current_ws = None
                    self.current_client_id = None
    
    # ==================== 标签接收 ====================
    
    def on_keys_ready(self):
        self._keys_ready = True
        logger.info("🔑【客户端】密钥已就绪，获得工作权限")
    
    # ======================================================================
    # 🏠 房间1：WebSocket 和 HTTP API 处理
    # ======================================================================
    
    async def _handle_websocket(self, request):
        """
        处理WebSocket连接 - 简化版
        """
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        client_ip = request.remote
        client_id = f"qd_{client_ip}_{int(time.time())}"
        
        logger.info(f"🔌【客户端】新连接建立，等待认证: {client_id}")
        
        # 1. 等待认证（5秒超时）
        try:
            msg = await ws.receive(timeout=5)
            if msg.type != web.WSMsgType.TEXT:
                await ws.close()
                return ws
            
            data = json.loads(msg.data)
            if data.get('type') != 'auth':
                await ws.send_json({"type": "error", "error": "先发送 auth"})
                await ws.close()
                return ws
            
            password = data.get('token', '')
            
            # 2. 失败次数限制
            failed_count = self._failed_attempts.get(client_ip, 0)
            if failed_count >= self._max_failed_attempts:
                logger.warning(f"🚫【客户端】IP {client_ip} 失败次数已达上限，拒绝连接")
                await ws.send_json({"type": "error", "error": "尝试次数过多"})
                await ws.close()
                return ws
            
            # 3. 验证密码
            if not self._token_enc:
                logger.error("❌ FRONTEND_TOKEN 未设置")
                await ws.send_json({"type": "auth_failed", "error": "服务器配置错误"})
                await ws.close()
                return ws
            
            try:
                decrypted_token = self._decrypt(self._token_enc, password)
                # 解密成功，密码正确
                
                # 4. 踢掉旧连接（如果存在）
                if self.current_ws is not None:
                    try:
                        await self.current_ws.close()
                    except:
                        pass
                    logger.info(f"🔌【客户端】踢掉旧连接: {self.current_client_id}")
                
                # 5. 接管新连接
                self.current_ws = ws
                self.current_client_id = client_id
                
                # 清除该IP的失败记录
                if client_ip in self._failed_attempts:
                    del self._failed_attempts[client_ip]
                
                # 6. 发送认证成功
                await ws.send_json({
                    "type": "auth_success",
                    "client_id": client_id,
                    "timestamp": time.time()
                })
                logger.info(f"✅【客户端】客户端认证成功: {client_id}")
                
                # 7. 消息循环
                async for msg2 in ws:
                    if msg2.type == web.WSMsgType.TEXT:
                        await self._handle_message(ws, msg2.data, client_id)
                    elif msg2.type in (web.WSMsgType.CLOSE, web.WSMsgType.ERROR):
                        break
                        
            except Exception as e:
                # 密码错误
                self._failed_attempts[client_ip] = failed_count + 1
                logger.warning(f"❌【客户端】密码错误 ({self._failed_attempts[client_ip]}/{self._max_failed_attempts})")
                await ws.send_json({"type": "auth_failed", "error": "Invalid token"})
                await ws.close()
                return ws
                
        except asyncio.TimeoutError:
            logger.warning(f"⏰【客户端】认证超时（5秒）: {client_id}")
        except Exception as e:
            logger.debug(f"WebSocket异常: {e}")
        finally:
            # 8. 清理：如果当前断开的是活动连接，清空状态
            if self.current_ws == ws:
                self.current_ws = None
                self.current_client_id = None
            self.stats["current_connections"] = len([1 for _ in [self.current_ws] if self.current_ws is not None])
            logger.info(f"🔌【客户端】连接断开，已清理: {client_id}")
        
        return ws
    
    async def _handle_message(self, ws, message_data: str, client_id: str):
        """处理前端发来的消息"""
        try:
            data = json.loads(message_data)
            msg_type = data.get('type')
            
            if msg_type == 'ping':
                await ws.send_json({"type": "pong", "timestamp": time.time()})
            
            elif msg_type == 'order':
                logger.debug(f"💰【客户端】收到开仓指令")
                await self.brain.handle_frontend_command({
                    "command": "place_order",
                    "params": data.get('data', {}),
                    "client_id": client_id
                })
                self.stats["commands_processed"] += 1
            
            elif msg_type == 'set_sl_tp':
                logger.debug(f"⚙️【客户端】收到止损止盈指令")
                await self.brain.handle_frontend_command({
                    "command": "set_sl_tp",
                    "params": data.get('data', {}),
                    "client_id": client_id
                })
                self.stats["commands_processed"] += 1
            
            elif msg_type == 'close_position':
                logger.debug(f"🔚【客户端】收到平仓指令")
                await self.brain.handle_frontend_command({
                    "command": "close_position",
                    "params": data.get('data', {}),
                    "client_id": client_id
                })
                self.stats["commands_processed"] += 1
            
            elif msg_type == 'config':
                logger.info(f"💾【客户端】收到配置指令")
                from smart_brain import get_config_handler
                config_handler = get_config_handler()
                if config_handler:
                    config_handler.set_config(data.get('data', ''))
            
            elif msg_type == 'set_trade_mode':
                logger.debug(f"🎮【客户端】收到交易模式指令")
                await self.brain.handle_frontend_command({
                    "command": "set_trade_mode",
                    "params": {"mode": data.get('mode', '')},
                    "client_id": client_id
                })
            
            elif msg_type == 'get_stats':
                if not self._keys_ready:
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
                    await self.stats_handler.handle(data)
            
            elif 'info' in data:
                if hasattr(self.brain, 'tag_dispatcher') and self.brain.tag_dispatcher:
                    await self.brain.tag_dispatcher.receive(data)
            
            else:
                logger.debug(f"📨【客户端】收到未知消息类型: {msg_type}")
                
        except Exception as e:
            logger.error(f"❌ 处理消息异常: {e}", exc_info=True)
    
    async def _handle_command(self, request):
        """处理前端HTTP指令（保持不变）"""
        try:
            # 简化：不再验证 token（WebSocket 已经验证过了）
            data = await request.json()
            command = data.get('command', '')
            params = data.get('params', {})
            client_id = data.get('client_id', 'unknown')
            
            logger.info(f"📨【客户端】收到前端HTTP指令: {command}")
            
            if not self.brain:
                return web.json_response({"success": False, "error": "大脑未连接"}, status=503)
            
            await self.brain.handle_frontend_command({
                "command": command,
                "params": params,
                "client_id": client_id
            })
            
            self.stats["commands_processed"] += 1
            
            return web.json_response({"success": True, "command": command, "timestamp": time.time()})
            
        except Exception as e:
            logger.error(f"❌ 处理前端指令失败: {e}")
            return web.json_response({"success": False, "error": str(e)}, status=500)
    
    async def _handle_status(self, request):
        """状态查询接口"""
        uptime = time.time() - self.stats["server_start"]
        
        return web.json_response({
            "service": "frontend_relay",
            "status": "running",
            "port": self.port,
            "uptime_seconds": uptime,
            "uptime_human": f"{int(uptime // 3600)}小时{int((uptime % 3600) // 60)}分钟",
            "stats": self.stats,
            "clients": {
                "total": 1 if self.current_ws is not None else 0,
                "authenticated": 1 if self.current_ws is not None else 0,
                "unauthenticated": 0
            },
            "current_client": self.current_client_id,
            "timestamp": time.time()
        })
    
    async def _handle_health(self, request):
        return web.json_response({"status": "healthy", "service": "frontend_relay"})

    # ======================================================================
    # 🏠 房间2：数据广播方法（保持不变）
    # ======================================================================
    
    async def broadcast_market_data(self, market_data):
        if self.current_ws is None:
            return
        try:
            await self.current_ws.send_json({
                "type": "market_data",
                "data": market_data,
                "timestamp": time.time()
            })
        except Exception as e:
            logger.error(f"❌ 广播失败: {e}")
            self.current_ws = None
    
    async def broadcast_private_data(self, private_data):
        if self.current_ws is None:
            return
        try:
            await self.current_ws.send_json({
                "type": "private_data",
                "data": private_data,
                "timestamp": time.time()
            })
        except Exception:
            self.current_ws = None
    
    async def broadcast_reference_data(self, reference_data):
        if self.current_ws is None:
            return
        try:
            await self.current_ws.send_json({
                "type": "reference_data",
                "data": reference_data,
                "timestamp": time.time()
            })
        except Exception:
            self.current_ws = None
    
    async def broadcast_system_status(self, status_data):
        if self.current_ws is None:
            return
        try:
            await self.current_ws.send_json({
                "type": "system_status",
                "data": status_data,
                "timestamp": time.time()
            })
        except Exception:
            self.current_ws = None
    
    async def broadcast_execution_results(self, results):
        if self.current_ws is None:
            return
        try:
            await self.current_ws.send_json({
                "type": "execution_results",
                "data": results,
                "timestamp": time.time()
            })
        except Exception:
            self.current_ws = None
    
    async def broadcast_binance_ticker_24hr(self, ticker_data: Dict):
        if self.current_ws is None:
            return
        try:
            await self.current_ws.send_json({
                "type": "binance_ticker_24hr",
                "data": ticker_data,
                "timestamp": time.time()
            })
        except Exception:
            self.current_ws = None
    
    async def broadcast_stats_result(self, stats_data: Dict):
        if self.current_ws is None:
            return
        try:
            await self.current_ws.send_json({
                "type": "stats_result",
                "data": stats_data,
                "timestamp": time.time()
            })
        except Exception:
            self.current_ws = None
    
    async def _safe_broadcast(self, message):
        if self.current_ws is None:
            return
        try:
            await self.current_ws.send_json(message)
        except Exception:
            self.current_ws = None

    # ======================================================================
    # 🏠 房间3：辅助方法和服务器控制
    # ======================================================================
    
    async def start(self):
        try:
            logger.info(f"🚀【客户端】启动前端中继服务器，端口: {self.port}")
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            self.site = web.TCPSite(self.runner, '0.0.0.0', self.port)
            await self.site.start()
            
            # 启动健康检查任务
            self._health_task = asyncio.create_task(self._health_check())
            
            logger.info(f"✅【客户端】前端中继服务器启动成功")
            return True
        except Exception as e:
            logger.error(f"❌ 启动失败: {e}")
            return False
    
    async def stop(self):
        logger.info("🛑【客户端】停止前端中继服务器...")
        if self._health_task:
            self._health_task.cancel()
        if self.current_ws is not None:
            try:
                await self.current_ws.close()
            except:
                pass
        if self.runner:
            await self.runner.cleanup()
        logger.info("✅【客户端】前端中继服务器已停止")
    
    def get_stats_summary(self) -> Dict[str, Any]:
        return {
            "running": self.runner is not None,
            "port": self.port,
            "clients_connected": 1 if self.current_ws is not None else 0,
            "authenticated_clients": 1 if self.current_ws is not None else 0,
            "total_connections": self.stats["total_connections"],
            "messages_broadcast": self.stats["messages_broadcast"],
            "commands_processed": self.stats["commands_processed"],
            "uptime_seconds": time.time() - self.stats["server_start"]
        }