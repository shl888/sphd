#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# frontend_relay/qd_server.py
"""
前端中继服务器 - qd表示前端，避免与http_server/server.py冲突
功能：1.接收前端连接 2.推送数据 3.执行指令 4.提供容器日志查询（新增）
"""

import asyncio
import time
import logging
import json
import os
import subprocess
import shlex
from typing import List, Dict, Any, Optional
from aiohttp import web

logger = logging.getLogger(__name__)


class FrontendRelayServer:
    """前端中继服务器 - 完整实现"""
    
    def __init__(self, brain_instance, stats_handler_instance, port: int = 10001):
        """
        初始化前端中继服务器
        
        Args:
            brain_instance: 大脑实例引用（用于处理指令）
            stats_handler_instance: 统计处理器实例引用
            port: 服务端口，默认10001（避免与现有服务冲突）
        """
        self.brain = brain_instance
        self.stats_handler = stats_handler_instance
        self.port = port
        
        # 从环境变量读取密钥
        self.valid_token = os.getenv('FRONTEND_TOKEN', '')
        if not self.valid_token:
            logger.warning(f"⚠️【客户端】 FRONTEND_TOKEN未设置，使用默认密钥（不安全）")
            self.valid_token = 'default_token_change_me'
        
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
        
        # 创建aiohttp应用
        self.app = web.Application()
        self._setup_routes()
        
        # 服务器运行器
        self.runner: Optional[web.AppRunner] = None
        self.site: Optional[web.TCPSite] = None
        
        logger.info(f"🔄【客户端】 前端中继初始化完成，端口: {self.port}")
        logger.info(f"🔐【客户端】 密钥验证已启用（连接后发送auth消息）")
    
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
        
        # 日志接口
        self.app.router.add_get('/api/logs/stream', self._handle_logs_stream)
        self.app.router.add_get('/api/logs/history', self._handle_logs_history)
    
    # ======================================================================
    # 🏠 房间1：WebSocket 和 HTTP API 处理
    # ======================================================================
    
    async def _handle_websocket(self, request):
        """
        处理WebSocket连接
        先建立连接，等客户端发送auth消息验证
        """
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        
        client_ip = request.remote
        client_id = f"qd_{client_ip}_{int(time.time())}"
        client_info = {
            'ws': ws,
            'authenticated': False,
            'client_id': client_id,
            'ip': client_ip
        }
        self.ws_clients.append(client_info)
        self.stats["total_connections"] += 1
        self.stats["current_connections"] = len(self.ws_clients)
        
        logger.info(f"🔌【客户端】新连接建立，等待认证: {client_id} (当前: {len(self.ws_clients)}个)")
        
        try:
            auth_received = False
            
            async for msg in ws:
                if msg.type == web.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        
                        if data.get('type') == 'auth':
                            token = data.get('token', '')
                            if self._validate_token(token):
                                client_info['authenticated'] = True
                                auth_received = True
                                
                                await ws.send_json({
                                    "type": "auth_success",
                                    "client_id": client_id,
                                    "timestamp": time.time()
                                })
                                logger.info(f"✅【客户端】客户端认证成功: {client_id}")
                                
                                async for msg2 in ws:
                                    if msg2.type == web.WSMsgType.TEXT:
                                        try:
                                            data2 = json.loads(msg2.data)
                                            msg_type = data2.get('type')
                                            
                                            if msg_type == 'ping':
                                                await ws.send_json({
                                                    "type": "pong",
                                                    "timestamp": time.time()
                                                })
                                            
                                            elif msg_type == 'order':
                                                logger.debug(f"💰【客户端】收到开仓指令，准备转发给大脑")
                                                await self.brain.handle_frontend_command({
                                                    "command": "place_order",
                                                    "params": data2.get('data', {}),
                                                    "client_id": client_id
                                                })
                                                self.stats["commands_processed"] += 1
                                            
                                            elif msg_type == 'set_sl_tp':
                                                logger.debug(f"⚙️【客户端】收到止损止盈指令，准备转发给大脑")
                                                await self.brain.handle_frontend_command({
                                                    "command": "set_sl_tp",
                                                    "params": data2.get('data', {}),
                                                    "client_id": client_id
                                                })
                                                self.stats["commands_processed"] += 1
                                            
                                            elif msg_type == 'close_position':
                                                logger.debug(f"🔚【客户端】收到平仓指令，准备转发给大脑")
                                                await self.brain.handle_frontend_command({
                                                    "command": "close_position",
                                                    "params": data2.get('data', {}),
                                                    "client_id": client_id
                                                })
                                                self.stats["commands_processed"] += 1
                                            
                                            elif msg_type == 'config':
                                                logger.debug(f"💾【客户端】收到配置指令，转发给大脑")
                                                await self.brain.handle_frontend_command({
                                                    "command": "save_config",
                                                    "params": {"config_data": data2.get('data', '')},
                                                    "client_id": client_id
                                                })
                                            
                                            elif msg_type == 'set_trade_mode':
                                                logger.debug(f"🎮【客户端】收到交易模式指令，转发给大脑")
                                                await self.brain.handle_frontend_command({
                                                    "command": "set_trade_mode",
                                                    "params": {"mode": data2.get('mode', '')},
                                                    "client_id": client_id
                                                })
                                            
                                            elif msg_type == 'get_stats':
                                                logger.info(f"📊【客户端】收到统计指令数据，转发给 stats_handler")
                                                await self.stats_handler.handle_stats_command({
                                                    "command": "get_stats",
                                                    "params": data2.get('params', {}),
                                                    "client_id": client_id,
                                                    "ws": ws
                                                })
                                                self.stats["commands_processed"] += 1
                                            
                                            else:
                                                logger.debug(f"📨【客户端】收到未知消息类型: {msg_type}")
                                                
                                        except Exception as e:
                                            logger.error(f"❌【客户端】处理消息异常: {e}")
                                    
                                    elif msg2.type in (web.WSMsgType.CLOSE, web.WSMsgType.ERROR):
                                        break
                                break
                            else:
                                await ws.send_json({
                                    "type": "auth_failed",
                                    "error": "Invalid token",
                                    "timestamp": time.time()
                                })
                                logger.warning(f"📛【客户端】客户端认证失败: {client_id}")
                                break
                        else:
                            await ws.send_json({
                                "type": "error",
                                "error": "Please authenticate first. Send: {'type':'auth', 'token':'your_token'}",
                                "timestamp": time.time()
                            })
                            
                    except json.JSONDecodeError:
                        pass
                        
                elif msg.type in (web.WSMsgType.CLOSE, web.WSMsgType.ERROR):
                    break
            
            if not auth_received and client_info in self.ws_clients:
                logger.warning(f"⏰【客户端】客户端认证超时: {client_id}")
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
            if client_info in self.ws_clients:
                self.ws_clients.remove(client_info)
                self.stats["current_connections"] = len(self.ws_clients)
                logger.info(f"❌【客户端】连接断开: {client_id} (剩余: {len(self.ws_clients)}个)")
        
        return ws
    
    async def _handle_command(self, request):
        """处理前端HTTP指令"""
        try:
            token = self._get_token_from_request(request)
            if not self._validate_token(token):
                return web.json_response({
                    "success": False,
                    "error": "认证失败"
                }, status=401)
            
            data = await request.json()
            command = data.get('command', '')
            params = data.get('params', {})
            client_id = data.get('client_id', 'unknown')
            
            logger.info(f"📨【客户端】收到前端HTTP指令: {command} from {client_id}")
            
            if not self.brain:
                return web.json_response({
                    "success": False,
                    "error": "大脑实例未连接"
                }, status=503)
            
            await self.brain.handle_frontend_command({
                "command": command,
                "params": params,
                "client_id": client_id
            })
            
            self.stats["commands_processed"] += 1
            
            return web.json_response({
                "success": True,
                "command": command,
                "timestamp": time.time()
            })
            
        except json.JSONDecodeError:
            return web.json_response({
                "success": False,
                "error": "无效的JSON格式"
            }, status=400)
        except Exception as e:
            logger.error(f"❌【客户端】处理前端指令失败: {e}")
            return web.json_response({
                "success": False,
                "error": str(e)
            }, status=500)
    
    async def _handle_status(self, request):
        """状态查询接口"""
        uptime = time.time() - self.stats["server_start"]
        
        authenticated = len([c for c in self.ws_clients if c.get('authenticated', False)])
        unauthenticated = len(self.ws_clients) - authenticated
        
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
            "auth_enabled": True,
            "timestamp": time.time()
        })
    
    async def _handle_health(self, request):
        """健康检查"""
        return web.json_response({
            "status": "healthy",
            "service": "frontend_relay",
            "timestamp": time.time()
        })


    # ======================================================================
    # 🏠 房间2：日志接口
    # ======================================================================

    async def _handle_logs_stream(self, request):
        """实时日志流"""
        if not self._is_running_in_docker():
            return web.Response(
                text="⚠️ 服务未运行在 Docker 容器中。\n请将后端部署到 Docker 容器并挂载 /var/run/docker.sock 后使用此功能。\n",
                status=503
            )
        
        tail = request.query.get('tail', '200')
        keyword = request.query.get('keyword', '').strip()
        
        try:
            tail_num = int(tail)
            if tail_num > 1000:
                tail_num = 1000
            elif tail_num < 10:
                tail_num = 10
        except ValueError:
            tail_num = 200
        
        response = web.StreamResponse()
        response.headers['Content-Type'] = 'text/plain; charset=utf-8'
        response.headers['Cache-Control'] = 'no-cache'
        response.headers['X-Accel-Buffering'] = 'no'
        await response.prepare(request)
        
        logger.info(f"📋【客户端】【日志流】开始推送，tail={tail_num}, keyword={keyword if keyword else '无'}")
        
        try:
            history_cmd = f"docker logs --tail {tail_num} {self._get_container_id()}"
            if keyword:
                history_cmd += f" | grep --line-buffered -i {shlex.quote(keyword)}"
            
            history_logs = await self._execute_docker_logs(history_cmd)
            if history_logs:
                await response.write(history_logs.encode('utf-8'))
            
            follow_cmd = f"docker logs -f --tail 0 {self._get_container_id()}"
            if keyword:
                follow_cmd += f" | grep --line-buffered -i {shlex.quote(keyword)}"
            
            process = await asyncio.create_subprocess_shell(
                follow_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                await response.write(line)
                
        except asyncio.CancelledError:
            logger.info("📋【客户端】【日志流】客户端断开连接")
            raise
        except Exception as e:
            logger.error(f"❌【日志流】推送失败: {e}")
            error_msg = f"\n[错误] 日志流中断: {e}\n"
            await response.write(error_msg.encode('utf-8'))
        finally:
            if 'process' in locals():
                try:
                    process.terminate()
                    await process.wait()
                except:
                    pass
        
        return response

    async def _handle_logs_history(self, request):
        """历史日志查询"""
        if not self._is_running_in_docker():
            return web.json_response({
                "success": False,
                "error": "服务未运行在 Docker 容器中，请部署到容器后使用",
                "hint": "需要挂载 /var/run/docker.sock"
            }, status=503)
        
        time_range = request.query.get('range', '1h')
        since = request.query.get('since', '')
        until = request.query.get('until', '')
        keyword = request.query.get('keyword', '').strip()
        limit = request.query.get('limit', '500')
        
        try:
            limit_num = int(limit)
            if limit_num > 2000:
                limit_num = 2000
        except ValueError:
            limit_num = 500
        
        cmd_parts = ["docker", "logs"]
        
        if since:
            cmd_parts.extend(["--since", shlex.quote(since)])
        else:
            cmd_parts.extend(["--since", shlex.quote(time_range)])
        
        if until:
            cmd_parts.extend(["--until", shlex.quote(until)])
        
        cmd_parts.append(self._get_container_id())
        
        base_cmd = " ".join(cmd_parts)
        
        if keyword:
            full_cmd = f"{base_cmd} 2>&1 | grep -i {shlex.quote(keyword)} | tail -n {limit_num}"
        else:
            full_cmd = f"{base_cmd} 2>&1 | tail -n {limit_num}"
        
        logger.info(f"📋【客户端】【历史日志】查询: range={time_range}, keyword={keyword if keyword else '无'}, limit={limit_num}")
        
        try:
            output = await self._execute_docker_logs(full_cmd)
            lines = output.strip().split('\n') if output.strip() else []
            
            return web.json_response({
                "success": True,
                "logs": lines,
                "total": len(lines),
                "query": {
                    "range": time_range if not since else None,
                    "since": since if since else None,
                    "until": until if until else None,
                    "keyword": keyword if keyword else None,
                    "limit": limit_num
                },
                "timestamp": time.time()
            })
            
        except Exception as e:
            logger.error(f"❌【历史日志】查询失败: {e}")
            return web.json_response({
                "success": False,
                "error": str(e),
                "logs": [],
                "total": 0
            }, status=500)

    def _is_running_in_docker(self) -> bool:
        """检测是否在 Docker 容器内运行"""
        if os.path.exists('/.dockerenv'):
            return True
        
        try:
            with open('/proc/1/cgroup', 'r') as f:
                content = f.read()
                if 'docker' in content or 'containerd' in content:
                    return True
        except:
            pass
        
        return False

    def _get_container_id(self) -> str:
        """获取当前容器的 ID"""
        try:
            with open('/etc/hostname', 'r') as f:
                return f.read().strip()
        except:
            return os.getenv('HOSTNAME', 'unknown')

    async def _execute_docker_logs(self, command: str) -> str:
        """安全执行 docker logs 命令"""
        try:
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=30.0
            )
            
            if process.returncode != 0 and process.returncode != 1:
                stderr_msg = stderr.decode('utf-8', errors='ignore')
                if "grep" not in command or process.returncode != 1:
                    logger.warning(f"⚠️ 命令执行警告 (code={process.returncode}): {stderr_msg}")
            
            return stdout.decode('utf-8', errors='ignore')
            
        except asyncio.TimeoutError:
            logger.error("❌ 命令执行超时")
            return ""
        except Exception as e:
            logger.error(f"❌ 命令执行失败: {e}")
            raise


    # ======================================================================
    # 🏠 房间3：数据广播方法
    # ======================================================================
    
    async def broadcast_market_data(self, market_data):
        """广播市场数据到所有前端"""
        if not self.ws_clients:
            return
        
        message = {
            "type": "market_data",
            "data": market_data,
            "timestamp": time.time()
        }
        
        await self._safe_broadcast(message)
    
    async def broadcast_private_data(self, private_data):
        """广播私人数据到所有前端"""
        if not self.ws_clients:
            return
        
        message = {
            "type": "private_data",
            "data": private_data,
            "timestamp": time.time()
        }
        
        await self._safe_broadcast(message)
    
    async def broadcast_reference_data(self, reference_data):
        """广播面值数据到所有前端"""
        if not self.ws_clients:
            return
        
        message = {
            "type": "reference_data",
            "data": reference_data,
            "timestamp": time.time()
        }
        
        await self._safe_broadcast(message)
    
    async def broadcast_system_status(self, status_data):
        """广播系统状态到所有前端"""
        if not self.ws_clients:
            return
        
        message = {
            "type": "system_status",
            "data": status_data,
            "timestamp": time.time()
        }
        
        await self._safe_broadcast(message)
    
    async def broadcast_execution_results(self, results):
        """广播订单执行结果到前端"""
        if not self.ws_clients:
            return
        
        message = {
            "type": "execution_results",
            "data": results,
            "timestamp": time.time()
        }
        
        await self._safe_broadcast(message)
    
    async def broadcast_binance_ticker_24hr(self, ticker_data: Dict):
        """广播币安24小时涨跌幅数据到所有前端"""
        if not self.ws_clients:
            return
        
        message = {
            "type": "binance_ticker_24hr",
            "data": ticker_data,
            "timestamp": time.time()
        }
        
        await self._safe_broadcast(message)
    
    async def _safe_broadcast(self, message):
        """安全广播 - 只推送给已认证的客户端"""
        authenticated_clients = [c for c in self.ws_clients if c.get('authenticated', False)]
        
        if not authenticated_clients:
            return
        
        message_type = message.get('type', 'unknown')
        logger.debug(f"🔥【客户端】【广播】类型: {message_type}, 已认证客户端数: {len(authenticated_clients)}")
        
        dead_clients = []
        message_json = json.dumps(message, default=str)
        
        for client in authenticated_clients:
            ws = client['ws']
            client_id = client.get('client_id', 'unknown')
            try:
                await ws.send_str(message_json)
            except Exception as e:
                logger.error(f"❌【客户端】【广播失败】类型: {message_type}, 客户端: {client_id}, 错误: {e}")
                dead_clients.append(client)
        
        if dead_clients:
            for client in dead_clients:
                if client in self.ws_clients:
                    self.ws_clients.remove(client)
            self.stats["current_connections"] = len(self.ws_clients)
        
        self.stats["messages_broadcast"] += len(authenticated_clients) - len(dead_clients)
    
    async def receive_stats_result(self, client_id: str, result: Dict):
        """
        接收 stats_handler 发来的统计结果，推送给指定客户端
        """
        logger.info(f"📊【客户端】收到 交易数据统计结果，推送给客户端 {client_id}")
        
        for client in self.ws_clients:
            if client.get('client_id') == client_id and client.get('authenticated', False):
                try:
                    await client['ws'].send_json({
                        "type": "stats_result",
                        "data": result,
                        "timestamp": time.time()
                    })
                    logger.info(f"✅ 交易数据统计结果已推送给客户端 {client_id}")
                except Exception as e:
                    logger.error(f"❌ 推送统计结果失败: {e}")
                break


    # ======================================================================
    # 🏠 房间4：辅助方法和服务器控制
    # ======================================================================
    
    def _validate_token(self, token: str) -> bool:
        """验证token"""
        if not token:
            return False
        return token == self.valid_token
    
    def _get_token_from_request(self, request) -> str:
        """从HTTP请求获取token"""
        auth_header = request.headers.get('Authorization', '')
        if auth_header.startswith('Bearer '):
            return auth_header[7:]
        
        token = request.query.get('token', '')
        if token:
            return token
        
        return ''
    
    async def start(self):
        """启动前端中继服务器"""
        try:
            logger.info(f"🚀【客户端】 启动前端中继服务器，端口: {self.port}")
            
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            
            self.site = web.TCPSite(self.runner, '0.0.0.0', self.port)
            await self.site.start()
            
            logger.info(f"✅【客户端】 前端中继服务器启动成功")
            logger.info(f"📡【客户端】 WebSocket: ws://0.0.0.0:{self.port}/ws")
            logger.info(f"📨【客户端】 HTTP API: http://0.0.0.0:{self.port}/api/cmd")
            logger.info(f"📊【客户端】状态查询: http://0.0.0.0:{self.port}/status")
            logger.info(f"❤️【客户端】健康检查: http://0.0.0.0:{self.port}/health")
            logger.info(f"📋【客户端】日志流: http://0.0.0.0:{self.port}/api/logs/stream")
            logger.info(f"📋【客户端】历史日志: http://0.0.0.0:{self.port}/api/logs/history")
            logger.info(f"🔐【客户端】认证方式: 连接WebSocket后发送 {{'type':'auth', 'token':'YOUR_TOKEN'}}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌【客户端】 启动前端中继服务器失败: {e}")
            return False
    
    async def stop(self):
        """停止前端中继服务器"""
        logger.info("🛑【客户端】 停止前端中继服务器...")
        
        for client in self.ws_clients:
            try:
                await client['ws'].close()
            except:
                pass
        self.ws_clients.clear()
        
        if self.runner:
            await self.runner.cleanup()
            self.runner = None
            self.site = None
        
        logger.info("✅【客户端】 前端中继服务器已停止")
    
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
            "uptime_seconds": uptime,
            "auth_enabled": True
        }