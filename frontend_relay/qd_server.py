# frontend_relay/qd_server.py
"""
前端中继服务器 - qd表示前端，避免与http_server/server.py冲突
功能：1.接收前端连接 2.推送数据 3.执行指令 4.提供容器日志查询 5.转发统计指令（新增）
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

# ========== 🆕 统计处理器导入 ==========
# 导入 StatsHandler，用于处理前端的 get_stats 指令
# StatsHandler 独立工作，qd_server 只负责转发指令和推送结果
from .stats_handler import StatsHandler
# ========== 统计处理器导入结束 ==========

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
        
        # ========== 🆕 初始化统计处理器 ==========
        # 创建 StatsHandler 实例，把自己（self）传进去
        # 这样 StatsHandler 干完活后可以调用 qd_server 的广播方法推送结果给前端
        # qd_server 在这里纯粹是个推送工具，不参与任何统计计算逻辑
        logger.info(f"📊【客户端】 正在初始化统计处理器...")
        self.stats_handler = StatsHandler(self)
        logger.info(f"✅【客户端】 统计处理器已初始化完成")
        # ========== 初始化统计处理器结束 ==========
        
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
        
        # ========== 日志接口（房间2的路由注册） ==========
        self.app.router.add_get('/api/logs/stream', self._handle_logs_stream)
        self.app.router.add_get('/api/logs/history', self._handle_logs_history)
    
    # ======================================================================
    # 🏠 房间1：WebSocket 和 HTTP API 处理（现有代码，保持不动）
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
        client_info = {
            'ws': ws,
            'authenticated': False,
            'client_id': client_id,
            'ip': client_ip
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
                            token = data.get('token', '')
                            logger.info(f"🔐【客户端】收到认证请求，客户端: {client_id}")
                            if self._validate_token(token):
                                client_info['authenticated'] = True
                                auth_received = True
                                
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
                                                logger.info(f"💾【客户端】收到配置指令，转发给大脑")
                                                logger.debug(f"   客户端: {client_id}")
                                                
                                                await self.brain.handle_frontend_command({
                                                    "command": "save_config",
                                                    "params": {"config_data": data2.get('data', '')},
                                                    "client_id": client_id
                                                })
                                            
                                            elif msg_type == 'set_trade_mode':
                                                logger.debug(f"🎮【客户端】收到交易模式指令，转发给大脑")
                                                logger.debug(f"   模式: {data2.get('mode')}")
                                                logger.debug(f"   客户端: {client_id}")
                                                
                                                await self.brain.handle_frontend_command({
                                                    "command": "set_trade_mode",
                                                    "params": {"mode": data2.get('mode', '')},
                                                    "client_id": client_id
                                                })
                                            
                                            # ========== 🆕 统计指令处理 ==========
                                            elif msg_type == 'get_stats':
                                                # qd_server 收到前端的 get_stats 指令
                                                # 只负责转发数据给 StatsHandler，不参与任何业务逻辑
                                                # StatsHandler 干完活会自己调用 broadcast_stats_result 推送结果给前端
                                                logger.debug(f"📊【客户端】收到统计指令")
                                                logger.debug(f"   请求参数: {data2}")
                                                logger.debug(f"   客户端: {client_id}")
                                                logger.debug(f"📤【客户端】转发统计指令给 StatsHandler 处理...")
                                                
                                                await self.stats_handler.handle(data2)
                                                
                                                logger.info(f"✅【客户端】统计指令已转发给 StatsHandler")
                                                # ========== 统计指令处理结束 ==========
                                            
                                            else:
                                                logger.debug(f"📨【客户端】收到未知消息类型: {msg_type}，客户端: {client_id}")
                                                
                                        except Exception as e:
                                            logger.error(f"❌【客户端】处理消息异常，客户端: {client_id}, 错误: {e}", exc_info=True)
                                    
                                    elif msg2.type in (web.WSMsgType.CLOSE, web.WSMsgType.ERROR):
                                        logger.info(f"🔌【客户端】WebSocket 连接关闭或出错，客户端: {client_id}")
                                        break
                                break
                            else:
                                # 认证失败
                                logger.warning(f"❌【客户端】客户端认证失败，token无效，客户端: {client_id}")
                                await ws.send_json({
                                    "type": "auth_failed",
                                    "error": "Invalid token",
                                    "timestamp": time.time()
                                })
                                break
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
    # 🏠 房间2：日志接口 —— 前端查看容器日志专用（现有代码，保持不动）
    # ======================================================================
    # 
    # 功能说明：
    #   - /api/logs/stream  → 实时模式：持续推送容器日志（SSE 流式）
    #   - /api/logs/history → 查询模式：按时间范围 + 关键词搜索历史日志
    #
    # 底层原理：
    #   - 容器必须挂载 /var/run/docker.sock 才能执行 docker logs 命令
    #   - 关键词过滤在容器内用 grep 完成，只返回匹配的行
    #   - 通过容器内的 hostname 自动获取当前容器 ID
    #
    # 当前状态：
    #   - 未部署到 Docker 容器时，返回 503 友好提示
    #   - 部署到容器并挂载 docker.sock 后自动生效
    #
    # 前端调用示例：
    #   - 实时模式：GET /api/logs/stream?tail=200
    #   - 查询模式：GET /api/logs/history?range=6h&keyword=error
    # ======================================================================

    async def _handle_logs_stream(self, request):
        """
        实时日志流（SSE 格式 - Server-Sent Events）
        
        前端连接后：
        1. 先返回最近 N 条历史日志（默认200条）
        2. 然后持续推送新产生的日志
        
        查询参数：
        - tail: 返回最近多少条（默认200，最大1000）
        - keyword: 可选，只返回包含关键词的行
        
        注意：由于 aiohttp 的流式响应特性，这里使用分块传输
        """
        # 1. 检查是否在 Docker 环境
        if not self._is_running_in_docker():
            logger.warning(f"⚠️【日志流】服务未运行在 Docker 容器中")
            return web.Response(
                text="⚠️ 服务未运行在 Docker 容器中。\n请将后端部署到 Docker 容器并挂载 /var/run/docker.sock 后使用此功能。\n",
                status=503
            )
        
        # 2. 解析参数
        tail = request.query.get('tail', '200')
        keyword = request.query.get('keyword', '').strip()
        
        # 限制 tail 最大值
        try:
            tail_num = int(tail)
            if tail_num > 1000:
                tail_num = 1000
            elif tail_num < 10:
                tail_num = 10
        except ValueError:
            tail_num = 200
        
        # 3. 准备流式响应
        response = web.StreamResponse()
        response.headers['Content-Type'] = 'text/plain; charset=utf-8'
        response.headers['Cache-Control'] = 'no-cache'
        response.headers['X-Accel-Buffering'] = 'no'  # 禁用 Nginx 缓冲
        await response.prepare(request)
        
        logger.info(f"📋【客户端】【日志流】开始推送，tail={tail_num}, keyword={keyword if keyword else '无'}")
        
        try:
            # 4. 先推送最近 N 条历史日志
            history_cmd = f"docker logs --tail {tail_num} {self._get_container_id()}"
            if keyword:
                history_cmd += f" | grep --line-buffered -i {shlex.quote(keyword)}"
            
            logger.debug(f"📋【日志流】执行历史命令: {history_cmd}")
            history_logs = await self._execute_docker_logs(history_cmd)
            if history_logs:
                await response.write(history_logs.encode('utf-8'))
                logger.debug(f"📋【日志流】已推送历史日志，长度: {len(history_logs)} 字符")
            
            # 5. 持续推送新日志（-f 模式）
            follow_cmd = f"docker logs -f --tail 0 {self._get_container_id()}"
            if keyword:
                follow_cmd += f" | grep --line-buffered -i {shlex.quote(keyword)}"
            
            logger.debug(f"📋【日志流】执行跟随命令: {follow_cmd}")
            process = await asyncio.create_subprocess_shell(
                follow_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            # 持续读取输出并推送给前端
            while True:
                line = await process.stdout.readline()
                if not line:
                    break
                await response.write(line)
                
        except asyncio.CancelledError:
            logger.info("📋【客户端】【日志流】客户端断开连接")
            raise
        except Exception as e:
            logger.error(f"❌【客户端】【日志流】推送失败: {e}", exc_info=True)
            error_msg = f"\n[错误] 日志流中断: {e}\n"
            await response.write(error_msg.encode('utf-8'))
        finally:
            # 清理子进程
            if 'process' in locals():
                try:
                    process.terminate()
                    await process.wait()
                    logger.debug(f"📋【日志流】子进程已清理")
                except:
                    pass
        
        return response

    async def _handle_logs_history(self, request):
        """
        历史日志查询
        
        查询参数：
        - range : 时间范围，如 5m, 30m, 1h, 6h, 24h（默认1h）
        - since : 起始时间（ISO格式，与 range 二选一）
        - until : 结束时间（ISO格式，可选）
        - keyword: 关键词过滤（可选）
        - limit : 最大返回行数（默认500）
        """
        # 1. 检查是否在 Docker 环境
        if not self._is_running_in_docker():
            logger.warning(f"⚠️【历史日志】服务未运行在 Docker 容器中")
            return web.json_response({
                "success": False,
                "error": "服务未运行在 Docker 容器中，请部署到容器后使用",
                "hint": "需要挂载 /var/run/docker.sock"
            }, status=503)
        
        # 2. 解析参数
        time_range = request.query.get('range', '1h')
        since = request.query.get('since', '')
        until = request.query.get('until', '')
        keyword = request.query.get('keyword', '').strip()
        limit = request.query.get('limit', '500')
        
        # 限制返回行数
        try:
            limit_num = int(limit)
            if limit_num > 2000:
                limit_num = 2000
        except ValueError:
            limit_num = 500
        
        # 3. 构建 docker logs 命令
        cmd_parts = ["docker", "logs"]
        
        # 时间范围参数
        if since:
            cmd_parts.extend(["--since", shlex.quote(since)])
        else:
            cmd_parts.extend(["--since", shlex.quote(time_range)])
        
        if until:
            cmd_parts.extend(["--until", shlex.quote(until)])
        
        cmd_parts.append(self._get_container_id())
        
        base_cmd = " ".join(cmd_parts)
        
        # 添加关键词过滤和行数限制
        if keyword:
            full_cmd = f"{base_cmd} 2>&1 | grep -i {shlex.quote(keyword)} | tail -n {limit_num}"
        else:
            full_cmd = f"{base_cmd} 2>&1 | tail -n {limit_num}"
        
        logger.info(f"📋【客户端】【历史日志】查询: range={time_range}, keyword={keyword if keyword else '无'}, limit={limit_num}")
        logger.debug(f"📋【历史日志】执行命令: {full_cmd}")
        
        try:
            # 4. 执行命令
            output = await self._execute_docker_logs(full_cmd)
            
            # 5. 按行分割
            lines = output.strip().split('\n') if output.strip() else []
            
            logger.info(f"✅【客户端】【历史日志】查询完成，返回 {len(lines)} 行")
            
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
            logger.error(f"❌【客户端】【历史日志】查询失败: {e}", exc_info=True)
            return web.json_response({
                "success": False,
                "error": str(e),
                "logs": [],
                "total": 0
            }, status=500)

    def _is_running_in_docker(self) -> bool:
        """
        检测当前是否在 Docker 容器内运行
        
        判断依据：
        1. 检查 /.dockerenv 文件是否存在
        2. 检查 /proc/1/cgroup 是否包含 docker
        """
        # 方法1：检查 .dockerenv 文件
        if os.path.exists('/.dockerenv'):
            logger.debug(f"📋【环境检测】检测到 /.dockerenv 文件，判定为 Docker 环境")
            return True
        
        # 方法2：检查 cgroup
        try:
            with open('/proc/1/cgroup', 'r') as f:
                content = f.read()
                if 'docker' in content or 'containerd' in content:
                    logger.debug(f"📋【环境检测】cgroup 包含 docker/containerd，判定为 Docker 环境")
                    return True
        except:
            pass
        
        logger.debug(f"📋【环境检测】未检测到 Docker 环境特征")
        return False

    def _get_container_id(self) -> str:
        """
        获取当前容器的 ID
        
        在容器内，hostname 就是容器 ID（短格式）
        """
        try:
            with open('/etc/hostname', 'r') as f:
                container_id = f.read().strip()
                logger.debug(f"📋【容器ID】从 /etc/hostname 获取: {container_id}")
                return container_id
        except:
            # 降级：尝试通过环境变量获取
            container_id = os.getenv('HOSTNAME', 'unknown')
            logger.debug(f"📋【容器ID】从环境变量 HOSTNAME 获取: {container_id}")
            return container_id

    async def _execute_docker_logs(self, command: str) -> str:
        """
        安全执行 docker logs 命令
        
        Args:
            command: 完整的 shell 命令字符串
            
        Returns:
            命令的标准输出
            
        Raises:
            Exception: 命令执行失败
        """
        try:
            process = await asyncio.create_subprocess_shell(
                command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=30.0  # 30秒超时
            )
            
            if process.returncode != 0 and process.returncode != 1:
                # grep 没有匹配时返回 1，这是正常的
                stderr_msg = stderr.decode('utf-8', errors='ignore')
                if "grep" not in command or process.returncode != 1:
                    logger.warning(f"⚠️ 命令执行警告 (code={process.returncode}): {stderr_msg}")
            
            return stdout.decode('utf-8', errors='ignore')
            
        except asyncio.TimeoutError:
            logger.error("❌ 【客户端】命令执行超时")
            return ""
        except Exception as e:
            logger.error(f"❌ 【客户端】命令执行失败: {e}", exc_info=True)
            raise


    # ======================================================================
    # 🏠 房间3：数据广播方法（现有代码 + 新增统计推送方法）
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
        # ========== 收到数据时打印 ==========
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
        
        # ========== 发送前打印 ==========
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
    
    # ========== 🆕 统计结果推送方法 ==========
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
    # 🏠 房间4：辅助方法和服务器控制（现有代码，保持不动）
    # ======================================================================
    
    def _validate_token(self, token: str) -> bool:
        """验证token"""
        if not token:
            logger.debug(f"🔐【token验证】token 为空")
            return False
        
        # 从环境变量读取的密钥
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
            "uptime_seconds": uptime,
            "auth_enabled": True
        }