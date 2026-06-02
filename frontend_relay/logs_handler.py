# frontend_relay/logs_handler.py
"""
日志处理器
==================================================
【文件职责】
专门处理前端查看容器日志的请求，从 qd_server 中抽离出来

【对外接口】
- stream(request): 实时日志流（SSE 格式）
- history(request): 历史日志查询

【依赖说明】
- 不依赖 qd_server 实例
- 不依赖大脑实例
- 只依赖 Docker 环境和 aiohttp

【前端调用】
- 实时模式：GET /api/logs/stream?tail=200
- 查询模式：GET /api/logs/history?range=6h&keyword=error
==================================================
"""

import asyncio
import logging
import os
import re
import shlex
import time
from aiohttp import web

logger = logging.getLogger(__name__)


class LogsHandler:
    """
    日志处理器
    ==================================================
    负责处理所有与容器日志相关的请求
    ==================================================
    """
    
    def __init__(self):
        """初始化日志处理器"""
        logger.debug("📋【日志处理器】初始化完成")
    
    # ==================== 对外接口 ====================
    
    async def stream(self, request):
        """
        实时日志流（SSE 格式 - Server-Sent Events）
        
        前端连接后：
        1. 先返回最近 N 条历史日志（默认200条）
        2. 然后持续推送新产生的日志
        
        查询参数：
        - tail: 返回最近多少条（默认200，最大1000）
        - keyword: 可选，只返回包含关键词的行
        """
        # 1. 检查是否在 Docker 环境
        if not self._is_running_in_docker():
            logger.warning(f"⚠️【日志处理器】服务未运行在 Docker 容器中")
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
        
        logger.info(f"📋【日志处理器】开始推送，tail={tail_num}, keyword={keyword if keyword else '无'}")
        
        try:
            # 4. 先推送最近 N 条历史日志
            history_cmd = f"docker logs --tail {tail_num} {self._get_container_id()}"
            if keyword:
                history_cmd += f" | grep --line-buffered -i {shlex.quote(keyword)}"
            
            logger.debug(f"📋【日志处理器】执行历史命令: {history_cmd}")
            history_logs = await self._execute_docker_logs(history_cmd)
            if history_logs:
                await response.write(history_logs.encode('utf-8'))
                logger.debug(f"📋【日志处理器】已推送历史日志，长度: {len(history_logs)} 字符")
            
            # 5. 持续推送新日志（-f 模式）
            follow_cmd = f"docker logs -f --tail 0 {self._get_container_id()}"
            if keyword:
                follow_cmd += f" | grep --line-buffered -i {shlex.quote(keyword)}"
            
            logger.debug(f"📋【日志处理器】执行跟随命令: {follow_cmd}")
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
            logger.info("📋【日志处理器】客户端断开连接")
            raise
        except Exception as e:
            logger.error(f"❌【日志处理器】推送失败: {e}", exc_info=True)
            error_msg = f"\n[错误] 日志流中断: {e}\n"
            await response.write(error_msg.encode('utf-8'))
        finally:
            # 清理子进程
            if 'process' in locals():
                try:
                    process.terminate()
                    await process.wait()
                    logger.debug(f"📋【日志处理器】子进程已清理")
                except:
                    pass
        
        return response
    
    async def history(self, request):
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
            logger.warning(f"⚠️【日志处理器】服务未运行在 Docker 容器中")
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
        
        logger.info(f"📋【日志处理器】查询: range={time_range}, keyword={keyword if keyword else '无'}, limit={limit_num}")
        logger.debug(f"📋【日志处理器】执行命令: {full_cmd}")
        
        try:
            # 4. 执行命令
            output = await self._execute_docker_logs(full_cmd)
            
            # 5. 按行分割
            lines = output.strip().split('\n') if output.strip() else []
            
            logger.info(f"✅【日志处理器】查询完成，返回 {len(lines)} 行")
            
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
            logger.error(f"❌【日志处理器】查询失败: {e}", exc_info=True)
            return web.json_response({
                "success": False,
                "error": str(e),
                "logs": [],
                "total": 0
            }, status=500)
    
    # ==================== 内部辅助方法 ====================
    
    def _is_running_in_docker(self) -> bool:
        """
        检测当前是否在 Docker 容器内运行
        
        判断依据：
        1. 检查 /.dockerenv 文件是否存在
        2. 检查 /proc/1/cgroup 是否包含 docker
        """
        # 方法1：检查 .dockerenv 文件
        if os.path.exists('/.dockerenv'):
            logger.debug(f"📋【日志处理器】检测到 /.dockerenv 文件，判定为 Docker 环境")
            return True
        
        # 方法2：检查 cgroup
        try:
            with open('/proc/1/cgroup', 'r') as f:
                content = f.read()
                if 'docker' in content or 'containerd' in content:
                    logger.debug(f"📋【日志处理器】cgroup 包含 docker/containerd，判定为 Docker 环境")
                    return True
        except:
            pass
        
        logger.debug(f"📋【日志处理器】未检测到 Docker 环境特征")
        return False
    
    def _get_container_id(self) -> str:
        """
        获取当前容器的完整 ID
        从 /proc/self/mountinfo 中读取完整 64 位 ID（支持 cgroup v1/v2）
        """
        try:
            with open('/proc/self/mountinfo', 'r') as f:
                content = f.read()
                # 匹配 64 位十六进制字符串（容器完整 ID）
                match = re.search(r'[a-f0-9]{64}', content)
                if match:
                    full_id = match.group(0)
                    logger.debug(f"📋【日志处理器】从 mountinfo 获取完整容器ID: {full_id}")
                    return full_id
        except Exception as e:
            logger.debug(f"📋【日志处理器】从 mountinfo 读取失败: {e}")
        
        # 降级方案1：从 /proc/self/cgroup 读取（cgroup v1）
        try:
            with open('/proc/self/cgroup', 'r') as f:
                for line in f:
                    if 'docker' in line:
                        parts = line.strip().split('/')
                        if len(parts) >= 2:
                            # 可能是完整 ID 也可能是短 ID，都返回
                            cid = parts[-1]
                            logger.debug(f"📋【日志处理器】从 cgroup 获取容器ID: {cid}")
                            return cid
        except Exception as e:
            logger.debug(f"📋【日志处理器】从 cgroup 读取失败: {e}")
        
        # 降级方案2：使用 hostname（短ID）
        try:
            with open('/etc/hostname', 'r') as f:
                short_id = f.read().strip()
                logger.debug(f"📋【日志处理器】降级使用 hostname: {short_id}")
                return short_id
        except:
            return os.getenv('HOSTNAME', 'unknown')
    
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
                    logger.warning(f"⚠️【日志处理器】命令执行警告 (code={process.returncode}): {stderr_msg}")
            
            return stdout.decode('utf-8', errors='ignore')
            
        except asyncio.TimeoutError:
            logger.error("❌【日志处理器】命令执行超时")
            return ""
        except Exception as e:
            logger.error(f"❌【日志处理器】命令执行失败: {e}", exc_info=True)
            raise