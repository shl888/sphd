"""
极简启动器 - 重构版：接管所有模块启动
支持单线程/多线程一键切换
"""

import asyncio
import logging
import sys
import traceback
import os
import signal
from datetime import datetime
import threading

# 添加写日志专员 全局替换，所有文件自动变异步
# 改成这样就行（默认屏蔽DEBUG）：
from async_logger import patch_logging
patch_logging()  # 默认屏蔽DEBUG，显示INFO及以上

# 如果你想看DEBUG日志（调试时）：
# from async_logger import enable_debug
# enable_debug()  # 显示所有日志（包括DEBUG）


# 测试日志
test_logger = logging.getLogger("test")
test_logger.info("如果看到这行日志，说明日志成功异步！")


# ==================== 运行模式配置 ====================
# True  = 多线程模式（生产环境，榨干CPU）
# False = 单线程模式（调试模式，方便定位问题）
MULTI_THREAD_MODE = True  # 默认多线程
# ====================================================

# ==================== 强制启动标记 ====================
print("🚨🚨🚨 LAUNCHER.PY 开始执行", file=sys.stderr)
sys.stderr.flush()  # 强制刷新，确保输出
# ====================================================

# ==================== 新增：加载环境变量 ====================
from dotenv import load_dotenv
load_dotenv()  # 从 .env 文件加载环境变量
# =======================================================

# 设置路径
CURRENT_FILE = os.path.abspath(__file__)
PROJECT_ROOT = os.path.dirname(CURRENT_FILE)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from websocket_pool.admin import WebSocketAdmin
from http_server.server import HTTPServer
from shared_data.pipeline_manager import PipelineManager
from frontend_relay import FrontendRelayServer

from public_http_fetcher.binance_funding_rate import FundingSettlementManager
from smart_brain.core import SmartBrain

# ✅ 导入设置brain实例的函数
from smart_brain import set_brain_instance

logger = logging.getLogger(__name__)

def start_keep_alive_background():
    """启动保活服务（后台线程）"""
    try:
        from keep_alive import start_with_http_check
        
        def run_keeper():
            try:
                start_with_http_check()
            except Exception as e:
                logger.error(f"【智能大脑】保活服务异常: {e}")
        
        thread = threading.Thread(target=run_keeper, daemon=True)
        thread.start()
        logger.info("✅ 【智能大脑】保活服务已启动")
    except Exception as e:
        logger.warning(f"⚠️ 【智能大脑】保活服务未启动: {e}")

async def start_http_server(http_server):
    """启动HTTP服务器"""
    try:
        from aiohttp import web
        port = int(os.getenv('PORT', 10000))
        host = '0.0.0.0'
        
        runner = web.AppRunner(http_server.app)
        await runner.setup()
        
        site = web.TCPSite(runner, host, port)
        await site.start()
        
        logger.info(f"✅ HTTP服务器已启动: http://{host}:{port}")
        return runner
    except Exception as e:
        logger.error(f"启动HTTP服务器失败: {e}")
        raise

async def delayed_ws_init(ws_admin):
    """延迟启动WebSocket连接池"""
    await asyncio.sleep(10)
    try:
        logger.info("⏳ 延迟启动WebSocket...")
        await ws_admin.start()
        logger.info("✅ WebSocket初始化完成")
    except Exception as e:
        logger.error(f"WebSocket初始化失败: {e}")

async def safe_get_pool_status(pool):
    """安全获取连接池状态"""
    try:
        if not pool:
            return {'connections': {}}
        
        status = pool.get_status()
        # 确保 status 是字典
        if not isinstance(status, dict):
            return {'connections': {}}
        
        # 确保有 connections 键
        if 'connections' not in status:
            status['connections'] = {}
        
        return status
    except Exception as e:
        logger.error(f"获取连接池状态失败: {e}")
        return {'connections': {}}

# ==================== 多线程模式相关函数 ====================
# 只有在 MULTI_THREAD_MODE = True 时才使用这些函数

def run_async_in_thread(coro_func, name, daemon=True):
    """
    在独立线程中运行异步函数
    每个线程有自己的事件循环
    
    Args:
        coro_func: 要运行的异步函数
        name: 线程名称
        daemon: 是否为守护线程
    
    Returns:
        threading.Thread: 启动的线程对象
    """
    def target():
        try:
            # 每个线程创建自己的事件循环
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(coro_func())
        except Exception as e:
            logger.error(f"❌ 线程 {name} 异常退出: {e}")
            logger.error(traceback.format_exc())
        finally:
            loop.close()
    
    thread = threading.Thread(target=target, name=name, daemon=daemon)
    thread.start()
    return thread


# ==================== 各模块的运行函数（多线程用）====================
# 这些函数在单线程模式下不会用到

async def run_public_websocket(brain):
    """运行公共WebSocket模块（多线程用）"""
    logger.info("🌐【公共WebSocket线程】已启动")
    try:
        # 如果已有ws_admin，让它运行
        if brain.ws_admin:
            # ws_admin 已经在运行，只需要保持
            while brain.running:
                await asyncio.sleep(1)
        else:
            # 如果没有，就等着
            await asyncio.Future()
    except Exception as e:
        logger.error(f"❌ 公共WebSocket线程异常: {e}")

async def run_private_websocket(brain):
    """运行私人WebSocket模块（多线程用）"""
    logger.info("🔒【私人WebSocket线程】已启动")
    try:
        # 私人连接池已经在运行，只需要保持
        while brain.running:
            await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"❌ 私人WebSocket线程异常: {e}")

async def run_public_pipeline(brain):
    """运行公开数据处理流水线（多线程用）"""
    logger.info("📊【公开数据处理线程】已启动")
    try:
        # pipeline_manager 已经在运行，只需要保持
        while brain.running:
            await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"❌ 公开数据处理线程异常: {e}")

async def run_private_pipeline(brain):
    """运行私人数据处理流水线（多线程用）"""
    logger.info("📝【私人数据处理线程】已启动")
    try:
        # 私人数据处理模块已经在运行，只需要保持
        while brain.running:
            await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"❌ 私人数据处理线程异常: {e}")

async def run_completion_module(brain):
    """运行数据完成部门模块（多线程用）"""
    logger.info("✅【数据完成部门线程】已启动")
    try:
        # 数据完成部门已经在运行，只需要保持
        while brain.running:
            await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"❌ 数据完成部门线程异常: {e}")

async def run_ticker_manager(brain):
    """运行币安24h涨跌幅数据管理器（多线程用）"""
    logger.info("📈【币安Ticker线程】已启动")
    try:
        # ticker_manager 已经在运行，只需要保持
        while brain.running:
            await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"❌ 币安Ticker线程异常: {e}")


# ==================== 主启动函数 ====================
async def main():
    """主启动函数"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stdout
    )
    
    logger.info("🚨🚨🚨 MAIN 函数开始执行")
    
    # 显示当前运行模式
    mode_str = "多线程" if MULTI_THREAD_MODE else "单线程"
    logger.info(f"⚙️ 运行模式: {mode_str}")
    
    # 启动保活服务（已经在独立线程中）
    start_keep_alive_background()
    
    logger.info("=" * 60)
    logger.info("🚀 智能大脑启动中...")
    logger.info("=" * 60)
    
    brain = None
    module_threads = []  # 保存所有模块线程（仅多线程模式用）
    
    try:
        # ==================== 1. 创建大脑实例 ====================
        logger.info("【1️⃣】创建大脑实例...")
        brain = SmartBrain(
            http_server=None,
            http_runner=None,
            pipeline_manager=None,
            funding_manager=None,
            frontend_relay=None
        )
        
        set_brain_instance(brain)
        logger.info("✅ 全局大脑实例已设置")
        
        # ==================== 2. 创建HTTP服务器 ====================
        logger.info("【2️⃣】创建HTTP服务器...")
        port = int(os.getenv('PORT', 10000))
        http_server = HTTPServer(host='0.0.0.0', port=port, brain=brain)
        brain.http_server = http_server
        
        # ==================== 3. 启动HTTP服务器 ====================
        logger.info("【3️⃣】启动HTTP服务器...")
        http_runner = await start_http_server(http_server)
        brain.http_runner = http_runner
        
        from shared_data.data_store import data_store
        data_store.set_http_server_ready(True)
        logger.info("✅ HTTP服务已就绪！")

        # ==================== 4. 初始化PipelineManager ====================
        logger.info("【4️⃣】初始化PipelineManager...")
        pipeline_manager = PipelineManager()
        brain.pipeline_manager = pipeline_manager
        
        # ==================== 5. 初始化资金费率管理器 ====================
        logger.info("【5️⃣】初始化资金费率管理器...")
        funding_manager = FundingSettlementManager()
        brain.funding_manager = funding_manager
        data_store.pipeline_manager = pipeline_manager
        
        # ==================== 6. 大脑初始化 ====================
        logger.info("【6️⃣】大脑初始化...")
        brain_init_success = await brain.initialize()
        if not brain_init_success:
            logger.error("❌ 大脑初始化失败")
            return
        
        # ==================== 7. 初始化前端中继 ====================
        logger.info("【7️⃣】初始化前端中继服务器...")
        try:
            frontend_relay = FrontendRelayServer(
                brain_instance=brain,
                port=10001
            )
            start_result = await frontend_relay.start()
            if start_result:
                brain.frontend_relay = frontend_relay
            # 日志已经在 qd_server.py 里输出，这里不再重复打印
        except ImportError:
            logger.warning("⚠️ 前端中继模块未找到")
        except Exception as e:
            logger.error(f"❌ 前端中继启动失败: {e}")
        
        # ==================== 8. 设置PipelineManager回调 ====================
        logger.info("【8️⃣】设置数据处理回调...")
        pipeline_manager.set_brain_callback(brain.data_manager.receive_market_data)
        
        # ==================== 9. 启动数据处理管道 ====================
        logger.info("【9️⃣】启动数据处理管道...")
        await pipeline_manager.start()
        
        # ==================== 10. 延迟启动WebSocket ====================
        logger.info("【🔟】准备延迟启动WebSocket...")
        ws_admin = WebSocketAdmin()
        asyncio.create_task(delayed_ws_init(ws_admin))
        brain.ws_admin = ws_admin
        
        # ==================== 11. 启动私人WebSocket连接池 ====================
        logger.info("【🅱️】启动私人WebSocket连接池...")
        try:
            from private_ws_pool import PrivateWebSocketPool
            private_pool = PrivateWebSocketPool()
            await private_pool.start(brain.data_manager)
            brain.private_pool = private_pool
            logger.info("✅ 私人WebSocket连接池启动成功")
        except ImportError as e:
            logger.error(f"❌ 无法导入私人连接池模块: {e}")
        except Exception as e:
            logger.error(f"❌ 启动私人连接池失败: {e}")
        
        # ==================== 12. 启动币安令牌任务 ====================
        logger.info("【🪙】启动币安令牌任务...")
        try:
            from private_http_fetcher.binance_token.listen_key_manager import ListenKeyManager
            token_manager = ListenKeyManager(brain.data_manager)
            await token_manager.start()
            brain.token_manager = token_manager
            logger.info("✅ 币安令牌任务已启动")
        except Exception as e:
            logger.error(f"❌ 启动币安令牌任务失败: {e}")
        
        # ==================== 13. 启动OKX合约面值系统 ====================
        logger.info("【📄】启动OKX合约面值系统...")
        try:
            from public_http_fetcher.okx_contract_info.fetcher import OKXContractFetcher
            from public_http_fetcher.okx_contract_info.cleaner import OKXContractCleaner
            
            okx_fetcher = OKXContractFetcher()
            raw_data = await okx_fetcher.startup_fetch()
            
            if raw_data:
                okx_cleaner = OKXContractCleaner()
                await okx_cleaner.clean_and_push(raw_data)
                brain.okx_cleaner = okx_cleaner
            
            brain.okx_fetcher = okx_fetcher
            logger.info("✅ OKX合约面值系统启动完成")
        except Exception as e:
            logger.error(f"❌ 启动OKX合约面值系统失败: {e}")
        
        # ==================== 14. 启动币安合约精度系统 ====================
        logger.info("【📐】启动币安合约精度系统...")
        try:
            from public_http_fetcher.binance_contract_info.fetcher import BinanceContractFetcher
            from public_http_fetcher.binance_contract_info.cleaner import BinanceContractCleaner
            
            binance_fetcher = BinanceContractFetcher()
            raw_data = await binance_fetcher.startup_fetch()
            
            if raw_data:
                binance_cleaner = BinanceContractCleaner()
                await binance_cleaner.clean_and_push(raw_data)
                brain.binance_cleaner = binance_cleaner
            
            brain.binance_fetcher = binance_fetcher
            logger.info("✅ 币安合约精度系统启动完成")
        except Exception as e:
            logger.error(f"❌ 启动币安合约精度系统失败: {e}")
        
        # ==================== 15. 启动币安资产获取任务 ====================
        logger.info("【💰】启动币安资产获取任务...")
        try:
            from private_http_fetcher.binance_account.fetcher import PrivateHTTPFetcher
            account_fetcher = PrivateHTTPFetcher()
            await account_fetcher.start(brain.data_manager)
            brain.private_fetcher = account_fetcher
            logger.info("✅ 币安资产获取任务已启动")
        except Exception as e:
            logger.error(f"❌ 启动币安资产获取任务失败: {e}")
        
        # ==================== 16. 启动数据完成部门模块 ====================
        logger.info("【启动文件】========== 开始启动【数据完成部门】模块 ==========")
        try:
            from data_completion_department import (
                get_receiver,
                DataDetector,
                Scheduler,
                Database,
                BinanceRepairArea,
                OkxMissingRepair,
            )
            logger.info("✅ 成功导入数据完成部门模块")
            
            data_receiver = get_receiver()
            logger.info("✅ 【启动文件】【数据完成部门】接收存储区已初始化")
            
            database = Database()
            await database.initialize()
            logger.info("✅ 【启动文件】【数据完成部门】数据库区已初始化")
            
            scheduler = Scheduler(brain.data_manager)
            logger.info("✅ 【启动文件】【数据完成部门】调度区已初始化")
            
            detector = DataDetector(scheduler)
            logger.info("✅ 【启动文件】【数据完成部门】检测区已初始化")
            
            binance_repair = BinanceRepairArea(scheduler)
            logger.info("✅【启动文件】【数据完成部门】 币安修复区已初始化")
            
            okx_repair = OkxMissingRepair(scheduler)
            logger.info("✅【启动文件】【数据完成部门】 欧易修复区已初始化")
            
            data_receiver.subscribe(detector.handle_store_snapshot)
            data_receiver.subscribe(binance_repair.handle_store_snapshot)
            data_receiver.subscribe(okx_repair.handle_store_snapshot)
            logger.info("✅【启动文件】【数据完成部门】 接收存储区已连接检测区和修复区")
            
            scheduler.set_database(database)
            scheduler.set_repair_binance(binance_repair)
            scheduler.set_repair_okx(okx_repair)
            logger.info("✅【启动文件】【数据完成部门】 调度区已连接数据库和修复区")
            
            brain.data_receiver = data_receiver
            brain.data_detector = detector
            brain.data_scheduler = scheduler
            brain.data_database = database
            brain.binance_repair = binance_repair
            brain.okx_repair = okx_repair
            
            logger.info("✅【启动文件】 数据完成部门模块全部启动完成")
            
        except Exception as e:
            logger.error(f"❌ 启动数据完成模块失败: {e}")
            logger.error(traceback.format_exc())
        
        # ==================== 17. 启动币安24h涨跌幅数据管理器 ====================
        logger.info("【📈】启动币安24h涨跌幅数据管理器...")
        try:
            from public_http_fetcher.binance_ticker import BinanceTickerManager
            ticker_manager = BinanceTickerManager(brain.data_manager, update_interval=60)
            await ticker_manager.start()
            brain.ticker_manager = ticker_manager
            logger.info("✅ 币安24h涨跌幅数据管理器已启动")
        except Exception as e:
            logger.error(f"❌ 启动币安24h涨跌幅数据管理器失败: {e}")
        
        # ==================== 18. 完成初始化 ====================
        brain.running = True
        logger.info("=" * 60)
        logger.info("🎉 所有模块初始化完成！")
        logger.info("=" * 60)
        
        # ==================== 19. 根据模式选择运行方式 ====================
        if MULTI_THREAD_MODE:
            # ===== 多线程模式 =====
            logger.info("🚀 进入多模块并行运行模式...")
            
            # 启动各个模块的独立线程
            module_threads = []
            
            # 1. 公共WebSocket线程
            if brain.ws_admin:
                module_threads.append(run_async_in_thread(
                    lambda: run_public_websocket(brain),
                    "PublicWS"
                ))
                logger.info("  ├─ 公共WebSocket线程已启动")
            
            # 2. 私人WebSocket线程
            if brain.private_pool:
                module_threads.append(run_async_in_thread(
                    lambda: run_private_websocket(brain),
                    "PrivateWS"
                ))
                logger.info("  ├─ 私人WebSocket线程已启动")
            
            # 3. 公开数据处理线程
            if brain.pipeline_manager:
                module_threads.append(run_async_in_thread(
                    lambda: run_public_pipeline(brain),
                    "Pipeline"
                ))
                logger.info("  ├─ 公开数据处理线程已启动")
            
            # 4. 私人数据处理线程（如果有）
            module_threads.append(run_async_in_thread(
                lambda: run_private_pipeline(brain),
                "PrivatePipeline"
            ))
            logger.info("  ├─ 私人数据处理线程已启动")
            
            # 5. 数据完成部门线程
            if brain.data_scheduler:
                module_threads.append(run_async_in_thread(
                    lambda: run_completion_module(brain),
                    "Completion"
                ))
                logger.info("  ├─ 数据完成部门线程已启动")
            
            # 6. 币安24h涨跌幅数据管理器线程
            if brain.ticker_manager:
                module_threads.append(run_async_in_thread(
                    lambda: run_ticker_manager(brain),
                    "TickerManager"
                ))
                logger.info("  └─ 币安Ticker线程已启动")
            
            logger.info(f"✅ 共启动 {len(module_threads)} 个模块线程")
            logger.info("=" * 60)
            logger.info("🛑 按 Ctrl+C 停止")
            logger.info("=" * 60)
            
            # 主线程只做监控
            while brain.running:
                await asyncio.sleep(5)
                
                # 检查所有线程是否健康
                for i, thread in enumerate(module_threads):
                    if not thread.is_alive():
                        logger.error(f"⚠️ 模块线程 {thread.name} 已停止，尝试重启...")
                        # 重启线程
                        thread_funcs = [
                            lambda: run_public_websocket(brain),
                            lambda: run_private_websocket(brain),
                            lambda: run_public_pipeline(brain),
                            lambda: run_private_pipeline(brain),
                            lambda: run_completion_module(brain),
                            lambda: run_ticker_manager(brain)
                        ]
                        # 根据线程名称找到对应的函数
                        name_to_func = {
                            "PublicWS": thread_funcs[0],
                            "PrivateWS": thread_funcs[1],
                            "Pipeline": thread_funcs[2],
                            "PrivatePipeline": thread_funcs[3],
                            "Completion": thread_funcs[4],
                            "TickerManager": thread_funcs[5]
                        }
                        func = name_to_func.get(thread.name)
                        if func:
                            new_thread = run_async_in_thread(func, thread.name)
                            module_threads[i] = new_thread
        
        else:
            # ===== 单线程模式 =====
            logger.info("🚀 进入单事件循环模式（调试模式）...")
            logger.info("=" * 60)
            logger.info("🛑 按 Ctrl+C 停止")
            logger.info("=" * 60)
            
            # 主循环 - 所有模块共享同一个事件循环
            while brain.running:
                await asyncio.sleep(0)  # 让出CPU
                await asyncio.sleep(1)  # 每秒唤醒一次
        
    except KeyboardInterrupt:
        logger.info("收到键盘中断")
    except Exception as e:
        logger.error(f"运行错误: {e}")
        logger.error(traceback.format_exc())
    finally:
        if brain:
            brain.running = False
            await brain.shutdown()
            logger.info("✅ 所有模块已停止")

if __name__ == "__main__":
    print("🚨🚨🚨 进入 __main__", file=sys.stderr)
    sys.stderr.flush()
    asyncio.run(main())