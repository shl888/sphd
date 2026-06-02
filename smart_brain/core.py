"""
大脑核心主控 - 调度版（只转发，不干活）

职责：
- 初始化和管理所有工人（半自动、全自动、下单工人）
- 接收前端指令，转发给对应工人
- 接收下单工人返回的执行结果，推送到前端
- 管理交易模式（禁止交易 / 半自动 / 全自动）
- 生成并发送内部标签（开启全自动 / 结束全自动）
- 不解析指令内容，不处理业务逻辑
- 不处理任何配置相关的指令（由 ConfigHandler 负责）
"""

import asyncio
import logging
import signal
import sys
import os
import traceback
from datetime import datetime

# 设置路径
CURRENT_FILE = os.path.abspath(__file__)
SMART_BRAIN_DIR = os.path.dirname(CURRENT_FILE)
PROJECT_ROOT = os.path.dirname(SMART_BRAIN_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

logger = logging.getLogger(__name__)


class SmartBrain:
    """
    智能大脑 - 调度中心
    
    架构：
    - 大脑只做转发，不处理业务逻辑
    - 工人各自独立，通过数据驱动工作
    - 交易模式控制：禁止交易（禁止）/ 半自动（半自动）/ 全自动（全自动）
    - 标签调度器：专门处理外部标签的接收与转发
    - 配置管理：完全交给 ConfigHandler，大脑不参与
    """
    
    def __init__(self, http_server=None, http_runner=None, 
                 pipeline_manager=None, funding_manager=None, 
                 frontend_relay=None):
        
        # ========== 注入的外部服务 ==========
        self.http_server = http_server
        self.http_runner = http_runner
        self.pipeline_manager = pipeline_manager
        self.funding_manager = funding_manager
        self.frontend_relay = frontend_relay
        
        # ========== 数据管理器 ==========
        from .data_manager import DataManager
        self.data_manager = DataManager(self)

        # ========== HTTP模块服务（用于执行交易） ==========
        self.http_module = None
        
        # ========== 标签调度器 ==========
        self.tag_dispatcher = None
        
        # ========== 配置处理器 ==========
        self.config_handler = None

        # ========== 下单工人（只负责执行，大脑不直接发数据给它） ==========
        self.trader = None
        
        # ========== 半自动工人 ==========
        self.leverage_worker = None      # 杠杆设置
        self.open_worker = None          # 开仓
        self.sl_tp_worker = None         # 止损止盈
        self.close_worker = None         # 平仓
        
        # ========== 全自动工人 - 资金费套利 ==========
        self.funding_open = None         # 开仓，检测开仓条件
        self.funding_sltp = None         # 止损止盈
        self.funding_close = None        # 持续监控清仓
        
        # ========== 全自动工人 - 价差套利 ==========
        self.spread_open = None          # 开仓，检测开仓条件
        self.spread_sltp = None          # 止损止盈
        self.spread_close = None         # 持续监控清仓
        
        # ========== 运行状态 ==========
        self.running = False
        self.status_log_task = None
        
        # ========== 交易模式 ==========
        # 禁止交易: 禁止交易
        # 半自动: 半自动模式（前端手动操作）
        # 全自动: 全自动模式（策略自动执行）
        self.trade_mode = "禁止交易"
        
        # ========== 信号处理 ==========
        signal.signal(signal.SIGINT, self.handle_signal)
        signal.signal(signal.SIGTERM, self.handle_signal)
    
    # ==================== 初始化 ====================
    
    async def initialize(self):
        """初始化智能大脑核心"""
        logger.info("🧠【智能大脑】大脑核心初始化中...")
        logger.info(f"🔒【智能大脑】初始交易模式: {self.trade_mode}（禁止交易）")
        
        try:
            # 1. 初始化HTTP模块服务
            try:
                from http_server.service import HTTPModuleService
                self.http_module = HTTPModuleService()
                http_init_success = await self.http_module.initialize(self)
                if not http_init_success:
                    logger.error("❌【智能大脑】HTTP模块服务初始化失败")
                    return False
                logger.info("✅【智能大脑】HTTP模块服务初始化成功")
            except ImportError as e:
                logger.error(f"❌【智能大脑】无法导入HTTP模块服务: {e}")
                return False
            except Exception as e:
                logger.error(f"❌【智能大脑】HTTP模块服务初始化异常: {e}")
                return False
            
            # 2. 创建半自动工人（先创建，因为标签调度器需要它们）
            from .trading.semi_auto.leverage_worker import LeverageWorker
            from .trading.semi_auto.open_position_worker import OpenPositionWorker
            from .trading.semi_auto.sl_tp_worker import SlTpWorker
            from .trading.semi_auto.close_position_worker import ClosePositionWorker
            
            self.leverage_worker = LeverageWorker(self)
            self.open_worker = OpenPositionWorker(self)
            self.sl_tp_worker = SlTpWorker(self)
            self.close_worker = ClosePositionWorker(self)
            logger.info("✅【智能大脑】半自动工人已创建（杠杆、开仓、止损止盈、平仓）")
            
            # 3. 创建全自动工人 - 资金费套利
            from .trading.full_auto.funding import FundingOpen, FundingClose, FundingSlTp
            
            self.funding_open = FundingOpen(self)
            self.funding_sltp = FundingSlTp(self)
            self.funding_close = FundingClose(self)
            logger.info("✅【智能大脑】资金费套利工人已创建（开仓、止损止盈、清仓）")
            
            # 4. 创建全自动工人 - 价差套利
            from .trading.full_auto.spread import SpreadOpen, SpreadClose, SpreadSlTp
            
            self.spread_open = SpreadOpen(self)
            self.spread_sltp = SpreadSlTp(self)
            self.spread_close = SpreadClose(self)
            logger.info("✅【智能大脑】价差套利工人已创建（开仓、止损止盈、清仓）")
            
            # 5. 创建标签调度器
            from .tag_dispatcher import TagDispatcher
            self.tag_dispatcher = TagDispatcher(
                open_worker=self.open_worker,
                funding_sltp=self.funding_sltp,
                funding_close=self.funding_close,
                spread_sltp=self.spread_sltp,
                spread_close=self.spread_close
            )
            logger.info("✅【智能大脑】标签调度器已创建")
            
            # 6. 创建配置处理器（必须在标签调度器之后）
            from .config_handler import ConfigHandler
            from . import set_config_handler
            self.config_handler = ConfigHandler(self.data_manager)
            set_config_handler(self.config_handler)  # 设置全局实例，供 qd_server 获取
            self.config_handler.load_credentials()
            logger.info("✅【智能大脑】配置处理器已创建")
            
            # 7. 创建下单工人
            from http_server.trader import Trader
            #控制下单工人的开关，True为模拟交易，False为真实交易
#            self.trader = Trader(self, use_sandbox=True)
            self.trader = Trader(self, use_sandbox=False)  
            
            # 将标签调度器注入给下单工人
            self.trader.tag_dispatcher = self.tag_dispatcher
            asyncio.create_task(self.trader.start())
            logger.info("✅【智能大脑】下单工人已创建并启动（已注入标签调度器）")
            
            # 8. 启动状态日志任务
            self.status_log_task = asyncio.create_task(self.data_manager._log_data_status())
            
            # 9. 完成初始化
            self.running = True
            logger.info("✅【智能大脑】大脑核心初始化完成")
            
            # 输出HTTP模块状态
            if self.http_module:
                http_status = self.http_module.get_status()
                logger.info(f"📊【智能大脑】HTTP模块状态: {http_status}")
            
            return True
            
        except Exception as e:
            logger.error(f"🚨【智能大脑】大脑初始化失败: {e}")
            logger.error(traceback.format_exc())
            return False
    
    # ==================== 数据接收 ====================
    
    async def receive_market_data(self, processed_data):
        """接收市场数据（委托给data_manager）"""
        return await self.data_manager.receive_market_data(processed_data)
    
    async def receive_private_data(self, private_data):
        """接收私人数据（委托给data_manager）"""
        return await self.data_manager.receive_private_data(private_data)
    
    # ==================== 接收下单工人返回的执行结果 ====================
    
    async def on_trader_results(self, data):
        """
        接收下单工人发来的执行结果数据（不包含标签）
        
        标签已经由下单工人直接发给标签调度器，大脑不再接收标签。
        这里只接收纯执行结果数据，直接推送到前端。
        """
        # 直接推送到前端
        if self.frontend_relay:
            await self.frontend_relay.broadcast_execution_results([data])
            logger.info(f"📤【智能大脑】交易执行结果已推送到前端")
        else:
            logger.warning("⚠️【智能大脑】frontend_relay 未设置，无法推送执行结果")
    
    # ==================== 前端指令处理 ====================
    
    async def handle_frontend_command(self, command_data):
        """
        接收前端指令
        
        大脑不再解析指令内容，直接转发给对应的工人
        注意：配置相关的指令（如解密密码）不经过这里，由 qd_server 直接发给 ConfigHandler
        """
        command = command_data.get('command')
        params = command_data.get('params', {})
        client_id = command_data.get('client_id', 'unknown')
        
        # ========== 交易模式指令 ==========
        if command == 'set_trade_mode':
            new_mode = params.get('mode', '禁止交易')
            old_mode = self.trade_mode
            self.trade_mode = new_mode
            
            # 切换到全自动模式：发送「开启全自动」标签
            if new_mode == '全自动' and old_mode != '全自动':
                # 资金费套利工人
                if self.funding_open:
                    self.funding_open.on_data({"info": "开启全自动"})
                if self.funding_sltp:
                    self.funding_sltp.on_data({"info": "开启全自动"})
                if self.funding_close:
                    self.funding_close.on_data({"info": "开启全自动"})

                # 价差套利工人
                if self.spread_open:
                    self.spread_open.on_data({"info": "开启全自动"})
                if self.spread_sltp:
                    self.spread_sltp.on_data({"info": "开启全自动"})
                if self.spread_close:
                    self.spread_close.on_data({"info": "开启全自动"})
                logger.info("🎮【智能大脑】已向全自动工人发送「开启全自动」标签")
            
            # 从全自动切换到其他模式：发送「结束全自动」标签
            elif old_mode == '全自动' and new_mode != '全自动':
                # 资金费套利工人
                if self.funding_open:
                    self.funding_open.on_data({"info": "结束全自动"})
                if self.funding_sltp:
                    self.funding_sltp.on_data({"info": "结束全自动"})
                if self.funding_close:
                    self.funding_close.on_data({"info": "结束全自动"})

                # 价差套利工人
                if self.spread_open:
                    self.spread_open.on_data({"info": "结束全自动"})
                if self.spread_sltp:
                    self.spread_sltp.on_data({"info": "结束全自动"})
                if self.spread_close:
                    self.spread_close.on_data({"info": "结束全自动"})
                logger.info("🎮【智能大脑】已向全自动工人发送「结束全自动」标签")
            
            logger.info(f"🎮【智能大脑】交易模式已切换: {old_mode} → {self.trade_mode}")
            return {
                "success": True,
                "received": True,
                "command": command,
                "message": f"交易模式已切换为 {self.trade_mode}",
                "old_mode": old_mode,
                "new_mode": self.trade_mode
            }
        
        # ========== 开仓指令（半自动） ==========
        if command == 'place_order':
            # 检查是否是禁止交易模式
            if self.trade_mode == "禁止交易":
                logger.warning(f"🚫【智能大脑】当前为禁止交易模式，开仓指令被拒绝")
                return {
                    "success": False,
                    "received": True,
                    "command": command,
                    "error": "当前为禁止交易模式，无法执行开仓"
                }
            
            logger.info(f"💰【智能大脑】收到开仓指令，直接转发给半自动工人")
            
            # 大脑不解析，直接转发给杠杆工人和开仓工人
            if self.leverage_worker:
                self.leverage_worker.on_data({"command": "place_order", "params": params})
                logger.info(f"📤【智能大脑】开仓指令已转发给半自动杠杆工人")
            if self.open_worker:
                self.open_worker.on_data({"command": "place_order", "params": params})
                logger.info(f"📤【智能大脑】开仓指令已转发给半自动开仓工人")
            
            return {
                "success": True,
                "received": True,
                "command": command,
                "message": "开仓指令已转发给半自动开仓工人"
            }
        
        # ========== 止损止盈指令（半自动） ==========
        if command == 'set_sl_tp':
            if self.trade_mode == "禁止交易":
                logger.warning(f"🚫【智能大脑】当前为禁止交易模式，止损止盈指令被拒绝")
                return {
                    "success": False,
                    "received": True,
                    "command": command,
                    "error": "当前为禁止交易模式，无法执行止损止盈"
                }
            
            logger.info(f"⚙️【智能大脑】收到止损止盈指令，直接转发给工人")
            
            if self.sl_tp_worker:
                self.sl_tp_worker.on_data({"type": "set_sl_tp", "data": params})
                logger.info(f"📤【智能大脑】止损止盈指令已转发给半自动止损止盈工人")
            
            return {
                "success": True,
                "received": True,
                "command": command,
                "message": "止损止盈指令已转发给半自动止损止盈工人"
            }
        
        # ========== 平仓指令（半自动） ==========
        if command == 'close_position':
            if self.trade_mode == "禁止交易":
                logger.warning(f"🚫【智能大脑】当前为禁止交易模式，平仓指令被拒绝")
                return {
                    "success": False,
                    "received": True,
                    "command": command,
                    "error": "当前为禁止交易模式，无法执行平仓"
                }
            
            logger.info(f"🔚【智能大脑】收到平仓指令，直接转发给半自动清仓工人")
            
            if self.close_worker:
                self.close_worker.on_data({"type": "close_position", "data": params})
                logger.info(f"📤【智能大脑】平仓指令已转发给半自动清仓工人")
            
            return {
                "success": True,
                "received": True,
                "command": command,
                "message": "平仓指令已转发给半自动清仓工人"
            }
        
        # ========== 未知指令 ==========
        logger.warning(f"⚠️【智能大脑】收到未知指令: {command}")
        return {
            "success": False,
            "received": True,
            "error": f"未知指令: {command}",
            "command": command
        }
    
    # ==================== 运行与关闭 ====================
    
    async def run(self):
        """运行大脑核心"""
        try:
            logger.info("🧠【智能大脑】大脑核心运行中...")
            
            # 主循环
            while self.running:
                await asyncio.sleep(1)
        
        except KeyboardInterrupt:
            logger.info("🚫【智能大脑】收到键盘中断")
        except Exception as e:
            logger.error(f"🚫【智能大脑】运行错误: {e}")
            logger.error(traceback.format_exc())
        finally:
            await self.shutdown()
    
    def handle_signal(self, signum, frame):
        """处理系统信号"""
        logger.info(f"☑️【智能大脑】收到信号 {signum}，开始关闭...")
        self.running = False
    
    async def shutdown(self):
        """关闭大脑核心"""
        self.running = False
        logger.info("☑️【智能大脑】正在关闭大脑核心...")
        
        try:
            # 1. 如果当前是全自动模式，先发送结束标签
            if self.trade_mode == '全自动':
                # 资金费套利工人
                if self.funding_open:
                    self.funding_open.on_data({"info": "结束全自动"})
                if self.funding_sltp:
                    self.funding_sltp.on_data({"info": "结束全自动"})
                if self.funding_close:
                    self.funding_close.on_data({"info": "结束全自动"})
                # 价差套利工人
                if self.spread_open:
                    self.spread_open.on_data({"info": "结束全自动"})
                if self.spread_sltp:
                    self.spread_sltp.on_data({"info": "结束全自动"})
                if self.spread_close:
                    self.spread_close.on_data({"info": "结束全自动"})
                logger.info("🎮【智能大脑】已向全自动工人发送「结束全自动」标签")
            
            # 2. 关闭HTTP模块服务
            if self.http_module:
                await self.http_module.shutdown()
            
            # 3. 取消状态日志任务
            if self.status_log_task:
                self.status_log_task.cancel()
                try:
                    await self.status_log_task
                except asyncio.CancelledError:
                    pass
            
            # 4. 关闭前端中继服务器
            if self.frontend_relay:
                await self.frontend_relay.stop()
            
            # 5. 关闭下单工人
            if self.trader:
                await self.trader.stop()
            
            logger.info("✅【智能大脑】大脑核心已关闭")
        except Exception as e:
            logger.error(f"❌【智能大脑】关闭出错: {e}")