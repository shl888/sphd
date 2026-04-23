"""
配置处理器
==================================================
【文件职责】
专门负责从环境变量读取敏感凭证，接收前端配置内容，存入 data_manager
==================================================
"""

import os
import logging
import asyncio

logger = logging.getLogger(__name__)


class ConfigHandler:
    """
    配置处理器
    ==================================================
    负责：
        1. 启动时从环境变量读取凭证
        2. 接收前端发来的配置内容
        3. 存入 data_manager
    ==================================================
    """
    
    def __init__(self, data_manager):
        """
        初始化配置处理器
        
        :param data_manager: DataManager 实例，用于存储配置
        """
        self.data_manager = data_manager
        self.config_data = None
        self._credentials_loaded = False
    
    # ==================== 对外接口 ====================
    
    def load_credentials(self):
        """加载凭证（启动时调用）"""
        if self._credentials_loaded:
            logger.debug("📋【配置处理器】环境变量凭证已加载过，跳过")
            return
        
        logger.info("📋【配置处理器】开始加载环境变量凭证...")
        
        self._load_api_credentials()
        self._load_database_credentials()
        
        self._credentials_loaded = True
        logger.info("✅【配置处理器】所有环境变量凭证加载完成")
        
        # ========== 发送标签给 TagDispatcher ==========
        try:
            from smart_brain import get_brain_instance
            brain = get_brain_instance()
            if brain and brain.tag_dispatcher:
                # 异步发送标签
                asyncio.create_task(brain.tag_dispatcher.receive({"info": "密钥已就绪"}))
                logger.info("📢【配置处理器】已发送「密钥已就绪」标签给标签调度器")
            else:
                logger.warning("⚠️【配置处理器】TagDispatcher 未初始化，无法发送标签")
        except Exception as e:
            logger.error(f"❌【配置处理器】发送标签失败: {e}")
    
    def set_config(self, config_content: str):
        """接收前端发来的配置内容"""
        if config_content:
            self.config_data = config_content
            logger.info(f"💾【配置处理器】收到配置内容: {config_content}")
    
    # ==================== 内部方法 ====================
    
    def _load_api_credentials(self):
        """从环境变量加载 API 凭证"""
        # 币安 API
        binance_key = os.getenv('BINANCE_API_KEY')
        binance_secret = os.getenv('BINANCE_API_SECRET')
        
        if not binance_key or not binance_secret:
            logger.error("❌【配置处理器】币安 API 凭证不完整，程序将无法正常交易")
        else:
            self.data_manager.set_api_credentials('binance', binance_key, binance_secret)
            logger.info("✅【配置处理器】币安 API 凭证已加载")
        
        # OKX API
        okx_key = os.getenv('OKX_API_KEY')
        okx_secret = os.getenv('OKX_API_SECRET')
        okx_passphrase = os.getenv('OKX_API_PASSPHRASE') or os.getenv('OKX_passphrase')
        
        if not okx_key or not okx_secret or not okx_passphrase:
            logger.error("❌【配置处理器】OKX API 凭证不完整，程序将无法正常交易")
            missing = []
            if not okx_key:
                missing.append("OKX_API_KEY")
            if not okx_secret:
                missing.append("OKX_API_SECRET")
            if not okx_passphrase:
                missing.append("OKX_API_PASSPHRASE/OKX_passphrase")
            logger.error(f"   缺失的变量: {', '.join(missing)}")
        else:
            self.data_manager.set_api_credentials('okx', okx_key, okx_secret, okx_passphrase)
            logger.info("✅【配置处理器】OKX API 凭证已加载")
    
    def _load_database_credentials(self):
        """从环境变量加载数据库凭证"""
        mongodb_uri = os.getenv('MONGODB_URI')
        
        if not mongodb_uri:
            logger.error("❌【配置处理器】MONGODB_URI 未设置，数据库功能将无法使用")
        else:
            self.data_manager.set_database_config('mongodb_uri', mongodb_uri)
            logger.info("✅【配置处理器】MongoDB 凭证已加载")