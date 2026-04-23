"""
配置处理器
==================================================
【文件职责】
专门负责从环境变量读取敏感凭证，接收前端配置内容，存入 data_manager

【工作流程】
1. 启动完成 → 被动等待前端配置内容（不占 CPU）
2. 收到配置内容 → 用配置内容解密环境变量密文
3. 存入 data_manager → 发送「密钥已就绪」标签
==================================================
"""

import os
import logging
import asyncio
import base64
from Crypto.Cipher import AES

logger = logging.getLogger(__name__)


class ConfigHandler:
    """
    配置处理器
    ==================================================
    负责：
        1. 启动后被动等待前端配置内容
        2. 收到配置内容后，读取环境变量密文并解密
        3. 存入 data_manager
        4. 发送「密钥已就绪」标签
    ==================================================
    """
    
    def __init__(self, data_manager):
        """
        初始化配置处理器
        
        :param data_manager: DataManager 实例，用于存储配置
        """
        self.data_manager = data_manager
        self._credentials_loaded = False
    
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
    
    # ==================== 对外接口 ====================
    
    def load_credentials(self):
        """
        启动时调用
        完全被动，不读环境变量，不发标签，不占 CPU
        """
        logger.info("📋【配置处理器】启动完成，被动等待前端配置内容...")
    
    def set_config(self, config_content: str):
        """
        接收前端发来的配置内容（被动触发）
        
        :param config_content: 前端发来的配置内容（解密密码）
        """
        if not config_content:
            logger.warning("⚠️【配置处理器】收到空的配置内容")
            return
        
        logger.info("💾【配置处理器】收到配置内容，开始解密环境变量...")
        
        try:
            # 解密 API 凭证
            self._decrypt_and_store_api_credentials(config_content)
            
            # 解密数据库凭证
            self._decrypt_and_store_database_credentials(config_content)
            
            self._credentials_loaded = True
            logger.info("✅【配置处理器】所有环境变量凭证解密完成")
            
            # ========== 发送标签给 TagDispatcher ==========
            try:
                from smart_brain import get_brain_instance
                brain = get_brain_instance()
                if brain and brain.tag_dispatcher:
                    asyncio.create_task(brain.tag_dispatcher.receive({"info": "密钥已就绪"}))
                    logger.info("📢【配置处理器】已发送「密钥已就绪」标签给标签调度器")
                else:
                    logger.warning("⚠️【配置处理器】TagDispatcher 未初始化，无法发送标签")
            except Exception as e:
                logger.error(f"❌【配置处理器】发送标签失败: {e}")
                
        except Exception as e:
            logger.error(f"❌【配置处理器】解密失败（配置内容错误或密文损坏）: {e}")
            raise
    
    # ==================== 内部方法 ====================
    
    def _decrypt_and_store_api_credentials(self, password: str):
        """从环境变量读取密文，解密后存入"""
        # 币安 API
        binance_key_enc = os.getenv('BINANCE_API_KEY')
        binance_secret_enc = os.getenv('BINANCE_API_SECRET')
        
        if not binance_key_enc or not binance_secret_enc:
            logger.error("❌【配置处理器】币安 API 密文不完整")
        else:
            binance_key = self._decrypt(binance_key_enc, password)
            binance_secret = self._decrypt(binance_secret_enc, password)
            self.data_manager.set_api_credentials('binance', binance_key, binance_secret)
            logger.info("✅【配置处理器】币安 API 所有凭证已解密并加载")
        
        # OKX API
        okx_key_enc = os.getenv('OKX_API_KEY')
        okx_secret_enc = os.getenv('OKX_API_SECRET')
        okx_passphrase_enc = os.getenv('OKX_API_PASSPHRASE') or os.getenv('OKX_passphrase')
        
        if not okx_key_enc or not okx_secret_enc or not okx_passphrase_enc:
            logger.error("❌【配置处理器】OKX API 密文不完整")
            missing = []
            if not okx_key_enc:
                missing.append("OKX_API_KEY")
            if not okx_secret_enc:
                missing.append("OKX_API_SECRET")
            if not okx_passphrase_enc:
                missing.append("OKX_API_PASSPHRASE/OKX_passphrase")
            logger.error(f"   缺失的变量: {', '.join(missing)}")
        else:
            okx_key = self._decrypt(okx_key_enc, password)
            okx_secret = self._decrypt(okx_secret_enc, password)
            okx_passphrase = self._decrypt(okx_passphrase_enc, password)
            self.data_manager.set_api_credentials('okx', okx_key, okx_secret, okx_passphrase)
            logger.info("✅【配置处理器】OKX API 所有凭证已解密并加载")
    
    def _decrypt_and_store_database_credentials(self, password: str):
        """从环境变量读取数据库密文，解密后存入"""
        mongodb_uri_enc = os.getenv('MONGODB_URI')
        
        if not mongodb_uri_enc:
            logger.error("❌【配置处理器】MONGODB_URI 密文未设置")
        else:
            mongodb_uri = self._decrypt(mongodb_uri_enc, password)
            self.data_manager.set_database_config('mongodb_uri', mongodb_uri)
            logger.info("✅【配置处理器】MongoDB 凭证已解密并加载")