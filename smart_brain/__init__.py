"""
智能大脑模块 - 重构版
"""

from .core import SmartBrain
from .data_manager import DataManager
from .tag_dispatcher import TagDispatcher
from .config_handler import ConfigHandler

# 注意：command_router 已删除，不再导入

# ==================== 全局brain实例管理 ====================
_brain_instance = None
_config_handler_instance = None

def set_brain_instance(brain):
    """供launcher设置brain实例"""
    global _brain_instance
    _brain_instance = brain
    print("✅ [智能大脑] 全局brain实例已设置")

def get_brain_instance():
    """供其他模块获取brain实例"""
    return _brain_instance

def set_config_handler(handler):
    """供core设置Confighandler实例"""
    global _config_handler_instance
    _config_handler_instance = handler
    print("✅ [智能大脑] 全局Confighandler实例已设置")

def get_config_handler():
    """供qd_server获取Confighandler实例"""
    return _config_handler_instance

def receive_private_data(data):
    """
    供外部模块直接调用 - 转发给brain实例
    使用方式和private_data_processing完全一致
    """
    if _brain_instance is None:
        raise Exception("❌ [智能大脑] 大脑实例未设置，请先调用set_brain_instance")
    
    # 转发给brain的data_manager
    return _brain_instance.data_manager.receive_private_data(data)
# ============================================================

__all__ = [
    'SmartBrain',
    'DataManager',
    'TagDispatcher',
    'Confighandler',
    'set_brain_instance',
    'get_brain_instance',
    'set_config_handler',
    'get_config_handler',
    'receive_private_data',
]
__version__ = '2.1.0'