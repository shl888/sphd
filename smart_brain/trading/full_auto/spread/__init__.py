# trading/full_auto/spread/__init__.py
"""
全自动交易之价差套利策略

包含三个工人：
- open: 开仓工人，负责检测开仓条件，生成开仓指令
- sltp: 止损止盈工人，负责设置止损止盈
- close: 清仓工人，负责持续监控并触发清仓
"""

from .open import SpreadOpen
from .close import SpreadClose
from .sltp import SpreadSlTp

__all__ = ['SpreadOpen', 'SpreadClose', 'SpreadSlTp']