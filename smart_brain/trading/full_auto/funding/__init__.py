# trading/full_auto/funding/__init__.py
"""
全自动交易之资金费套利策略

包含三个工人：
- open: 开仓工人，负责检测开仓条件，生成开仓指令
- sltp: 止损止盈工人，负责设置止损止盈
- close: 清仓工人，负责持续监控并触发清仓
"""

from .open import FundingOpen
from .close import FundingClose
from .sltp import FundingSlTp

__all__ = ['FundingOpen', 'FundingClose', 'FundingSlTp']