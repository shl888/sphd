"""
币安订单事件分类器 - 纯函数，无状态
16种事件分类规则，输入原始data，返回分类字符串
"""
from typing import Dict, Any


def classify_binance_order(data: Dict[str, Any]) -> str:
    """
    币安订单更新事件分类 (ORDER_TRADE_UPDATE)
    返回: 
    '01_开仓(部分成交)', '02_开仓(全部成交)',
    '03_设置止损', '04_设置止盈',
    '05_触发止损(部分成交)', '06_触发止损(全部成交)',
    '07_触发止盈(部分成交)', '08_触发止盈(全部成交)',
    '09_主动平仓(部分成交)', '10_主动平仓(全部成交)',
    '11_取消止损', '12_取消止盈',
    '13_止损过期(被触发)', '14_止损过期(被取消)',
    '15_止盈过期(被触发)', '16_止盈过期(被取消)',
    '99_其他'
    """
    try:
        o = data['data']['o']
        
        s = o.get('S', '')        # 方向 BUY/SELL
        ps = o.get('ps', '')      # 持仓方向 LONG/SHORT
        ot = o.get('ot', '')      # 原始订单类型
        o_type = o.get('o', '')   # 当前订单类型
        x_status = o.get('X', '') # 订单状态
        sp = o.get('sp', '0')     # 触发价
        cp = o.get('cp', False)   # 是否条件单
        er = o.get('er', '0')     # 错误码
        
        # ============================================================
        # 🩹 补丁：算法条件触发平仓（针对 st == "ALGO_CONDITION"）
        # 说明：币安算法单被触发后，除了推送 ALGO_UPDATE 事件外，
        #       还会在 ORDER_TRADE_UPDATE 中推送一条订单成交数据。
        #       这条数据的 ot 为 MARKET，sp 为 0，无法被现有逻辑识别，
        #       因此需要单独拦截。
        # 判定条件：
        #   1. st == "ALGO_CONDITION"（算法条件触发）
        #   2. X == "FILLED"（只处理全部成交，部分成交不管）
        #   3. 方向与持仓相反（确认是平仓）
        #   4. 根据 rp 正负判定是止盈还是止损
        # ============================================================
        st = o.get('st', '')
        if st == 'ALGO_CONDITION' and x_status == 'FILLED':
            # 确认是平仓动作：平多 SELL+LONG，平空 BUY+SHORT
            if (s == 'SELL' and ps == 'LONG') or (s == 'BUY' and ps == 'SHORT'):
                rp = float(o.get('rp', '0'))
                if rp > 0:
                    return '08_触发止盈(全部成交)'
                elif rp < 0:
                    return '06_触发止损(全部成交)'
                else:
                    # 保底：rp == 0 时按 ot 判断
                    if ot == 'STOP_MARKET':
                        return '06_触发止损(全部成交)'
                    else:
                        return '08_触发止盈(全部成交)'
            # 方向不匹配，不是平仓，继续走后续逻辑
        
        # ===== 开仓 =====
        # 开多: BUY + LONG 或 开空: SELL + SHORT
        if ((s == 'BUY' and ps == 'LONG') or (s == 'SELL' and ps == 'SHORT')) and ot == 'MARKET':
            if x_status == 'PARTIALLY_FILLED':
                return '01_开仓(部分成交)'
            if x_status == 'FILLED':
                return '02_开仓(全部成交)'
        
        # ===== 设置类 =====
        if ot == 'STOP_MARKET' and x_status == 'NEW':
            return '03_设置止损'
        
        if ot == 'TAKE_PROFIT_MARKET' and x_status == 'NEW':
            return '04_设置止盈'
        
        # ===== 触发止损 =====
        # 触发止损平多: SELL + LONG 或 触发止损平空: BUY + SHORT
        if o_type == 'MARKET' and ot == 'STOP_MARKET' and sp != '0':
            # 确保这是平仓动作（方向与持仓相反）
            if (s == 'SELL' and ps == 'LONG') or (s == 'BUY' and ps == 'SHORT'):
                if x_status == 'PARTIALLY_FILLED':
                    return '05_触发止损(部分成交)'
                if x_status == 'FILLED':
                    return '06_触发止损(全部成交)'
        
        # ===== 触发止盈 =====
        # 触发止盈平多: SELL + LONG 或 触发止盈平空: BUY + SHORT
        if o_type == 'MARKET' and ot == 'TAKE_PROFIT_MARKET' and sp != '0':
            # 确保这是平仓动作（方向与持仓相反）
            if (s == 'SELL' and ps == 'LONG') or (s == 'BUY' and ps == 'SHORT'):
                if x_status == 'PARTIALLY_FILLED':
                    return '07_触发止盈(部分成交)'
                if x_status == 'FILLED':
                    return '08_触发止盈(全部成交)'
        
        # ===== 主动平仓 =====
        # 平多: SELL + LONG 或 平空: BUY + SHORT
        # cp is False 排除条件触发的平仓（条件触发的由上方补丁或触发止损/止盈逻辑处理）
        if ((s == 'SELL' and ps == 'LONG') or (s == 'BUY' and ps == 'SHORT')) and ot == 'MARKET' and sp == '0' and cp is False:
            if x_status == 'PARTIALLY_FILLED':
                return '09_主动平仓(部分成交)'
            if x_status == 'FILLED':
                return '10_主动平仓(全部成交)'
        
        # ===== 取消类 =====
        if ot == 'STOP_MARKET' and x_status == 'CANCELED':
            return '11_取消止损'
        
        if ot == 'TAKE_PROFIT_MARKET' and x_status == 'CANCELED':
            return '12_取消止盈'
        
        # ===== 过期类 =====
        if x_status == 'EXPIRED':
            # 止损过期
            if ot == 'STOP_MARKET':
                if er == '8':
                    return '13_止损过期(被触发)'
                else:
                    return '14_止损过期(被取消)'
            
            # 止盈过期
            if ot == 'TAKE_PROFIT_MARKET':
                if er == '8':
                    return '15_止盈过期(被触发)'
                else:
                    return '16_止盈过期(被取消)'
        
        # ===== 其他 =====
        return '99_其他'
    
    except (KeyError, TypeError, AttributeError):
        return '99_其他'


def is_closing_event(category: str) -> bool:
    """判断是否是平仓类事件（需要清理缓存）- 只有全部成交才触发"""
    return category in [
        '06_触发止损(全部成交)',
        '08_触发止盈(全部成交)', 
        '10_主动平仓(全部成交)'
    ]


# ============================================================
# 算法订单分类器（ALGO_UPDATE）
# 说明：这是独立于 ORDER_TRADE_UPDATE 的另一套事件系统
#       用于管理通过 /fapi/v1/algo 创建的算法订单
#       与上方的订单分类器互不干扰，各自独立运行
# ============================================================

def classify_binance_algo(data: Dict[str, Any]) -> str:
    """
    币安算法订单更新事件分类（止盈止损）
    输入: 完整的 private_data 或包含 o 字段的字典
    返回:
    'A01_设置止损'
    'A02_设置止盈'
    'A03_取消止损'
    'A04_取消止盈'
    'A05_触发止损'
    'A06_触发止盈'
    'A07_止损过期'
    'A08_止盈过期'
    'A09_其它'
    """
    try:
        # 兼容两种数据格式
        if 'data' in data and 'o' in data['data']:
            o = data['data']['o']
        elif 'o' in data:
            o = data['o']
        else:
            return 'A09_其它'
        
        x_status = o.get('X', '')           # 状态: NEW/CANCELED/TRIGGERED/EXPIRED/FINISHED
        order_type = o.get('o', '')         # 订单类型: STOP_MARKET / TAKE_PROFIT_MARKET
        
        # ===== 止损 =====
        if order_type == 'STOP_MARKET':
            if x_status == 'NEW':
                return 'A01_设置止损'
            if x_status == 'CANCELED':
                return 'A03_取消止损'
            if x_status == 'TRIGGERED':
                return 'A05_触发止损'
            if x_status == 'EXPIRED':
                return 'A07_止损过期'
            # FINISHED 是 TRIGGERED 的完成态，归入触发止损
            if x_status == 'FINISHED':
                return 'A05_触发止损'
        
        # ===== 止盈 =====
        if order_type == 'TAKE_PROFIT_MARKET':
            if x_status == 'NEW':
                return 'A02_设置止盈'
            if x_status == 'CANCELED':
                return 'A04_取消止盈'
            if x_status == 'TRIGGERED':
                return 'A06_触发止盈'
            if x_status == 'EXPIRED':
                return 'A08_止盈过期'
            # FINISHED 是 TRIGGERED 的完成态，归入触发止盈
            if x_status == 'FINISHED':
                return 'A06_触发止盈'
        
        return 'A09_其它'
        
    except (KeyError, TypeError, AttributeError):
        return 'A09_其它'