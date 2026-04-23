"""
标签调度器 - 专门负责标签的接收与转发

设计哲学：
- 单向流动：只接收标签，转发给工人，不返回任何结果
- 标签驱动：根据标签内容决定转发目标，不关心来源
- 完全解耦：发送方不知道谁接收，接收方不知道谁发送
- 广播机制：一个标签可以同时发给多个工人

职责：
- 接收下单工人发来的 info 标签
- 根据标签内容转发给对应的工人（支持广播）
- 不处理业务逻辑，只做路由
"""

import logging
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)


class TagDispatcher:
    """
    标签调度器
    
    工作流程：
    1. 接收带 info 字段的标签数据
    2. 根据标签内容匹配转发目标列表
    3. 遍历目标列表，执行发送动作
    """
    
    def __init__(
        self,
        open_worker=None,
        funding_sltp=None,
        funding_close=None,
        spread_sltp=None,
        spread_close=None
    ):
        """
        初始化标签调度器
        
        参数:
            open_worker: 半自动开仓工人
            funding_sltp: 资金费套利 - 止损止盈工人
            funding_close: 资金费套利 - 清仓工人
            spread_sltp: 价差套利 - 止损止盈工人
            spread_close: 价差套利 - 清仓工人
        """
        self.open_worker = open_worker
        self.funding_sltp = funding_sltp
        self.funding_close = funding_close
        self.spread_sltp = spread_sltp
        self.spread_close = spread_close
        
        # 密钥使用者列表（API和数据库使用者）
        self.key_consumers: List[Any] = []
        
        # 标签路由表（值可以是单个工人属性名，也可以是列表）
        self._route_table = self._build_route_table()
        
        logger.info("🏷️【标签调度器】初始化完成")
        logger.info(f"📋【标签调度器】路由表: {list(self._route_table.keys())}")
    
    def _build_route_table(self) -> Dict[str, Any]:
        """
        构建标签路由表
        
        返回格式: {标签内容: 目标工人属性名 或 目标工人属性名列表}
        """
        return {
            # 杠杆设置成功 → 半自动开仓工人
            "欧易杠杆设置成功": "open_worker",
            "币安杠杆设置成功": "open_worker",
            
            # 开仓成功 → 两套策略的止损止盈工人（广播）
            "欧易开仓成功": ["funding_sltp", "spread_sltp"],  
            "币安开仓成功": ["funding_sltp", "spread_sltp"],  
            
            # ========== 策略标签（广播给两套策略的止损止盈和清仓工人） ==========
            # 等价差工人实现后，把注释去掉
            "当前策略:资金费套利": ["funding_sltp", "funding_close"],
            "当前策略:价差套利": ["spread_sltp", "spread_close"],
        }
    
    def register_key_consumers(self, consumers: List[Any]) -> None:
        """
        注册密钥使用者（由启动文件调用）
        
        参数:
            consumers: 密钥使用者列表
        """
        self.key_consumers = consumers
        logger.info(f"📋【标签调度器】已注册 {len(consumers)} 个密钥使用者")
    
    async def receive(self, tag_data: Dict[str, Any]) -> None:
        """
        接收标签数据（单向，不返回）
        
        参数:
            tag_data: 包含 info 字段的标签数据，格式: {"info": "欧易杠杆设置成功"}
        """
        info = tag_data.get("info")
        
        if not info:
            logger.warning(f"⚠️【标签调度器】收到的数据没有 info 字段: {tag_data}")
            return
        
        # ========== 特殊处理：密钥已就绪标签 ==========
        if info == "密钥已就绪":
            if not self.key_consumers:
                logger.warning("⚠️【标签调度器】收到「密钥已就绪」标签，但没有注册的密钥使用者")
                return
            
            for consumer in self.key_consumers:
                try:
                    # 执行发送动作：调用使用者的 on_keys_ready() 方法
                    if hasattr(consumer, 'on_keys_ready'):
                        consumer.on_keys_ready()
                        logger.info(f"📤【标签调度器】「密钥已就绪」已发送: {type(consumer).__name__}")
                    else:
                        logger.warning(f"⚠️【标签调度器】使用者没有 on_keys_ready 方法: {type(consumer).__name__}")
                except Exception as e:
                    logger.error(f"❌【标签调度器】发送「密钥已就绪」失败: {type(consumer).__name__}, 错误: {e}")
            return
        # ==========================================
        
        # 查找路由
        target = self._route_table.get(info)
        
        if target is None:
            # 路由表中明确配置为 None，表示暂时不转发
            logger.info(f"📭【标签调度器】收到标签但暂不转发: {info}")
            return
        
        if target == "":
            # 未配置路由
            logger.warning(f"⚠️【标签调度器】未知标签: {info}")
            return
        
        # 统一转成列表处理
        target_list = target if isinstance(target, list) else [target]
        
        for worker_attr in target_list:
            worker = getattr(self, worker_attr, None)
            
            if not worker:
                logger.warning(f"⚠️【标签调度器】目标工人未初始化: {worker_attr}")
                continue
            
            # 执行发送动作给工人
            try:
                worker.on_data(tag_data)
                logger.info(f"📤【标签调度器】标签已发送: {info} → {worker_attr}")
            except Exception as e:
                logger.error(f"❌【标签调度器】发送标签失败: {info} → {worker_attr}, 错误: {e}")
    
    def update_workers(
        self,
        open_worker=None,
        funding_sltp=None,
        funding_close=None,
        spread_sltp=None,
        spread_close=None
    ) -> None:
        """
        更新工人引用（用于工人重新创建后更新）
        """
        if open_worker is not None:
            self.open_worker = open_worker
            logger.info(f"🔄【标签调度器】更新 open_worker")
        
        if funding_sltp is not None:
            self.funding_sltp = funding_sltp
            logger.info(f"🔄【标签调度器】更新 funding_sltp")
        
        if funding_close is not None:
            self.funding_close = funding_close
            logger.info(f"🔄【标签调度器】更新 funding_close")
        
        if spread_sltp is not None:
            self.spread_sltp = spread_sltp
            logger.info(f"🔄【标签调度器】更新 spread_sltp")
        
        if spread_close is not None:
            self.spread_close = spread_close
            logger.info(f"🔄【标签调度器】更新 spread_close")
    
    def add_route(self, tag: str, target: Any) -> None:
        """
        动态添加标签路由
        
        参数:
            tag: 标签内容，如 "欧易平仓成功"
            target: 目标工人属性名，或属性名列表
        """
        self._route_table[tag] = target
        logger.info(f"➕【标签调度器】添加路由: {tag} → {target}")
    
    def remove_route(self, tag: str) -> None:
        """
        动态移除标签路由
        """
        if tag in self._route_table:
            target = self._route_table.pop(tag)
            logger.info(f"➖【标签调度器】移除路由: {tag} → {target}")
    
    def get_routes(self) -> Dict[str, Any]:
        """
        获取当前所有路由
        """
        return self._route_table.copy()