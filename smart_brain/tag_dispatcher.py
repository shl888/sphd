"""
标签调度器 - 专门负责标签的接收与转发

设计哲学：
- 单向流动：只接收标签，转发给工人，不返回任何结果
- 标签驱动：根据标签内容决定转发目标，不关心来源
- 完全解耦：发送方不知道谁接收，接收方不知道谁发送

职责：
- 接收下单工人发来的 info 标签
- 根据标签内容转发给对应的工人
- 不处理业务逻辑，只做路由
"""

import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class TagDispatcher:
    """
    标签调度器
    
    工作流程：
    1. 接收带 info 字段的标签数据
    2. 根据标签内容匹配转发目标
    3. 调用目标工人的 on_data() 方法
    """
    
    def __init__(self, open_worker=None, auto_sltp=None):
        """
        初始化标签调度器
        
        参数:
            open_worker: 半自动开仓工人
            auto_sltp: 全自动止损止盈工人
        """
        self.open_worker = open_worker
        self.auto_sltp = auto_sltp
        
        # 标签路由表
        self._route_table = self._build_route_table()
        
        logger.info("🏷️【标签调度器】初始化完成")
        logger.info(f"📋【标签调度器】路由表: {list(self._route_table.keys())}")
    
    def _build_route_table(self) -> Dict[str, str]:
        """
        构建标签路由表
        
        返回格式: {标签内容: 目标工人属性名}
        """
        return {
            # 杠杆设置成功 → 半自动开仓工人
            "欧易杠杆设置成功": "open_worker",
            "币安杠杆设置成功": "open_worker",
            
            # 开仓成功 → 全自动止损止盈工人
            "欧易开仓成功": "auto_sltp",
            "币安开仓成功": "auto_sltp",
            
            # ========== 🆕 新增：策略标签（暂时只接收，不转发） ==========
            # 等价差策略的平仓文件写好后，再配置转发目标
            "当前策略:资金费套利": None,
            "当前策略:价差套利": None,
        }
    
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
        
        # 查找路由
        worker_attr = self._route_table.get(info)
        
        if worker_attr is None:
            # 路由表中明确配置为 None，表示暂时不转发
            logger.info(f"📭【标签调度器】收到标签但暂不转发: {info}")
            return
        
        if worker_attr == "":
            # 未配置路由
            logger.warning(f"⚠️【标签调度器】未知标签: {info}")
            return
        
        # 获取目标工人
        worker = getattr(self, worker_attr, None)
        
        if not worker:
            logger.warning(f"⚠️【标签调度器】目标工人未初始化: {worker_attr}")
            return
        
        # 转发标签给工人（单向发送，不等待）
        try:
            worker.on_data(tag_data)
            logger.info(f"📤【标签调度器】标签已转发: {info} → {worker_attr}")
        except Exception as e:
            logger.error(f"❌【标签调度器】转发标签失败: {info} → {worker_attr}, 错误: {e}")
    
    def update_workers(self, open_worker=None, auto_sltp=None) -> None:
        """
        更新工人引用（用于工人重新创建后更新）
        """
        if open_worker is not None:
            self.open_worker = open_worker
            logger.info(f"🔄【标签调度器】更新 open_worker")
        
        if auto_sltp is not None:
            self.auto_sltp = auto_sltp
            logger.info(f"🔄【标签调度器】更新 auto_sltp")
    
    def add_route(self, tag: str, worker_attr: str) -> None:
        """
        动态添加标签路由
        """
        self._route_table[tag] = worker_attr
        logger.info(f"➕【标签调度器】添加路由: {tag} → {worker_attr}")
    
    def remove_route(self, tag: str) -> None:
        """
        动态移除标签路由
        """
        if tag in self._route_table:
            worker_attr = self._route_table.pop(tag)
            logger.info(f"➖【标签调度器】移除路由: {tag} → {worker_attr}")
    
    def get_routes(self) -> Dict[str, str]:
        """
        获取当前所有路由
        返回:
            路由表副本
        """
        return self._route_table.copy()