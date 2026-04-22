"""
下单执行器（下单工人）- 数据驱动模式

运行方式：
1. 下单工人启动后，一直循环等待消息
2. 收到消息（订单参数）后，异步处理
3. 处理完成后：
   - 执行结果数据发给大脑
   - 标签发给标签调度器
4. 空闲时：歇着 / 定时校准币安时间
5. 收到开仓参数时，额外触发清理残留止损单任务

数据流向：
- 各个工人文件 → 下单工人：订单参数（send_orders）
- 下单工人 → 大脑：执行结果（on_trader_results）
- 下单工人 → 标签调度器：标签（receive）

各个工人文件，单方向> 下单工人
下单工人文件，单方向> 大脑 / 标签调度器
之间通过数据传递，数据驱动工作，没有调用关系
"""

import asyncio
import logging
import time
import hmac
import hashlib
import base64
import json
import urllib.parse
from datetime import datetime, timezone
from typing import Dict, Any, List
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)


class Trader:
    def __init__(self, brain, use_sandbox: bool = True):
        """
        工人初始化
        
        参数:
            brain: 大脑实例（用于发消息回大脑）
            use_sandbox: 是否使用模拟交易
        """
        self.brain = brain
        self.use_sandbox = use_sandbox
        self._executor = ThreadPoolExecutor(max_workers=10)
        
        # 标签调度器（由大脑注入）
        self.tag_dispatcher = None
        
        # 消息队列：大脑发来的订单放这里
        self._order_queue = asyncio.Queue()
        
        # 币安时间同步
        self._binance_time_offset = 0
        self._binance_last_sync = 0
        self._binance_sync_interval = 600  # 10分钟
        
        # 欧易时间同步
        self._okx_time_offset = 0
        self._okx_last_sync = 0
        
        # 控制工人运行状态
        self._running = False
        
        # 清理防重复（2秒内不重复执行）
        self._last_cleanup_time = 0
        self._cleanup_lock = asyncio.Lock()
        
        logger.info(f"👷【下单工人】初始化完成 | 模式: {'模拟交易' if use_sandbox else '真实交易'}")
    
    # ========== 对外接口（给大脑用） ==========
    
    def send_orders(self, orders: List[Dict]) -> None:
        """
        大脑发数据给工人（不等待，发完就走）
        
        大脑调用这个方法，把订单参数扔给工人，然后继续干自己的事。
        """
        self._order_queue.put_nowait(orders)
        logger.info(f"📤【下单工人】大脑发来 {len(orders)} 个订单，已放入队列")
    
    # ========== 工人主循环 ==========
    
    async def start(self):
        """启动工人"""
        self._running = True
        logger.info("👷【下单工人】启动，等待接收订单...")
        
        asyncio.create_task(self._binance_time_sync_loop())
        asyncio.create_task(self._okx_time_sync_loop())
        
        while self._running:
            try:
                orders = await self._order_queue.get()
                asyncio.create_task(self._process_orders(orders))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌【下单工人】主循环异常: {e}")
        
        logger.info("👷【下单工人】已停止")
    
    async def stop(self):
        """停止工人"""
        self._running = False
        while not self._order_queue.empty():
            try:
                self._order_queue.get_nowait()
            except:
                break
    
    # ========== 处理订单 ==========
    
    async def _process_orders(self, orders: List[Dict]):
        """处理收到的订单"""
        try:
            logger.info(f"🔧【下单工人】开始处理 {len(orders)} 个订单")
            
            expanded_orders = []
            for order in orders:
                exchange = order.get("exchange")
                order_type = order.get("type")
                
                if exchange == "binance" and order_type == "oco":
                    sl_order, tp_order = self._expand_binance_oco(order)
                    expanded_orders.append(sl_order)
                    expanded_orders.append(tp_order)
                else:
                    expanded_orders.append(order)
            
            creds_map = await self._fetch_all_credentials(expanded_orders)
            
            tasks = []
            for order in expanded_orders:
                exchange = order.get("exchange")
                creds = creds_map.get(exchange)
                if not creds:
                    tasks.append(self._error_result(order, f"无法获取 {exchange} API凭证"))
                else:
                    tasks.append(self._send_order(order, creds))
            
            results = await asyncio.gather(*tasks)
            await self._send_results(results)
            
            logger.info(f"✅【下单工人】处理完成，共 {len(results)} 个结果已发送")
            
            # ========== 新增：独立的清理任务 ==========
            # 触发条件：收到的参数中有开仓参数
            # 同步执行，阻塞主循环，确保止损参数不会并发
            for order in orders:
                if order.get("type") == "open_market":
                    await self._cleanup_algo_orders(creds_map)
                    break
            
        except Exception as e:
            logger.error(f"❌【下单工人】处理订单异常: {e}")
            await self._send_error_to_brain(f"工人处理异常: {str(e)}")
    
    async def _send_results(self, results: List[Dict]):
        """
        发送处理结果
        
        对于每个结果：
        1. 执行结果数据 → 发给大脑
        2. 如果成功，生成标签 → 发给标签调度器
        """
        for result in results:
            # 第一条：执行结果数据 → 发给大脑
            original_data = {
                "success": result.get("success"),
                "exchange": result.get("exchange"),
                "type": result.get("type"),
                "data": result.get("data", {}),
                "error": result.get("error")
            }
            
            if hasattr(self.brain, 'on_trader_results'):
                await self.brain.on_trader_results(original_data)
                logger.info(f"📤【下单工人】执行结果已发给大脑")
            else:
                logger.error("❌【下单工人】大脑没有实现 on_trader_results 方法")
            
            # 第二条：如果成功，生成标签 → 发给标签调度器
            if result.get("success"):
                info_tag = self._generate_info_tag(result)
                if info_tag:
                    if self.tag_dispatcher and hasattr(self.tag_dispatcher, 'receive'):
                        asyncio.create_task(self.tag_dispatcher.receive(info_tag))
                        logger.info(f"🏷️【下单工人】标签已发给调度器: {info_tag.get('info')}")
                    else:
                        logger.warning("⚠️【下单工人】标签调度器未注入或没有 receive 方法，标签丢弃")
    
    async def _send_error_to_brain(self, error_msg: str):
        """发送错误信息给大脑"""
        error_data = {
            "success": False,
            "error": error_msg
        }
        if hasattr(self.brain, 'on_trader_results'):
            await self.brain.on_trader_results(error_data)
    
    def _generate_info_tag(self, result: Dict) -> Dict:
        """
        根据结果生成信息标签
        
        返回格式：{"info": "xxx"}
        如果不生成标签，返回 None
        """
        exchange = result.get("exchange")
        order_type = result.get("type")
        data = result.get("data", {})
        
        # 欧易：code == "0" 表示成功
        if exchange == "okx":
            code = str(data.get("code", ""))
            if code != "0":
                return None
            
            if order_type == "set_leverage":
                return {"info": "欧易杠杆设置成功"}
            elif order_type == "open_market":
                return {"info": "欧易开仓成功"}
        
        # 币安：根据不同类型判断
        elif exchange == "binance":
            if order_type == "set_leverage":
                # 币安杠杆：有 leverage 字段表示成功
                if data.get("leverage") is not None:
                    return {"info": "币安杠杆设置成功"}
            
            elif order_type == "open_market":
                # 币安开仓：status 为 NEW/FILLED/PARTIALLY_FILLED 表示成功
                status = data.get("status")
                if status in ["NEW", "FILLED", "PARTIALLY_FILLED"]:
                    return {"info": "币安开仓成功"}
        
        return None
    
    # ========== 清理残留止损单（收到开仓参数时触发） ==========
    
    async def _cleanup_algo_orders(self, creds_map: Dict[str, Dict]):
        """清理残留的止损止盈单（带防重复，2秒内不重复执行）"""
        now = time.time()
        if now - self._last_cleanup_time < 2:
            logger.debug("🧹【下单工人】清理任务2秒内已执行过，跳过")
            return
        
        async with self._cleanup_lock:
            self._last_cleanup_time = time.time()
            
            # 欧易清理（独立 try 块，失败不影响币安）
            try:
                okx_creds = creds_map.get("okx")
                if okx_creds:
                    await self._cleanup_okx_algo_orders(okx_creds)
            except Exception as e:
                logger.warning(f"⚠️【下单工人】欧易清理残留单异常（可忽略）: {e}")
            
            # 币安清理（独立 try 块，失败不影响欧易）
            try:
                binance_creds = creds_map.get("binance")
                if binance_creds:
                    await self._cleanup_binance_open_orders(binance_creds)
            except Exception as e:
                logger.warning(f"⚠️【下单工人】币安清理残留单异常（可忽略）: {e}")
    
    async def _cleanup_okx_algo_orders(self, creds: Dict):
        """清理欧易残留的止盈止损单"""
        try:
            api_key = creds.get("api_key")
            api_secret = creds.get("api_secret")
            passphrase = creds.get("passphrase", "")
            
            # ========== 1. 查询未完成的策略委托（GET） ==========
            timestamp = self._okx_get_timestamp()
            endpoint = "/api/v5/trade/orders-algo-pending"
            params = {"ordType": "conditional,oco"}
            
            query_string = "&".join([f"{k}={v}" for k, v in params.items()])
            full_endpoint = f"{endpoint}?{query_string}"
            
            sign_str = timestamp + "GET" + full_endpoint
            signature = base64.b64encode(
                hmac.new(api_secret.encode(), sign_str.encode(), hashlib.sha256).digest()
            ).decode()
            
            url = self._okx_get_base_url() + full_endpoint
            headers = {
                "OK-ACCESS-KEY": api_key,
                "OK-ACCESS-SIGN": signature,
                "OK-ACCESS-TIMESTAMP": timestamp,
                "OK-ACCESS-PASSPHRASE": passphrase,
                "x-simulated-trading": self._okx_get_simulated_header()
            }
            
            loop = asyncio.get_running_loop()
            
            def _get_request():
                import requests
                return requests.get(url, headers=headers)
            
            response = await loop.run_in_executor(self._executor, _get_request)
            pending = response.json()
            
            if pending.get('code') != '0':
                logger.warning(f"⚠️【下单工人】欧易查询策略委托失败: {pending}")
                return
            
            orders = pending.get('data', [])
            if not orders:
                logger.info("🧹【下单工人】欧易无残留止损单")
                return
            
            # ========== 2. 提取 instId 和 algoId ==========
            cancel_list = []
            for order in orders:
                inst_id = order.get('instId')
                algo_id = order.get('algoId')
                if inst_id and algo_id:
                    cancel_list.append({"instId": inst_id, "algoId": algo_id})
            
            if not cancel_list:
                return
            
            # ========== 3. 批量撤销（每次最多10个） ==========
            batch_size = 10
            for i in range(0, len(cancel_list), batch_size):
                batch = cancel_list[i:i+batch_size]
                result = await self._okx_http_request(
                    api_key, api_secret, passphrase,
                    "POST",
                    "/api/v5/trade/cancel-algos",
                    batch
                )
                if result.get('code') == '0':
                    logger.info(f"🧹【下单工人】欧易已清理 {len(batch)} 个残留止损单")
                else:
                    logger.warning(f"⚠️【下单工人】欧易撤销失败: {result}")
                    
        except Exception as e:
            logger.warning(f"⚠️【下单工人】欧易清理残留单失败（可忽略）: {e}")
    
    async def _cleanup_binance_open_orders(self, creds: Dict):
        """
        清理币安残留的条件单
        
        注意：测试网（sandbox）环境下该接口不稳定，会返回空响应或502错误，
        因此模拟模式下直接跳过清理逻辑。
        """
        # 模拟环境下跳过（测试网不支持此接口稳定工作）
        if self.use_sandbox:
            logger.debug("🧹【下单工人】币安模拟模式，测试网策略单接口不稳定，跳过清理")
            return
        
        try:
            api_key = creds.get("api_key")
            api_secret = creds.get("api_secret")
            
            # 1. 查询所有未触发的条件单
            open_orders = await self._binance_http_request(
                api_key, api_secret,
                "GET",
                "/sapi/v1/algo/futures/openOrders",
                {}
            )
            
            # 币安返回格式：{"code": 200, "msg": "", "data": [...]}
            # 需要提取 data 字段
            if isinstance(open_orders, dict):
                if open_orders.get("code") != 200:
                    logger.warning(f"⚠️【下单工人】币安查询条件单失败: {open_orders}")
                    return
                orders_data = open_orders.get("data", [])
            else:
                orders_data = open_orders if isinstance(open_orders, list) else []
            
            if not orders_data:
                logger.info("🧹【下单工人】币安无残留条件单")
                return
            
            # 2. 提取需要撤销的 algoId（按 symbol 去重，每个合约只保留一个）
            seen_symbols = set()
            algo_ids_to_cancel = []
            
            for order in orders_data:
                symbol = order.get('symbol')
                algo_id = order.get('algoId')
                if symbol and algo_id and symbol not in seen_symbols:
                    seen_symbols.add(symbol)
                    algo_ids_to_cancel.append(algo_id)
            
            if not algo_ids_to_cancel:
                return
            
            # 3. 逐个撤销
            for algo_id in algo_ids_to_cancel:
                result = await self._binance_http_request(
                    api_key, api_secret,
                    "DELETE",
                    "/sapi/v1/algo/futures/order",
                    {"algoId": algo_id}
                )
                # 检查撤销结果
                if isinstance(result, dict) and result.get("code") == 200:
                    logger.info(f"🧹【下单工人】币安已清理 algoId: {algo_id} 的条件单")
                else:
                    logger.warning(f"⚠️【下单工人】币安撤销失败 algoId: {algo_id}, 响应: {result}")
            
        except Exception as e:
            logger.warning(f"⚠️【下单工人】币安清理残留单失败（可忽略）: {e}")
    
    # ========== 币安 OCO 展开 ==========
    
    def _expand_binance_oco(self, oco_order: Dict) -> tuple:
        """展开币安 OCO 订单为两个独立订单"""
        orders_list = oco_order.get("orders", [])
        if len(orders_list) != 2:
            logger.error(f"❌【下单工人】币安 OCO 需要 2 个订单，实际: {len(orders_list)}")
        
        result_orders = []
        for algo_order in orders_list:
            new_order = {
                "exchange": "binance",
                "type": "algo_order",
                "params": algo_order.copy()
            }
            result_orders.append(new_order)
        
        return result_orders[0], result_orders[1]
    
    # ========== 获取 API 凭证 ==========
    
    async def _fetch_all_credentials(self, orders: List[Dict]) -> Dict[str, Dict]:
        """并发获取所有需要的 API 凭证"""
        exchanges = set()
        for order in orders:
            exchanges.add(order.get("exchange"))
        
        tasks = {}
        for exchange in exchanges:
            tasks[exchange] = self.brain.data_manager.get_api_credentials(exchange)
        
        results = await asyncio.gather(*tasks.values(), return_exceptions=True)
        
        creds_map = {}
        for i, exchange in enumerate(tasks.keys()):
            creds = results[i]
            if isinstance(creds, Exception) or not creds:
                logger.error(f"❌【下单工人】无法获取 {exchange} API凭证: {creds}")
                creds_map[exchange] = None
            else:
                creds_map[exchange] = creds
        
        return creds_map
    
    # ========== 发送订单（路由） ==========
    
    async def _send_order(self, order: Dict, creds: Dict) -> Dict:
        """发送单个订单"""
        exchange = order.get("exchange")
        order_type = order.get("type")
        params = order.get("params", {})
        
        try:
            if exchange == "binance":
                result = await self._binance_send(creds, order_type, params)
            elif exchange == "okx":
                result = await self._okx_send(creds, order_type, params)
            else:
                return {"success": False, "error": f"未知交易所: {exchange}"}
            
            return {
                "success": True,
                "exchange": exchange,
                "type": order_type,
                "data": result
            }
        except Exception as e:
            logger.error(f"❌【下单工人】发送失败 [{exchange}/{order_type}]: {e}")
            return {
                "success": False,
                "exchange": exchange,
                "type": order_type,
                "error": str(e)
            }
    
    async def _error_result(self, order: Dict, error: str) -> Dict:
        return {
            "success": False,
            "exchange": order.get("exchange"),
            "type": order.get("type"),
            "error": error
        }
    
    # ========== 币安 ==========
    
    def _binance_get_base_url(self) -> str:
        if self.use_sandbox:
            return "https://testnet.binancefuture.com"
        return "https://fapi.binance.com"
    
    def _binance_get_timestamp(self) -> int:
        return int(time.time() * 1000) + self._binance_time_offset
    
    async def _binance_time_sync_loop(self):
        """后台定时同步币安时间"""
        while self._running:
            try:
                await self._binance_sync_time()
            except Exception as e:
                logger.error(f"❌【下单工人】币安时间同步失败: {e}")
            await asyncio.sleep(self._binance_sync_interval)
    
    async def _binance_sync_time(self):
        """同步币安服务器时间"""
        try:
            url = "https://fapi.binance.com/fapi/v1/time"
            loop = asyncio.get_running_loop()
            
            def _request():
                import requests
                return requests.get(url)
            
            response = await loop.run_in_executor(self._executor, _request)
            data = response.json()
            server_time = data["serverTime"]
            local_time = int(time.time() * 1000)
            self._binance_time_offset = server_time - local_time
            self._binance_last_sync = time.time()
            logger.info(f"⏱️【下单工人】币安时间同步 | 偏移量: {self._binance_time_offset}ms")
        except Exception as e:
            logger.error(f"❌【下单工人】币安时间同步失败: {e}")
    
    async def _binance_send(self, creds: Dict, order_type: str, params: Dict) -> Dict:
        """币安路由"""
        api_key = creds.get("api_key")
        api_secret = creds.get("api_secret")
        
        if order_type == "set_leverage":
            endpoint = "/fapi/v1/leverage"
            req_params = params.copy()
            return await self._binance_http_request(api_key, api_secret, "POST", endpoint, req_params)
        
        elif order_type == "open_market":
            endpoint = "/fapi/v1/order"
            req_params = params.copy()
            return await self._binance_http_request(api_key, api_secret, "POST", endpoint, req_params)
        
        elif order_type == "algo_order":
            endpoint = "/fapi/v1/algoOrder"
            req_params = params.copy()
            return await self._binance_http_request(api_key, api_secret, "POST", endpoint, req_params)
        
        elif order_type == "close_position":
            endpoint = "/fapi/v1/order"
            req_params = params.copy()
            return await self._binance_http_request(api_key, api_secret, "POST", endpoint, req_params)
        
        else:
            raise Exception(f"币安未知 order_type: {order_type}")
    
    async def _binance_http_request(self, api_key: str, api_secret: str,
                                     method: str, endpoint: str, params: Dict) -> Dict:
        """
        执行币安 HTTP 请求（新版签名规则）
        """
        base_url = self._binance_get_base_url()
        
        params = params.copy()
        params['timestamp'] = self._binance_get_timestamp()
        params['recvWindow'] = 5000
        
        sign_params = {k: v for k, v in params.items() if k != 'signature'}
        sorted_params = sorted(sign_params.items())
        raw_query_string = "&".join([f"{k}={v}" for k, v in sorted_params])
        
        encoded_payload = urllib.parse.quote(raw_query_string, safe='=&')
        signature = hmac.new(api_secret.encode(), encoded_payload.encode(), hashlib.sha256).hexdigest()
        final_query_string = encoded_payload + "&signature=" + signature
        
        logger.info(f"📤【下单工人】币安请求 [{endpoint}] 最终请求体: {final_query_string[:200]}...")
        
        url = base_url + endpoint
        headers = {
            "X-MBX-APIKEY": api_key,
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        loop = asyncio.get_running_loop()
        
        def _request():
            import requests
            if method == "POST":
                return requests.post(url, data=final_query_string, headers=headers)
            elif method == "DELETE":
                full_url = f"{url}?{final_query_string}"
                return requests.delete(full_url, headers=headers)
            else:
                full_url = f"{url}?{final_query_string}"
                return requests.get(full_url, headers=headers)
        
        response = await loop.run_in_executor(self._executor, _request)
        
        if response.status_code >= 400:
            logger.error(f"❌【下单工人】币安 HTTP 错误 [{endpoint}]: {response.status_code} - {response.text[:200]}")
            return {"error": f"HTTP {response.status_code}", "raw_response": response.text[:500]}
        
        try:
            result = response.json()
            logger.info(f"📡【下单工人】币安响应 [{endpoint}] -> {result}")
            return result
        except Exception as e:
            raw_text = response.text
            logger.warning(f"⚠️【下单工人】币安响应解析失败 [{endpoint}] -> {raw_text[:500]}")
            return {"raw_response": raw_text, "error": str(e)}
    
    # ========== 欧易 ==========
    
    def _okx_get_base_url(self) -> str:
        return "https://www.okx.com"
    
    def _okx_get_simulated_header(self) -> str:
        return "1" if self.use_sandbox else "0"
    
    def _okx_get_timestamp(self) -> str:
        return datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    
    async def _okx_time_sync_loop(self):
        while self._running:
            try:
                await self._okx_sync_time()
            except Exception as e:
                logger.error(f"❌【下单工人】欧易时间同步失败: {e}")
            await asyncio.sleep(600)
    
    async def _okx_sync_time(self):
        try:
            import requests
            url = "https://www.okx.com/api/v5/public/time"
            loop = asyncio.get_running_loop()
            
            def _request():
                return requests.get(url, timeout=5)
            
            response = await loop.run_in_executor(self._executor, _request)
            data = response.json()
            if data.get("code") == "0":
                server_time = int(data["data"][0]["ts"])
                local_time = int(time.time() * 1000)
                self._okx_time_offset = server_time - local_time
                self._okx_last_sync = time.time()
                logger.info(f"⏱️【下单工人】欧易时间同步 | 偏移量: {self._okx_time_offset}ms")
            else:
                logger.warning(f"⏱️【下单工人】欧易时间同步返回异常: {data}")
        except Exception as e:
            logger.error(f"❌【下单工人】欧易时间同步失败: {e}")
    
    async def _okx_send(self, creds: Dict, order_type: str, params: Dict) -> Dict:
        api_key = creds.get("api_key")
        api_secret = creds.get("api_secret")
        passphrase = creds.get("passphrase", "")
        
        if order_type == "set_leverage":
            endpoint = "/api/v5/account/set-leverage"
            body_params = params.copy()
            return await self._okx_http_request(api_key, api_secret, passphrase, "POST", endpoint, body_params)
        
        elif order_type == "open_market":
            endpoint = "/api/v5/trade/order"
            body_params = params.copy()
            if 'sz' in body_params:
                body_params['sz'] = round(float(body_params['sz']), 8)
            return await self._okx_http_request(api_key, api_secret, passphrase, "POST", endpoint, body_params)
        
        elif order_type == "oco":
            endpoint = "/api/v5/trade/order-algo"
            body_params = params.copy()
            return await self._okx_http_request(api_key, api_secret, passphrase, "POST", endpoint, body_params)
        
        elif order_type == "close_position":
            endpoint = "/api/v5/trade/close-position"
            body_params = params.copy()
            return await self._okx_http_request(api_key, api_secret, passphrase, "POST", endpoint, body_params)
        
        else:
            raise Exception(f"欧易未知 order_type: {order_type}")
    
    async def _okx_http_request(self, api_key: str, api_secret: str, passphrase: str,
                                 method: str, endpoint: str, params: Dict) -> Dict:
        base_url = self._okx_get_base_url()
        
        timestamp = self._okx_get_timestamp()
        body = json.dumps(params) if params else ""
        sign_str = timestamp + method + endpoint + body
        signature = base64.b64encode(
            hmac.new(api_secret.encode(), sign_str.encode(), hashlib.sha256).digest()
        ).decode()
        
        url = base_url + endpoint
        headers = {
            "OK-ACCESS-KEY": api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": passphrase,
            "Content-Type": "application/json",
            "x-simulated-trading": self._okx_get_simulated_header()
        }
        
        logger.info(f"📤【下单工人】欧易请求 [{endpoint}] Body: {body}")
        
        loop = asyncio.get_running_loop()
        
        def _request():
            import requests
            if method == "POST":
                return requests.post(url, data=body, headers=headers)
            else:
                return requests.request(method, url, params=params, headers=headers)
        
        response = await loop.run_in_executor(self._executor, _request)
        
        try:
            result = response.json()
            logger.info(f"📡【下单工人】欧易响应 [{endpoint}] -> {result}")
            return result
        except:
            raw_text = response.text
            logger.warning(f"⚠️【下单工人】欧易响应非JSON格式 [{endpoint}] -> {raw_text[:500]}")
            return {"raw_response": raw_text}