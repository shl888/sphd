"""
WebSocket连接池总管理器 - 增强版（独立获取 + 智能降级 + 精确双平台匹配）
"""
import asyncio
import logging
import sys
import os
import time
import json
import aiohttp
from typing import Dict, Any, List, Optional, Set, Callable

# 设置导入路径
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))  # smart_brain目录
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from shared_data.data_store import data_store
from .exchange_pool import ExchangeWebSocketPool
from .config import EXCHANGE_CONFIGS
from .static_symbols import STATIC_SYMBOLS  # 导入静态合约

logger = logging.getLogger(__name__)

# ============ 🚫 合约黑名单 ============
# 原因：两个交易所合约名相同，但实际标的完全不一样
# 币安格式: BBUSDT
# 欧易格式: BB-USDT-SWAP
# 发现新的同名不同标合约，直接加在这里，两边都要加
SYMBOL_BLACKLIST = {
    "binance": [
        "BBUSDT",
        "QNTUSDT",
        "ONUSDT",
        # 发现新的就加在这里
    ],
    "okx": [
        "BB-USDT-SWAP",
        "QNT-USDT-SWAP",
        "ON-USDT-SWAP",
        # 发现新的就加在这里
    ]
}

# ============ 【固定数据回调函数】============
async def default_data_callback(data):
    """默认数据回调函数 - 带阈值清零版"""
    try:
        if not data:
            logger.debug("[数据回调] 收到空数据")
            return
            
        exchange = data.get("exchange", "")
        symbol = data.get("symbol", "")
        data_type = data.get("data_type", "unknown")
        
        if not exchange:
            logger.warning(f"[数据回调] 数据缺少exchange字段")
            return
        if not symbol:
            logger.warning(f"[数据回调] 数据缺少symbol字段")
            return
        
        # 计数器初始化
        if not hasattr(default_data_callback, 'counter'):
            default_data_callback.counter = 0
            logger.info(f"🌎【数据回调初始化】计数器创建")
        
        # 先增加计数
        default_data_callback.counter += 1
        current_count = default_data_callback.counter
        
        # 等于或超过300万就清零
        if current_count >= 3000000:
            default_data_callback.counter = 0
            current_count = 0
            logger.info(f"🫗【数据回调阈值重置】达到300万条，计数器清零重新开始")
        
        # 第一条数据
        if current_count == 1:
            logger.info(f"🎉【数据回调第一条数据】{exchange} {symbol} ({data_type})")
        
        # 每30000条记录一次数据流动
        if current_count % 30000 == 0:
            logger.info(f"✅【数据回调已接收】{current_count:,}条数据 - 最新: {exchange} {symbol}")
        
        # 每300000条里程碑
        if current_count % 300000 == 0:
            logger.info(f"🏆【数据回调里程碑】{current_count:,} 条数据,已存储到data_store")
        
        # 直接存储到data_store
        await data_store.update_market_data(exchange, symbol, data)
            
    except Exception as e:
        logger.error(f"❌[数据回调] 存储失败: {e}")

# ============ 【极简HTTP合约获取器】============
class SimpleSymbolFetcher:
    """极简合约获取器 - 直接HTTP请求，3次重试+换IP"""
    
    # 币安API
    BINANCE_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    
    # 欧意API - 直接获取U本位永续合约
    OKX_URL = "https://www.okx.com/api/v5/public/instruments?instType=SWAP&quoteCcy=USDT"
    
    async def _fetch_with_retry(self, exchange_name: str, url: str, parser_func: Callable) -> List[str]:
        """通用重试获取函数 - 3次重试，每次新连接"""
        for attempt in range(1, 4):  # 3次重试
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环开始让出CPU
            try:
                # 每次创建新会话，强制新连接（换IP）
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        
                        # 处理418被封的情况
                        if resp.status == 418:
                            logger.warning(f"[{exchange_name}] IP被封（418），第{attempt}次尝试，等待后换IP重试")
                            await asyncio.sleep(attempt * 3)  # 3秒、6秒、9秒递增
                            continue
                        
                        # 处理其他4xx错误（不重试）
                        if 400 <= resp.status < 500 and resp.status != 418:
                            logger.error(f"[{exchange_name}] 客户端错误 {resp.status}，不重试")
                            return []
                        
                        # 处理5xx错误（重试）
                        if resp.status >= 500:
                            logger.warning(f"[{exchange_name}] 服务端错误 {resp.status}，第{attempt}次尝试")
                            await asyncio.sleep(attempt * 2)
                            continue
                        
                        # 200成功
                        if resp.status == 200:
                            data = await resp.json()
                            # ✅ [蚂蚁基因修复] 将同步解析函数放到线程池执行
                            loop = asyncio.get_event_loop()
                            symbols = await loop.run_in_executor(None, parser_func, data)
                            
                            if symbols:
                                logger.info(f"✅ [{exchange_name}] HTTP获取成功: {len(symbols)}个合约 (第{attempt}次)")
                                return symbols
                            else:
                                logger.warning(f"[{exchange_name}] 解析后为空列表，第{attempt}次尝试")
                                await asyncio.sleep(2)
                                continue
                        
                        # 其他状态码
                        logger.warning(f"[{exchange_name}] 未知状态码 {resp.status}，第{attempt}次尝试")
                        await asyncio.sleep(2)
                        continue
                        
            except asyncio.TimeoutError:
                logger.warning(f"[{exchange_name}] 请求超时，第{attempt}次尝试")
                await asyncio.sleep(attempt * 2)
                
            except aiohttp.ClientConnectorError as e:
                logger.warning(f"[{exchange_name}] 连接错误: {e}，第{attempt}次尝试")
                await asyncio.sleep(attempt * 2)
                
            except Exception as e:
                logger.warning(f"[{exchange_name}] 请求异常: {e}，第{attempt}次尝试")
                await asyncio.sleep(attempt * 2)
        
        logger.error(f"❌ [{exchange_name}] 所有3次尝试失败")
        return []
    
    # ✅ [蚂蚁基因修复] 将解析函数改为同步，因为会在线程池中执行
    def _parse_binance_sync(self, data: dict) -> List[str]:
        """同步解析币安返回数据（在线程池中执行）"""
        symbols = []
        for s in data.get('symbols', []):
            if (s.get('contractType') == 'PERPETUAL' and 
                s.get('quoteAsset') == 'USDT' and 
                s.get('status') == 'TRADING'):
                symbols.append(s.get('symbol'))
        return symbols
    
    def _parse_okx_sync(self, data: dict) -> List[str]:
        """同步解析欧意返回数据（在线程池中执行）"""
        if data.get('code') != '0':
            logger.warning(f"[欧意] API返回错误码: {data.get('code')}")
            return []
        
        data_list = data.get('data', [])
        return [i.get('instId') for i in data_list if i.get('instId')]
    
    async def fetch_binance(self) -> List[str]:
        """获取币安USDT永续合约 - 3次重试+换IP"""
        return await self._fetch_with_retry(
            exchange_name="币安",
            url=self.BINANCE_URL,
            parser_func=self._parse_binance_sync
        )
    
    async def fetch_okx(self) -> List[str]:
        """获取欧意USDT永续合约 - 3次重试+换IP"""
        return await self._fetch_with_retry(
            exchange_name="欧意",
            url=self.OKX_URL,
            parser_func=self._parse_okx_sync
        )

# ============ 【WebSocket连接池管理器类】============
class WebSocketPoolManager:
    """WebSocket连接池管理器 - 增强版（独立获取 + 智能降级 + 精确双平台匹配）"""
    
    def __init__(self, admin_instance=None):
        """初始化连接池管理器 - 固定使用default_data_callback"""
        self.data_callback = default_data_callback
        self.admin_instance = admin_instance
        
        self.exchange_pools = {}  # exchange_name -> ExchangeWebSocketPool
        self.initialized = False
        self._initializing = False
        self._shutting_down = False
        self._common_symbols_cache = None
        self._last_symbols_update = 0
        
        # 存储各交易所的原始合约列表和来源信息
        self._raw_symbols_info = {
            "binance": {"symbols": [], "source": "unknown", "count": 0},
            "okx": {"symbols": [], "source": "unknown", "count": 0}
        }
        
        # 极简HTTP获取器（3次重试+换IP）
        self.fetcher = SimpleSymbolFetcher()
        
        logger.info("✅ WebSocketPoolManager 初始化完成（3次重试+换IP版）")
        if admin_instance:
            logger.info("☎️【连接池】已设置管理员引用")
    
    # ============ 核心流程方法 ============
    
    async def initialize(self):
        """初始化所有交易所连接池"""
        if self.initialized or self._initializing:
            logger.info("WebSocket连接池已在初始化或已初始化")
            return
        
        self._initializing = True
        logger.info(f"{'=' * 60}")
        logger.info("🔄 正在初始化WebSocket连接池管理器（3次重试+换IP版）...")
        logger.info("🚀 流程：独立获取 → 智能降级 → 双平台匹配")
        logger.info(f"{'=' * 60}")
        
        try:
            # 1. 独立获取各交易所原始合约
            await self._fetch_all_exchange_symbols_independent()
            
            # 2. 双平台匹配
            common_symbols = await self._calculate_common_symbols()
            
            # 3. 初始化连接池
            await self._initialize_all_exchange_pools(common_symbols)
            
            self.initialized = True
            logger.info("✅✅✅ WebSocket连接池管理器初始化完成")
            logger.info(f"{'=' * 60}")
            
            self._print_initialization_summary()
            
        except Exception as e:
            logger.error(f"❌ 连接池管理器初始化失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self._initializing = False
    
    async def _fetch_all_exchange_symbols_independent(self):
        """独立获取各交易所的原始合约列表"""
        logger.info("📥【步骤1】开始独立获取各交易所合约列表...")
        
        tasks = []
        for exchange_name in ["binance", "okx"]:
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
            task = asyncio.create_task(
                self._fetch_exchange_symbols_with_fallback(exchange_name)
            )
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, exchange_name in enumerate(["binance", "okx"]):
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
            result = results[i]
            if isinstance(result, Exception):
                logger.error(f"❌[{exchange_name}] 获取合约失败: {result}")
                static_symbols = self._get_static_symbols(exchange_name)
                self._raw_symbols_info[exchange_name] = {
                    "symbols": static_symbols,
                    "source": "static_fallback",
                    "count": len(static_symbols)
                }
                logger.warning(f"⚠️[{exchange_name}] 使用静态列表兜底: {len(static_symbols)}个")
            else:
                self._raw_symbols_info[exchange_name] = result
                logger.info(f"✅[{exchange_name}] 获取完成: {result['count']}个合约（来源: {result['source']}）")
    
    async def _fetch_exchange_symbols_with_fallback(self, exchange_name: str) -> Dict[str, Any]:
        """获取单个交易所的合约列表（带智能降级）"""
        symbols = []
        source = "unknown"
        
        # 1. 优先尝试HTTP获取（3次重试+换IP）
        try:
            if exchange_name == "binance":
                symbols = await self.fetcher.fetch_binance()
            else:
                symbols = await self.fetcher.fetch_okx()
            
            if symbols:
                source = "http"
                logger.info(f"✅[{exchange_name}] HTTP获取成功: {len(symbols)}个")
                return {"symbols": symbols, "source": source, "count": len(symbols)}
            else:
                logger.warning(f"⚠️[{exchange_name}] HTTP获取返回空列表")
        except Exception as e:
            logger.warning(f"⚠️[{exchange_name}] HTTP获取异常: {e}")
        
        # 2. 降级：使用静态合约列表
        static_symbols = self._get_static_symbols(exchange_name)
        if static_symbols:
            symbols = static_symbols
            source = "static"
            logger.info(f"⚠️[{exchange_name}] 使用静态列表: {len(symbols)}个")
        else:
            logger.error(f"❌[{exchange_name}] 静态列表为空，无合约可用")
            symbols = []
            source = "empty"
        
        return {"symbols": symbols, "source": source, "count": len(symbols)}
    
    async def _calculate_common_symbols(self) -> Dict[str, List[str]]:
        """计算双平台共有合约"""
        logger.info("🔄【步骤2】计算双平台共有合约...")
        
        binance_info = self._raw_symbols_info.get("binance", {})
        okx_info = self._raw_symbols_info.get("okx", {})
        
        binance_symbols = binance_info.get("symbols", [])
        okx_symbols = okx_info.get("symbols", [])
        
        logger.info(f"📊 币安原始合约: {len(binance_symbols)}个 (来源: {binance_info.get('source', 'unknown')})")
        logger.info(f"📊 OKX原始合约: {len(okx_symbols)}个 (来源: {okx_info.get('source', 'unknown')})")
        
        if not binance_symbols or not okx_symbols:
            logger.warning("⚠️ 至少一个交易所无合约，无法进行双平台匹配")
            return {}
        
        # ✅ [蚂蚁基因修复] 将同步匹配函数放到线程池执行
        loop = asyncio.get_event_loop()
        common_result = await loop.run_in_executor(
            None, self._find_common_symbols_precise_sync, binance_symbols, okx_symbols
        )
        
        if common_result and common_result.get("binance") and common_result.get("okx"):
            self._common_symbols_cache = common_result
            self._last_symbols_update = time.time()
            
            binance_count = len(common_result["binance"])
            okx_count = len(common_result["okx"])
            
            logger.info(f"🎯 发现 {binance_count} 个双平台共有合约")
            logger.info(f"📈 匹配成功率: 币安 {binance_count}/{len(binance_symbols)} ({binance_count/len(binance_symbols)*100:.1f}%)")
            logger.info(f"📈 匹配成功率: OKX {okx_count}/{len(okx_symbols)} ({okx_count/len(okx_symbols)*100:.1f}%)")
            
            return common_result
        else:
            logger.warning("⚠️ 未找到任何双平台共有合约")
            return {}
    
    async def _initialize_all_exchange_pools(self, common_symbols: Dict[str, List[str]]):
        """初始化所有交易所连接池"""
        logger.info("🚀【步骤3】初始化交易所连接池...")
        
        tasks = []
        for exchange_name in ["binance", "okx"]:
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
            if common_symbols and exchange_name in common_symbols:
                symbols = common_symbols[exchange_name]
                mode = "双平台模式"
            else:
                symbols = self._raw_symbols_info[exchange_name].get("symbols", [])
                mode = "单平台模式"
            
            if not symbols:
                logger.warning(f"⚠️[{exchange_name}] 无合约可用，跳过初始化")
                continue
            
            task = asyncio.create_task(
                self._setup_exchange_pool_with_symbols(exchange_name, symbols, mode)
            )
            tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _setup_exchange_pool_with_symbols(self, exchange_name: str, symbols: List[str], mode: str):
        """使用指定合约列表初始化单个交易所连接池"""
        try:
            logger.info(f"[{exchange_name}] 正在初始化连接池 ({mode})...")
            
            active_connections = EXCHANGE_CONFIGS[exchange_name].get("active_connections", 3)
            symbols_per_conn = EXCHANGE_CONFIGS[exchange_name].get("symbols_per_connection", 300)
            max_symbols = symbols_per_conn * active_connections
            
            original_count = len(symbols)
            if original_count > max_symbols:
                logger.info(f"[{exchange_name}] 合约数量 {original_count} > 限制 {max_symbols}，进行裁剪")
                symbols = symbols[:max_symbols]
                logger.info(f"[{exchange_name}] 裁剪后: {len(symbols)}个合约")
            
            pool = ExchangeWebSocketPool(exchange_name, self.data_callback, self.admin_instance)
            await pool.initialize(symbols)
            self.exchange_pools[exchange_name] = pool
            
            logger.info(f"✅[{exchange_name}] 连接池初始化成功 ({mode})")
            logger.info(f"  使用合约: {len(symbols)}个")
            
        except Exception as e:
            logger.error(f"[{exchange_name}] ❌ 连接池初始化失败: {e}")
            raise
    
    def _print_initialization_summary(self):
        """打印初始化摘要"""
        logger.info(f"{'=' * 60}")
        logger.info("📋 【初始化完成摘要】")
        
        for exchange_name in ["binance", "okx"]:
            if exchange_name in self.exchange_pools:
                pool = self.exchange_pools[exchange_name]
                source_info = self._raw_symbols_info.get(exchange_name, {})
                
                logger.info(f"  [{exchange_name.upper()}]")
                logger.info(f"    状态: ✅ 运行中")
                logger.info(f"    数据源: {source_info.get('source', 'unknown')}")
                logger.info(f"    原始合约: {source_info.get('count', 0)}个")
                logger.info(f"    使用合约: {len(pool.symbols)}个")
            else:
                logger.info(f"  [{exchange_name.upper()}]")
                logger.info(f"    状态: ❌ 未运行")
        
        if self._common_symbols_cache:
            binance_count = len(self._common_symbols_cache.get("binance", []))
            logger.info(f"  [双平台匹配]")
            logger.info(f"    状态: ✅ 已匹配")
            logger.info(f"    共有合约: {binance_count}个")
        else:
            logger.info(f"  [双平台匹配]")
            logger.info(f"    状态: ⚠️ 未匹配（单平台模式）")
        
        logger.info(f"{'=' * 60}")
    
    # ============ 精确双平台匹配核心方法 ============
    
    # ✅ [蚂蚁基因修复] 改为同步方法，因为会在线程池中执行
    def _find_common_symbols_precise_sync(self, binance_symbols: List[str], okx_symbols: List[str]) -> Dict[str, List[str]]:
        """同步精确查找双平台共有合约（在线程池中执行）"""
        binance_coin_to_contract = {}
        okx_coin_to_contract = {}
        
        for symbol in binance_symbols:
            coin = self._extract_coin_precise(symbol, "binance")
            if coin and coin not in binance_coin_to_contract:
                binance_coin_to_contract[coin] = symbol
        
        for symbol in okx_symbols:
            coin = self._extract_coin_precise(symbol, "okx")
            if coin and coin not in okx_coin_to_contract:
                okx_coin_to_contract[coin] = symbol
        
        binance_coins = set(binance_coin_to_contract.keys())
        okx_coins = set(okx_coin_to_contract.keys())
        common_coins = sorted(list(binance_coins.intersection(okx_coins)))
        
        if not common_coins:
            return {}
        
        validated_common_coins = []
        
        for coin in common_coins:
            binance_contract = binance_coin_to_contract[coin]
            okx_contract = okx_coin_to_contract[coin]
            
            # ===== 🚫 黑名单过滤 =====
            # 检查币安合约是否在黑名单中
            if binance_contract in SYMBOL_BLACKLIST.get("binance", []):
                continue
            # 检查欧易合约是否在黑名单中
            if okx_contract in SYMBOL_BLACKLIST.get("okx", []):
                continue
            # ===== 黑名单过滤结束 =====
            
            binance_extracted = self._extract_coin_precise(binance_contract, "binance")
            okx_extracted = self._extract_coin_precise(okx_contract, "okx")
            
            if binance_extracted == okx_extracted == coin:
                if self._is_valid_match(coin, binance_contract, okx_contract):
                    validated_common_coins.append(coin)
        
        result = {
            "binance": [binance_coin_to_contract[coin] for coin in validated_common_coins],
            "okx": [okx_coin_to_contract[coin] for coin in validated_common_coins]
        }
        
        result["binance"] = sorted(result["binance"])
        result["okx"] = sorted(result["okx"])
        
        return result
    
    def _extract_coin_precise(self, contract_name: str, exchange: str) -> Optional[str]:
        """精确提取币种"""
        if not contract_name:
            return None
        
        contract_upper = contract_name.upper()
        
        if exchange == "binance":
            if contract_upper.endswith("USDT"):
                return contract_upper[:-4]
            return None
        
        elif exchange == "okx":
            if "-USDT-SWAP" in contract_upper:
                return contract_upper.replace("-USDT-SWAP", "")
            return None
        
        return None
    
    def _is_valid_match(self, coin: str, binance_contract: str, okx_contract: str) -> bool:
        """验证匹配是否合理"""
        common_mistakes = [
            ("BTC", "BTCDOM"),
            ("PUMP", "PUMPBTC"),
            ("BABY", "BABYDOGE"),
            ("DOGE", "BABYDOGE"),
            ("SHIB", "1000SHIB"),
            ("ETH", "ETHW"),
        ]
        
        binance_coin = self._extract_coin_precise(binance_contract, "binance")
        if binance_coin != coin:
            return False
        
        okx_coin = self._extract_coin_precise(okx_contract, "okx")
        if okx_coin != coin:
            return False
        
        for correct, wrong in common_mistakes:
            if coin == correct and (binance_coin == wrong or okx_coin == wrong):
                return False
        
        if binance_contract.startswith("1000") and not okx_contract.startswith("1000"):
            return False
        
        return True
    
    def _get_static_symbols(self, exchange_name: str) -> List[str]:
        """获取静态合约列表"""
        return STATIC_SYMBOLS.get(exchange_name, [])
    
    # ============ 管理和状态方法 ============
    
    async def get_all_status(self) -> Dict[str, Any]:
        """获取所有交易所连接状态"""
        status = {}
        
        for exchange_name, pool in self.exchange_pools.items():
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
            try:
                pool_status = await pool.get_status()
                status[exchange_name] = pool_status
            except Exception as e:
                logger.error(f"❌[{exchange_name}] 获取状态错误: {e}")
                status[exchange_name] = {"error": str(e)}
        
        return status
    
    async def shutdown(self):
        """关闭所有连接池"""
        if self._shutting_down:
            logger.info("⚠️ 连接池已在关闭中，跳过重复操作")
            return
        
        self._shutting_down = True
        logger.info("⚠️ 正在关闭所有WebSocket连接池...")
        
        for exchange_name, pool in self.exchange_pools.items():
            await asyncio.sleep(0)  # ✅ [蚂蚁基因修复] 循环内让出CPU
            try:
                await pool.shutdown()
            except Exception as e:
                logger.error(f"❌[{exchange_name}] 关闭连接池错误: {e}")
        
        logger.info("✅ 所有WebSocket连接池已关闭")
    
    def get_common_symbols_stats(self) -> Dict[str, Any]:
        """获取双平台合约统计信息"""
        if not self._common_symbols_cache:
            return {"status": "未计算", "binance_count": 0, "okx_count": 0}
        
        return {
            "status": "已计算",
            "binance_count": len(self._common_symbols_cache.get("binance", [])),
            "okx_count": len(self._common_symbols_cache.get("okx", [])),
            "last_update": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self._last_symbols_update)),
        }
    
    def get_raw_symbols_info(self) -> Dict[str, Any]:
        """获取原始合约信息"""
        return self._raw_symbols_info