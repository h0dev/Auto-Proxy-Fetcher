import re
import sys
import os
import json
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import aiohttp
from aiohttp_socks import ProxyConnector

# --- CONFIGURATIONS CẬP NHẬT CHUẨN ĐÃ TỐI ƯU ---
MAX_CONCURRENT_CHECKS = 1000  
CHECK_TIMEOUT = 5             # Đã nâng lên 5 giây theo yêu cầu của bạn
TEST_URL = 'http://httpbin.org/ip' # Thay đổi Google sang HTTPBin để tránh bị chặn nhầm
OUTPUT_FILE = 'proxies.txt'
SOURCES_FILE = 'sources.txt'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ProxyUpdater")

PROXY_RE = re.compile(
    r'(?:(?P<protocol>http|https|socks4|socks5)://)?'
    r'(?:(?P<username>[^:@\s]+):(?P<password>[^:@\s]+)@)?'
    r'(?P<ip>(?:[0-9]{1,3}\.){3}[0-9]{1,3}):(?P<port>[0-9]{1,5})', 
    re.IGNORECASE
)

class ProxyFetcher:
    def __init__(self):
        self.raw_proxies: Dict[str, Dict[str, Any]] = {}
        self.live_proxies: List[Dict[str, Any]] = []
        self.sources: List[str] = self.load_sources()

    def load_sources(self) -> List[str]:
        if not os.path.exists(SOURCES_FILE):
            logger.error(f"Không tìm thấy file cấu hình '{SOURCES_FILE}'!")
            try:
                with open(SOURCES_FILE, 'w', encoding='utf-8') as f:
                    f.write("# Danh sách link nguồn proxy (Mỗi dòng 1 URL)\n")
                logger.info(f"Đã tự động tạo file trống '{SOURCES_FILE}'. Hãy thêm link nguồn vào file này rồi chạy lại.")
            except IOError as e:
                logger.error(f"Không thể khởi tạo file {SOURCES_FILE}: {e}")
            sys.exit(1)

        sources = []
        try:
            with open(SOURCES_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        sources.append(line)
        except IOError as e:
            logger.error(f"Không thể đọc file {SOURCES_FILE}: {e}")
            sys.exit(1)
            
        if not sources:
            logger.error(f"File '{SOURCES_FILE}' đang trống!")
            sys.exit(1)

        logger.info(f"Khởi tạo thành công! Đã tải {len(sources)} nguồn proxy từ '{SOURCES_FILE}'")
        return sources

    async def fetch_url(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        headers = {'User-Agent': 'Mozilla/5.0 (Compatible; AutoProxyFetcher/5.0)'}
        try:
            async with session.get(url, headers=headers, timeout=15) as response:
                if response.status == 200:
                    return await response.text()
        except Exception as e:
            logger.debug(f"Fetch failed for {url}: {e}")
        return None

    def parse_proxy_list(self, content: str, url: str):
        if not content:
            return
            
        url_lower = url.lower()

        for match in PROXY_RE.finditer(content):
            ip = match.group('ip')
            port = match.group('port')
            username = match.group('username')
            password = match.group('password')

            if not ip or not port:
                continue

            if match.group('protocol'):
                protocols_to_try = [match.group('protocol').lower()]
            else:
                if 'socks5' in url_lower: protocols_to_try = ['socks5']
                elif 'socks4' in url_lower: protocols_to_try = ['socks4']
                elif 'http' in url_lower: protocols_to_try = ['http']
                else: protocols_to_try = ['http', 'socks5', 'socks4']

            key = f"{ip}:{port}"
            if key not in self.raw_proxies:
                self.raw_proxies[key] = {
                    'ip': ip, 'port': port, 
                    'protocols': protocols_to_try,
                    'username': username, 'password': password,
                    'country': 'Unknown', 'countryCode': '??',
                    'mobile': False, 'proxy': False, 'hosting': False
                }
            else:
                for proto in protocols_to_try:
                    if proto not in self.raw_proxies[key]['protocols']:
                        self.raw_proxies[key]['protocols'].append(proto)
                if username and not self.raw_proxies[key]['username']:
                    self.raw_proxies[key]['username'] = username
                    self.raw_proxies[key]['password'] = password

    async def enrich_geolocation(self, session: aiohttp.ClientSession):
        logger.info("Starting Deep Network Lookup via ip-api Batch Endpoint...")
        unique_ips = list({p['ip'] for p in self.raw_proxies.values()})
        chunk_size = 100 
        fields_param = "status,country,countryCode,mobile,proxy,hosting,query"
        
        for i in range(0, len(unique_ips), chunk_size):
            chunk = unique_ips[i:i + chunk_size]
            payload = [{"query": ip, "fields": fields_param} for ip in chunk]
            
            try:
                async with session.post('http://ip-api.com/batch', json=payload, timeout=15) as resp:
                    if resp.status == 200:
                        geo_results = await resp.json()
                        geo_map = {res['query']: res for res in geo_results if res.get('status') == 'success'}
                        
                        for proxy_data in self.raw_proxies.values():
                            ip = proxy_data['ip']
                            if ip in geo_map:
                                info = geo_map[ip]
                                proxy_data['country'] = info.get('country', 'Unknown')
                                proxy_data['countryCode'] = info.get('countryCode', '??')
                                proxy_data['mobile'] = info.get('mobile', False)
                                proxy_data['proxy'] = info.get('proxy', False)
                                proxy_data['hosting'] = info.get('hosting', False)
                    else:
                        logger.warning(f"Geo Lookup API limited. Status: {resp.status}")
                        break
            except Exception as e:
                logger.error(f"Error during network info chunk mapping: {e}")
            
            if i + chunk_size < len(unique_ips):
                await asyncio.sleep(4.2)

    async def verify_single_protocol_task(self, http_session: aiohttp.ClientSession, proxy_data: Dict[str, Any], proto: str) -> Optional[Dict[str, Any]]:
        ip, port = proxy_data['ip'], proxy_data['port']
        user, pwd = proxy_data['username'], proxy_data['password']
        auth_str = f"{user}:{pwd}@" if user and pwd else ""
        proxy_url = f"{proto}://{auth_str}{ip}:{port}"
        
        try:
            if 'socks' in proto:
                connector = ProxyConnector.from_url(proxy_url)
                async with aiohttp.ClientSession(connector=connector) as socks_session:
                    async with socks_session.get(TEST_URL, timeout=CHECK_TIMEOUT, ssl=False) as resp:
                        if resp.status == 200:
                            res = proxy_data.copy()
                            res['type'] = proto
                            return res
            else:
                async with http_session.get(TEST_URL, proxy=proxy_url, timeout=CHECK_TIMEOUT, ssl=False) as resp:
                    if resp.status == 200:
                        res = proxy_data.copy()
                        res['type'] = proto
                        return res
        except Exception:
            pass
        return None

    async def run(self):
        async with aiohttp.ClientSession() as session:
            logger.info("Phase 1: Parsing multidimensional proxy raw files...")
            fetch_tasks = [self.fetch_url(session, url) for url in self.sources]
            contents = await asyncio.gather(*fetch_tasks)
            
            for url, content in zip(self.sources, contents):
                self.parse_proxy_list(content, url)
            
            if not self.raw_proxies:
                logger.error("No raw proxy signatures found.")
                return

            logger.info(f"Phase 2: Verifying live status & Blazing Fast Auto-detecting ({MAX_CONCURRENT_CHECKS} Parallel Workers)...")
            sem = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)
            
            async def safe_check(proxy, proto):
                async with sem:
                    return await self.verify_single_protocol_task(session, proxy, proto)

            flattened_tasks = []
            for proxy in self.raw_proxies.values():
                for proto in proxy['protocols']:
                    flattened_tasks.append(safe_check(proxy, proto))
            
            logger.info(f"Tổng số lượng Connection Task được đẩy vào hàng đợi song song: {len(flattened_tasks)}")
            pre_results = await asyncio.gather(*flattened_tasks)
            
            unique_live = {}
            for r in pre_results:
                if r:
                    key = f"{r['ip']}:{r['port']}"
                    if key not in unique_live:
                        unique_live[key] = r
            
            self.raw_proxies = unique_live
            
            logger.info(f"Phase 3: Looking up infrastructure details for {len(self.raw_proxies)} live proxies...")
            await self.enrich_geolocation(session)
            
            self.live_proxies = list(self.raw_proxies.values())

        logger.info(f"Pipeline complete. Target artifacts ready.")
        self.save_to_file()

    def save_to_file(self):
        try:
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                f.write(f"# Auto Proxy List - Network Security Report\n")
                f.write(f"# Build Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
                f.write(f"# Live Proxy Count: {len(self.live_proxies)}\n")
                f.write(f"# Format: URI | Country (Code) | Mobile | Proxy | Hosting\n")
                f.write("-" * 90 + "\n")
                
                for p in self.live_proxies:
                    auth = f"{p['username']}:{p['password']}@" if p['username'] else ""
                    url_format = f"{p['type']}://{auth}{p['ip']}:{p['port']}"
                    
                    line = (
                        f"{url_format:<45} | "
                        f"{p['country']} ({p['countryCode']}) | "
                        f"Mob: {p['mobile']:<5} | "
                        f"Prx: {p['proxy']:<5} | "
                        f"Hst: {p['hosting']}\n"
                    )
                    f.write(line)
            logger.info(f"File stored successfully at: {OUTPUT_FILE}")
        except IOError as e:
            logger.error(f"Write deployment error: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(ProxyFetcher().run())
