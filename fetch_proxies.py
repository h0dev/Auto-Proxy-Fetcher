import re
import sys
import json
import time
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import aiohttp
from aiohttp_socks import ProxyConnector

# --- CONFIGURATIONS ---
MAX_CONCURRENT_CHECKS = 300
CHECK_TIMEOUT = 8
TEST_URL = 'http://www.google.com'
OUTPUT_FILE = 'proxies.txt'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ProxyUpdater")

# Regex thần thánh: Bắt mọi định dạng từ protocol://user:pass@ip:port đến ip:port
# Các Capture Group: protocol, username, password, ip, port
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
        self.sources = [
            'https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http%2Chttps%2Csocks4%2Csocks5',
            'https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.json',
            'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt',
            'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt'
            # Có thể thêm các URL khác vào đây
        ]

    async def fetch_url(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        headers = {'User-Agent': 'Mozilla/5.0 (Compatible; AutoProxyFetcher/3.0)'}
        try:
            async with session.get(url, headers=headers, timeout=20) as response:
                if response.status == 200:
                    return await response.text()
        except Exception as e:
            logger.debug(f"Fetch failed for {url}: {e}")
        return None

    def parse_proxy_list(self, content: str, url: str):
        if not content:
            return
            
        url_lower = url.lower()
        default_proto = 'http'
        if 'socks5' in url_lower: default_proto = 'socks5'
        elif 'socks4' in url_lower: default_proto = 'socks4'

        # Chuyển JSON thành String thô để Regex xử lý (nếu là dạng JSON chứa IP:Port)
        # Regex cực kỳ linh hoạt nên sẽ tự tìm thấy các block ip:port trong cả text lẫn json
        for match in PROXY_RE.finditer(content):
            ip = match.group('ip')
            port = match.group('port')
            protocol = (match.group('protocol') or default_proto).lower()
            username = match.group('username')
            password = match.group('password')

            if not ip or not port:
                continue

            # Sử dụng tuple cấu trúc làm key để tránh trùng lặp
            key = f"{ip}:{port}"
            if key not in self.raw_proxies:
                self.raw_proxies[key] = {
                    'ip': ip, 'port': port, 
                    'protocols': [protocol],
                    'username': username, 'password': password,
                    'country': 'Unknown', 'city': 'Unknown'
                }
            else:
                if protocol not in self.raw_proxies[key]['protocols']:
                    self.raw_proxies[key]['protocols'].append(protocol)
                # Cập nhật auth nếu record trước đó không có
                if username and not self.raw_proxies[key]['username']:
                    self.raw_proxies[key]['username'] = username
                    self.raw_proxies[key]['password'] = password

    async def enrich_geolocation(self, session: aiohttp.ClientSession):
        """Sử dụng ip-api.com Batch Endpoint để tra cứu Geolocation đồng loạt"""
        logger.info("Enriching geolocation data via IP Lookup Tool...")
        
        # Lấy danh sách IP unique (không chứa port)
        unique_ips = list({p['ip'] for p in self.raw_proxies.values()})
        chunk_size = 100 # Giới hạn của ip-api batch endpoint
        
        for i in range(0, len(unique_ips), chunk_size):
            chunk = unique_ips[i:i + chunk_size]
            payload = [{"query": ip, "fields": "country,city,query"} for ip in chunk]
            
            try:
                # ip-api batch endpoint (Max 15 requests per minute)
                async with session.post('http://ip-api.com/batch', json=payload, timeout=15) as resp:
                    if resp.status == 200:
                        geo_results = await resp.json()
                        # Tạo hashmap IP -> Geo để map lại vào raw_proxies (O(1) lookup)
                        geo_map = {res['query']: res for res in geo_results if res.get('country')}
                        
                        for proxy_data in self.raw_proxies.values():
                            ip = proxy_data['ip']
                            if ip in geo_map:
                                proxy_data['country'] = geo_map[ip].get('country', 'Unknown')
                                proxy_data['city'] = geo_map[ip].get('city', 'Unknown')
                    else:
                        logger.warning(f"Geo API Rate limit reached or error. Status: {resp.status}")
                        break # Dừng lookup nếu bị chặn
            except Exception as e:
                logger.error(f"Geo Lookup error: {e}")
            
            # Throttling: Chờ 4.2 giây giữa các batch để đảm bảo dưới 15 req/phút
            if i + chunk_size < len(unique_ips):
                await asyncio.sleep(4.2)

    async def verify_single_proxy(self, http_session: aiohttp.ClientSession, proxy_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ip, port = proxy_data['ip'], proxy_data['port']
        user, pwd = proxy_data['username'], proxy_data['password']
        
        target_proto = proxy_data['protocols'][0] # Test protocol đầu tiên
        
        # Xây dựng Authentication string nếu có
        auth_str = f"{user}:{pwd}@" if user and pwd else ""
        proxy_url = f"{target_proto}://{auth_str}{ip}:{port}"
        
        start_time = time.perf_counter()
        
        try:
            if 'socks' in target_proto:
                connector = ProxyConnector.from_url(proxy_url)
                async with aiohttp.ClientSession(connector=connector) as socks_session:
                    async with socks_session.get(TEST_URL, timeout=CHECK_TIMEOUT, ssl=False) as resp:
                        if resp.status == 200:
                            latency = int((time.perf_counter() - start_time) * 1000)
                            return {**proxy_data, 'latency': latency, 'type': target_proto}
            else:
                async with http_session.get(TEST_URL, proxy=proxy_url, timeout=CHECK_TIMEOUT, ssl=False) as resp:
                    if resp.status == 200:
                        latency = int((time.perf_counter() - start_time) * 1000)
                        return {**proxy_data, 'latency': latency, 'type': target_proto}
        except Exception:
            return None
        return None

    async def run(self):
        async with aiohttp.ClientSession() as session:
            logger.info("Phase 1: Harvesting proxies from sources...")
            fetch_tasks = [self.fetch_url(session, url) for url in self.sources]
            contents = await asyncio.gather(*fetch_tasks)
            
            for url, content in zip(self.sources, contents):
                self.parse_proxy_list(content, url)
            
            logger.info(f"Unique IPs extracted: {len(self.raw_proxies)}")
            if not self.raw_proxies:
                return

            logger.info("Phase 2: IP Lookup...")
            await self.enrich_geolocation(session)

            logger.info(f"Phase 3: Validating {len(self.raw_proxies)} proxies...")
            sem = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)
            
            async def safe_check(proxy):
                async with sem:
                    return await self.verify_single_proxy(session, proxy)

            tasks = [safe_check(proxy) for proxy in self.raw_proxies.values()]
            results = await asyncio.gather(*tasks)
            self.live_proxies = [r for r in results if r]

        self.live_proxies.sort(key=lambda x: x['latency'])
        logger.info(f"Pipeline finished! Live Proxies: {len(self.live_proxies)}")
        self.save_to_file()

    def save_to_file(self):
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write(f"# Auto Proxy List - Updated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
            f.write(f"# Live Count: {len(self.live_proxies)}\n")
            f.write(f"# Format: Protocol://User:Pass@IP:Port # Country # Latency\n")
            f.write("-" * 80 + "\n")
            for p in self.live_proxies:
                auth = f"{p['username']}:{p['password']}@" if p['username'] else ""
                url_format = f"{p['type']}://{auth}{p['ip']}:{p['port']}"
                f.write(f"{url_format:<40} # {p['country']:<15} # {p['latency']}ms\n")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(ProxyFetcher().run())
