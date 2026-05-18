import re
import sys
import json
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

# Regex vạn năng: Parse mọi định dạng text chứa proxy
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
        ]

    async def fetch_url(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        headers = {'User-Agent': 'Mozilla/5.0 (Compatible; AutoProxyFetcher/4.0)'}
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

        for match in PROXY_RE.finditer(content):
            ip = match.group('ip')
            port = match.group('port')
            protocol = (match.group('protocol') or default_proto).lower()
            username = match.group('username')
            password = match.group('password')

            if not ip or not port:
                continue

            key = f"{ip}:{port}"
            if key not in self.raw_proxies:
                self.raw_proxies[key] = {
                    'ip': ip, 'port': port, 
                    'protocols': [protocol],
                    'username': username, 'password': password,
                    # Khởi tạo Schema Metadata trống
                    'continent': 'Unknown', 'country': 'Unknown', 'countryCode': '??',
                    'regionName': 'Unknown', 'city': 'Unknown', 'isp': 'Unknown', 'as': 'Unknown'
                }
            else:
                if protocol not in self.raw_proxies[key]['protocols']:
                    self.raw_proxies[key]['protocols'].append(protocol)
                if username and not self.raw_proxies[key]['username']:
                    self.raw_proxies[key]['username'] = username
                    self.raw_proxies[key]['password'] = password

    async def enrich_geolocation(self, session: aiohttp.ClientSession):
        """Gọi API định danh hàng loạt và map sâu các trường dữ liệu ISP, ASN, Vùng miền"""
        logger.info("Starting Deep Network Lookup via ip-api Batch Endpoint...")
        
        unique_ips = list({p['ip'] for p in self.raw_proxies.values()})
        chunk_size = 100 
        
        # Cấu hình các trường cần lấy đúng theo format JSON của bạn để tiết kiệm băng thông API
        fields_param = "status,continent,country,countryCode,regionName,city,isp,as,query"
        
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
                                proxy_data['continent'] = info.get('continent', 'Unknown')
                                proxy_data['country'] = info.get('country', 'Unknown')
                                proxy_data['countryCode'] = info.get('countryCode', '??')
                                proxy_data['regionName'] = info.get('regionName', 'Unknown')
                                proxy_data['city'] = info.get('city', 'Unknown')
                                proxy_data['isp'] = info.get('isp', 'Unknown')
                                proxy_data['as'] = info.get('as', 'Unknown')
                    else:
                        logger.warning(f"Geo Lookup API blocked or limited. Status: {resp.status}")
                        break
            except Exception as e:
                logger.error(f"Error during network info chunk mapping: {e}")
            
            if i + chunk_size < len(unique_ips):
                await asyncio.sleep(4.2) # Throttling bảo vệ chính sách 15 req/min

    async def verify_single_proxy(self, http_session: aiohttp.ClientSession, proxy_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ip, port = proxy_data['ip'], proxy_data['port']
        user, pwd = proxy_data['username'], proxy_data['password']
        target_proto = proxy_data['protocols'][0] 
        
        auth_str = f"{user}:{pwd}@" if user and pwd else ""
        proxy_url = f"{target_proto}://{auth_str}{ip}:{port}"
        
        try:
            if 'socks' in target_proto:
                connector = ProxyConnector.from_url(proxy_url)
                async with aiohttp.ClientSession(connector=connector) as socks_session:
                    async with socks_session.get(TEST_URL, timeout=CHECK_TIMEOUT, ssl=False) as resp:
                        if resp.status == 200:
                            return proxy_data
            else:
                async with http_session.get(TEST_URL, proxy=proxy_url, timeout=CHECK_TIMEOUT, ssl=False) as resp:
                    if resp.status == 200:
                        return proxy_data
        except Exception:
            return None
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

            logger.info("Phase 2: Verifying live status first (Optimizing API call counts)...")
            sem = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)
            
            async def safe_check(proxy):
                async with sem:
                    return await self.verify_single_proxy(session, proxy)

            tasks = [safe_check(proxy) for proxy in self.raw_proxies.values()]
            pre_results = await asyncio.gather(*tasks)
            # Chỉ lấy các proxy thực sự sống để đem đi quét thông tin IP mạng (Tiết kiệm tài nguyên)
            live_unriched = [r for r in pre_results if r]
            
            # Reset lại raw_proxies chỉ chứa những con còn sống để hàm enrich chạy chuẩn xác
            self.raw_proxies = {f"{p['ip']}:{p['port']}": p for p in live_unriched}
            
            logger.info(f"Phase 3: Looking up infrastructure details for {len(self.raw_proxies)} live proxies...")
            await self.enrich_geolocation(session)
            
            self.live_proxies = list(self.raw_proxies.values())

        logger.info(f"Pipeline complete. Target artifacts ready.")
        self.save_to_file()

    def save_to_file(self):
        try:
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                f.write(f"# Auto Proxy List - Generative Infrastructure Report\n")
                f.write(f"# Build Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
                f.write(f"# Live Proxy Count: {len(self.live_proxies)}\n")
                f.write(f"# Format: URI | Continent | Country (Code) | Region | City | ISP | ASN\n")
                f.write("-" * 120 + "\n")
                
                for p in self.live_proxies:
                    auth = f"{p['username']}:{p['password']}@" if p['username'] else ""
                    url_format = f"{p['type']}://{auth}{p['ip']}:{p['port']}"
                    
                    # Format phân tách bằng ký tự | (Pipe) để dễ bóc tách tự động bằng code của bạn sau này
                    line = (
                        f"{url_format:<45} | "
                        f"{p['continent']:<10} | "
                        f"{p['country']} ({p['countryCode']}) | "
                        f"{p['regionName']} | "
                        f"{p['city']} | "
                        f"{p['isp']} | "
                        f"{p['as']}\n"
                    )
                    f.write(line)
            logger.info(f"File stored successfully at: {OUTPUT_FILE}")
        except IOError as e:
            logger.error(f"Write deployment error: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(ProxyFetcher().run())
