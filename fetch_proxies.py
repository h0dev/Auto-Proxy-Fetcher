import re
import sys
import os
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import aiohttp
from aiohttp_socks import ProxyConnector

# --- CẤU HÌNH SIÊU TỐC ĐỘ + ĐỊNH DANH GEOIP ---
MAX_WORKERS = 1500            
GEO_CONCURRENCY = 100         
CHECK_TIMEOUT = 5             
TEST_URL = 'http://httpbin.org/ip'
OUTPUT_FILE = 'proxies.txt'
SOURCES_FILE = 'sources.txt'

# LẤY API TOKEN BẢO MẬT TỪ GITHUB SECRETS
GEO_TOKEN = os.environ.get('FINDIP_TOKEN', '').strip()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ProxyGeoFinder")

PROXY_RE = re.compile(
    r'(?:(?P<protocol>http|https|socks4|socks5)://)?'
    r'(?:(?P<username>[^:@\s]+):(?P<password>[^:@\s]+)@)?'
    r'(?P<ip>(?:[0-9]{1,3}\.){3}[0-9]{1,3}):(?P<port>[0-9]{1,5})', 
    re.IGNORECASE
)

class BigDataGeoProxyFetcher:
    def __init__(self):
        self.raw_proxies: Dict[str, Dict[str, Any]] = {}
        self.live_proxies: Dict[str, Dict[str, Any]] = {}
        self.sources: List[str] = []

    def load_sources(self) -> List[str]:
        if not os.path.exists(SOURCES_FILE):
            logger.error(f"Không tìm thấy file '{SOURCES_FILE}'!")
            sys.exit(1)
        sources = []
        with open(SOURCES_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    sources.append(line)
        return sources

    async def fetch_url(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        try:
            async with session.get(url, timeout=25) as response:
                if response.status == 200:
                    return await response.text()
        except Exception:
            pass
        return None

    def parse_proxy_list(self, content: str, url: str):
        if not content: return
        url_lower = url.lower()

        for match in PROXY_RE.finditer(content):
            ip, port = match.group('ip'), match.group('port')
            if not ip or not port: continue

            if match.group('protocol'):
                protocols = [match.group('protocol').lower()]
            else:
                if 'socks5' in url_lower: protocols = ['socks5']
                elif 'socks4' in url_lower: protocols = ['socks4']
                elif 'http' in url_lower: protocols = ['http']
                else: protocols = ['http', 'socks5', 'socks4']

            key = f"{ip}:{port}"
            if key not in self.raw_proxies:
                self.raw_proxies[key] = {
                    'ip': ip, 'port': port, 'protocols': protocols,
                    'username': match.group('username'), 'password': match.group('password'),
                    'country': 'Unknown', 'countryCode': '??', 'isp': 'Unknown', 'user_type': 'Unknown'
                }
            else:
                for proto in protocols:
                    if proto not in self.raw_proxies[key]['protocols']:
                        self.raw_proxies[key]['protocols'].append(proto)

    async def verify_task(self, http_session: aiohttp.ClientSession, proxy_data: Dict[str, Any], proto: str):
        ip, port = proxy_data['ip'], proxy_data['port']
        user, pwd = proxy_data['username'], proxy_data['password']
        auth_str = f"{user}:{pwd}@" if user and pwd else ""
        proxy_url = f"{proto}://{auth_str}{ip}:{port}"
        
        timeout_cfg = aiohttp.ClientTimeout(total=CHECK_TIMEOUT, connect=CHECK_TIMEOUT, sock_connect=CHECK_TIMEOUT)
        
        try:
            if 'socks' in proto:
                async def do_socks():
                    connector = ProxyConnector.from_url(proxy_url)
                    async with aiohttp.ClientSession(connector=connector, timeout=timeout_cfg) as socks_session:
                        async with socks_session.get(TEST_URL, ssl=False) as resp:
                            return resp.status == 200
                is_live = await asyncio.wait_for(do_socks(), timeout=CHECK_TIMEOUT + 1)
            else:
                async def do_http():
                    async with http_session.get(TEST_URL, proxy=proxy_url, timeout=timeout_cfg, ssl=False) as resp:
                        return resp.status == 200
                is_live = await asyncio.wait_for(do_http(), timeout=CHECK_TIMEOUT + 1)

            if is_live:
                key = f"{ip}:{port}"
                if key not in self.live_proxies:
                    res = proxy_data.copy()
                    res['type'] = proto
                    self.live_proxies[key] = res
        except Exception:
            pass

    async def worker(self, queue: asyncio.Queue, session: aiohttp.ClientSession):
        while not queue.empty():
            proxy_data, proto = await queue.get()
            await self.verify_task(session, proxy_data, proto)
            queue.task_done()

    async def fetch_single_geo(self, session: aiohttp.ClientSession, ip: str, sem: asyncio.Semaphore) -> Optional[Dict[str, Any]]:
        url = f"https://api.findip.net/{ip}/?token={GEO_TOKEN}"
        async with sem:
            try:
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        return await resp.json()
            except Exception:
                pass
        return None

    async def enrich_geolocation(self, session: aiohttp.ClientSession):
        if not GEO_TOKEN:
            logger.warning("Không tìm thấy FINDIP_TOKEN trong cấu hình hệ thống. Bỏ qua Phase GeoIP.")
            return

        unique_ips = list({p['ip'] for p in self.live_proxies.values()})
        if not unique_ips: return
        
        logger.info(f"Phase 3: Bắt đầu định danh loại hình IP (user_type) cho {len(unique_ips)} proxy sống...")
        sem = asyncio.Semaphore(GEO_CONCURRENCY)
        
        tasks = [self.fetch_single_geo(session, ip, sem) for ip in unique_ips]
        results = await asyncio.gather(*tasks)
        
        geo_map = {}
        for ip, res in zip(unique_ips, results):
            if res and 'country' in res:
                # ĐỔI THÀNH TRÍCH XUẤT TRƯỜNG USER_TYPE THEO YÊU CẦU CỦA BẠN
                geo_map[ip] = {
                    'country': res['country']['names'].get('en', 'Unknown'),
                    'countryCode': res['country'].get('iso_code', '??'),
                    'isp': res['traits'].get('isp', 'Unknown'),
                    'user_type': res['traits'].get('user_type', 'Unknown') 
                }
        
        for proxy_data in self.live_proxies.values():
            ip = proxy_data['ip']
            if ip in geo_map:
                info = geo_map[ip]
                proxy_data['country'] = info['country']
                proxy_data['countryCode'] = info['countryCode']
                proxy_data['isp'] = info['isp']
                proxy_data['user_type'] = info['user_type']

    async def run(self):
        self.sources = self.load_sources()
        connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=600, limit_per_host=0)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            logger.info("Phase 1: Đang tải và giải mã danh sách nguồn proxy thô...")
            fetch_tasks = [self.fetch_url(session, url) for url in self.sources]
            contents = await asyncio.gather(*fetch_tasks)
            
            for url, content in zip(self.sources, contents):
                self.parse_proxy_list(content, url)
            
            total_raw = len(self.raw_proxies)
            if total_raw == 0:
                logger.error("Không tìm thấy dữ liệu proxy đầu vào.")
                return
            logger.info(f"Đã nạp {total_raw} IP:Port riêng biệt từ file nguồn.")

            logger.info("Phase 2: Khởi tạo hàng đợi phân phối công việc...")
            queue = asyncio.Queue()
            for proxy in self.raw_proxies.values():
                for proto in proxy['protocols']:
                    await queue.put((proxy, proto))
            
            logger.info(f"Tổng số Connection Tasks trong hàng đợi: {queue.qsize()}")
            logger.info(f"Kích hoạt {MAX_WORKERS} Workers xử lý song song siêu tốc...")
            
            workers = [asyncio.create_task(self.worker(queue, session)) for _ in range(MAX_WORKERS)]
            await asyncio.gather(*workers)
            
            logger.info(f"Quét hoàn tất! Phát hiện {len(self.live_proxies)} proxy hoạt động ngon lành.")
            
            await self.enrich_geolocation(session)
            
        self.save_to_file()

    def save_to_file(self):
        try:
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                f.write(f"# Auto Proxy List - Secure Built with FindIP\n")
                f.write(f"# Build Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
                f.write(f"# Total Live Proxies: {len(self.live_proxies)}\n")
                f.write(f"# Format: Proxy URI | Country (Code) | User Type | ISP\n")
                f.write("-" * 110 + "\n")
                
                for p in self.live_proxies.values():
                    auth = f"{p['username']}:{p['password']}@" if p.get('username') else ""
                    url_format = f"{p['type']}://{auth}{p['ip']}:{p['port']}"
                    
                    # Thay đổi hiển thị cột từ Connection Type sang User Type
                    line = (
                        f"{url_format:<45} | "
                        f"{p['country']} ({p['countryCode']}) | "
                        f"User: {p['user_type']:<12} | "
                        f"ISP: {p['isp']}\n"
                    )
                    f.write(line)
                    
            logger.info(f"Đã xuất file cấu trúc mới bảo mật thành công ra tệp: {OUTPUT_FILE}")
        except IOError as e:
            logger.error(f"Lỗi ghi file: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(BigDataGeoProxyFetcher().run())
