import re
import sys
import os
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import aiohttp
from aiohttp_socks import ProxyConnector
import geoip2.database  # Thư viện đọc DB Local

# --- CẤU HÌNH SIÊU TỐC ĐỘ ---
MAX_WORKERS = 1500            
CHECK_TIMEOUT = 5             
TEST_URL = 'http://cp.cloudflare.com/'
OUTPUT_FILE = 'proxies.txt'
SOURCES_FILE = 'sources.txt'

# Đường dẫn Database Local
DB_COUNTRY = 'geoip/GeoLite2-Country.mmdb'
DB_ASN = 'geoip/GeoLite2-ASN.mmdb'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("LocalGeoProxy")

PROXY_RE = re.compile(
    r'(?:(?P<protocol>http|https|socks4|socks5)://)?'
    r'(?:(?P<username>[^:@\s]+):(?P<password>[^:@\s]+)@)?'
    r'(?P<ip>(?:[0-9]{1,3}\.){3}[0-9]{1,3}):(?P<port>[0-9]{1,5})', 
    re.IGNORECASE
)

class LocalGeoProxyFetcher:
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
                            return resp.status == 204
                is_live = await asyncio.wait_for(do_socks(), timeout=CHECK_TIMEOUT + 1)
            else:
                async def do_http():
                    async with http_session.get(TEST_URL, proxy=proxy_url, timeout=timeout_cfg, ssl=False) as resp:
                        return resp.status == 204
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

    def detect_user_type(self, isp_name: str) -> str:
        """Engine phân loại User Type thông minh dựa trên tên ISP mạng định danh"""
        name = isp_name.lower()
        
        # Nhóm Hosting / VPS / Data Center
        hosting_keywords = [
            'vps', 'hosting', 'server', 'cloud', 'datacenter', 'data center', 'digitalocean', 
            'amazon', 'google', 'ovh', 'hetzner', 'linode', 'alibaba', 'tencent', 'microsoft', 
            'azure', 'leaseweb', 'choopa', 'vultr', 'akamai', 'cloudflare', 'fastly', 'contabo'
        ]
        if any(kw in name for kw in hosting_keywords):
            return 'hosting'
            
        # Nhóm Mạng Di Động (Cellular)
        cellular_keywords = ['mobile', 'wireless', 'cellular', 'mobifone', 'vinaphone', 't-mobile', 'verizon wireless', 'vodafone', 'orange']
        if any(kw in name for kw in cellular_keywords):
            return 'cellular'
            
        # Nhóm Mạng Nhà Dân (Residential)
        residential_keywords = [
            'telecom', 'communication', 'viettel', 'fpt', 'vnpt', 'comcast', 'charter', 'at&t', 
            'broadband', 'dsl', 'cable', 'networks', 'internet'
        ]
        if any(kw in name for kw in residential_keywords):
            return 'residential'
            
        return 'business' if 'business' in name else 'residential'

    def enrich_geolocation_local(self):
        """Phân tích vị trí & hạ tầng mạng 100% OFFLINE bằng MaxMind DB"""
        if not os.path.exists(DB_COUNTRY) or not os.path.exists(DB_ASN):
            logger.error("Thiếu file cơ sở dữ liệu .mmdb cục bộ! Bỏ qua định danh.")
            return

        logger.info(f"Phase 3: Thực hiện định danh Local siêu tốc cho {len(self.live_proxies)} proxy...")
        
        try:
            reader_country = geoip2.database.Reader(DB_COUNTRY)
            reader_asn = geoip2.database.Reader(DB_ASN)
            
            for key, proxy in self.live_proxies.items():
                ip = proxy['ip']
                
                # 1. Trích xuất Quốc gia
                try:
                    c_res = reader_country.country(ip)
                    proxy['country'] = c_res.country.name or 'Unknown'
                    proxy['countryCode'] = c_res.country.iso_code or '??'
                except Exception:
                    pass
                    
                # 2. Trích xuất ISP & Phân loại loại hình IP
                try:
                    a_res = reader_asn.asn(ip)
                    isp = a_res.autonomous_system_organization or 'Unknown'
                    proxy['isp'] = isp
                    proxy['user_type'] = self.detect_user_type(isp)
                except Exception:
                    pass
                    
            reader_country.close()
            reader_asn.close()
            logger.info("Hoàn tất bóc tách dữ liệu địa lý cục bộ.")
        except Exception as e:
            logger.error(f"Lỗi khi đọc file GeoIP Local: {e}")

    async def run(self):
        self.sources = self.load_sources()
        connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=600, limit_per_host=0)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            logger.info("Phase 1: Đang tải danh sách nguồn thô...")
            fetch_tasks = [self.fetch_url(session, url) for url in self.sources]
            contents = await asyncio.gather(*fetch_tasks)
            
            for url, content in zip(self.sources, contents):
                self.parse_proxy_list(content, url)
            
            total_raw = len(self.raw_proxies)
            if total_raw == 0: return
            logger.info(f"Đã nạp {total_raw} IP:Port riêng biệt.")

            logger.info("Phase 2: Quét trực tuyến kiểm tra proxy sống...")
            queue = asyncio.Queue()
            for proxy in self.raw_proxies.values():
                for proto in proxy['protocols']:
                    await queue.put((proxy, proto))
            
            workers = [asyncio.create_task(self.worker(queue, session)) for _ in range(MAX_WORKERS)]
            await asyncio.gather(*workers)
            logger.info(f"Phát hiện {len(self.live_proxies)} proxy hoạt động.")
            
            # CHẠY ĐỊNH DANH OFFLINE KHÔNG CẦN CHỜ MẠNG
            self.enrich_geolocation_local()
            
        self.save_to_file()

    def save_to_file(self):
        try:
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                f.write(f"# Auto Proxy List - 100% Local GeoIP Engine\n")
                f.write(f"# Build Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
                f.write(f"# Total Live Proxies: {len(self.live_proxies)}\n")
                f.write(f"# Format: Proxy URI | Country (Code) | User Type | ISP\n")
                f.write("-" * 110 + "\n")
                
                for p in self.live_proxies.values():
                    auth = f"{p['username']}:{p['password']}@" if p.get('username') else ""
                    url_format = f"{p['type']}://{auth}{p['ip']}:{p['port']}"
                    
                    line = (
                        f"{url_format:<45} | "
                        f"{p['country']} ({p['countryCode']}) | "
                        f"User: {p['user_type']:<12} | "
                        f"ISP: {p['isp']}\n"
                    )
                    f.write(line)
            logger.info(f"Đã xuất file thành công: {OUTPUT_FILE}")
        except IOError as e:
            logger.error(f"Lỗi ghi file: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(LocalGeoProxyFetcher().run())
