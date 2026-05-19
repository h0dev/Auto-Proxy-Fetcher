import re
import sys
import os
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import aiohttp
from aiohttp_socks import ProxyConnector

# --- CẤU HÌNH SIÊU TỐC ĐỘ CHO QUY MÔ KHỦNG ---
MAX_WORKERS = 1200            # Số lượng "công nhân" check song song (Tối ưu cho RAM/Băng thông)
CHECK_TIMEOUT = 5             # 5 giây cứu vớt proxy chậm theo yêu cầu của bạn
TEST_URL = 'http://httpbin.org/ip'
OUTPUT_FILE = 'proxies.txt'
SOURCES_FILE = 'sources.txt'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ProxyBigData")

PROXY_RE = re.compile(
    r'(?:(?P<protocol>http|https|socks4|socks5)://)?'
    r'(?:(?P<username>[^:@\s]+):(?P<password>[^:@\s]+)@)?'
    r'(?P<ip>(?:[0-9]{1,3}\.){3}[0-9]{1,3}):(?P<port>[0-9]{1,5})', 
    re.IGNORECASE
)

class BigDataProxyFetcher:
    def __init__(self):
        self.raw_proxies: Dict[str, Dict[str, Any]] = {}
        self.live_proxies: Dict[str, Dict[str, Any]] = {}
        self.sources: List[str] = self.load_sources()

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
            async with session.get(url, timeout=20) as response:
                if response.status == 200:
                    return await response.text()
        except Exception:
            pass
        return None

    def parse_proxy_list(self, content: str, url: str):
        if not content: return
        url_lower = url.lower()

        # Dùng finditer để bóc tách cực nhanh hàng vạn dòng text
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
                    'country': 'Unknown', 'countryCode': '??',
                    'mobile': False, 'proxy': False, 'hosting': False
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
        """Hàm Worker xử lý cuốn chiếu - Tiết kiệm RAM tuyệt đối"""
        while not queue.empty():
            proxy_data, proto = await queue.get()
            await self.verify_task(session, proxy_data, proto)
            queue.task_done()

    async def enrich_geolocation(self, session: aiohttp.ClientSession):
        """Định danh Quốc gia/Cơ sở hạ tầng cho các proxy đã xác nhận sống"""
        unique_ips = list({p['ip'] for p in self.live_proxies.values()})
        if not unique_ips: return
        
        logger.info(f"Đang thực hiện phân tích sâu hạ tầng IP cho {len(unique_ips)} proxy sống sót...")
        chunk_size = 100 
        fields_param = "status,country,countryCode,mobile,proxy,hosting,query"
        
        for i in range(0, len(unique_ips), chunk_size):
            chunk = unique_ips[i:i + chunk_size]
            payload = [{"query": ip, "fields": fields_param} for ip in chunk]
            
            try:
                async with session.post('http://ip-api.com/batch', json=payload, timeout=15) as resp:
                    if resp.status == 200:
                        geo_map = {res['query']: res for res in await resp.json() if res.get('status') == 'success'}
                        for proxy_data in self.live_proxies.values():
                            ip = proxy_data['ip']
                            if ip in geo_map:
                                info = geo_map[ip]
                                proxy_data['country'] = info.get('country', 'Unknown')
                                proxy_data['countryCode'] = info.get('countryCode', '??')
                                proxy_data['mobile'] = info.get('mobile', False)
                                proxy_data['proxy'] = info.get('proxy', False)
                                proxy_data['hosting'] = info.get('hosting', False)
                    else:
                        logger.warning(f"ip-api Rate limit hit (Status {resp.status}). Tạm dừng tra cứu.")
                        break
            except Exception as e:
                logger.error(f"Lỗi phân tích địa lý: {e}")
            
            if i + chunk_size < len(unique_ips):
                await asyncio.sleep(4.2)  # Giãn cách tránh bị API Block Ban IP

    async def run(self):
        # Tối ưu hóa bộ kết nối TCP: Bật DNS Cache giúp tăng tốc truy vấn hệ thống
        connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
        
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

            # --- KHỞI TẠO HÀNG ĐỢI ĐỘNG (QUEUE ARHITECTURE) ---
            logger.info("Phase 2: Khởi tạo hàng đợi phân phối công việc...")
            queue = asyncio.Queue()
            for proxy in self.raw_proxies.values():
                for proto in proxy['protocols']:
                    await queue.put((proxy, proto))
            
            logger.info(f"Tổng số Connection Tasks trong hàng đợi: {queue.qsize()}")
            logger.info(f"Kích hoạt {MAX_WORKERS} Workers xử lý song song siêu tốc...")
            
            # Kích hoạt nhóm Worker chạy đồng thời
            workers = [asyncio.create_task(self.worker(queue, session)) for _ in range(MAX_WORKERS)]
            await asyncio.gather(*workers) # Đợi hàng đợi xử lý sạch sẽ hoàn toàn
            
            logger.info(f"Quét xong! Phát hiện {len(self.live_proxies)} proxy thực sự hoạt động.")
            
            # Phase 3: Chỉ check vị trí địa lý của những con ĐÃ SỐNG -> Tiết kiệm tài nguyên tuyệt đối
            logger.info("Phase 3: Thực hiện gán nhãn định danh quốc gia mạng...")
            await self.enrich_geolocation(session)
            
        self.save_to_file()

    def save_to_file(self):
        try:
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                f.write(f"# Auto Proxy List - Network Security Report\n")
                f.write(f"# Build Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
                f.write(f"# Live Proxy Count: {len(self.live_proxies)}\n")
                f.write(f"# Format: URI | Country (Code) | Mobile | Proxy | Hosting\n")
                f.write("-" * 90 + "\n")
                
                for p in self.live_proxies.values():
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
            logger.info(f"Đã xuất file báo cáo hoàn chỉnh ra tệp: {OUTPUT_FILE}")
        except IOError as e:
            logger.error(f"Lỗi ghi file: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(BigDataProxyFetcher().run())
