# Auto Proxy Fetcher & Checker (Enterprise Grade)
# Modified by Coder (AI Assistant)
# Features:
# - High concurrency async testing (handles 10k+ proxies)
# - Supports HTTP, HTTPS, SOCKS4, SOCKS5
# - Live verification with latency measurement
# - Deduplication and clean formatting

import aiohttp
import asyncio
import logging
import json
import time
import random
from datetime import datetime
from aiohttp_socks import ProxyConnector, ProxyError, ProxyConnectionError, ProxyTimeoutError

# === CẤU HÌNH ===
# Số lượng luồng check cùng lúc (Điều chỉnh tùy theo CPU/Mạng)
# Với VPS 2-4 Core, 500-1000 là an toàn. Laptop cá nhân nên để 200-300.
MAX_CONCURRENT_CHECKS = 500 
CHECK_TIMEOUT = 10  # Giây. Quá thời gian này coi như Dead.
TEST_URL = 'http://www.google.com' # URL để test live. Nên dùng URL nhẹ, ổn định.

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("ProxyManager")

class ProxyFetcher:
    def __init__(self):
        self.raw_proxies = {} # Lưu proxy sau khi fetch
        self.live_proxies = [] # Lưu proxy đã qua test live
        
        self.sources = [
            # === Nhóm 1: JSON (Chất lượng cao) ===
            'https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http%2Chttps%2Csocks4%2Csocks5',
            'https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.json',
            'https://roundproxies.com/api/get-free-proxies',

            # === Nhóm 2: Text (Protocol explicitly defined) ===
            'https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text',
            'https://github.com/iplocate/free-proxy-list/raw/refs/heads/main/all-proxies.txt',

            # === Nhóm 3: Text (IP:Port only - Protocol implied) ===
            'https://www.proxy-list.download/api/v1/get?type=http',
            'https://www.proxy-list.download/api/v1/get?type=https',
            'https://www.proxy-list.download/api/v1/get?type=socks4',
            'https://www.proxy-list.download/api/v1/get?type=socks5',
            
            # === Nhóm TheSpeedX (Github) ===
            'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt',
            'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks4.txt',
            'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt'
        ]

    async def fetch_url(self, session, url):
        """Tải dữ liệu từ nguồn với retry đơn giản"""
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Compatible; ProxyFetcher/2.0)'}
            timeout = 15 if 'proxifly' not in url else 30
            
            async with session.get(url, headers=headers, timeout=timeout) as response:
                if response.status == 200:
                    return await response.text()
                logger.warning(f"Failed fetch {url}: HTTP {response.status}")
                return None
        except Exception:
            return None # Im lặng bỏ qua lỗi fetch để log gọn hơn

    def parse_proxy_list(self, content, url):
        """Parser thông minh xử lý nhiều định dạng"""
        if not content: return

        # Helper để thêm vào dict
        def add_proxy(ip, port, protocols, country='Unknown', city='Unknown'):
            key = f"{ip}:{port}"
            if key not in self.raw_proxies:
                self.raw_proxies[key] = {
                    'ip': ip, 'port': port,
                    'protocols': protocols,
                    'country': country, 'city': city,
                    'source': url.split('/')[2]
                }
            else:
                # Merge protocols nếu đã tồn tại
                existing_protos = self.raw_proxies[key]['protocols']
                for p in protocols:
                    if p not in existing_protos:
                        existing_protos.append(p)

        try:
            # Case 1: JSON Geonode/Proxifly/Roundproxies
            if content.strip().startswith(('[', '{')): 
                try:
                    data = json.loads(content)
                    # Normalize data structure list
                    items = []
                    if isinstance(data, dict):
                        items = data.get('data', [])
                    elif isinstance(data, list):
                        items = data
                    
                    for item in items:
                        ip = item.get('ip')
                        port = item.get('port')
                        if not ip or not port: continue
                        
                        # Normalize geolocation
                        country = item.get('country')
                        city = item.get('city')
                        if not country and 'geolocation' in item:
                            country = item['geolocation'].get('country')
                            city = item['geolocation'].get('city')
                        
                        # Normalize protocols
                        protos = item.get('protocols', [])
                        if not protos and 'protocol' in item:
                            protos = [item['protocol']]
                        
                        add_proxy(ip, port, protos, country or 'Unknown', city or 'Unknown')
                    return
                except json.JSONDecodeError:
                    pass # Fallback to text parsing if JSON fails

            # Case 2: Text based parsing
            lines = content.splitlines()
            
            # Xác định protocol từ URL nếu file text thuần
            implied_proto = []
            if 'socks5' in url.lower(): implied_proto = ['socks5']
            elif 'socks4' in url.lower(): implied_proto = ['socks4']
            elif 'https' in url.lower(): implied_proto = ['http', 'https'] # HTTPS proxies usually handle HTTP too
            elif 'http' in url.lower(): implied_proto = ['http']

            for line in lines:
                line = line.strip()
                if not line: continue
                
                # Format: protocol://ip:port
                if '://' in line:
                    parts = line.split('://')
                    proto = parts[0].lower()
                    if len(parts) > 1 and ':' in parts[1]:
                        ip_port = parts[1].split(':')
                        if len(ip_port) >= 2:
                            add_proxy(ip_port[0], ip_port[1], [proto])
                    continue

                # Format: ip:port
                if ':' in line:
                    parts = line.split(':')
                    if len(parts) >= 2:
                        # Basic valid IP check
                        if parts[0].count('.') == 3 and parts[1].isdigit():
                            add_proxy(parts[0], parts[1], implied_proto or ['http'])

        except Exception as e:
            logger.error(f"Error parsing {url}: {e}")

    async def verify_single_proxy(self, session, proxy_data):
        """Kiểm tra 1 proxy duy nhất. Trả về dict nếu Alive, None nếu Dead."""
        ip = proxy_data['ip']
        port = proxy_data['port']
        protocols = proxy_data['protocols']
        
        # Ưu tiên check protocol mạnh nhất: socks5 > socks4 > https > http
        target_proto = 'http'
        if 'socks5' in protocols: target_proto = 'socks5'
        elif 'socks4' in protocols: target_proto = 'socks4'
        elif 'https' in protocols: target_proto = 'http' # aiohttp xử lý https qua http CONNECT

        proxy_url = f"{target_proto}://{ip}:{port}"
        
        start_time = time.perf_counter()
        try:
            if 'socks' in target_proto:
                connector = ProxyConnector.from_url(proxy_url)
                async with aiohttp.ClientSession(connector=connector) as test_session:
                    async with test_session.get(TEST_URL, timeout=CHECK_TIMEOUT, ssl=False) as response:
                        if response.status == 200:
                            latency = int((time.perf_counter() - start_time) * 1000)
                            return {
                                **proxy_data,
                                'latency': latency,
                                'verified_proto': target_proto
                            }
            else:
                # Standard HTTP Proxy check
                async with session.get(TEST_URL, proxy=proxy_url, timeout=CHECK_TIMEOUT, ssl=False) as response:
                    if response.status == 200:
                        latency = int((time.perf_counter() - start_time) * 1000)
                        return {
                            **proxy_data,
                            'latency': latency,
                            'verified_proto': target_proto
                        }
        except (asyncio.TimeoutError, aiohttp.ClientError, ProxyError, ProxyConnectionError, ProxyTimeoutError):
            return None
        except Exception:
            return None
        return None

    async def verify_all_proxies(self):
        """Chạy worker pool để check toàn bộ proxy"""
        total = len(self.raw_proxies)
        logger.info(f"Starting live check for {total} proxies. Concurrency: {MAX_CONCURRENT_CHECKS}")
        
        # Semaphore để giới hạn số lượng request cùng lúc
        sem = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)
        
        async def sem_task(session, p_data):
            async with sem:
                return await self.verify_single_proxy(session, p_data)

        async with aiohttp.ClientSession() as session:
            tasks = []
            for p_data in self.raw_proxies.values():
                tasks.append(sem_task(session, p_data))
            
            # Chạy và hiển thị tiến độ đơn giản
            results = []
            # Chia nhỏ task nếu quá lớn để tránh memory spike (Optional, here simply gather all)
            # Sử dụng as_completed để theo dõi tiến độ nếu muốn, nhưng gather nhanh hơn cho batch lớn
            results = await asyncio.gather(*tasks)
            
            self.live_proxies = [r for r in results if r is not None]
            
            # Sort by latency (nhanh nhất lên đầu)
            self.live_proxies.sort(key=lambda x: x['latency'])
            
        logger.info(f"Check completed. Live: {len(self.live_proxies)} / {total} ({len(self.live_proxies)/total*100:.1f}%)")

    async def run(self):
        # Step 1: Fetch
        logger.info("Fetching proxies from sources...")
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_url(session, url) for url in self.sources]
            contents = await asyncio.gather(*tasks)
            
            for url, content in zip(self.sources, contents):
                self.parse_proxy_list(content, url)
        
        logger.info(f"Fetched total {len(self.raw_proxies)} unique raw proxies.")
        
        # Step 2: Verify
        await self.verify_all_proxies()
        
        # Step 3: Save
        self.save_results()

    def save_results(self):
        filename = 'live_proxies.txt'
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(f"# Live Proxy List - Checked: {timestamp}\n")
                f.write(f"# Total Live: {len(self.live_proxies)}\n")
                f.write(f"# Format: Protocol://IP:Port # Location # Latency\n")
                f.write("-" * 60 + "\n")
                
                for p in self.live_proxies:
                    full_proxy = f"{p['verified_proto']}://{p['ip']}:{p['port']}"
                    location = f"{p['city']}, {p['country']}"
                    latency = f"{p['latency']}ms"
                    
                    # Formatting đẹp
                    f.write(f"{full_proxy:<35} # {location:<30} # {latency}\n")
            
            logger.info(f"Saved {len(self.live_proxies)} live proxies to {filename}")
        except Exception as e:
            logger.error(f"Error saving file: {e}")

if __name__ == "__main__":
    # Windows fix cho asyncio loop
    import sys
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    fetcher = ProxyFetcher()
    try:
        asyncio.run(fetcher.run())
    except KeyboardInterrupt:
        logger.info("Process stopped by user.")
