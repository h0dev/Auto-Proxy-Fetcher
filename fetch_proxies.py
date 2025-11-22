import aiohttp
import asyncio
import logging
import json
import time
import sys
from datetime import datetime
from aiohttp_socks import ProxyConnector, ProxyError, ProxyConnectionError, ProxyTimeoutError

MAX_CONCURRENT_CHECKS = 500
CHECK_TIMEOUT = 10
TEST_URL = 'http://www.google.com'
OUTPUT_FILE = 'proxies.txt'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ProxyUpdater")

class ProxyFetcher:
    def __init__(self):
        self.raw_proxies = {}
        self.live_proxies = []
        self.sources = [
            'https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http%2Chttps%2Csocks4%2Csocks5',
            'https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.json',
            'https://roundproxies.com/api/get-free-proxies',
            'https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text',
            'https://github.com/iplocate/free-proxy-list/raw/refs/heads/main/all-proxies.txt',
            'https://www.proxy-list.download/api/v1/get?type=http',
            'https://www.proxy-list.download/api/v1/get?type=https',
            'https://www.proxy-list.download/api/v1/get?type=socks4',
            'https://www.proxy-list.download/api/v1/get?type=socks5',
            'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt',
            'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks4.txt',
            'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt'
        ]

    async def fetch_url(self, session, url):
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Compatible; GitHubActionsBot/1.0)'}
            timeout = 20
            async with session.get(url, headers=headers, timeout=timeout) as response:
                if response.status == 200:
                    return await response.text()
        except Exception:
            pass
        return None

    def parse_proxy_list(self, content, url):
        if not content: return
        
        default_proto = ['http']
        if 'socks5' in url.lower(): default_proto = ['socks5']
        elif 'socks4' in url.lower(): default_proto = ['socks4']
        elif 'https' in url.lower(): default_proto = ['http', 'https']

        def add(ip, port, protos, country='Unknown', city='Unknown'):
            ip = ip.strip()
            port = str(port).strip()
            if not ip or not port.isdigit(): return
            
            key = f"{ip}:{port}"
            if key not in self.raw_proxies:
                self.raw_proxies[key] = {
                    'ip': ip, 'port': port, 'protocols': protos,
                    'country': country, 'city': city
                }
            else:
                current = self.raw_proxies[key]['protocols']
                for p in protos:
                    if p not in current: current.append(p)

        try:
            if content.strip().startswith(('[', '{')):
                try:
                    data = json.loads(content)
                    items = data if isinstance(data, list) else data.get('data', [])
                    for item in items:
                        ip = item.get('ip')
                        port = item.get('port')
                        geo = item.get('geolocation', {})
                        country = item.get('country') or geo.get('country') or 'Unknown'
                        city = item.get('city') or geo.get('city') or 'Unknown'
                        protos = item.get('protocols', [])
                        if 'protocol' in item: protos = [item['protocol']]
                        add(ip, port, protos, country, city)
                    return
                except: pass

            for line in content.splitlines():
                line = line.strip()
                if not line: continue
                if '://' in line:
                    parts = line.split('://')
                    proto = parts[0].lower()
                    if len(parts) > 1 and ':' in parts[1]:
                        ip_port = parts[1].split(':')
                        add(ip_port[0], ip_port[1], [proto])
                elif ':' in line:
                    parts = line.split(':')
                    if len(parts) >= 2:
                        add(parts[0], parts[1], default_proto)
        except Exception:
            pass

    async def verify_single_proxy(self, session, proxy_data):
        ip, port = proxy_data['ip'], proxy_data['port']
        protocols = proxy_data['protocols']
        target_proto = 'http'
        if 'socks5' in protocols: target_proto = 'socks5'
        elif 'socks4' in protocols: target_proto = 'socks4'

        proxy_url = f"{target_proto}://{ip}:{port}"
        start = time.perf_counter()
        try:
            if 'socks' in target_proto:
                connector = ProxyConnector.from_url(proxy_url)
                async with aiohttp.ClientSession(connector=connector) as s:
                    async with s.get(TEST_URL, timeout=CHECK_TIMEOUT, ssl=False) as resp:
                        if resp.status == 200:
                            latency = int((time.perf_counter() - start) * 1000)
                            return {**proxy_data, 'latency': latency, 'type': target_proto}
            else:
                async with session.get(TEST_URL, proxy=proxy_url, timeout=CHECK_TIMEOUT, ssl=False) as resp:
                    if resp.status == 200:
                        latency = int((time.perf_counter() - start) * 1000)
                        return {**proxy_data, 'latency': latency, 'type': target_proto}
        except:
            return None
        return None

    async def run(self):
        logger.info("Downloading proxies...")
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_url(session, u) for u in self.sources]
            contents = await asyncio.gather(*tasks)
            for u, c in zip(self.sources, contents): self.parse_proxy_list(c, u)
        
        logger.info(f"Total Raw Proxies: {len(self.raw_proxies)}")

        logger.info(f"Checking with {MAX_CONCURRENT_CHECKS} threads...")
        sem = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)
        
        async def safe_check(s, p):
            async with sem: return await self.verify_single_proxy(s, p)

        async with aiohttp.ClientSession() as session:
            tasks = [safe_check(session, p) for p in self.raw_proxies.values()]
            results = await asyncio.gather(*tasks)
            self.live_proxies = [r for r in results if r]

        self.live_proxies.sort(key=lambda x: x['latency'])
        logger.info(f"Live Proxies Found: {len(self.live_proxies)}")
        self.save_to_file()

    def save_to_file(self):
        try:
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"# Auto Proxy List - Updated: {timestamp}\n")
                f.write(f"# Live Count: {len(self.live_proxies)}\n")
                f.write(f"# Format: IP:Port # Protocol # Country # Latency\n")
                f.write("-" * 70 + "\n")
                
                for p in self.live_proxies:
                    line = f"{p['ip']}:{p['port']:<6} # {p['type']:<6} # {p['country']:<15} # {p['latency']}ms\n"
                    f.write(line)
            logger.info(f"Successfully saved to {OUTPUT_FILE}")
        except Exception as e:
            logger.error(f"File save error: {e}")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    fetcher = ProxyFetcher()
    asyncio.run(fetcher.run())
