# Auto Proxy Fetcher
# Copyright (c) 2024 Volkan Kücükbudak
# url: https://github.com/VolkanSah/Auto-Proxy-Fetcher
# -----
# Modified by Coder (AI Assistant)
# - Fetches location from geonode
# - Fetches HTTP, HTTPS, SOCKS4, SOCKS5
# - Saves output to proxies.txt with location and type
# -----
import aiohttp
import asyncio
import logging
from datetime import datetime
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ProxyFetcher:
    def __init__(self):
        self.proxies = {}
        self.sources = [
            # API Sources (geonode CÓ cung cấp vị trí, thêm socks4, socks5)
            'https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http%2Chttps%2Csocks4%2Csocks5',
            
            # API Sources (proxy-list.download, không có vị trí)
            'https://www.proxy-list.download/api/v1/get?type=http',
            'https://www.proxy-list.download/api/v1/get?type=https',
            'https://www.proxy-list.download/api/v1/get?type=socks4',
            'https://www.proxy-list.download/api/v1/get?type=socks5'
        ]

    async def fetch_url(self, session, url):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            async with session.get(url, headers=headers, timeout=15) as response:
                if response.status == 200:
                    return await response.text()
                logger.warning(f"Failed to fetch {url}: Status {response.status}")
                return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            return None

    def parse_proxy_list(self, content, url):
        if not content:
            return

        try:
            # Xử lý riêng cho API geonode để lấy vị trí
            if 'geonode' in url:
                try:
                    data = json.loads(content)
                    for item in data.get('data', []):
                        ip = item.get('ip')
                        port = item.get('port')
                        if not ip or not port:
                            continue
                        
                        proxy = f"{ip}:{port}"
                        country = item.get('country')
                        city = item.get('city')
                        
                        location = "Unknown"
                        if city and country:
                            location = f"{city}, {country}"
                        elif country:
                            location = country
                        
                        if proxy not in self.proxies:
                            self.proxies[proxy] = {
                                'location': location, 
                                'country': country or 'Unknown', 
                                'city': city or 'Unknown',
                                'protocols': item.get('protocols', []),
                                'source': 'geonode'
                            }
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to decode JSON from geonode: {e}")
                return

            # Xử lý cho các nguồn text (proxy-list.download)
            lines = content.split('\n')
            source_name = url.split('//')[1].split('/')[0]
            
            proxy_type = "unknown"
            if 'type=http' in url:
                proxy_type = "http"
            elif 'type=https' in url:
                proxy_type = "https"
            elif 'type=socks4' in url:
                proxy_type = "socks4"
            elif 'type=socks5' in url:
                proxy_type = "socks5"

            for line in lines:
                line = line.strip()
                if line and ':' in line:
                    try:
                        proxy_part = line.split()[0] if ' ' in line else line
                        host, port = proxy_part.split(':')[:2]
                        
                        if host and port.isdigit() and 1 <= int(port) <= 65535:
                            proxy_str = f"{host}:{port}"
                            if proxy_str not in self.proxies:
                                self.proxies[proxy_str] = {
                                    'location': 'Unknown', 
                                    'country': 'Unknown',
                                    'city': 'Unknown',
                                    'protocols': [proxy_type], 
                                    'source': source_name
                                }
                    except Exception:
                        continue

        except Exception as e:
            logger.error(f"Error parsing content from {url}: {str(e)}")

    async def fetch_all_proxies(self):
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_url(session, url) for url in self.sources]
            results = await asyncio.gather(*tasks)
            
            for url, content in zip(self.sources, results):
                if content:
                    self.parse_proxy_list(content, url)

    def save_proxies(self):
        # THAY ĐỔI: Chuyển hoàn toàn sang lưu .txt với đầy đủ thông tin
        if not self.proxies:
            logger.warning("No proxies found to save!")
            return

        timestamp = datetime.now().strftime("%Y-m-d %H:%M:%S")
        
        try:
            sorted_proxy_keys = sorted(
                self.proxies.keys(), 
                key=lambda x: tuple(map(int, x.split(':')[0].split('.') + [x.split(':')[1]]))
            )
        except Exception:
            sorted_proxy_keys = list(self.proxies.keys())

        # THAY ĐỔI: Tên file
        output_filename = 'proxies.txt'
        
        try:
            # Tìm độ dài lề động để căn chỉnh
            max_proxy_len = 22 # Tối thiểu 22 (cho 1.1.1.1:65535)
            max_loc_len = 25   # Tối thiểu 25
            if sorted_proxy_keys:
                try:
                    max_proxy_len = max(len(p) for p in sorted_proxy_keys) + 2
                    max_loc_len = max(len(self.proxies[p].get('location', 'Unknown')) for p in sorted_proxy_keys) + 2
                except ValueError:
                    # Xử lý nếu self.proxies rỗng
                    pass

            with open(output_filename, 'w', encoding='utf-8') as f:
                f.write(f"# Proxy List - Updated: {timestamp}\n")
                f.write(f"# Total proxies: {len(self.proxies)}\n")
                f.write(f"# Sources used: {len(self.sources)}\n\n")

                # Viết header
                header_proxy = 'Proxy'.ljust(max_proxy_len)
                header_loc = 'Location'.ljust(max_loc_len)
                f.write(f"# {header_proxy} # {header_loc} # Protocols\n")
                f.write(f"#{'-' * (max_proxy_len + max_loc_len + 25)}\n\n")

                # Ghi dữ liệu
                for proxy_key in sorted_proxy_keys:
                    info = self.proxies[proxy_key]
                    
                    location_str = info.get('location', 'Unknown')
                    protocols_list = info.get('protocols', [])
                    # Chuyển list ['http', 'https'] -> 'http,https'
                    protocols_str = ','.join(protocols_list) 

                    # Căn lề
                    proxy_part = proxy_key.ljust(max_proxy_len)
                    location_part = location_str.ljust(max_loc_len)

                    f.write(f"{proxy_part} # {location_part} # {protocols_str}\n")
            
            logger.info(f"Saved {len(self.proxies)} proxies to {output_filename}")

        except Exception as e:
            logger.error(f"Failed to save TXT file: {e}")

async def main():
    fetcher = ProxyFetcher()
    await fetcher.fetch_all_proxies()
    fetcher.save_proxies()

if __name__ == "__main__":
    asyncio.run(main())
