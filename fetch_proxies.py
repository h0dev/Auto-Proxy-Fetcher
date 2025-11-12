# Auto Proxy Fetcher
# Copyright (c) 2024 Volkan Kücükbudak
# url: https://github.com/VolkanSah/Auto-Proxy-Fetcher
# -----
# Modified by Coder (AI Assistant)
# - Fetches location from geonode, proxifly, roundproxies
# - Fetches HTTP, HTTPS, SOCKS4, SOCKS5 from multiple sources
# - Parses 'protocol://ip:port' format
# - Parses 'ip:port' format
# - Saves output to proxies.txt
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
            # === Nhóm 1: JSON (Chất lượng cao, có vị trí) ===
            'https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http%2Chttps%2Csocks4%2Csocks5',
            'https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.json',
            'https://roundproxies.com/api/get-free-proxies',

            # === Nhóm 2: Text (Có protocol trong nội dung) ===
            'https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&proxy_format=protocolipport&format=text',
            'https://github.com/iplocate/free-proxy-list/raw/refs/heads/main/all-proxies.txt',

            # === Nhóm 3: Text (ip:port, protocol suy từ URL) ===
            'https://www.proxy-list.download/api/v1/get?type=http',
            'https://www.proxy-list.download/api/v1/get?type=https',
            'https://www.proxy-list.download/api/v1/get?type=socks4',
            'https://www.proxy-list.download/api/v1/get?type=socks5',
            
            # THAY ĐỔI: Thêm 3 nguồn TheSpeedX
            'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt',
            'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks4.txt',
            'https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt'
        ]

    async def fetch_url(self, session, url):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            timeout = 15
            if 'proxifly' in url:
                timeout = 30 
                
            async with session.get(url, headers=headers, timeout=timeout) as response:
                if response.status == 200:
                    return await response.text()
                logger.warning(f"Failed to fetch {url}: Status {response.status}")
                return None
        except asyncio.TimeoutError:
            logger.error(f"Timeout error fetching {url}")
            return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {str(e)}")
            return None

    def parse_proxy_list(self, content, url):
        if not content:
            return

        try:
            # === XỬ LÝ NHÓM 1: JSON (geonode) ===
            if 'geonode' in url:
                source_name = "geonode"
                try:
                    data = json.loads(content)
                    for item in data.get('data', []):
                        ip = item.get('ip')
                        port = item.get('port')
                        if not ip or not port: continue
                        
                        proxy_key = f"{ip}:{port}"
                        if proxy_key in self.proxies: continue

                        country = item.get('country')
                        city = item.get('city')
                        
                        location = "Unknown"
                        if city and city != "Unknown":
                            location = city
                            if country and country != "Unknown" and country != "ZZ":
                                location = f"{city}, {country}"
                        elif country and country != "Unknown" and country != "ZZ":
                            location = country
                        
                        self.proxies[proxy_key] = {
                            'location': location, 
                            'country': country or 'Unknown', 
                            'city': city or 'Unknown',
                            'protocols': item.get('protocols', []),
                            'source': source_name
                        }
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to decode JSON from {source_name}: {e}")
                return

            # === XỬ LÝ NHÓM 1: JSON (proxifly) ===
            elif 'proxifly' in url:
                source_name = "proxifly"
                try:
                    data = json.loads(content) 
                    if not isinstance(data, list):
                        logger.warning(f"Proxifly data is not a list: {url}")
                        return

                    for item in data:
                        ip = item.get('ip')
                        port = item.get('port')
                        if not ip or not port: continue
                        
                        proxy_key = f"{ip}:{port}"
                        if proxy_key in self.proxies: continue

                        geolocation = item.get('geolocation', {})
                        country = geolocation.get('country')
                        city = geolocation.get('city')
                        
                        location = "Unknown"
                        if city and city != "Unknown":
                            location = city
                            if country and country != "Unknown" and country != "ZZ":
                                location = f"{city}, {country}"
                        elif country and country != "Unknown" and country != "ZZ":
                            location = country
                        
                        protocol = item.get('protocol', 'unknown')
                        
                        self.proxies[proxy_key] = {
                            'location': location, 
                            'country': country or 'Unknown', 
                            'city': city or 'Unknown',
                            'protocols': [protocol] if protocol != 'unknown' else [],
                            'source': source_name
                        }
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to decode JSON from {source_name}: {e}")
                return

            # === XỬ LÝ NHÓM 1: JSON (roundproxies) ===
            elif 'roundproxies' in url:
                source_name = "roundproxies"
                try:
                    data = json.loads(content)
                    data_list = data.get('data')
                    if not isinstance(data_list, list):
                        logger.warning(f"Roundproxies data is not a list: {url}")
                        return
                        
                    for item in data_list:
                        ip = item.get('ip')
                        port = item.get('port')
                        if not ip or not port: continue
                        
                        proxy_key = f"{ip}:{port}"
                        if proxy_key in self.proxies: continue

                        country = item.get('country')
                        city = item.get('city')
                        
                        location = "Unknown"
                        if city and city != "Unknown":
                            location = city
                            if country and country != "Unknown" and country != "ZZ":
                                location = f"{city}, {country}"
                        elif country and country != "Unknown" and country != "ZZ":
                            location = country
                        
                        self.proxies[proxy_key] = {
                            'location': location, 
                            'country': country or 'Unknown', 
                            'city': city or 'Unknown',
                            'protocols': item.get('protocols', []),
                            'source': source_name
                        }
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to decode JSON from {source_name}: {e}")
                return

            # === XỬ LÝ NHÓM 2: Text (protocol://ip:port) ===
            elif 'proxyscrape' in url or 'iplocate' in url:
                source_name = "unknown"
                if 'proxyscrape' in url:
                    source_name = "proxyscrape.com"
                elif 'iplocate' in url:
                    source_name = "iplocate (github)"
                
                lines = content.split('\n')
                
                for line in lines:
                    line = line.strip()
                    if '://' not in line: continue
                    
                    try:
                        parts = line.split('://')
                        if len(parts) != 2: continue
                        
                        protocol = parts[0].strip()
                        proxy_part = parts[1].strip()
                        
                        host, port = proxy_part.split(':')[:2]
                        if host and port.isdigit() and 1 <= int(port) <= 65535:
                            if proxy_part not in self.proxies:
                                self.proxies[proxy_part] = {
                                    'location': 'Unknown',
                                    'country': 'Unknown',
                                    'city': 'Unknown',
                                    'protocols': [protocol],
                                    'source': source_name
                                }
                    except Exception:
                        continue
                return

            # === THAY ĐỔI: XỬ LÝ NHÓM 3: Text (ip:port) ===
            else:
                source_name = "Unknown"
                try:
                    source_name = url.split('/')[2] # Lấy domain
                except Exception:
                    pass

                lines = content.split('\n')
                
                # Xác định protocol dựa trên URL
                proxy_type = "unknown"
                if 'proxy-list.download' in url:
                    source_name = "proxy-list.download"
                    if 'type=http' in url: proxy_type = "http"
                    elif 'type=https' in url: proxy_type = "https"
                    elif 'type=socks4' in url: proxy_type = "socks4"
                    elif 'type=socks5' in url: proxy_type = "socks5"
                elif 'TheSpeedX' in url:
                    source_name = "TheSpeedX (github)"
                    if 'socks5.txt' in url: proxy_type = "socks5"
                    elif 'socks4.txt' in url: proxy_type = "socks4"
                    elif 'http.txt' in url: proxy_type = "http"

                # Bắt đầu parse
                for line in lines:
                    line = line.strip()
                    if line and ':' in line:
                        try:
                            proxy_part = line.split()[0] if ' ' in line else line
                            host, port = proxy_part.split(':')[:2]
                            
                            if host and port.isdigit() and 1 <= int(port) <= 65535:
                                if proxy_part not in self.proxies:
                                    self.proxies[proxy_part] = {
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
            
            logger.info(f"Fetched {len(results)} sources. Now parsing...")
            
            for url, content in zip(self.sources, results):
                if content:
                    self.parse_proxy_list(content, url)

    def save_proxies(self):
        if not self.proxies:
            logger.warning("No proxies found to save!")
            return

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            sorted_proxy_keys = sorted(
                self.proxies.keys(), 
                key=lambda x: tuple(map(int, x.split(':')[0].split('.') + [x.split(':')[1]]))
            )
        except Exception as e:
            logger.warning(f"Could not sort all IPs, using unsorted list. Error: {e}")
            sorted_proxy_keys = list(self.proxies.keys())

        output_filename = 'proxies.txt'
        
        try:
            max_proxy_len = 22 
            max_loc_len = 25   
            if sorted_proxy_keys:
                try:
                    max_proxy_len = max(len(p) for p in sorted_proxy_keys) + 2
                    max_loc_len = max(len(self.proxies[p].get('location', 'Unknown')) for p in sorted_proxy_keys) + 2
                except (ValueError, KeyError):
                    pass 

            with open(output_filename, 'w', encoding='utf-8') as f:
                f.write(f"# Proxy List - Updated: {timestamp}\n")
                f.write(f"# Total proxies: {len(self.proxies)}\n")
                f.write(f"# Sources used: {len(self.sources)}\n\n")

                header_proxy = 'Proxy'.ljust(max_proxy_len)
                header_loc = 'Location'.ljust(max_loc_len)
                f.write(f"# {header_proxy} # {header_loc} # Protocols\n")
                f.write(f"#{'-' * (max_proxy_len + max_loc_len + 25)}\n\n")

                for proxy_key in sorted_proxy_keys:
                    info = self.proxies.get(proxy_key)
                    if not info: continue 

                    location_str = info.get('location', 'Unknown')
                    protocols_list = info.get('protocols', ['unknown'])
                    protocols_str = ','.join(filter(None, protocols_list)) 

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
