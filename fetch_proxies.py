import re
import sys
import os
import asyncio
import logging
import json
import base64
from datetime import datetime
from typing import Dict, List, Optional, Any
import aiohttp
from aiohttp_socks import ProxyConnector
import geoip2.database

# --- CẤU HÌNH HỆ THỐNG ---
MAX_WORKERS = 1500            # Số luồng check đồng thời (tối ưu cho GitHub Actions)
CHECK_TIMEOUT = 5             # Thời gian chờ phản hồi tối đa (giây)
TEST_URL = 'https://cp.cloudflare.com/' # BẮT BUỘC DÙNG HTTPS ĐỂ LỌC PROXY XỊN
OUTPUT_FILE = 'proxies.txt'
SOURCES_FILE = 'sources.txt'

DB_COUNTRY = 'geoip/GeoLite2-Country.mmdb'
DB_ASN = 'geoip/GeoLite2-ASN.mmdb'

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("ProxyMaster")

# Regex nhận diện proxy nâng cao (chấp nhận ip:port, protocol://ip:port, user:pass@ip:port)
PROXY_RE = re.compile(r'(?:(?P<protocol>http|https|socks4|socks5)://)?(?:(?P<username>[^:@\s]+):(?P<password>[^:@\s]+)@)?(?P<ip>(?:[0-9]{1,3}\.){3}[0-9]{1,3}):(?P<port>[0-9]{1,5})', re.IGNORECASE)

class ProxyFetcher:
    def __init__(self):
        self.raw_proxies: Dict[str, Dict[str, Any]] = {}
        self.live_proxies: Dict[str, Dict[str, Any]] = {}
        self.sources: List[str] = []
        self.working_sources: List[str] = [] # Lưu các link nguồn thực sự hoạt động

    def load_sources(self) -> List[str]:
        if not os.path.exists(SOURCES_FILE):
            logger.error(f"Không tìm thấy file '{SOURCES_FILE}'! Vui lòng tạo file này trước.")
            sys.exit(1)
        sources = []
        with open(SOURCES_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    sources.append(line)
        return sources

    async def fetch_url(self, session: aiohttp.ClientSession, url: str) -> Optional[tuple]:
        try:
            async with session.get(url, timeout=15) as response:
                if response.status == 200:
                    text = await response.text()
                    return (url, text)
        except Exception:
            pass
        return (url, None)

    def parse_proxy_list(self, content: str, url: str) -> bool:
        if not content: return False
        url_lower = url.lower()
        found = False

        for match in PROXY_RE.finditer(content):
            ip, port = match.group('ip'), match.group('port')
            if not ip or not port: continue
            found = True

            # Xác định giao thức dựa trên link nguồn hoặc mặc định quét hết
            if match.group('protocol'): protocols = [match.group('protocol').lower()]
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
        return found

    async def verify_task(self, http_session: aiohttp.ClientSession, proxy_data: Dict[str, Any], proto: str):
        ip, port = proxy_data['ip'], proxy_data['port']
        user, pwd = proxy_data['username'], proxy_data['password']
        auth_str = f"{user}:{pwd}@" if user and pwd else ""
        proxy_url = f"{proto}://{auth_str}{ip}:{port}"
        
        timeout_cfg = aiohttp.ClientTimeout(total=CHECK_TIMEOUT, connect=CHECK_TIMEOUT, sock_connect=CHECK_TIMEOUT)
        try:
            is_live = False
            if 'socks' in proto:
                async def do_socks():
                    connector = ProxyConnector.from_url(proxy_url)
                    async with aiohttp.ClientSession(connector=connector, timeout=timeout_cfg) as socks_session:
                        async with socks_session.get(TEST_URL, ssl=False) as resp:
                            return resp.status in [200, 204]
                is_live = await asyncio.wait_for(do_socks(), timeout=CHECK_TIMEOUT + 1)
            else:
                async def do_http():
                    async with http_session.get(TEST_URL, proxy=proxy_url, timeout=timeout_cfg, ssl=False) as resp:
                        return resp.status in [200, 204]
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

    def enrich_geolocation_local(self):
        if not os.path.exists(DB_COUNTRY) or not os.path.exists(DB_ASN): 
            logger.warning("Không tìm thấy cơ sở dữ liệu GeoIP địa phương. Bỏ qua định vị.")
            return
        try:
            reader_country = geoip2.database.Reader(DB_COUNTRY)
            reader_asn = geoip2.database.Reader(DB_ASN)
            for key, proxy in self.live_proxies.items():
                ip = proxy['ip']
                try:
                    c_res = reader_country.country(ip)
                    proxy['country'] = c_res.country.name or 'Unknown'
                    proxy['countryCode'] = c_res.country.iso_code or '??'
                except: pass
                try:
                    a_res = reader_asn.asn(ip)
                    isp = a_res.autonomous_system_organization or 'Unknown'
                    proxy['isp'] = isp
                    name = isp.lower()
                    hosting_kw = ['vps', 'hosting', 'server', 'cloud', 'datacenter', 'digitalocean', 'amazon', 'google', 'ovh', 'hetzner']
                    cell_kw = ['mobile', 'wireless', 'cellular', 't-mobile', 'vodafone']
                    res_kw = ['telecom', 'viettel', 'fpt', 'vnpt', 'comcast', 'broadband', 'dsl', 'cable']
                    
                    if any(kw in name for kw in hosting_kw): proxy['user_type'] = 'hosting'
                    elif any(kw in name for kw in cell_kw): proxy['user_type'] = 'cellular'
                    elif any(kw in name for kw in res_kw): proxy['user_type'] = 'residential'
                    else: proxy['user_type'] = 'business'
                except: pass
            reader_country.close()
            reader_asn.close()
        except Exception as e:
            logger.error(f"Lỗi định vị dữ liệu IP: {e}")

    async def run(self):
        self.sources = self.load_sources()
        connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=600, limit_per_host=0)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            logger.info("Đang cào dữ liệu từ các nguồn...")
            fetch_tasks = [self.fetch_url(session, url) for url in self.sources]
            results = await asyncio.gather(*fetch_tasks)
            
            for url, content in results:
                if content:
                    found = self.parse_proxy_list(content, url)
                    if found:
                        self.working_sources.append(url)
            
            # TỰ ĐỘNG DỌN RÁC LINK DIE (Nếu có ít nhất 1 nguồn sống để tránh lỗi xóa sạch do mạng sập)
            if self.working_sources:
                self.clean_dead_sources()

            if not self.raw_proxies:
                logger.warning("Không tìm thấy proxy nào từ các nguồn cung cấp!")
                return
            
            logger.info(f"Tìm thấy tổng cộng {len(self.raw_proxies)} proxy thô. Bắt đầu kiểm tra chất lượng (HTTPS Only)...")
            queue = asyncio.Queue()
            for proxy in self.raw_proxies.values():
                for proto in proxy['protocols']:
                    await queue.put((proxy, proto))
            
            workers = [asyncio.create_task(self.worker(queue, session)) for _ in range(MAX_WORKERS)]
            await asyncio.gather(*workers)
            
            logger.info("Đang phân tích quốc gia và loại hình nhà mạng...")
            self.enrich_geolocation_local()
            
        self.export_all_formats()

    def clean_dead_sources(self):
        with open(SOURCES_FILE, 'w', encoding='utf-8') as f:
            f.write("# =========================================================\n")
            f.write(f"# DANH SÁCH NGUỒN ĐÃ ĐƯỢC TỰ ĐỘNG LỌC SẠCH LINK DIE ({datetime.utcnow().strftime('%Y-%m-%d')})\n")
            f.write("# =========================================================\n")
            f.write('\n'.join(self.working_sources) + '\n')
        logger.info(f"Đã cập nhật sources.txt: Giữ lại {len(self.working_sources)}/{len(self.sources)} link hoạt động.")

    def export_all_formats(self):
        live_list = list(self.live_proxies.values())
        
        # 1. XUẤT FILE TXT TRUYỀN THỐNG
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write(f"# Auto Proxy List\n# Build Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
            for p in live_list:
                auth = f"{p['username']}:{p['password']}@" if p.get('username') else ""
                uri = f"{p['type']}://{auth}{p['ip']}:{p['port']}"
                f.write(f"{uri:<45} | {p['country']} ({p['countryCode']}) | User: {p['user_type']:<12} | ISP: {p['isp']}\n")
                
        # 2. XUẤT THƯ MỤC STATIC JSON API PHÂN LOẠI CHI TIẾT
        os.makedirs('api/types', exist_ok=True)
        os.makedirs('api/countries', exist_ok=True)
        
        # Xuất API tổng
        with open('api/all.json', 'w', encoding='utf-8') as f:
            json.dump({'updated_at': datetime.utcnow().isoformat(), 'total': len(live_list), 'data': live_list}, f, ensure_ascii=False, indent=2)
        
        types_dict = {}
        countries_dict = {}
        
        for p in live_list:
            t_type = p['user_type']
            c_code = p['countryCode']
            
            if t_type not in types_dict: types_dict[t_type] = []
            types_dict[t_type].append(p)
            
            if c_code != '??':
                if c_code not in countries_dict: countries_dict[c_code] = []
                countries_dict[c_code].append(p)
                
        for t, data in types_dict.items():
            with open(f'api/types/{t}.json', 'w', encoding='utf-8') as f:
                json.dump({'updated_at': datetime.utcnow().isoformat(), 'total': len(data), 'data': data}, f, ensure_ascii=False, indent=2)
                
        for c, data in countries_dict.items():
            with open(f'api/countries/{c}.json', 'w', encoding='utf-8') as f:
                json.dump({'updated_at': datetime.utcnow().isoformat(), 'total': len(data), 'data': data}, f, ensure_ascii=False, indent=2)
                
        # 3. TẠO FILE SUB CHO CLIENT (VỚI TÊN ĐƯỢC TÁCH RIÊNG THEO QUỐC GIA)
        sub_uris = []
        country_counters = {} # Bộ đếm STT riêng biệt cho từng quốc gia
        
        for p in live_list:
            auth = f"{p['username']}:{p['password']}@" if p.get('username') else ""
            c_code = p['countryCode']
            
            # Tăng STT lũy tiến cho quốc gia đó để tránh trùng lặp danh sách
            country_counters[c_code] = country_counters.get(c_code, 0) + 1
            stt = country_counters[c_code]
            
            # Định dạng chuẩn: Country Code [STT] (Ví dụ: VN [1], VN [2], US [1]...)
            remark_name = f"{c_code} [{stt}]"
            uri_with_remark = f"{p['type']}://{auth}{p['ip']}:{p['port']}#{remark_name}"
            sub_uris.append(uri_with_remark)
            
        sub_content = '\n'.join(sub_uris)
        b64_sub = base64.b64encode(sub_content.encode('utf-8')).decode('utf-8')
        
        with open('sub.txt', 'w', encoding='utf-8') as f:
            f.write(b64_sub)
            
        logger.info(f"🚀 Thành công! Xuất {len(live_list)} proxy chạy ngon ra file {OUTPUT_FILE}, API JSON, và sub.txt")

if __name__ == "__main__":
    if sys.platform == 'win32': asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(ProxyFetcher().run())
