import re
import sys
import os
import asyncio
import logging
import json
import base64
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Set
import aiohttp
from aiohttp_socks import ProxyConnector
import geoip2.database

# ============================================================
# PERFORMANCE BOOSTS — Tối ưu hiệu năng
# ============================================================
# 1. uvloop: Event loop nhanh hơn 2-4x so với asyncio mặc định
try:
    import uvloop
    uvloop.install()
    _HAS_UVLOOP = True
except ImportError:
    _HAS_UVLOOP = False

# 2. Tăng file descriptor limit — mặc định Linux chỉ ~1024,
#    không đủ cho hàng nghìn kết nối đồng thời
try:
    import resource
    _soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(_hard, 65536), _hard))
except Exception:
    pass

# ============================================================
# CẤU HÌNH TỐI ƯU HÓA
# ============================================================
MAX_CONCURRENT = 5000        # Số check đồng thời tối đa (semaphore-controlled)
CHECK_TIMEOUT = 2.0          # Timeout mỗi proxy (giây) — giảm từ 5s xuống 2s
CONNECT_TIMEOUT = 1.5        # Timeout kết nối TCP (giây)
SOURCE_TIMEOUT = 12          # Timeout tải danh sách nguồn (giây)
TEST_URL = 'https://cp.cloudflare.com/'  # Endpoint test HTTPS
OUTPUT_FILE = 'proxies.txt'
SOURCES_FILE = 'sources.txt'

DB_COUNTRY = 'geoip/GeoLite2-Country.mmdb'
DB_ASN = 'geoip/GeoLite2-ASN.mmdb'

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("ProxyMaster")

# Regex nhận diện proxy nâng cao
PROXY_RE = re.compile(r'(?:(?P<protocol>http|https|socks4|socks5)://)?(?:(?P<username>[^:@\s]+):(?P<password>[^:@\s]+)@)?(?P<ip>(?:[0-9]{1,3}\.){3}[0-9]{1,3}):(?P<port>[0-9]{1,5})', re.IGNORECASE)


class ProxyFetcher:
    def __init__(self):
        self.raw_proxies: Dict[str, Dict[str, Any]] = {}
        self.live_proxies: Dict[str, Dict[str, Any]] = {}
        self.failed_ips: Set[str] = set()       # [TỐI ƯU] Track IP đã chết để skip protocol khác
        self.sources: List[str] = []
        self.working_sources: List[str] = []
        self.all_source_lines: List[str] = []
        self.semaphore: Optional[asyncio.Semaphore] = None
        self.start_time: float = 0
        self.checked_count: int = 0
        self.total_checks: int = 0

    def load_sources(self) -> List[str]:
        if not os.path.exists(SOURCES_FILE):
            logger.error(f"Không tìm thấy file '{SOURCES_FILE}'!")
            sys.exit(1)
        sources = []
        with open(SOURCES_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                self.all_source_lines.append(line)
                line_strip = line.strip()
                if not line_strip: continue
                if line_strip.startswith('# [DIE]'):
                    url = line_strip.replace('# [DIE]', '').strip()
                    if url and url not in sources:
                        sources.append(url)
                elif not line_strip.startswith('#'):
                    if line_strip not in sources:
                        sources.append(line_strip)
        return sources

    async def fetch_url(self, session: aiohttp.ClientSession, url: str) -> Optional[tuple]:
        try:
            timeout = aiohttp.ClientTimeout(total=SOURCE_TIMEOUT)
            async with session.get(url, timeout=timeout) as response:
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
        key = f"{ip}:{port}"

        # [TỐI ƯU] Nếu IP này đã fail ở protocol khác → skip ngay lập tức
        if key in self.failed_ips:
            return

        user, pwd = proxy_data['username'], proxy_data['password']
        auth_str = f"{user}:{pwd}@" if user and pwd else ""

        async with self.semaphore:
            is_live = False
            try:
                if 'socks' in proto:
                    proxy_url = f"{proto}://{auth_str}{ip}:{port}"
                    is_live = await self._check_socks(proxy_url)
                else:
                    proxy_proto = 'http' if proto == 'https' else proto
                    proxy_url = f"{proxy_proto}://{auth_str}{ip}:{port}"
                    is_live = await self._check_http(http_session, proxy_url)
            except Exception:
                pass

            self.checked_count += 1

            if is_live:
                if key not in self.live_proxies:
                    res = proxy_data.copy()
                    res['type'] = proto
                    self.live_proxies[key] = res
            else:
                # [TỐI ƯU] Đánh dấu IP chết → các protocol khác sẽ skip
                self.failed_ips.add(key)

            # Progress logging mỗi 5000 checks
            if self.checked_count % 5000 == 0:
                elapsed = time.time() - self.start_time
                rate = self.checked_count / elapsed if elapsed > 0 else 0
                remaining = self.total_checks - self.checked_count
                eta = remaining / rate if rate > 0 else 0
                logger.info(
                    f"📊 Checked: {self.checked_count}/{self.total_checks} | "
                    f"Live: {len(self.live_proxies)} | "
                    f"Failed IPs: {len(self.failed_ips)} | "
                    f"Speed: {rate:.0f}/s | "
                    f"ETA: {eta:.0f}s"
                )

    async def _check_http(self, session: aiohttp.ClientSession, proxy_url: str) -> bool:
        timeout_cfg = aiohttp.ClientTimeout(total=CHECK_TIMEOUT, connect=CONNECT_TIMEOUT)
        try:
            async with session.get(TEST_URL, proxy=proxy_url, timeout=timeout_cfg, ssl=False) as resp:
                return resp.status in [200, 204]
        except Exception:
            return False

    async def _check_socks(self, proxy_url: str) -> bool:
        timeout_cfg = aiohttp.ClientTimeout(total=CHECK_TIMEOUT, connect=CONNECT_TIMEOUT)
        try:
            connector = ProxyConnector.from_url(proxy_url)
            async with aiohttp.ClientSession(connector=connector, timeout=timeout_cfg) as socks_session:
                async with socks_session.get(TEST_URL, ssl=False) as resp:
                    return resp.status in [200, 204]
        except Exception:
            return False

    def enrich_geolocation_local(self):
        if not os.path.exists(DB_COUNTRY) or not os.path.exists(DB_ASN):
            logger.warning("Không thấy file DB GeoIP. Bỏ qua định vị.")
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
        self.start_time = time.time()
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT)
        self.sources = self.load_sources()

        connector = aiohttp.TCPConnector(
            limit=MAX_CONCURRENT,
            ttl_dns_cache=600,
            limit_per_host=0,
            enable_cleanup_closed=True
        )

        async with aiohttp.ClientSession(connector=connector) as session:
            # ===== PHASE 1: Fetch nguồn + Parse proxy (pipeline) =====
            logger.info(f"🔍 Đang cào {len(self.sources)} nguồn proxy...")

            fetch_tasks = [self.fetch_url(session, url) for url in self.sources]
            results = await asyncio.gather(*fetch_tasks)

            for url, content in results:
                if content:
                    found = self.parse_proxy_list(content, url)
                    if found:
                        self.working_sources.append(url)

            if self.working_sources:
                self.clean_dead_sources()

            if not self.raw_proxies:
                logger.warning("Không tìm thấy proxy thô nào!")
                return

            # ===== PHASE 2: Verify proxy song song =====
            self.total_checks = sum(len(p['protocols']) for p in self.raw_proxies.values())
            logger.info(f"🔎 Tìm thấy {len(self.raw_proxies)} proxy thô ({self.total_checks} checks). Bắt đầu kiểm tra...")

            verify_coros = []
            for proxy in self.raw_proxies.values():
                for proto in proxy['protocols']:
                    verify_coros.append(self.verify_task(session, proxy, proto))

            await asyncio.gather(*verify_coros)

            elapsed = time.time() - self.start_time
            logger.info(f"✅ Hoàn tất kiểm tra trong {elapsed:.1f}s — {len(self.live_proxies)} proxy live")

            # ===== PHASE 3: Enrich geolocation =====
            logger.info("🌍 Đang phân tích nhà mạng và quốc gia...")
            self.enrich_geolocation_local()

        # ===== PHASE 4: Export =====
        self.export_all_formats()

    def clean_dead_sources(self):
        with open(SOURCES_FILE, 'w', encoding='utf-8') as f:
            for line in self.all_source_lines:
                line_strip = line.strip()
                if not line_strip:
                    f.write("\n")
                    continue
                current_url = line_strip
                if line_strip.startswith('# [DIE]'):
                    current_url = line_strip.replace('# [DIE]', '').strip()
                elif line_strip.startswith('#'):
                    f.write(line)
                    continue
                if current_url in self.working_sources:
                    f.write(f"{current_url}\n")
                else:
                    f.write(f"# [DIE] {current_url}\n")
        logger.info(f"Đã cập nhật trạng thái nguồn trong sources.txt.")

    def export_all_formats(self):
        live_list = list(self.live_proxies.values())

        # 1. XUẤT FILE TXT
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write(f"# Auto Proxy List\n# Build Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
            for p in live_list:
                auth = f"{p['username']}:{p['password']}@" if p.get('username') else ""
                uri = f"{p['type']}://{auth}{p['ip']}:{p['port']}"
                f.write(f"{uri:<45} | {p['country']} ({p['countryCode']}) | User: {p['user_type']:<12} | ISP: {p['isp']}\n")

        # 2. XUẤT API JSON TĨNH PHÂN LOẠI
        os.makedirs('api/types', exist_ok=True)
        os.makedirs('api/countries', exist_ok=True)

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

        # 3. TẠO FILE SUB (STT RIÊNG THEO QUỐC GIA)
        sub_uris = []
        country_counters = {}
        for p in live_list:
            auth = f"{p['username']}:{p['password']}@" if p.get('username') else ""
            c_code = p['countryCode']
            country_counters[c_code] = country_counters.get(c_code, 0) + 1
            stt = country_counters[c_code]
            remark_name = f"{c_code} [{stt}]"
            uri_with_remark = f"{p['type']}://{auth}{p['ip']}:{p['port']}#{remark_name}"
            sub_uris.append(uri_with_remark)

        sub_content = '\n'.join(sub_uris)
        b64_sub = base64.b64encode(sub_content.encode('utf-8')).decode('utf-8')

        with open('sub.txt', 'w', encoding='utf-8') as f:
            f.write(b64_sub)

        elapsed = time.time() - self.start_time
        logger.info(f"🚀 Thành công! Xuất {len(live_list)} proxy ra các định dạng. Tổng thời gian: {elapsed:.1f}s")


if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(ProxyFetcher().run())
