import re
import sys
import os
import asyncio
import logging
import json
import base64
import time
import socket
from urllib.parse import urlparse
from datetime import datetime
from typing import Dict, List, Optional, Any, Set, Tuple
import aiohttp
import geoip2.database

# ============================================================
# PERFORMANCE BOOSTS
# ============================================================
try:
    import uvloop
    uvloop.install()
except ImportError:
    pass

try:
    import resource
    _soft, _hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (min(_hard, 65536), _hard))
except Exception:
    pass

# ============================================================
# CONFIG
# ============================================================
MAX_CONCURRENT = 8000
CHECK_TIMEOUT = 1.5
CONNECT_TIMEOUT = 1.0
SOURCE_TIMEOUT = 8
TEST_URL = 'cp.cloudflare.com'
OUTPUT_FILE = 'proxies.txt'
SOURCES_FILE = 'sources.txt'
DB_COUNTRY = 'geoip/GeoLite2-Country.mmdb'
DB_ASN = 'geoip/GeoLite2-ASN.mmdb'

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger("ProxyMaster")

PROXY_RE = re.compile(
    r'(?:(?P<protocol>http|https|socks4|socks5)://)?'
    r'(?:(?P<username>[^:@\s]+):(?P<password>[^:@\s]+)@)?'
    r'(?P<ip>(?:[0-9]{1,3}\.){3}[0-9]{1,3}):(?P<port>[0-9]{1,5})',
    re.IGNORECASE
)


class ProxyFetcher:
    def __init__(self):
        self.raw_proxies: Dict[str, Dict[str, Any]] = {}
        self.live_proxies: Dict[str, Dict[str, Any]] = {}
        self.failed_ips: Set[str] = set()
        self.sources: List[str] = []
        self.working_sources: List[str] = []
        self.all_source_lines: List[str] = []
        self.semaphore: Optional[asyncio.Semaphore] = None
        self.start_time: float = 0
        self.checked_count: int = 0
        self.total_checks: int = 0

    # ----------------------------------------------------------------
    # SOURCE LOADING
    # ----------------------------------------------------------------
    def load_sources(self) -> List[str]:
        if not os.path.exists(SOURCES_FILE):
            logger.error(f"Không tìm thấy file '{SOURCES_FILE}'!")
            sys.exit(1)
        sources = []
        with open(SOURCES_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                self.all_source_lines.append(line)
                line_strip = line.strip()
                if not line_strip:
                    continue
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
                    return (url, await response.text())
        except Exception:
            pass
        return (url, None)

    def parse_proxy_list(self, content: str, url: str) -> bool:
        if not content:
            return False
        url_lower = url.lower()
        found = False
        for match in PROXY_RE.finditer(content):
            ip, port = match.group('ip'), match.group('port')
            if not ip or not port:
                continue
            found = True
            if match.group('protocol'):
                protocols = [match.group('protocol').lower()]
            else:
                if 'socks5' in url_lower:
                    protocols = ['socks5']
                elif 'socks4' in url_lower:
                    protocols = ['socks4']
                elif 'http' in url_lower:
                    protocols = ['http']
                else:
                    protocols = ['http', 'socks5', 'socks4']
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

    # ----------------------------------------------------------------
    # IP VALIDATION
    # ----------------------------------------------------------------
    @staticmethod
    def _is_private_ip(ip: str) -> bool:
        parts = ip.split('.')
        if len(parts) != 4:
            return True
        try:
            first, second = int(parts[0]), int(parts[1])
            if first == 127 or first == 0:
                return True
            if first == 10:
                return True
            if first == 172 and 16 <= second <= 31:
                return True
            if first == 192 and second == 168:
                return True
            if first >= 224:
                return True
            return False
        except (ValueError, IndexError):
            return True

    # ----------------------------------------------------------------
    # RAW PROXY CHECKS — 1 connection, no aiohttp overhead
    # ----------------------------------------------------------------
    @staticmethod
    def _build_auth_header(username: Optional[str], password: Optional[str]) -> str:
        if username and password:
            cred = base64.b64encode(f"{username}:{password}".encode()).decode()
            return f"Proxy-Authorization: Basic {cred}\r\n"
        return ""

    @staticmethod
    async def _check_http_proxy(reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                                username: Optional[str], password: Optional[str]) -> bool:
        """Gửi CONNECT request qua connection có sẵn"""
        try:
            auth = ProxyFetcher._build_auth_header(username, password)
            req = (
                f"CONNECT {TEST_URL}:443 HTTP/1.1\r\n"
                f"Host: {TEST_URL}:443\r\n"
                f"{auth}"
                f"Connection: close\r\n"
                f"\r\n"
            )
            writer.write(req.encode())
            await asyncio.wait_for(writer.drain(), timeout=CHECK_TIMEOUT)
            data = await asyncio.wait_for(reader.read(1024), timeout=CHECK_TIMEOUT)
            return b" 200" in data
        except Exception:
            return False

    @staticmethod
    async def _check_socks5_proxy(reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                                  username: Optional[str], password: Optional[str]) -> bool:
        """SOCKS5 handshake + CONNECT qua connection có sẵn"""
        try:
            # Greeting
            if username and password:
                writer.write(b'\x05\x02\x00\x02')
            else:
                writer.write(b'\x05\x01\x00')
            await writer.drain()

            data = await asyncio.wait_for(reader.read(2), timeout=CONNECT_TIMEOUT)
            if len(data) < 2 or data[0] != 0x05:
                return False

            method = data[1]
            if method == 0x02:
                auth = (
                    b'\x01'
                    + bytes([len(username)]) + username.encode()
                    + bytes([len(password)]) + password.encode()
                )
                writer.write(auth)
                await writer.drain()
                auth_resp = await asyncio.wait_for(reader.read(2), timeout=CONNECT_TIMEOUT)
                if len(auth_resp) < 2 or auth_resp[1] != 0x00:
                    return False
            elif method == 0xFF:
                return False

            # Connect to TEST_URL:443
            domain = TEST_URL.encode()
            writer.write(
                b'\x05\x01\x00\x03'
                + bytes([len(domain)]) + domain
                + b'\x01\xBB'  # port 443
            )
            await writer.drain()

            resp = await asyncio.wait_for(reader.read(32), timeout=CHECK_TIMEOUT)
            return len(resp) >= 2 and resp[1] == 0x00
        except Exception:
            return False

    @staticmethod
    async def _check_socks4_proxy(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> bool:
        """SOCKS4 CONNECT qua connection có sẵn"""
        try:
            dst_ip = socket.gethostbyname(TEST_URL)
            writer.write(
                b'\x04\x01'
                + (443).to_bytes(2, 'big')
                + socket.inet_aton(dst_ip)
                + b'\x00'
            )
            await writer.drain()

            resp = await asyncio.wait_for(reader.read(8), timeout=CHECK_TIMEOUT)
            return len(resp) >= 2 and resp[1] == 0x5A
        except Exception:
            return False

    # ----------------------------------------------------------------
    # VERIFY TASK — 1 connection: TCP connect + protocol check
    # ----------------------------------------------------------------
    async def verify_task(self, proxy_data: Dict[str, Any], proto: str):
        ip, port = proxy_data['ip'], proxy_data['port']
        key = f"{ip}:{port}"

        if key in self.failed_ips:
            return

        if self._is_private_ip(ip):
            self.failed_ips.add(key)
            return

        user, pwd = proxy_data['username'], proxy_data['password']

        async with self.semaphore:
            # 1 TCP connection: connect + protocol check (no separate pre-check)
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(ip, int(port)),
                    timeout=CONNECT_TIMEOUT
                )
            except Exception:
                self.failed_ips.add(key)
                self.checked_count += 1
                return

            # Protocol check trên connection có sẵn
            is_live = False
            try:
                if proto == 'socks5':
                    is_live = await self._check_socks5_proxy(reader, writer, user, pwd)
                elif proto == 'socks4':
                    is_live = await self._check_socks4_proxy(reader, writer)
                else:
                    is_live = await self._check_http_proxy(reader, writer, user, pwd)
            except Exception:
                pass

            try:
                writer.close()
            except Exception:
                pass

            self.checked_count += 1

            if is_live:
                if key not in self.live_proxies:
                    res = proxy_data.copy()
                    res['type'] = proto
                    self.live_proxies[key] = res
            else:
                self.failed_ips.add(key)

            if self.checked_count % 5000 == 0:
                elapsed = time.time() - self.start_time
                rate = self.checked_count / elapsed if elapsed > 0 else 0
                remaining = self.total_checks - self.checked_count
                eta = remaining / rate if rate > 0 else 0
                logger.info(
                    f"📊 Checked: {self.checked_count}/{self.total_checks} | "
                    f"Live: {len(self.live_proxies)} | "
                    f"Failed IPs: {len(self.failed_ips)} | "
                    f"Speed: {rate:.0f}/s | ETA: {eta:.0f}s"
                )

    # ----------------------------------------------------------------
    # CONFIRMATION — re-check live proxies to filter unstable ones
    # ----------------------------------------------------------------
    async def confirm_task(self, proxy_data: Dict[str, Any]):
        ip, port = proxy_data['ip'], proxy_data['port']
        key = f"{ip}:{port}"
        proto = proxy_data.get('type', 'http')
        user, pwd = proxy_data.get('username'), proxy_data.get('password')

        async with self.semaphore:
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(ip, int(port)),
                    timeout=CONNECT_TIMEOUT
                )
            except Exception:
                self.failed_ips.add(key)
                self.live_proxies.pop(key, None)
                return

            is_live = False
            try:
                if proto == 'socks5':
                    is_live = await self._check_socks5_proxy(reader, writer, user, pwd)
                elif proto == 'socks4':
                    is_live = await self._check_socks4_proxy(reader, writer)
                else:
                    is_live = await self._check_http_proxy(reader, writer, user, pwd)
            except Exception:
                pass

            try:
                writer.close()
            except Exception:
                pass

            if not is_live:
                self.failed_ips.add(key)
                self.live_proxies.pop(key, None)

    # ----------------------------------------------------------------
    # GEOLOCATION
    # ----------------------------------------------------------------
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
                except Exception:
                    pass
                try:
                    a_res = reader_asn.asn(ip)
                    isp = a_res.autonomous_system_organization or 'Unknown'
                    proxy['isp'] = isp
                    name = isp.lower()
                    hosting_kw = ['vps', 'hosting', 'server', 'cloud', 'datacenter', 'digitalocean', 'amazon', 'google', 'ovh', 'hetzner']
                    cell_kw = ['mobile', 'wireless', 'cellular', 't-mobile', 'vodafone']
                    res_kw = ['telecom', 'viettel', 'fpt', 'vnpt', 'comcast', 'broadband', 'dsl', 'cable']
                    if any(kw in name for kw in hosting_kw):
                        proxy['user_type'] = 'hosting'
                    elif any(kw in name for kw in cell_kw):
                        proxy['user_type'] = 'cellular'
                    elif any(kw in name for kw in res_kw):
                        proxy['user_type'] = 'residential'
                    else:
                        proxy['user_type'] = 'business'
                except Exception:
                    pass
            reader_country.close()
            reader_asn.close()
        except Exception as e:
            logger.error(f"Lỗi định vị dữ liệu IP: {e}")

    # ----------------------------------------------------------------
    # MAIN RUN
    # ----------------------------------------------------------------
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
            # ===== PHASE 1: Fetch sources =====
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

            # ===== PHASE 2: Verify =====
            self.total_checks = sum(len(p['protocols']) for p in self.raw_proxies.values())
            logger.info(f"🔎 Tìm thấy {len(self.raw_proxies)} proxy thô ({self.total_checks} checks). Bắt đầu kiểm tra...")

            verify_coros = []
            for proxy in self.raw_proxies.values():
                for proto in proxy['protocols']:
                    verify_coros.append(self.verify_task(proxy, proto))

            await asyncio.gather(*verify_coros)

            elapsed = time.time() - self.start_time
            logger.info(f"✅ Hoàn tất kiểm tra lần 1 trong {elapsed:.1f}s — {len(self.live_proxies)} proxy live")

            # ===== PHASE 2.5: Confirmation check =====
            if self.live_proxies:
                pre_confirm = len(self.live_proxies)
                logger.info(f"🔍 Đang xác nhận lại {pre_confirm} proxy live...")
                confirm_coros = [self.confirm_task(p) for p in list(self.live_proxies.values())]
                await asyncio.gather(*confirm_coros)
                removed = pre_confirm - len(self.live_proxies)
                logger.info(f"✅ Xác nhận xong: {pre_confirm} → {len(self.live_proxies)} proxy sống sót (loại bỏ {removed} proxy chập chờn)")

            # ===== PHASE 3: Enrich =====
            logger.info("🌍 Đang phân tích nhà mạng và quốc gia...")
            self.enrich_geolocation_local()

        # ===== PHASE 4: Export =====
        self.export_all_formats()

    # ----------------------------------------------------------------
    # DEAD SOURCE MANAGEMENT
    # ----------------------------------------------------------------
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
        logger.info("Đã cập nhật trạng thái nguồn trong sources.txt.")

    # ----------------------------------------------------------------
    # EXPORT
    # ----------------------------------------------------------------
    def export_all_formats(self):
        live_list = list(self.live_proxies.values())

        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            f.write(f"# Auto Proxy List\n# Build Time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}\n")
            for p in live_list:
                auth = f"{p['username']}:{p['password']}@" if p.get('username') else ""
                uri = f"{p['type']}://{auth}{p['ip']}:{p['port']}"
                f.write(f"{uri:<45} | {p['country']} ({p['countryCode']}) | User: {p['user_type']:<12} | ISP: {p['isp']}\n")

        os.makedirs('api/types', exist_ok=True)
        os.makedirs('api/countries', exist_ok=True)

        with open('api/all.json', 'w', encoding='utf-8') as f:
            json.dump({'updated_at': datetime.utcnow().isoformat(), 'total': len(live_list), 'data': live_list}, f, ensure_ascii=False, indent=2)

        types_dict = {}
        countries_dict = {}
        for p in live_list:
            t_type = p['user_type']
            c_code = p['countryCode']
            if t_type not in types_dict:
                types_dict[t_type] = []
            types_dict[t_type].append(p)
            if c_code != '??':
                if c_code not in countries_dict:
                    countries_dict[c_code] = []
                countries_dict[c_code].append(p)

        for t, data in types_dict.items():
            with open(f'api/types/{t}.json', 'w', encoding='utf-8') as f:
                json.dump({'updated_at': datetime.utcnow().isoformat(), 'total': len(data), 'data': data}, f, ensure_ascii=False, indent=2)

        for c, data in countries_dict.items():
            with open(f'api/countries/{c}.json', 'w', encoding='utf-8') as f:
                json.dump({'updated_at': datetime.utcnow().isoformat(), 'total': len(data), 'data': data}, f, ensure_ascii=False, indent=2)

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
        logger.info(f"🚀 Thành công! Xuất {len(live_list)} proxy. Tổng: {elapsed:.1f}s")


if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(ProxyFetcher().run())
