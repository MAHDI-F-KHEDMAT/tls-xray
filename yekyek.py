# -- coding: utf-8 --

import requests
import os
import re
import base64
import threading
import concurrent.futures
import socket
import time
import random
import statistics
import sys
import urllib.parse
import json
import subprocess
from typing import List, Dict, Tuple, Optional, Set, Union

# --- Global Constants & Variables ---

PRINT_LOCK = threading.Lock()
OUTPUT_DIR = "data"
XRAY_PATH = "./xray" 

CONFIG_URLS: List[str] = [
    "https://raw.githubusercontent.com/itsyebekhe/PSG/main/subscriptions/xray/base64/mix",
    "https://raw.githubusercontent.com/shaoyouvip/free/refs/heads/main/base64.txt",
    "https://raw.githubusercontent.com/telegeam/freenode/refs/heads/master/v2ray.txt",
    "https://raw.githubusercontent.com/DukeMehdi/FreeList-V2ray-Configs/refs/heads/main/Configs/VLESS-V2Ray-Configs-By-DukeMehdi.txt",
    "https://raw.githubusercontent.com/Flikify/Free-Node/refs/heads/main/v2ray.txt",
    "https://raw.githubusercontent.com/RaitonRed/ConfigsHub/refs/heads/main/Splitted-By-Protocol/vless.txt",
    "https://raw.githubusercontent.com/shuaidaoya/FreeNodes/refs/heads/main/nodes/base64.txt",
    "https://raw.githubusercontent.com/penhandev/AutoAiVPN/refs/heads/main/allConfigs.txt",
    "https://raw.githubusercontent.com/Firmfox/Proxify/refs/heads/main/v2ray_configs/seperated_by_protocol/vless.txt",
    "https://raw.githubusercontent.com/crackbest/V2ray-Config/refs/heads/main/config.txt",
    "https://raw.githubusercontent.com/kismetpro/NodeSuber/refs/heads/main/Splitted-By-Protocol/vless.txt",
    "https://raw.githubusercontent.com/jagger235711/V2rayCollector/refs/heads/main/results/vless.txt",
    "https://raw.githubusercontent.com/mohamadfg-dev/telegram-v2ray-configs-collector/refs/heads/main/category/vless.txt",
    "https://raw.githubusercontent.com/SoroushImanian/BlackKnight/refs/heads/main/sub/vless",
    "https://raw.githubusercontent.com/Matin-RK0/ConfigCollector/refs/heads/main/subscription.txt",
    "https://raw.githubusercontent.com/Argh73/VpnConfigCollector/refs/heads/main/All_Configs_Sub.txt",
    "https://raw.githubusercontent.com/3yed-61/configs-collector/refs/heads/main/classified_output/vless.txt",
    "https://raw.githubusercontent.com/Leon406/SubCrawler/refs/heads/main/sub/share/vless",
    "https://raw.githubusercontent.com/ircfspace/XraySubRefiner/refs/heads/main/export/soliSpirit/normal",
    "https://raw.githubusercontent.com/ircfspace/XraySubRefiner/refs/heads/main/export/psgV6/normal",
    "https://raw.githubusercontent.com/ircfspace/XraySubRefiner/refs/heads/main/export/psgMix/normal",
    "https://raw.githubusercontent.com/MhdiTaheri/V2rayCollector_Py/refs/heads/main/sub/Mix/mix.txt",
    "https://raw.githubusercontent.com/T3stAcc/V2Ray/refs/heads/main/Splitted-By-Protocol/vless.txt",
    "https://raw.githubusercontent.com/F0rc3Run/F0rc3Run/refs/heads/main/splitted-by-protocol/vless.txt",
    "https://raw.githubusercontent.com/V2RayRoot/V2RayConfig/refs/heads/main/Config/vless.txt",
    "https://raw.githubusercontent.com/LalatinaHub/Mineral/refs/heads/master/result/nodes",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/refs/heads/main/All_Configs_Sub.txt",
    "https://raw.githubusercontent.com/hamedcode/port-based-v2ray-configs/refs/heads/main/sub/vless.txt",
    "https://raw.githubusercontent.com/iboxz/free-v2ray-collector/refs/heads/main/main/vless",
    "https://raw.githubusercontent.com/Epodonios/v2ray-configs/refs/heads/main/Splitted-By-Protocol/vless.txt",
    "https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list/refs/heads/main/vless_configs.txt",
    "https://raw.githubusercontent.com/Pasimand/v2ray-config-agg/refs/heads/main/config.txt",
    "https://raw.githubusercontent.com/arshiacomplus/v2rayExtractor/refs/heads/main/vless.html",
    "https://raw.githubusercontent.com/xyfqzy/free-nodes/refs/heads/main/nodes/vless.txt",
    "https://raw.githubusercontent.com/AvenCores/goida-vpn-configs/refs/heads/main/githubmirror/14.txt",
    "https://raw.githubusercontent.com/Awmiroosen/awmirx-v2ray/refs/heads/main/blob/main/v2-sub.txt",
    "https://raw.githubusercontent.com/SoliSpirit/v2ray-configs/refs/heads/main/Protocols/vless.txt",
    "https://media.githubusercontent.com/media/gfpcom/free-proxy-list/refs/heads/main/list/vless.txt"
]

OUTPUT_FILENAME: str = os.getenv("REALITY_OUTPUT_FILENAME", "khanevadeh") + "_base64.txt"

# تنظیمات زمانی تست‌ها
REQUEST_TIMEOUT: int = 15
TCP_CONNECT_TIMEOUT: int = 3
NUM_TCP_TESTS: int = 3
MIN_SUCCESSFUL_TESTS_RATIO: float = 0.6

# مدیریت محدودیت‌ها در GitHub Actions
MAX_CONFIGS_FOR_XRAY: int = 1500 
MAX_CONFIGS_FOR_IRAN_CHECK: int = 500  # طبق درخواست شما به ۵۰۰ افزایش یافت
FINAL_MAX_OUTPUT_CONFIGS: int = 20

SEEN_IDENTIFIERS: Set[Tuple[str, int, str]] = set()
USER_AGENTS = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36']

# --- Helper Functions ---

def safe_print(message: str) -> None:
    with PRINT_LOCK:
        print(message)

def print_progress(iteration: int, total: int, prefix: str = '', suffix: str = '', bar_length: int = 40) -> None:
    with PRINT_LOCK:
        if total == 0: total = 1
        percent = ("{0:.1f}").format(100 * (iteration / float(total)))
        filled_length = int(bar_length * iteration // total)
        bar = '█' * filled_length + '-' * (bar_length - filled_length)
        sys.stdout.write(f'\r{prefix} |{bar}| {percent}% {suffix}')
        sys.stdout.flush()
        if iteration >= total:
            sys.stdout.write('\n')

def get_free_port() -> int:
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    return port

def parse_vless_config(config_str: str) -> Optional[Dict]:
    if not config_str.startswith("vless://"): return None
    try:
        parsed = urllib.parse.urlparse(config_str)
        uuid, server_port = parsed.netloc.split('@', 1)
        query_params = urllib.parse.parse_qs(parsed.query)
        if query_params.get('security', [''])[0] != 'reality': return None
        return {
            "uuid": uuid, "server": parsed.hostname, "port": int(parsed.port),
            "pbk": query_params.get('pbk', [''])[0], "fp": query_params.get('fp', [''])[0],
            "sni": query_params.get('sni', [''])[0], "sid": query_params.get('sid', [''])[0],
            "spx": query_params.get('spx', [''])[0],
            "name": urllib.parse.unquote(parsed.fragment) if parsed.fragment else "",
            "original_config": config_str
        }
    except Exception: return None

def is_base64_content(s: str) -> bool:
    if not isinstance(s, str) or not s: return False
    if not re.match(r'^[A-Za-z0-9+/=\s]+$', s) or len(s.strip()) % 4 != 0: return False
    try:
        base64.b64decode(s, validate=True)
        return True
    except Exception: return False

# --- Core Logic ---

def fetch_subscription_content(url: str) -> Optional[str]:
    try:
        res = requests.get(url, timeout=REQUEST_TIMEOUT, headers={'User-Agent': USER_AGENTS[0]})
        return res.text.strip() if res.status_code == 200 else None
    except Exception: return None

def process_subscription_content(content: str) -> List[Dict]:
    if not content: return []
    decoded = content
    if is_base64_content(content):
        try: decoded = base64.b64decode(content).decode('utf-8', errors='ignore')
        except Exception: return []
            
    valid_configs = []
    for line in decoded.splitlines():
        line = line.strip()
        if line.startswith("vless://") and "security=reality" in line:
            parsed_data = parse_vless_config(line)
            if parsed_data:
                identifier = (parsed_data["server"], parsed_data["port"], parsed_data["uuid"])
                if identifier not in SEEN_IDENTIFIERS:
                    SEEN_IDENTIFIERS.add(identifier)
                    valid_configs.append(parsed_data)
    return valid_configs

def test_tcp_latency(host: str, port: int, timeout: int) -> Optional[float]:
    try:
        start = time.perf_counter()
        with socket.create_connection((host, port), timeout=timeout):
            return (time.perf_counter() - start) * 1000
    except Exception: return None

def quick_tcp_filter(config: Dict) -> Optional[Dict]:
    latencies = []
    for _ in range(NUM_TCP_TESTS):
        lat = test_tcp_latency(config['server'], config['port'], TCP_CONNECT_TIMEOUT)
        if lat: latencies.append(lat)
        time.sleep(0.01)
    if not latencies or len(latencies) < (NUM_TCP_TESTS * MIN_SUCCESSFUL_TESTS_RATIO): return None
    config['tcp_latency'] = statistics.mean(latencies)
    return config

def build_xray_config(config: Dict, local_port: int) -> Dict:
    return {
        "log": {"loglevel": "none"},
        "inbounds": [{"port": local_port, "listen": "127.0.0.1", "protocol": "http"}],
        "outbounds": [{
            "protocol": "vless",
            "settings": {"vnext": [{"address": config["server"], "port": config["port"], "users": [{"id": config["uuid"], "encryption": "none"}]}]},
            "streamSettings": {
                "network": "tcp", "security": "reality",
                "realitySettings": {
                    "show": False, 
                    "fingerprint": config.get("fp", "chrome"), "serverName": config.get("sni", ""),
                    "publicKey": config.get("pbk", ""), "shortId": config.get("sid", ""), "spiderX": config.get("spx", "")
                }
            }
        }]
    }

def validate_with_xray(config: Dict) -> Optional[Dict]:
    local_port = get_free_port()
    config_file_path = f"temp_cfg_{threading.get_ident()}_{local_port}.json"
    with open(config_file_path, 'w') as f: json.dump(build_xray_config(config, local_port), f)
        
    proc = None
    try:
        proc = subprocess.Popen([XRAY_PATH, "-c", config_file_path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(0.2)
        proxies = {"http": f"http://127.0.0.1:{local_port}", "https://": f"http://127.0.0.1:{local_port}"}
        start = time.perf_counter()
        response = requests.get("http://cp.cloudflare.com/generate_204", proxies=proxies, timeout=3.5)
        if response.status_code == 204:
            config['real_latency'] = (time.perf_counter() - start) * 1000
            return config
    except Exception: pass
    finally:
        if proc:
            proc.terminate()
            proc.wait()
        if os.path.exists(config_file_path): os.remove(config_file_path)
    return None

def is_accessible_in_iran(config: Dict) -> Optional[Dict]:
    try:
        host, port = config['server'], config['port']
        url = f"https://check-host.net/check-tcp?host={host}:{port}&node=ir1.node.check-host.net&node=ir4.node.check-host.net"
        res = requests.get(url, headers={'Accept': 'application/json'}, timeout=8)
        
        if res.status_code in [403, 429]:
            return config 
            
        if res.status_code != 200: return None
        request_id = res.json().get("request_id")
        if not request_id: return None
        
        for _ in range(5):
            time.sleep(2.5)
            result_res = requests.get(f"https://check-host.net/check-result/{request_id}", timeout=8)
            if result_res.status_code != 200: continue
            
            results = result_res.json()
            if not results: continue
            
            has_iran_node = False
            for node, node_data in results.items():
                if "ir" in node and node_data is not None:
                    has_iran_node = True
                    if isinstance(node_data, list) and len(node_data) > 0 and node_data[0]:
                        if "time" in node_data[0] or "connected" in str(node_data[0]).lower():
                            return config
            
            if has_iran_node: 
                return None
                
    except Exception: pass
    return None

def evaluate_configs(configs: List[Dict]) -> List[Dict]:
    # مرحله ۲: پورت اسکن سریع
    safe_print(f"\n🔍 مرحله ۲/۴: پورت اسکن سریع روی {len(configs)} کانفیگ ورودی...")
    tcp_alive = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=80) as executor:
        futures = {executor.submit(quick_tcp_filter, cfg): cfg for cfg in configs}
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            res = future.result()
            if res: tcp_alive.append(res)
            if (i + 1) % 50 == 0 or (i + 1) == len(configs): print_progress(i + 1, len(configs), prefix='اسکن شبکه:')

    tcp_alive.sort(key=lambda x: x['tcp_latency'])
    target_for_xray = tcp_alive[:MAX_CONFIGS_FOR_XRAY]
    
    # مرحله ۳: تست فنی با هسته Xray
    safe_print(f"\n🛡️ مرحله ۳/۴: تست صحت اعتبارسنجی با Xray-core روی {len(target_for_xray)} سرور زنده...")
    xray_verified = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(12, os.cpu_count() * 3)) as executor:
        futures = {executor.submit(validate_with_xray, cfg): cfg for cfg in target_for_xray}
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            res = future.result()
            if res: xray_verified.append(res)
            if (i + 1) % 10 == 0 or (i + 1) == len(target_for_xray): print_progress(i + 1, len(target_for_xray), prefix='تست عمیق Xray:')

    xray_verified.sort(key=lambda x: x['real_latency'])
    target_for_iran = xray_verified[:MAX_CONFIGS_FOR_IRAN_CHECK] # حالا تا سقف ۵۰۰ کانفیگ را استخراج میکند
    
    # مرحله ۴: تست فیلترینگ نهایی در ایران
    safe_print(f"\n🇮🇷 مرحله ۴/۴: بررسی وضعیت فیلترینگ در ایران (Check-Host) روی {len(target_for_iran)} کانفیگ برتر...")
    final_clean_configs = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(is_accessible_in_iran, cfg): cfg for cfg in target_for_iran}
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            res = future.result()
            if res: final_clean_configs.append(res)
            print_progress(i + 1, len(target_for_iran), prefix='تست فیلترینگ ایران:')
            time.sleep(1.2)
            
    # سیستم زاپاس (Fallback) طبق دستور شما کاملاً حذف گردید.
            
    return final_clean_configs

def save_results(configs: List[Dict]) -> None:
    if not configs: 
        safe_print("\n❌ هیچ کانفیگی که در ایران فیلتر نباشد پیدا نشد.")
        return
    top_configs = configs[:FINAL_MAX_OUTPUT_CONFIGS]
    output_lines = []
    for i, cfg in enumerate(top_configs, 1):
        clean_link = cfg['original_config'].split('#')[0]
        output_lines.append(f"{clean_link}#🇮🇷_Verified_{i}_Ping-{int(cfg.get('real_latency', 0))}")
        
    base64_str = base64.b64encode("\n".join(output_lines).encode('utf-8')).decode('utf-8')
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)
    with open(path, 'w', encoding='utf-8') as f: f.write(base64_str)
    safe_print(f"\n💾 فایل خروجی با موفقیت ثبت شد: {path} | تعداد کانفیگ‌ها: {len(top_configs)}")

def main():
    if not os.path.exists(XRAY_PATH): sys.exit(1)
    start = time.time()
    all_configs = []
    total_links = len(CONFIG_URLS)
    safe_print("🚀 مرحله ۱/۴: در حال دریافت دیتای اولیه سابسکریپشن‌ها...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(fetch_subscription_content, url): url for url in CONFIG_URLS}
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            content = future.result()
            if content: all_configs.extend(process_subscription_content(content))
            print_progress(i + 1, total_links, prefix='دریافت لینک‌ها:')
            
    ranked_configs = evaluate_configs(all_configs)
    save_results(ranked_configs)
    safe_print(f"\n⏱️ پایان کل فرآیند فیلترینگ و اسکن هوشمند در: {time.time() - start:.2f} ثانیه")

if __name__ == "__main__":
    main()
