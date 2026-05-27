# -*- coding: utf-8 -*-

import requests
import os
import re
import base64
import threading
import concurrent.futures
import socket
import time
import statistics
import sys
import urllib.parse
import json
import subprocess
from typing import List, Dict, Tuple, Optional, Set

# =========================================================
# Global Configuration
# =========================================================

PRINT_LOCK = threading.Lock()
PORT_LOCK = threading.Lock()
SEEN_IDENTIFIERS: Set[Tuple[str, int, str]] = set()

OUTPUT_DIR = "data"
XRAY_PATH = "./xray"

START_PORT = 30000

OUTPUT_FILENAME = os.getenv(
    "TLS_OUTPUT_FILENAME",
    "stable_nodes"
) + "_base64.txt"

# =========================================================
# Tunable Parameters
# =========================================================

REQUEST_TIMEOUT = 12
TCP_CONNECT_TIMEOUT = 6

NUM_TCP_TESTS = 5
MIN_SUCCESSFUL_TESTS_RATIO = 0.4

MAX_CONFIGS_FOR_XRAY = 3000
FINAL_MAX_OUTPUT_CONFIGS = 500

XRAY_TEST_TIMEOUT = 8
DOWNLOAD_TEST_SIZE = 300000

MIN_DOWNLOAD_SPEED_KBPS = 50
MAX_ACCEPTABLE_JITTER = 250

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
]

# =========================================================
# Subscription Sources
# =========================================================

CONFIG_URLS = [
    "https://raw.githubusercontent.com/itsyebekhe/PSG/main/subscriptions/xray/base64/mix",
    "https://raw.githubusercontent.com/shaoyouvip/free/refs/heads/main/base64.txt",
    "https://raw.githubusercontent.com/telegeam/freenode/refs/heads/master/v2ray.txt",
    "https://raw.githubusercontent.com/DukeMehdi/FreeList-V2ray-Configs/refs/heads/main/Configs/VLESS-V2Ray-Configs-By-DukeMehdi.txt",
    "https://raw.githubusercontent.com/Flikify/Free-Node/refs/heads/main/v2ray.txt",
    "https://raw.githubusercontent.com/RaitonRed/ConfigsHub/refs/heads/main/Splitted-By-Protocol/vless.txt",
]

# =========================================================
# Utilities
# =========================================================


def safe_print(msg: str):
    with PRINT_LOCK:
        print(msg)



def print_progress(iteration, total, prefix='', suffix='', length=40):
    with PRINT_LOCK:
        if total == 0:
            total = 1

        percent = f"{100 * (iteration / float(total)):.1f}"
        filled = int(length * iteration // total)

        bar = '█' * filled + '-' * (length - filled)

        sys.stdout.write(
            f'\r{prefix} |{bar}| {percent}% {suffix}'
        )

        sys.stdout.flush()

        if iteration >= total:
            sys.stdout.write('\n')



def get_free_port() -> int:
    global START_PORT

    with PORT_LOCK:
        port = START_PORT
        START_PORT += 1
        return port



def is_base64_content(s: str) -> bool:
    if not isinstance(s, str) or not s:
        return False

    s = s.strip()

    if not re.match(r'^[A-Za-z0-9+/=\s]+$', s):
        return False

    try:
        padded = s + '=' * ((4 - len(s) % 4) % 4)
        base64.b64decode(padded, validate=False)
        return True

    except Exception:
        return False

# =========================================================
# Fetching
# =========================================================


def fetch_subscription_content(url: str) -> Optional[str]:
    try:
        response = requests.get(
            url,
            timeout=REQUEST_TIMEOUT,
            headers={
                'User-Agent': USER_AGENTS[0]
            }
        )

        if response.status_code == 200:
            return response.text.strip()

    except Exception:
        pass

    return None

# =========================================================
# Parsing
# =========================================================


def parse_vless_config(config_str: str) -> Optional[Dict]:
    if not config_str.startswith("vless://"):
        return None

    try:
        parsed = urllib.parse.urlparse(config_str)

        if '@' not in parsed.netloc:
            return None

        uuid, _ = parsed.netloc.split('@', 1)

        params = urllib.parse.parse_qs(parsed.query)

        security = params.get('security', [''])[0]

        # support tls + reality
        if security not in ['tls', 'reality']:
            return None

        try:
            port = int(parsed.port)
        except Exception:
            return None

        network = params.get(
            'type',
            params.get('net', ['tcp'])
        )[0]

        return {
            'uuid': uuid,
            'server': parsed.hostname,
            'port': port,
            'network': network,
            'security': security,
            'sni': params.get('sni', [''])[0],
            'fp': params.get('fp', ['chrome'])[0],
            'pbk': params.get('pbk', [''])[0],
            'sid': params.get('sid', [''])[0],
            'flow': params.get('flow', [''])[0],
            'path': params.get('path', [''])[0],
            'host': params.get('host', [''])[0],
            'serviceName': params.get('serviceName', [''])[0],
            'alpn': params.get('alpn', [''])[0],
            'name': urllib.parse.unquote(parsed.fragment)
            if parsed.fragment else '',
            'original_config': config_str
        }

    except Exception:
        return None



def process_subscription_content(content: str) -> List[Dict]:
    if not content:
        return []

    decoded = content

    if is_base64_content(content):
        try:
            padded = content.strip() + '=' * (
                (4 - len(content.strip()) % 4) % 4
            )

            decoded = base64.b64decode(padded).decode(
                'utf-8',
                errors='ignore'
            )

        except Exception:
            return []

    valid_configs = []

    for line in decoded.splitlines():
        line = line.strip()

        if not line.startswith('vless://'):
            continue

        parsed = parse_vless_config(line)

        if not parsed:
            continue

        identifier = (
            parsed['server'],
            parsed['port'],
            parsed['uuid']
        )

        if identifier in SEEN_IDENTIFIERS:
            continue

        SEEN_IDENTIFIERS.add(identifier)
        valid_configs.append(parsed)

    return valid_configs

# =========================================================
# TCP Tests
# =========================================================


def test_tcp_latency(host, port, timeout):
    try:
        start = time.perf_counter()

        with socket.create_connection(
            (host, port),
            timeout=timeout
        ):
            latency = (
                time.perf_counter() - start
            ) * 1000

            return latency

    except Exception:
        return None



def quick_tcp_filter(config: Dict) -> Optional[Dict]:
    latencies = []

    for _ in range(NUM_TCP_TESTS):
        lat = test_tcp_latency(
            config['server'],
            config['port'],
            TCP_CONNECT_TIMEOUT
        )

        if lat:
            latencies.append(lat)

        time.sleep(0.05)

    if not latencies:
        return None

    success_ratio = len(latencies) / NUM_TCP_TESTS

    if success_ratio < MIN_SUCCESSFUL_TESTS_RATIO:
        return None

    avg_latency = statistics.mean(latencies)

    jitter = (
        statistics.stdev(latencies)
        if len(latencies) > 1
        else 0
    )

    if jitter > MAX_ACCEPTABLE_JITTER:
        return None

    config['tcp_latency'] = avg_latency
    config['jitter'] = jitter
    config['success_ratio'] = success_ratio

    return config

# =========================================================
# Xray Config Builder
# =========================================================


def build_xray_config(config: Dict, local_port: int) -> Dict:

    stream_settings = {
        'network': config.get('network', 'tcp'),
        'security': config.get('security', 'tls')
    }

    # =====================================================
    # TLS
    # =====================================================

    if config['security'] == 'tls':

        tls_settings = {
            'serverName': config.get('sni', ''),
            'fingerprint': config.get('fp', 'chrome')
        }

        if config.get('alpn'):
            tls_settings['alpn'] = [
                x.strip()
                for x in config['alpn'].split(',')
                if x.strip()
            ]

        stream_settings['tlsSettings'] = tls_settings

    # =====================================================
    # Reality
    # =====================================================

    elif config['security'] == 'reality':

        stream_settings['realitySettings'] = {
            'serverName': config.get('sni', ''),
            'fingerprint': config.get('fp', 'chrome'),
            'publicKey': config.get('pbk', ''),
            'shortId': config.get('sid', '')
        }

    # =====================================================
    # Transport
    # =====================================================

    network = config.get('network', 'tcp')

    if network == 'ws':

        stream_settings['wsSettings'] = {
            'path': config.get('path', '/'),
            'headers': {
                'Host': config.get('host', '')
            }
        }

    elif network == 'grpc':

        stream_settings['grpcSettings'] = {
            'serviceName': config.get('serviceName', '')
        }

    elif network in ['http', 'h2']:

        stream_settings['httpSettings'] = {
            'host': [config.get('host', '')],
            'path': config.get('path', '/')
        }

    outbound = {
        'protocol': 'vless',
        'settings': {
            'vnext': [
                {
                    'address': config['server'],
                    'port': config['port'],
                    'users': [
                        {
                            'id': config['uuid'],
                            'encryption': 'none',
                            'flow': config.get('flow', '')
                        }
                    ]
                }
            ]
        },
        'streamSettings': stream_settings
    }

    return {
        'log': {
            'loglevel': 'none'
        },
        'inbounds': [
            {
                'listen': '127.0.0.1',
                'port': local_port,
                'protocol': 'http'
            }
        ],
        'outbounds': [outbound]
    }

# =========================================================
# Real Validation
# =========================================================


def validate_with_xray(config: Dict) -> Optional[Dict]:

    local_port = get_free_port()

    config_path = (
        f"temp_{threading.get_ident()}_{local_port}.json"
    )

    with open(config_path, 'w') as f:
        json.dump(
            build_xray_config(config, local_port),
            f
        )

    proc = None

    try:
        proc = subprocess.Popen(
            [XRAY_PATH, '-c', config_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        time.sleep(1.2)

        proxies = {
            'http': f'http://127.0.0.1:{local_port}',
            'https': f'http://127.0.0.1:{local_port}'
        }

        latencies = []
        success = 0

        # =============================================
        # Multiple HTTP Tests
        # =============================================

        for _ in range(3):
            try:
                start = time.perf_counter()

                r = requests.get(
                    'https://connectivitycheck.gstatic.com/generate_204',
                    proxies=proxies,
                    timeout=XRAY_TEST_TIMEOUT
                )

                latency = (
                    time.perf_counter() - start
                ) * 1000

                if r.status_code == 204:
                    success += 1
                    latencies.append(latency)

            except Exception:
                pass

        if success < 2:
            return None

        # =============================================
        # Download Speed Test
        # =============================================

        speed_start = time.perf_counter()

        r = requests.get(
            f'https://speed.cloudflare.com/__down?bytes={DOWNLOAD_TEST_SIZE}',
            proxies=proxies,
            timeout=XRAY_TEST_TIMEOUT,
            stream=True
        )

        total = 0

        for chunk in r.iter_content(chunk_size=8192):
            total += len(chunk)

        elapsed = time.perf_counter() - speed_start

        if elapsed <= 0:
            return None

        speed_kbps = (total / 1024) / elapsed

        if speed_kbps < MIN_DOWNLOAD_SPEED_KBPS:
            return None

        avg_latency = statistics.mean(latencies)

        jitter = (
            statistics.stdev(latencies)
            if len(latencies) > 1
            else 0
        )

        # =============================================
        # Final Quality Score
        # =============================================

        score = (
            (1000 / (avg_latency + 1)) * 0.45 +
            speed_kbps * 0.35 +
            (100 - jitter) * 0.20
        )

        config['real_latency'] = avg_latency
        config['download_speed_kbps'] = speed_kbps
        config['real_jitter'] = jitter
        config['quality_score'] = score

        return config

    except Exception:
        return None

    finally:
        if proc:
            proc.terminate()
            proc.wait()

        if os.path.exists(config_path):
            try:
                os.remove(config_path)
            except Exception:
                pass

# =========================================================
# Evaluation
# =========================================================


def evaluate_configs(configs: List[Dict]) -> List[Dict]:

    safe_print(
        f"\n🔍 TCP Filtering on {len(configs)} configs..."
    )

    tcp_alive = []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=80
    ) as executor:

        futures = {
            executor.submit(quick_tcp_filter, cfg): cfg
            for cfg in configs
        }

        for i, future in enumerate(
            concurrent.futures.as_completed(futures)
        ):

            result = future.result()

            if result:
                tcp_alive.append(result)

            if (i + 1) % 50 == 0 or (i + 1) == len(configs):
                print_progress(
                    i + 1,
                    len(configs),
                    prefix='TCP Scan:'
                )

    tcp_alive.sort(
        key=lambda x: (
            -x['success_ratio'],
            x['tcp_latency']
        )
    )

    target = tcp_alive[:MAX_CONFIGS_FOR_XRAY]

    safe_print(
        f"\n🛡 Deep Xray Validation on {len(target)} configs..."
    )

    verified = []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(24, os.cpu_count() * 4)
    ) as executor:

        futures = {
            executor.submit(validate_with_xray, cfg): cfg
            for cfg in target
        }

        for i, future in enumerate(
            concurrent.futures.as_completed(futures)
        ):

            result = future.result()

            if result:
                verified.append(result)

            if (i + 1) % 10 == 0 or (i + 1) == len(target):
                print_progress(
                    i + 1,
                    len(target),
                    prefix='Xray Test:'
                )

    verified.sort(
        key=lambda x: x['quality_score'],
        reverse=True
    )

    return verified

# =========================================================
# Save Results
# =========================================================


def save_results(configs: List[Dict]):

    if not configs:
        safe_print("\n❌ No healthy configs found.")
        return

    top = configs[:FINAL_MAX_OUTPUT_CONFIGS]

    lines = []

    for i, cfg in enumerate(top, 1):

        clean_link = cfg['original_config'].split('#')[0]

        label = (
            f"Verified_{i}"
            f"_Ping-{int(cfg['real_latency'])}"
            f"_Speed-{int(cfg['download_speed_kbps'])}KB"
            f"_Jitter-{int(cfg['real_jitter'])}"
        )

        lines.append(f"{clean_link}#{label}")

    encoded = base64.b64encode(
        "\n".join(lines).encode('utf-8')
    ).decode('utf-8')

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    path = os.path.join(
        OUTPUT_DIR,
        OUTPUT_FILENAME
    )

    with open(path, 'w', encoding='utf-8') as f:
        f.write(encoded)

    safe_print(
        f"\n💾 Saved {len(top)} stable configs to: {path}"
    )

# =========================================================
# Main
# =========================================================


def main():

    if not os.path.exists(XRAY_PATH):
        safe_print(
            "❌ xray core not found."
        )
        sys.exit(1)

    start = time.time()

    safe_print(
        "🚀 Fetching subscription sources..."
    )

    all_configs = []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=20
    ) as executor:

        futures = {
            executor.submit(
                fetch_subscription_content,
                url
            ): url
            for url in CONFIG_URLS
        }

        for i, future in enumerate(
            concurrent.futures.as_completed(futures)
        ):

            content = future.result()

            if content:
                all_configs.extend(
                    process_subscription_content(content)
                )

            print_progress(
                i + 1,
                len(CONFIG_URLS),
                prefix='Fetch:'
            )

    safe_print(
        f"\n📦 Total parsed configs: {len(all_configs)}"
    )

    verified = evaluate_configs(all_configs)

    safe_print(
        f"\n✅ Final healthy configs: {len(verified)}"
    )

    save_results(verified)

    safe_print(
        f"\n⏱ Finished in {time.time() - start:.2f} sec"
    )


if __name__ == '__main__':
    main()
