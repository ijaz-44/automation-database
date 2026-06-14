import os
import sys
import time
import threading
import traceback
import json
import importlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from pairs import (get_pairs_by_market, is_crypto_symbol,
                   get_real_currency_pairs, get_all_pairs)
from data_sources.binance_ws   import BinanceWebSocket
from data_sources.binance_rest import BinanceREST
from data_sources.finnhub_ws   import FinnhubWebSocket
from data_sources.finnhub_rest import FinnhubREST
from data_sources.iqoption_ws  import IQOptionWS

# ==================== Import all X modules ====================
try:
    from Groups.group_x.X01_klines_rest import run_download as x01_download
except ImportError:
    x01_download = None
    print("[WARN] X01_klines_rest not available")

try:
    from Groups.group_x.X03_cvd_rest import run_download as x03_download
except ImportError:
    x03_download = None
    print("[WARN] X03_cvd_rest not available")

try:
    from Groups.group_x.X05_depth_rest import run_download as x05_download
except ImportError:
    x05_download = None
    print("[WARN] X05_depth_rest not available")

try:
    from Groups.group_x.X07_derivative_rest import run_download as x07_download
except ImportError:
    x07_download = None
    print("[WARN] X07_derivative_rest not available")

try:
    from Groups.group_x.X09_correlation_rest import run_download as x09_download
except ImportError:
    x09_download = None
    print("[WARN] X09_correlation_rest not available")

try:
    from Groups.group_x.X11_macro_rest import run_download as x11_download
except ImportError:
    x11_download = None
    print("[WARN] X11_macro_rest not available")

try:
    from Groups.group_x.X13_liquidation_rest import run_download as x13_download
except ImportError:
    x13_download = None
    print("[WARN] X13_liquidation_rest not available")

try:
    from Groups.group_x.X15_session_rest import run_download as x15_download
except ImportError:
    x15_download = None
    print("[WARN] X15_session_rest not available")

try:
    from Groups.group_x.X17_sentiment_rest import run_download as x17_download
except ImportError:
    x17_download = None
    print("[WARN] X17_sentiment_rest not available")

try:
    from Groups.group_x.X19_volProfile_rest import run_download as x19_download
except ImportError:
    x19_download = None
    print("[WARN] X19_volProfile_rest not available")

try:
    from Groups.group_x.X21_mstructure_rest import run_download as x21_download
except ImportError:
    x21_download = None
    print("[WARN] X21_mstructure_rest not available")

try:
    from Groups.group_x.X23_onchain_rest import run_download as x23_download
except ImportError:
    x23_download = None
    print("[WARN] X23_onchain_rest not available")

try:
    from Groups.group_x.X25_tick_rest import run_download as x25_download
except ImportError:
    x25_download = None
    print("[WARN] X25_tick_rest not available")

# ==================== Import all P modules ====================
try:
    from Groups.group_p.P01_volatility_price_action import process_and_save as p01_process
except ImportError:
    p01_process = None
    print("[WARN] P01_volatility_price_action not available")

try:
    from Groups.group_p.P02_cvd_flow import process_cvd as p02_process
except ImportError:
    p02_process = None
    print("[WARN] P02_cvd_flow not available")

try:
    from Groups.group_p.P03_orderbook_imbalance import process_depth as p03_process
except ImportError:
    p03_process = None
    print("[WARN] P03_orderbook_imbalance not available")

try:
    from Groups.group_p.P04_derivatives_flow import process_derivatives as p04_process
except ImportError:
    p04_process = None
    print("[WARN] P04_derivatives_flow not available")

try:
    from Groups.group_p.P05_correlation_regime import process_correlation as p05_process
except ImportError:
    p05_process = None
    print("[WARN] P05_correlation_regime not available")

try:
    from Groups.group_p.P06_macro_impact import process_macro as p06_process
except ImportError:
    p06_process = None
    print("[WARN] P06_macro_impact not available")

try:
    from Groups.group_p.P07_liquidation_heat import process_liquidations as p07_process
except ImportError:
    p07_process = None
    print("[WARN] P07_liquidation_heat not available")

try:
    from Groups.group_p.P08_session_activity import process_sessions as p08_process
except ImportError:
    p08_process = None
    print("[WARN] P08_session_activity not available")

try:
    from Groups.group_p.P09_sentiment_contrarian import process_sentiment as p09_process
except ImportError:
    p09_process = None
    print("[WARN] P09_sentiment_contrarian not available")

try:
    from Groups.group_p.P10_volume_profile import process_volume_profile as p10_process
except ImportError:
    p10_process = None
    print("[WARN] P10_volume_profile not available")

try:
    from Groups.group_p.P11_market_structure import process_market_structure as p11_process
except ImportError:
    p11_process = None
    print("[WARN] P11_market_structure not available")

try:
    from Groups.group_p.P12_onchain_capital import process_onchain as p12_process
except ImportError:
    p12_process = None
    print("[WARN] P12_onchain_capital not available")

try:
    from Groups.group_p.P13_tick_flow import process_tick as p13_process
except ImportError:
    p13_process = None
    print("[WARN] P13_tick_flow not available")

print("[SysData] Loading...")

# ==================== E modules list (for later execution) ====================
E_MODULE_NAMES = [
    "E01_candles_expert",
    "E02_derivative_expert",
    "E03_tick_expert",
    "E04_cvd_expert",
    "E05_depth_expert",
    "E06_correlation_expert",
    "E07_macro_expert",
    "E08_liquidation_expert",
    "E09_sessions_expert",
    "E10_sentiment_expert",
    "E11_volProfile_expert",
    "E12_mstructure_expert",
    "E13_onchain_expert",
    "E14_regime_expert",
    "E15_indicators_expert",
    "E16_manipulation_expert"
]

# ── Global status tracking for call operations ──
_fill_status = {}
_status_lock = threading.Lock()

def update_status(symbol, component, completed=True):
    with _status_lock:
        if symbol not in _fill_status:
            _fill_status[symbol] = {}
        _fill_status[symbol][component] = completed
        print(f"[CallStatus] {symbol} - {component} = {completed}", flush=True)

def get_fill_status(symbol):
    with _status_lock:
        return _fill_status.get(symbol, {})

# ── Helper: check if .tmp_p file exists (single or multiple parts) ──
def _tmp_p_exists(symbol, suffix=""):
    """Return True if the required .tmp_p file(s) exist for the given component."""
    clean = symbol.upper().replace("/", "").replace(" (OTC)", "").lower()
    base_dir = os.path.join(os.path.dirname(__file__), "market_data", "binance", "symbols")
    
    # Special cases: CVD and VolProfile have two parts
    if suffix == "cvd":
        f1 = os.path.join(base_dir, f"{clean}_cvd1.tmp_p")
        f2 = os.path.join(base_dir, f"{clean}_cvd2.tmp_p")
        return os.path.exists(f1) and os.path.exists(f2)
    elif suffix == "volProfile":
        f1 = os.path.join(base_dir, f"{clean}_volProfile1.tmp_p")
        f2 = os.path.join(base_dir, f"{clean}_volProfile2.tmp_p")
        return os.path.exists(f1) and os.path.exists(f2)
    else:
        # Single file (including liquidations)
        if suffix:
            fname = f"{clean}_{suffix}.tmp_p"
        else:
            fname = f"{clean}.tmp_p"
        path = os.path.join(base_dir, fname)
        return os.path.exists(path)

# Helper for .tmp_x existence (multi‑part support for X modules)
def _tmp_x_exists(symbol, suffix=""):
    """Return True if X module's .tmp_x file(s) exist. Handles multi‑part modules."""
    clean = symbol.upper().replace("/", "").replace(" (OTC)", "").lower()
    base_dir = os.path.join(os.path.dirname(__file__), "market_data", "binance", "symbols")
    
    # Multi‑part X modules: CVD and volProfile produce two files each
    if suffix == "cvd":
        f1 = os.path.join(base_dir, f"{clean}_cvd1.tmp_x")
        f2 = os.path.join(base_dir, f"{clean}_cvd2.tmp_x")
        return os.path.exists(f1) and os.path.exists(f2)
    elif suffix == "volProfile":
        f1 = os.path.join(base_dir, f"{clean}_volProfile1.tmp_x")
        f2 = os.path.join(base_dir, f"{clean}_volProfile2.tmp_x")
        return os.path.exists(f1) and os.path.exists(f2)
    else:
        # Single file
        if suffix:
            fname = f"{clean}_{suffix}.tmp_x"
        else:
            fname = f"{clean}.tmp_x"
        path = os.path.join(base_dir, fname)
        return os.path.exists(path)

# ── RealHandler (Binance + Finnhub) ──
class RealHandler:
    def __init__(self):
        self.binance_ws = BinanceWebSocket()
        self.finnhub_ws = FinnhubWebSocket()
        self.binance_rest = BinanceREST()
        self.finnhub_rest = FinnhubREST()
        self._started = False
        self.data_dir = os.path.join(os.path.dirname(__file__), "market_data", "binance")

    def start(self, pairs):
        if self._started:
            return
        crypto_syms = [s for s in pairs if is_crypto_symbol(s.upper())]
        non_crypto   = [s for s in pairs if not is_crypto_symbol(s.upper())]
        if crypto_syms:
            self.binance_ws.connect(crypto_syms)
        if non_crypto:
            self.finnhub_ws.connect(non_crypto)
        self._started = True
        print(f"✅ [RealHandler] WS started | crypto={len(crypto_syms)} non_crypto={len(non_crypto)}", flush=True)

    def get_candles(self, symbol, interval="1m", limit=100):
        sym = symbol.lower()
        if is_crypto_symbol(sym.upper()):
            return self.binance_ws.get_candles(sym, limit)
        else:
            return self.finnhub_ws.get_candles(sym, interval, limit)

    def get_live_candle(self, symbol):
        sym = symbol.lower()
        if is_crypto_symbol(sym.upper()):
            return self.binance_ws.get_live_candle(sym)
        else:
            return self.finnhub_ws.get_live_candle(sym)

    def get_price(self, symbol):
        sym = symbol.lower()
        if is_crypto_symbol(sym.upper()):
            return self.binance_ws.get_price(sym)
        else:
            return self.finnhub_ws.get_price(sym)

    def get_closed_count(self, symbol, interval="1m"):
        sym = symbol.lower()
        if is_crypto_symbol(sym.upper()):
            return self.binance_ws.get_closed_count(sym)
        else:
            return self.finnhub_ws.get_closed_count(sym)

    def get_last_update(self, symbol):
        sym = symbol.lower()
        if is_crypto_symbol(sym.upper()):
            return self.binance_ws.get_last_update(sym)
        else:
            return self.finnhub_ws.get_last_update(sym)

    def stop(self):
        self.binance_ws.disconnect()
        self.finnhub_ws.disconnect()

    # ---------- Run X then P, then E modules ----------
    def call_single(self, symbol, minutes=120):
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        print(f"[Call] Starting download for {clean} (parallel X+P, then E experts)", flush=True)

        def run_x_then_p(module_name, x_func, p_func, suffix):
            # SPECIAL HANDLING FOR LIQUIDATIONS: wait for CVD to complete first
            if module_name == "liquidations":
                print(f"[Call] Waiting for CVD module to complete before running liquidations...", flush=True)
                # Wait up to 90 seconds for CVD .tmp_p files to appear
                timeout = 90
                start_wait = time.time()
                while (time.time() - start_wait) < timeout:
                    if _tmp_p_exists(clean, "cvd"):
                        print(f"[Call] CVD ready after {int(time.time()-start_wait)}s, proceeding with liquidations", flush=True)
                        break
                    time.sleep(2)
                else:
                    print(f"[Call] Timeout waiting for CVD (90s), skipping liquidation processing", flush=True)
                    update_status(clean, module_name, False)
                    return

            print(f"[Call] Running X{module_name}...", flush=True)
            try:
                if x_func is None:
                    update_status(clean, module_name, False)
                    print(f"[Call] X{module_name} not available", flush=True)
                    return
                x_func(clean)
                x_exists = _tmp_x_exists(clean, suffix)
                if not x_exists:
                    print(f"[Call] X{module_name} failed to produce .tmp_x", flush=True)
                    update_status(clean, module_name, False)
                    return
                # X successful, now run P module
                print(f"[Call] Running P{module_name} (processing) for {clean}...", flush=True)
                if p_func is None:
                    print(f"[Call] P{module_name} not available, skipping", flush=True)
                    p_success = False
                else:
                    p_func(clean)
                    # For multi‑part modules, check all required .tmp_p files
                    if module_name in ("cvd", "volProfile"):
                        p_success = _tmp_p_exists(clean, suffix)
                    else:
                        p_success = _tmp_p_exists(clean, suffix)
                if p_success:
                    update_status(clean, module_name, True)
                    print(f"[Call] P{module_name} success -> {clean}_{suffix}.tmp_p (or parts)", flush=True)
                else:
                    update_status(clean, module_name, False)
                    print(f"[Call] P{module_name} failed to produce .tmp_p", flush=True)
            except Exception as e:
                print(f"[Call] X/P {module_name} error: {e}", flush=True)
                traceback.print_exc()
                update_status(clean, module_name, False)

        # Step 1: Run X01 + P01 (candles) sequentially first
        run_x_then_p("candles", x01_download, p01_process, "")
        print("[Call] X01+P01 completed.", flush=True)

        remaining = [
            ("cvd",           x03_download, p02_process, "cvd"),
            ("depth",         x05_download, p03_process, "depth"),
            ("derivative",    x07_download, p04_process, "derivative"),
            ("correlation",   x09_download, p05_process, "correlation"),
            ("macro",         x11_download, p06_process, "macro"),
            ("liquidations",  x13_download, p07_process, "liquidations"),
            ("sessions",      x15_download, p08_process, "sessions"),
            ("sentiment",     x17_download, p09_process, "sentiment"),
            ("volProfile",    x19_download, p10_process, "volProfile"),
            ("mstructure",    x21_download, p11_process, "mstructure"),
            ("onchain",       x23_download, p12_process, "onchain"),
            ("tick",          x25_download, p13_process, "tick"),
        ]

        print(f"[Call] Running {len(remaining)} modules in parallel (X+P)...", flush=True)
        with ThreadPoolExecutor(max_workers=len(remaining)) as executor:
            futures = {}
            for name, xf, pf, sfx in remaining:
                future = executor.submit(run_x_then_p, name, xf, pf, sfx)
                futures[future] = name
            for future in as_completed(futures):
                name = futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"[Call] {name} thread exception: {e}", flush=True)

        # ========== After all X+P modules, run E experts ==========
        print(f"[Call] All X+P modules completed for {clean}, now running E experts...", flush=True)
        for mod_name in E_MODULE_NAMES:
            try:
                # Dynamically import the module from Groups.group_e
                module = importlib.import_module(f"Groups.group_e.{mod_name}")
                if hasattr(module, "run_expert"):
                    print(f"[Call] Running E expert: {mod_name}...", flush=True)
                    module.run_expert(clean)
                else:
                    print(f"[Call] Module {mod_name} has no run_expert function, skipping", flush=True)
            except ImportError as e:
                print(f"[Call] Could not import E module {mod_name}: {e}", flush=True)
            except Exception as e:
                print(f"[Call] Error running E module {mod_name}: {e}", flush=True)
                traceback.print_exc()

        print(f"[Call] All modules (X, P, E) completed for {clean}", flush=True)

    def get_candle_count(self, symbol):
        if not self._started:
            return 0
        return self.get_closed_count(symbol, "1m")

# ── IQOptionHandler and QuotexHandler (unchanged) ──
class IQOptionHandler:
    def __init__(self, email="", password=""):
        self.ws = IQOptionWS()
        self._email = email
        self._pwd = password
        self._started = False
    def start(self, pairs):
        if self._started: return
        self.ws.connect(pairs, email=self._email, password=self._pwd)
        self._started = True
        print("✅ [IQOptionHandler] WS started", flush=True)
    def get_candles(self, symbol, interval="1m", limit=100):
        return self.ws.get_candles(symbol, interval, limit)
    def get_live_candle(self, symbol):
        return None
    def get_price(self, symbol):
        return self.ws.get_price(symbol)
    def get_closed_count(self, symbol, interval="1m"):
        return self.ws.get_closed_count(symbol)
    def stop(self):
        self.ws.disconnect()
    def call_single(self, symbol, minutes=60):
        print(f"[IQOptionHandler] Manual call not implemented for {symbol}", flush=True)
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        for comp in ["candles", "cvd", "depth", "derivative", "correlation", "macro",
                     "liquidations", "sessions", "sentiment", "volProfile", "mstructure", "onchain", "tick"]:
            update_status(clean, comp, True)
    def get_candle_count(self, symbol):
        return self.get_closed_count(symbol, "1m")

class QuotexHandler:
    def __init__(self):
        self.ws = None
        self._started = False
    def start(self, pairs):
        print("[QuotexHandler] Not implemented yet", flush=True)
        self._started = True
    def get_candles(self, symbol, interval="1m", limit=100):
        return []
    def get_live_candle(self, symbol):
        return None
    def get_price(self, symbol):
        return 0
    def get_closed_count(self, symbol, interval="1m"):
        return 0
    def stop(self):
        pass
    def call_single(self, symbol, minutes=60):
        print(f"[QuotexHandler] Manual call not implemented for {symbol}", flush=True)
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        for comp in ["candles", "cvd", "depth", "derivative", "correlation", "macro",
                     "liquidations", "sessions", "sentiment", "volProfile", "mstructure", "onchain", "tick"]:
            update_status(clean, comp, True)
    def get_candle_count(self, symbol):
        return 0

# ── Helper to clean dummy status strings (unchanged) ──
def _clean_status(val):
    dummy_placeholders = ["--", "--", "--", "--", "--", "--", "--", "--", "--", "--", "--"]
    if val in dummy_placeholders:
        return "--"
    return val

# ── SysData main class (unchanged) ──
_current_scores = {}
_score_lock = threading.Lock()

class SysData:
    def __init__(self):
        print("[SysData] Init...", flush=True)
        self._platform = None
        self._current_platform_name = None
        self._interval = "5m"
        self._market = ""
        self._pairs = []
        self._checker_started = False
        self._checker_cache = {}
        self._checker_lock = threading.Lock()
        print("[SysData] Ready", flush=True)

    def _start_checker_updater(self):
        if self._checker_started:
            return
        self._checker_started = True
        try:
            from Groups.group_z.Z10_checker import start_background_updater
            start_background_updater(self._pairs, interval_sec=5)
            print("[SysData] Checker background updater started (5 sec interval)")
        except Exception as e:
            print(f"[SysData] Failed to start checker updater: {e}")

    def _read_checker_tsv(self):
        checker_tsv = os.path.join(os.path.dirname(__file__), "market_data", "checker.tsv")
        if not os.path.exists(checker_tsv):
            return {}
        try:
            with open(checker_tsv, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            data_lines = [l for l in lines if not l.strip().startswith('#')]
            if len(data_lines) < 2:
                return {}
            result = {}
            for line in data_lines[1:]:
                parts = line.strip().split('\t')
                if len(parts) >= 14:
                    sym = parts[1]
                    result[sym] = {
                        "preflight_score": int(parts[2]),
                        "decision": parts[3],
                        "statuses": {
                            "sr": _clean_status(parts[4]),
                            "trend": _clean_status(parts[5]),
                            "news": _clean_status(parts[6]),
                            "volatility": _clean_status(parts[7]),
                            "correlation": _clean_status(parts[8]),
                            "spread": _clean_status(parts[9]),
                            "orderflow": _clean_status(parts[10]),
                            "adr": _clean_status(parts[11])
                        },
                        "news_sentiment": float(parts[12]),
                        "news_status": _clean_status(parts[6]),
                        "news_word": parts[13] if len(parts) > 13 else "--"
                    }
                elif len(parts) >= 13:
                    sym = parts[1]
                    result[sym] = {
                        "preflight_score": int(parts[2]),
                        "decision": parts[3],
                        "statuses": {
                            "sr": _clean_status(parts[4]),
                            "trend": _clean_status(parts[5]),
                            "news": _clean_status(parts[6]),
                            "volatility": _clean_status(parts[7]),
                            "correlation": _clean_status(parts[8]),
                            "spread": _clean_status(parts[9]),
                            "orderflow": _clean_status(parts[10]),
                            "adr": _clean_status(parts[11])
                        },
                        "news_sentiment": float(parts[12]),
                        "news_status": _clean_status(parts[6]),
                        "news_word": "--"
                    }
            return result
        except Exception as e:
            print(f"[SysData] Error reading checker.tsv: {e}")
            return {}

    def _ensure_platform(self, src: str, pairs: list, iq_email="", iq_pwd=""):
        if self._platform and self._current_platform_name == src:
            return self._platform
        if self._platform:
            self._platform.stop()
        if src == "real":
            self._platform = RealHandler()
        elif src == "iqoption":
            self._platform = IQOptionHandler(iq_email, iq_pwd)
        elif src == "quotex":
            self._platform = QuotexHandler()
        else:
            raise ValueError(f"Unknown platform: {src}")
        self._platform.start(pairs)
        self._current_platform_name = src
        return self._platform

    def get_data_age(self, symbol, data_type='candles'):
        return None

    def _feel(self, symbol: str, interval: str) -> dict:
        if not self._platform:
            return {"steps": 0, "pct": 0}
        sym = symbol.lower().replace("/", "").replace(" (OTC)", "")
        candles = self._platform.get_candles(sym, "1m", 200)
        if not candles:
            return {"steps": 0, "pct": 0}
        last_ts = self._platform.get_last_update(sym)
        now = int(time.time())
        ws_active = (last_ts is not None and (now - last_ts) <= 60)
        consecutive = 0
        if len(candles) >= 1:
            consecutive = 1
            for i in range(len(candles)-1, 0, -1):
                diff = candles[i]['timestamp'] - candles[i-1]['timestamp']
                if diff == 60:
                    consecutive += 1
                else:
                    break
        if ws_active:
            pct = 5 + min(95, consecutive * 5)
        else:
            pct = min(95, consecutive * 5)
        pct = min(100, pct)
        steps = pct // 5
        return {"steps": steps, "pct": pct}

    def get_feel(self, symbol: str) -> dict:
        if not self._platform:
            return {"steps": 0, "pct": 0}
        return self._feel(symbol, self._interval)

    def _get_rows(self, symbol: str, interval: str, limit=100):
        if not self._platform:
            return []
        return self._platform.get_candles(symbol, interval, limit)

    def get_file_status(self, symbol: str) -> dict:
        return {"exists": False, "candles": 0, "age_sec": 0, "gap": 0, "stale": True}

    def get_ws_alive(self, symbol: str, max_age_sec: int = 120) -> bool:
        if not self._platform or self._current_platform_name != "real":
            return False
        sym = symbol.lower().replace("/", "").replace(" (OTC)", "")
        if not is_crypto_symbol(sym.upper()):
            return False
        if hasattr(self._platform, 'binance_ws'):
            last_up = self._platform.binance_ws.get_last_update(sym)
            if last_up == 0:
                return False
            return self._platform.binance_ws.is_ws_alive(sym, max_age_sec)
        return False

    def scan(self, market: str, pairs: list, src: str = "real",
             interval: str = "5m", iq_email="", iq_pwd="") -> list:
        global _current_scores
        print(f"[SysData] Scanning {len(pairs)} pairs | market={market} tf={interval} src={src}", flush=True)
        self._interval = interval
        self._market = market
        self._pairs = pairs
        
        if not self._checker_started:
            self._start_checker_updater()
        
        try:
            from Groups.group_z.Z01_news import get_news_score
        except ImportError:
            get_news_score = None
            print("[SysData] Z01_news not available")
        
        platform = self._ensure_platform(src, pairs, iq_email, iq_pwd)
        results = []
        for pair in pairs:
            try:
                sym = pair.upper().replace("/", "").replace(" (OTC)", "")
                rows = self._get_rows(sym, interval, 100)
                score = 50
                signal = "WAIT"
                reason = "No news data"
                if rows and len(rows) >= 10 and get_news_score:
                    try:
                        news_result = get_news_score(sym, interval, rows)
                        score_mod = news_result.get("score_mod", 0)
                        news_signal = news_result.get("signal", "WAIT")
                        news_reason = news_result.get("reason", "")
                        if score_mod > 0:
                            score = min(100, 50 + score_mod)
                            signal = news_signal if news_signal != "WAIT" else "BUY"
                            reason = news_reason
                        elif score_mod < 0:
                            score = max(0, 50 + score_mod)
                            signal = news_signal if news_signal != "WAIT" else "SELL"
                            reason = news_reason
                        else:
                            score = 50
                            signal = "WAIT"
                            reason = "Neutral news"
                    except Exception as e:
                        print(f"[Scan] News error for {sym}: {e}")
                        reason = f"News error: {str(e)[:20]}"
                quality = "HIGH" if score >= 65 else "MED" if score >= 40 else "LOW"
                result = {
                    "score": score,
                    "signal": signal,
                    "trend": "--",
                    "sr_position": "--",
                    "reason": reason,
                    "regime": "unknown",
                    "quality": quality,
                    "pair": pair,
                    "feel": self._feel(sym, interval)
                }
                results.append(result)
            except Exception as e:
                results.append({
                    "pair": pair, "score": "NA", "signal": "WAIT", "trend": "FLAT",
                    "sr_position": "—", "reason": f"Error: {str(e)[:30]}",
                    "feel": {"steps": 0, "pct": 0}
                })
        results.sort(key=lambda x: x.get("score", 0) if isinstance(x.get("score"), (int, float)) else -1, reverse=True)
        with _score_lock:
            _current_scores = {}
            for r in results:
                pair = r["pair"]
                sym = pair.upper().replace("/", "").replace(" (OTC)", "")
                _current_scores[pair] = {
                    "score": r.get("score", 0),
                    "signal": r.get("signal", "WAIT"),
                    "trend": r.get("trend", "FLAT"),
                    "reason": r.get("reason", ""),
                    "sr_position": r.get("sr_position", "—"),
                    "quality": r.get("quality", "LOW"),
                    "feel_pct": r.get("feel", {}).get("pct", 0),
                    "ws_alive": self.get_ws_alive(sym)
                }
        print(f"[SysData] Scan complete, {len(results)} results stored", flush=True)
        return results

    def refresh_scores(self) -> dict:
        global _current_scores
        if not self._platform or not self._market or not self._pairs:
            return {}
        checker_data = self._read_checker_tsv()
        try:
            from Groups.group_z.Z01_news import get_news_score
        except ImportError:
            get_news_score = None
        results = []
        for pair in self._pairs:
            try:
                sym = pair.upper().replace("/", "").replace(" (OTC)", "")
                rows = self._get_rows(sym, self._interval, 100)
                score = 50
                signal = "WAIT"
                reason = "No news data"
                if rows and len(rows) >= 10 and get_news_score:
                    try:
                        news_result = get_news_score(sym, self._interval, rows)
                        score_mod = news_result.get("score_mod", 0)
                        news_signal = news_result.get("signal", "WAIT")
                        news_reason = news_result.get("reason", "")
                        if score_mod > 0:
                            score = min(100, 50 + score_mod)
                            signal = news_signal if news_signal != "WAIT" else "BUY"
                            reason = news_reason
                        elif score_mod < 0:
                            score = max(0, 50 + score_mod)
                            signal = news_signal if news_signal != "WAIT" else "SELL"
                            reason = news_reason
                        else:
                            score = 50
                            signal = "WAIT"
                            reason = "Neutral news"
                    except Exception as e:
                        print(f"[Refresh] News error for {sym}: {e}")
                        reason = f"News error: {str(e)[:20]}"
                quality = "HIGH" if score >= 65 else "MED" if score >= 40 else "LOW"
                result = {
                    "score": score,
                    "signal": signal,
                    "trend": "--",
                    "sr_position": "--",
                    "reason": reason,
                    "regime": "unknown",
                    "quality": quality,
                    "pair": pair,
                    "feel": self._feel(sym, self._interval)
                }
                clean_sym = sym
                if clean_sym in checker_data:
                    result["gatekeeper"] = checker_data[clean_sym]
                else:
                    result["gatekeeper"] = {}
                results.append(result)
            except Exception as e:
                results.append({
                    "pair": pair, "score": "NA", "signal": "WAIT", "trend": "FLAT",
                    "sr_position": "—", "reason": f"Error: {str(e)[:30]}",
                    "feel": {"steps": 0, "pct": 0}
                })
        with _score_lock:
            _current_scores = {}
            for r in results:
                pair = r["pair"]
                sym = pair.upper().replace("/", "").replace(" (OTC)", "")
                gatekeeper = r.get("gatekeeper", {})
                news_word = gatekeeper.get("news_word", "--")
                news_sentiment = gatekeeper.get("news_sentiment", 0)
                news_status = gatekeeper.get("news_status", "No news")
                news_status = _clean_status(news_status)
                _current_scores[pair] = {
                    "score": r.get("score", 0),
                    "signal": r.get("signal", "WAIT"),
                    "trend": r.get("trend", "FLAT"),
                    "reason": r.get("reason", ""),
                    "sr_position": r.get("sr_position", "—"),
                    "quality": r.get("quality", "LOW"),
                    "feel_pct": r.get("feel", {}).get("pct", 0),
                    "news_word": news_word,
                    "news_sentiment": news_sentiment,
                    "news_status": news_status,
                    "preflight_score": gatekeeper.get("preflight_score", 0),
                    "statuses": gatekeeper.get("statuses", {}),
                    "ws_alive": self.get_ws_alive(sym)
                }
        return _current_scores

    def go(self, symbol: str, market: str, interval: str = "") -> dict:
        if not interval:
            interval = self._interval
        try:
            rows = self._get_rows(symbol, interval, 100)
            if not rows or len(rows) < 10:
                return {"error": "Not enough data for analysis"}
            try:
                from Groups.group_z.Z01_news import get_news_score
                news = get_news_score(symbol, interval, rows) if get_news_score else {"score_mod": 0}
            except:
                news = {"score_mod": 0}
            score_mod = news.get("score_mod", 0)
            a_score = 50 + score_mod
            a_score = max(0, min(100, a_score))
            a_signal = "BUY" if score_mod > 0 else "SELL" if score_mod < 0 else "WAIT"
            a_signal = "STRONG BUY" if score_mod >= 20 else "BUY" if score_mod > 0 else a_signal
            a_signal = "STRONG SELL" if score_mod <= -20 else "SELL" if score_mod < 0 else a_signal
            return {
                "a_score": a_score,
                "a_signal": a_signal,
                "go": a_score >= 70,
                "sl": 0,
                "tp": 0,
                "forecast": {},
                "reason": news.get("reason", "News based"),
                "requires_deep": False,
                "min_deep_score": 60,
                "details": {"news": news}
            }
        except Exception as e:
            return {"error": f"GO error: {str(e)}"}

    def get_current_scores(self) -> dict:
        with _score_lock:
            return _current_scores

    def get_call_stats(self) -> dict:
        from data_sources.binance_rest import BinanceREST
        from data_sources.finnhub_rest import FinnhubREST
        return {"Binance": BinanceREST.get_total_calls(), "Finnhub": FinnhubREST.get_total_calls()}

    def call_single(self, symbol: str):
        if not self._platform:
            raise RuntimeError("No platform active")
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        with _status_lock:
            _fill_status[clean] = {}
        thread = threading.Thread(target=self._platform.call_single, args=(symbol, 120), daemon=True)
        thread.start()
        print(f"[SysData] call_single thread started for {clean}", flush=True)

    def get_fill_status(self, symbol: str) -> dict:
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        return get_fill_status(clean)

    def get_candle_count(self, symbol: str) -> int:
        if not self._platform:
            return 0
        return self._platform.get_candle_count(symbol)

print("✅ [SysData] Loaded OK – X modules produce .tmp_x, P modules produce .tmp_p, E modules run after all P", flush=True)