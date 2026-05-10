import os
import sys
import time
import threading
import traceback
import json
import sqlite3
from pairs import (get_pairs_by_market, is_crypto_symbol,
                   get_real_currency_pairs, get_all_pairs)
from data_sources.binance_ws   import BinanceWebSocket
from data_sources.binance_rest import BinanceREST
from data_sources.finnhub_ws   import FinnhubWebSocket
from data_sources.finnhub_rest import FinnhubREST
from data_sources.iqoption_ws  import IQOptionWS

# ==================== Direct imports from group_x ====================
try:
    from Groups.group_x.X01_klines_rest import fetch_and_update_ws
except ImportError:
    fetch_and_update_ws = None
    print("[WARN] X01_klines_rest not available")

try:
    from Groups.group_x.X03_cvd_rest import backfill_cvd_advanced
except ImportError:
    backfill_cvd_advanced = None
    print("[WARN] X03_cvd_rest not available")

try:
    from Groups.group_x.X05_depth_rest import update_depth
except ImportError:
    update_depth = None
    print("[WARN] X05_depth_rest not available")

try:
    from Groups.group_x.X07_derivative_rest import DerivativeRest
except ImportError:
    DerivativeRest = None
    print("[WARN] X07_derivative_rest not available")

try:
    from Groups.group_x.X13_liquidation_rest import LiquidationDataREST
except ImportError:
    LiquidationDataREST = None
    print("[WARN] X13_liquidation_rest not available")

try:
    from Groups.group_x.X19_volProfile_rest import VolumeProfile
except ImportError:
    VolumeProfile = None
    print("[WARN] X19_volProfile_rest not available")

try:
    from Groups.group_x.X21_mstructure_rest import MarketStructure
except ImportError:
    MarketStructure = None
    print("[WARN] X21_mstructure_rest not available")

# Other disabled modules (keep as is)
try:
    from Groups.group_x.X08_derivative_ws import DerivativeWebSocket
except ImportError:
    DerivativeWebSocket = None

try:
    from Groups.group_x.X09_correlation_rest import CorrelationRest
except ImportError:
    CorrelationRest = None

try:
    from Groups.group_x.X10_correlation_ws import CorrelationWebSocket
except ImportError:
    CorrelationWebSocket = None

try:
    from Groups.group_x.X11_macro_rest import MacroDataFetcher
except ImportError:
    MacroDataFetcher = None

try:
    from Groups.group_x.X15_session_rest import session_collect
except ImportError:
    session_collect = None

try:
    from Groups.group_x.X17_sentiment_rest import SentimentData
except ImportError:
    SentimentData = None

try:
    from Groups.group_x.X23_onchain_rest import collect_and_save as onchain_collect
except ImportError:
    onchain_collect = None
    print("[WARN] X23_onchain_rest not available")

try:
    from Groups.group_x.X25_tick_rest import tick_collect
except ImportError:
    tick_collect = None
    print("[WARN] X25_tick_rest not available")
# ========================================================================

print("[SysData] Loading...")

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

# ── RealHandler (Binance + Finnhub) – updated for SQLite derivatives, liquidations, mstructure ──
class RealHandler:
    def __init__(self):
        self.binance_ws = BinanceWebSocket()
        self.finnhub_ws = FinnhubWebSocket()
        self.binance_rest = BinanceREST()
        self.finnhub_rest = FinnhubREST()
        self._started = False
        self.data_dir = os.path.join(os.path.dirname(__file__), "market_data", "binance")
        self.derivative_rest = None
        self.liquidation_rest = None
        self.mstructure_rest = None
        self.active_derivative_symbols = set()
        self.active_liquidation_symbols = set()
        self.active_mstructure_symbols = set()

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
        if self.derivative_rest:
            self.derivative_rest.stop()
        if self.liquidation_rest:
            self.liquidation_rest.stop()
        if self.mstructure_rest:
            self.mstructure_rest.stop()

    # ---------- CVD (X03) – DISABLED ----------
    def compute_and_save_cvd(self, symbol):
        print(f"[CVD] Temporarily disabled", flush=True)
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        update_status(clean, 'cvd', True)
        return

    # ---------- Depth (X05_depth_rest) – SQLite ----------
    def activate_depth_for_symbol(self, symbol):
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        print(f"[Depth] Activating for {clean}", flush=True)
        if not is_crypto_symbol(clean):
            print(f"[Depth] {clean} is not crypto, skipping", flush=True)
            update_status(clean, 'depth', True)
            return
        if update_depth is None:
            print(f"[Depth] X05_depth_rest.update_depth not available", flush=True)
            update_status(clean, 'depth', False)
            return
        try:
            update_depth(clean)
            db_path = os.path.join(self.data_dir, "symbols", f"{clean.lower()}_depth.db")
            if os.path.exists(db_path) and os.path.getsize(db_path) > 100:
                update_status(clean, 'depth', True)
                print(f"[Depth] Saved SQLite DB for {clean}", flush=True)
            else:
                update_status(clean, 'depth', False)
        except Exception as e:
            print(f"[Depth] Error: {e}", flush=True)
            traceback.print_exc()
            update_status(clean, 'depth', False)

    # ---------- Derivative (X07) – SQLITE ----------
    def _ensure_derivative_modules(self):
        if DerivativeRest is not None and self.derivative_rest is None:
            print("[Derivative] Initializing module...", flush=True)
            self.derivative_rest = DerivativeRest(self.data_dir)

    def activate_derivative_for_symbol(self, symbol):
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        if DerivativeRest is None:
            update_status(clean, 'derivative', False)
            return
        print(f"[Derivative] Activating for {clean}", flush=True)
        if not is_crypto_symbol(clean):
            print(f"[Derivative] {clean} is not crypto, skipping", flush=True)
            update_status(clean, 'derivative', True)
            return
        try:
            self._ensure_derivative_modules()
            self.active_derivative_symbols.add(clean)
            if self.derivative_rest:
                self.derivative_rest.collect_and_save(clean)
            db_path = os.path.join(self.data_dir, "symbols", f"{clean.lower()}_derivative.db")
            if os.path.exists(db_path) and os.path.getsize(db_path) > 100:
                update_status(clean, 'derivative', True)
                print(f"[Derivative] Saved SQLite DB for {clean}", flush=True)
            else:
                update_status(clean, 'derivative', False)
                print(f"[Derivative] DB not saved: {db_path}", flush=True)
        except Exception as e:
            print(f"[Derivative] Error: {e}", flush=True)
            traceback.print_exc()
            update_status(clean, 'derivative', False)

    # ---------- Liquidations (X13) – SQLITE (enabled) ----------
    def _ensure_liquidation_modules(self):
        if LiquidationDataREST is not None and self.liquidation_rest is None:
            print("[Liquidations] Initializing module...", flush=True)
            self.liquidation_rest = LiquidationDataREST(self.data_dir)

    def run_x13_liquidations(self, symbol):
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        if LiquidationDataREST is None:
            print("[Liquidations] Module not available", flush=True)
            update_status(clean, 'liquidations', False)
            return
        print(f"[Liquidations] Starting for {clean}", flush=True)
        if not is_crypto_symbol(clean):
            print(f"[Liquidations] {clean} is not crypto, skipping", flush=True)
            update_status(clean, 'liquidations', True)
            return
        try:
            self._ensure_liquidation_modules()
            if self.liquidation_rest:
                self.liquidation_rest.collect_and_save(clean)
            db_path = os.path.join(self.data_dir, "symbols", f"{clean.lower()}_liquidations.db")
            if os.path.exists(db_path) and os.path.getsize(db_path) > 100:
                update_status(clean, 'liquidations', True)
                print(f"[Liquidations] Saved SQLite DB for {clean}", flush=True)
            else:
                update_status(clean, 'liquidations', False)
        except Exception as e:
            print(f"[Liquidations] Error: {e}", flush=True)
            traceback.print_exc()
            update_status(clean, 'liquidations', False)

    # ---------- Market Structure (X21) – SQLITE (enabled) ----------
    def _ensure_mstructure_modules(self):
        if MarketStructure is not None and self.mstructure_rest is None:
            print("[MStructure] Initializing module...", flush=True)
            self.mstructure_rest = MarketStructure()

    def run_x21_mstructure(self, symbol):
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        if MarketStructure is None:
            print("[MStructure] Module not available", flush=True)
            update_status(clean, 'mstructure', False)
            return
        print(f"[MStructure] Starting for {clean}", flush=True)
        if not is_crypto_symbol(clean):
            print(f"[MStructure] {clean} is not crypto, skipping", flush=True)
            update_status(clean, 'mstructure', True)
            return
        try:
            self._ensure_mstructure_modules()
            if self.mstructure_rest:
                self.mstructure_rest.collect_and_save(clean)
            db_path = os.path.join(self.data_dir, "symbols", f"{clean.lower()}_mstructure.db")
            if os.path.exists(db_path) and os.path.getsize(db_path) > 100:
                update_status(clean, 'mstructure', True)
                print(f"[MStructure] Saved SQLite DB for {clean}", flush=True)
            else:
                update_status(clean, 'mstructure', False)
        except Exception as e:
            print(f"[MStructure] Error: {e}", flush=True)
            traceback.print_exc()
            update_status(clean, 'mstructure', False)

    # ---------- Other disabled modules ----------
    def activate_correlation_for_symbol(self, symbol):
        print(f"[Correlation] Temporarily disabled", flush=True)
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        update_status(clean, 'correlation', True)

    def fetch_macro_data(self, symbol):
        print(f"[Macro] Temporarily disabled", flush=True)
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        update_status(clean, 'macro', True)

    def run_x15_session(self, symbol):
        print(f"[X15] Temporarily disabled", flush=True)
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        update_status(clean, 'sessions', True)

    def run_x17_sentiment(self, symbol):
        print(f"[X17] Temporarily disabled", flush=True)
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        update_status(clean, 'sentiment', True)

    def run_x19_volprofile(self, symbol):
        if VolumeProfile is None:
            print("[X19] Module not available, skipping", flush=True)
            return
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        db_path = os.path.join(self.data_dir, "symbols", f"{clean.lower()}_volProfile.db")
        old_toon = os.path.join(self.data_dir, "symbols", f"{clean.lower()}_volProfile.toon")
        if os.path.exists(old_toon):
            os.remove(old_toon)
            print(f"[X19] Removed legacy .toon: {old_toon}", flush=True)
        print("[X19] Starting volume profile analysis (SQLite)", flush=True)
        try:
            vp = VolumeProfile()
            vp.collect_and_save(clean)
            if os.path.exists(db_path) and os.path.getsize(db_path) > 100:
                update_status(clean, 'volProfile', True)
                print(f"[X19] SQLite DB saved: {db_path}", flush=True)
            else:
                update_status(clean, 'volProfile', False)
                print(f"[X19] Failed to save SQLite DB: {db_path}", flush=True)
        except Exception as e:
            print(f"[X19] Error: {e}", flush=True)
            traceback.print_exc()
            update_status(clean, 'volProfile', False)

    def run_x23_onchain(self, symbol):
        print(f"[X23] Temporarily disabled", flush=True)
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        update_status(clean, 'onchain', True)

    def run_x25_tick(self, symbol):
        print(f"[X25] Temporarily disabled", flush=True)
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        update_status(clean, 'tick', True)

    # ---------- call_single: starts all active threads ----------
    def call_single(self, symbol, minutes=120):
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        print(f"[Call] Starting call for {clean}", flush=True)
        if is_crypto_symbol(clean):
            print("[Call] Step 1: Candles (via X01) -> SQLite .db", flush=True)
            try:
                if fetch_and_update_ws is None:
                    raise Exception("X01 module not available")
                success = fetch_and_update_ws(clean, self.binance_ws)
                if success:
                    update_status(clean, 'candles', True)
                    print(f"[Call] Candles saved to {clean.lower()}.db", flush=True)
                else:
                    update_status(clean, 'candles', False)
            except Exception as e:
                print(f"[Call] X01 error: {e}", flush=True)
                traceback.print_exc()
                update_status(clean, 'candles', False)

            print("[Call] Step 2: Depth (X05_depth_rest) -> SQLite .db", flush=True)
            try:
                if update_depth is not None:
                    update_depth(clean)
                    db_path = os.path.join(self.data_dir, "symbols", f"{clean.lower()}_depth.db")
                    if os.path.exists(db_path) and os.path.getsize(db_path) > 100:
                        update_status(clean, 'depth', True)
                        print(f"[Depth] Saved SQLite DB for {clean}", flush=True)
                    else:
                        update_status(clean, 'depth', False)
                else:
                    update_status(clean, 'depth', False)
            except Exception as e:
                print(f"[Depth] Error: {e}", flush=True)
                traceback.print_exc()
                update_status(clean, 'depth', False)

            print("[Call] Step 3: Starting background tasks (Derivative, Liquidations, MStructure, VolProfile)", flush=True)

            # Derivative
            t_deriv = threading.Thread(target=self.activate_derivative_for_symbol, args=(clean,), daemon=True)
            # Liquidations
            t_liq = threading.Thread(target=self.run_x13_liquidations, args=(clean,), daemon=True)
            # MStructure
            t_mstruct = threading.Thread(target=self.run_x21_mstructure, args=(clean,), daemon=True)
            # VolProfile
            t_vol = threading.Thread(target=self.run_x19_volprofile, args=(clean,), daemon=True)

            t_deriv.start()
            t_liq.start()
            t_mstruct.start()
            t_vol.start()

            print("[Call] Started background tasks: Derivative, Liquidations, MStructure, VolProfile", flush=True)
        else:
            print(f"[Call] Non-crypto {clean}, only candles", flush=True)
            try:
                self.finnhub_rest.fill_gaps(clean, minutes=minutes)
                candles = self.finnhub_rest.get_candles_for_symbol(clean)
                if candles:
                    self.finnhub_ws.add_candles(clean, candles)
                candles_file = os.path.join(self.data_dir, "symbols", f"{clean.lower()}.tsv")
                if os.path.exists(candles_file):
                    update_status(clean, 'candles', True)
                else:
                    update_status(clean, 'candles', False)
            except Exception as e:
                print(f"[Call] Non-crypto error: {e}", flush=True)
                update_status(clean, 'candles', False)
            # For non-crypto, set all derivative-related status as true (or false if you prefer)
            update_status(clean, 'cvd', True)
            update_status(clean, 'depth', True)
            update_status(clean, 'derivative', True)
            update_status(clean, 'correlation', True)
            update_status(clean, 'liquidations', True)
            update_status(clean, 'mstructure', True)

        print(f"[Call] call_single main thread finished (background tasks running)", flush=True)

    def get_candle_count(self, symbol):
        if not self._started:
            return 0
        return self.get_closed_count(symbol, "1m")

# ── IQOptionHandler, QuotexHandler (unchanged) ──
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
        update_status(clean, 'candles', True)
        update_status(clean, 'cvd', True)
        update_status(clean, 'depth', True)
        update_status(clean, 'derivative', True)
        update_status(clean, 'correlation', True)
        update_status(clean, 'liquidations', True)
        update_status(clean, 'mstructure', True)
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
        update_status(clean, 'candles', True)
        update_status(clean, 'cvd', True)
        update_status(clean, 'depth', True)
        update_status(clean, 'derivative', True)
        update_status(clean, 'correlation', True)
        update_status(clean, 'liquidations', True)
        update_status(clean, 'mstructure', True)
    def get_candle_count(self, symbol):
        return 0

# ── Helper to clean dummy status strings ──
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
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        base_dir = os.path.join(os.path.dirname(__file__), "market_data", "binance", "symbols")
        if data_type == 'candles':
            filepath = os.path.join(base_dir, f"{clean.lower()}.db")
        else:
            filepath = os.path.join(base_dir, f"{clean.lower()}_{data_type}.db")
        if not os.path.exists(filepath):
            return None
        try:
            mtime = os.path.getmtime(filepath)
            return int(time.time() - mtime)
        except Exception:
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
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        base_dir = os.path.join(os.path.dirname(__file__), "market_data", "binance", "symbols")
        db_path = os.path.join(base_dir, f"{clean.lower()}.db")
        if not os.path.exists(db_path):
            return {"exists": False, "candles": 0, "age_sec": 0, "gap": 0, "stale": True}
        try:
            mtime = os.path.getmtime(db_path)
            age = int(time.time() - mtime)
            stale = age > 300
            return {
                "exists": True,
                "candles": 1,
                "age_sec": age,
                "gap": 0,
                "stale": stale
            }
        except Exception as e:
            print(f"[SysData] get_file_status error for {symbol}: {e}", flush=True)
            return {"exists": True, "candles": 0, "age_sec": 9999, "gap": 0, "stale": True}

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

print("✅ [SysData] Loaded OK – Derivative, Liquidations, MStructure now SQLite", flush=True)