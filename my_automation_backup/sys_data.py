import os
import sys
import time
import threading
import traceback
import json
from engine import get_engine
from pairs import (get_pairs_by_market, is_crypto_symbol,
                   get_real_currency_pairs, get_all_pairs)
from data_sources.binance_ws   import BinanceWebSocket
from data_sources.binance_rest import BinanceREST
from data_sources.finnhub_ws   import FinnhubWebSocket
from data_sources.finnhub_rest import FinnhubREST
from data_sources.iqoption_ws  import IQOptionWS

# CVD module
from Groups.group_x.X03_cvd_rest import backfill_cvd_advanced
# Depth module
from Groups.group_x.X05_depth_ws import DepthWebSocket
# Derivative modules
from Groups.group_x.X07_derivative_rest import DerivativeRest
from Groups.group_x.X08_derivative_ws import DerivativeWebSocket
# Correlation modules
from Groups.group_x.X09_correlation_rest import CorrelationRest
from Groups.group_x.X10_correlation_ws import CorrelationWebSocket

# Conditional imports for optional modules (may not exist yet)
try:
    from Groups.group_x.X11_macro_rest import MacroDataFetcher
    X11_AVAILABLE = True
except ImportError:
    X11_AVAILABLE = False
    print("[WARN] X11_macro_rest not available, macro data disabled")

try:
    from Groups.group_x.X13_liquidations_rest import LiquidationDataREST
    X13_AVAILABLE = True
except ImportError:
    X13_AVAILABLE = False
    print("[WARN] X13_liquidations_rest not available, liquidation data disabled")

try:
    from Groups.group_x.X15_session_rest import collect_and_save as session_collect
    X15_AVAILABLE = True
except ImportError:
    X15_AVAILABLE = False
    print("[WARN] X15_session_rest not available, session data disabled")

try:
    from Groups.group_x.X17_sentiment_rest import SentimentData
    X17_AVAILABLE = True
except ImportError:
    X17_AVAILABLE = False
    print("[WARN] X17_sentiment_rest not available, sentiment data disabled")

try:
    from Groups.group_x.X19_volProfile_rest import VolumeProfile
    X19_AVAILABLE = True
except ImportError:
    X19_AVAILABLE = False
    print("[WARN] X19_volProfile_rest not available, volume profile disabled")

try:
    from Groups.group_x.X21_mstructure_rest import MarketStructure
    X21_AVAILABLE = True
except ImportError:
    X21_AVAILABLE = False
    print("[WARN] X21_mstructure_rest not available, market structure disabled")

try:
    from Groups.group_x.X23_onchain_rest import collect_and_save as onchain_collect
    X23_AVAILABLE = True
except ImportError:
    X23_AVAILABLE = False
    print("[WARN] X23_onchain_rest not available, on‑chain data disabled")

try:
    from Groups.group_x.X25_tick_rest import collect_and_save as tick_collect
    X25_AVAILABLE = True
except ImportError:
    X25_AVAILABLE = False
    print("[WARN] X25_tick_rest not available, tick data disabled")

print("[SysData] Loading...")

# ── Global status tracking for fill operations ──
_fill_status = {}
_status_lock = threading.Lock()

def update_status(symbol, component, completed=True):
    with _status_lock:
        if symbol not in _fill_status:
            _fill_status[symbol] = {'cvd': False, 'depth': False, 'derivative': False, 'correlation': False, 'candles': False}
        _fill_status[symbol][component] = completed
        print(f"[FillStatus] {symbol} - {component} = {completed}", flush=True)

def get_fill_status(symbol):
    with _status_lock:
        return _fill_status.get(symbol, {})

# ── RealHandler (Binance + Finnhub) ─────────────────────────────────────────
class RealHandler:
    def __init__(self):
        self.binance_ws = BinanceWebSocket()
        self.finnhub_ws = FinnhubWebSocket()
        self.binance_rest = BinanceREST()
        self.finnhub_rest = FinnhubREST()
        self._started = False

        self.data_dir = os.path.join(os.path.dirname(__file__), "market_data", "binance")
        
        self.depth_ws = None
        self.derivative_rest = None
        self.derivative_ws = None
        self.active_derivative_symbols = set()
        self.correlation_rest = None
        self.correlation_ws = None
        self.cvd_ws = None

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
            return self.binance_ws.get_candles(sym, interval, limit)
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

    def stop(self):
        self.binance_ws.disconnect()
        self.finnhub_ws.disconnect()
        if self.depth_ws:
            self.depth_ws.stop()
        if self.derivative_ws:
            self.derivative_ws.stop()
        if self.derivative_rest:
            self.derivative_rest.stop()
        if self.correlation_rest:
            self.correlation_rest.stop()
        if self.cvd_ws:
            self.cvd_ws.stop()

    # ---------- CVD (REST backfill + WebSocket) ----------
    def _ensure_cvd_ws(self):
        if self.cvd_ws is None:
            from Groups.group_x.X04_cvd_ws import CVDWebSocket
            self.cvd_ws = CVDWebSocket(self.data_dir)
            self.cvd_ws.set_symbols([])
            print("[RealHandler] CVD WebSocket created and started", flush=True)

    def compute_and_save_cvd(self, symbol):
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        print(f"[CVD] Starting for {clean}", flush=True)
        if not is_crypto_symbol(clean):
            print(f"[CVD] {clean} is not crypto, skipping", flush=True)
            update_status(clean, 'cvd', True)
            return
        print(f"[CVD] Computing for {clean}", flush=True)
        config = {
            'cvd': {'minutes': 300, 'max_ticks': 50000},
            'footprint': {'minutes': 90, 'max_ticks': 50000},
            'imbalance': {'minutes': 30, 'max_ticks': 5000},
            'absorption': {'minutes': 30, 'max_ticks': 10000}
        }
        try:
            result = backfill_cvd_advanced(clean, config, max_total_trades=50000, max_cluster_levels=50)
            if "error" in result:
                print(f"[CVD] Error: {result['error']}", flush=True)
                update_status(clean, 'cvd', False)
                return
            filepath = os.path.join(self.data_dir, "symbols", f"{clean.lower()}_cvd.tsv")
            if os.path.exists(filepath):
                update_status(clean, 'cvd', True)
                print(f"[CVD] Saved to {filepath}", flush=True)
            else:
                update_status(clean, 'cvd', False)
                print(f"[CVD] File not saved: {filepath}", flush=True)
            self._ensure_cvd_ws()
            self.cvd_ws.add_symbol_metrics(clean, {})
        except Exception as e:
            print(f"[CVD] Exception: {e}", flush=True)
            traceback.print_exc()
            update_status(clean, 'cvd', False)

    # ---------- Depth (REST snapshot only) ----------
    def activate_depth_for_symbol(self, symbol):
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        print(f"[Depth] Activating for {clean}", flush=True)
        if not is_crypto_symbol(clean):
            print(f"[Depth] {clean} is not crypto, skipping", flush=True)
            update_status(clean, 'depth', True)
            return
        try:
            from Groups.group_x.X06_depth_rest import fetch_depth_snapshot
            snapshot = fetch_depth_snapshot(clean, limit=500)
            if snapshot:
                symbols_dir = os.path.join(self.data_dir, "symbols")
                os.makedirs(symbols_dir, exist_ok=True)
                filepath = os.path.join(symbols_dir, f"{clean.lower()}_depth.tsv")
                with open(filepath, 'w') as f:
                    f.write("type\tprice\tquantity\n")
                    for p, q in snapshot['bids'][:200]:
                        f.write(f"bid\t{p}\t{q}\n")
                    for p, q in snapshot['asks'][:200]:
                        f.write(f"ask\t{p}\t{q}\n")
                if os.path.exists(filepath):
                    update_status(clean, 'depth', True)
                    print(f"[Depth] Saved snapshot for {clean}", flush=True)
                else:
                    update_status(clean, 'depth', False)
            else:
                print(f"[Depth] Failed to fetch snapshot for {clean}", flush=True)
                update_status(clean, 'depth', False)
        except Exception as e:
            print(f"[Depth] Error: {e}", flush=True)
            traceback.print_exc()
            update_status(clean, 'depth', False)

    # ---------- Derivative (Futures) with liquidation analysis ----------
    def _ensure_derivative_modules(self):
        if self.derivative_rest is None:
            print("[Derivative] Initializing modules...", flush=True)
            self.derivative_rest = DerivativeRest(self.data_dir)

    def activate_derivative_for_symbol(self, symbol):
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        print(f"[Derivative] Activating for {clean}", flush=True)
        if not is_crypto_symbol(clean):
            print(f"[Derivative] {clean} is not crypto, skipping", flush=True)
            update_status(clean, 'derivative', True)
            return
        try:
            self._ensure_derivative_modules()
            self.active_derivative_symbols.add(clean)
            if self.derivative_ws is None:
                self.derivative_ws = DerivativeWebSocket(self.data_dir, self.derivative_rest, min_quantity=1.0)
                self.derivative_ws.start(list(self.active_derivative_symbols))
            self.derivative_rest.collect_and_save(clean)
            filepath = os.path.join(self.data_dir, "symbols", f"{clean.lower()}_derivative.tsv")
            if os.path.exists(filepath):
                update_status(clean, 'derivative', True)
                print(f"[Derivative] Saved data and analysis for {clean}", flush=True)
            else:
                update_status(clean, 'derivative', False)
                print(f"[Derivative] File not saved: {filepath}", flush=True)
        except Exception as e:
            print(f"[Derivative] Error: {e}", flush=True)
            traceback.print_exc()
            update_status(clean, 'derivative', False)

    # ---------- Correlation (threshold 2 rows) ----------
    def _ensure_correlation_modules(self):
        if self.correlation_rest is None:
            print("[Correlation] Initializing modules...", flush=True)
            self.correlation_rest = CorrelationRest(self.data_dir)

    def _get_correlation_row_count(self, symbol):
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        filepath = os.path.join(self.data_dir, "symbols", f"{clean.lower()}_correlation.tsv")
        if not os.path.exists(filepath):
            return 0
        try:
            with open(filepath, 'r') as f:
                lines = f.readlines()
            return max(0, len(lines) - 1)
        except Exception:
            return 0

    def activate_correlation_for_symbol(self, symbol):
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        print(f"[Correlation] Activating for {clean}", flush=True)
        if not is_crypto_symbol(clean):
            print(f"[Correlation] {clean} is not crypto, skipping", flush=True)
            update_status(clean, 'correlation', True)
            return
        try:
            self._ensure_correlation_modules()
            self.correlation_rest.collect_and_save(clean)
            row_count = self._get_correlation_row_count(clean)
            if row_count >= 2:
                update_status(clean, 'correlation', True)
                print(f"[Correlation] File has {row_count} rows -> ENOUGH (>=2), status True", flush=True)
            else:
                update_status(clean, 'correlation', False)
                print(f"[Correlation] File has only {row_count} rows -> NOT enough, status False", flush=True)
        except Exception as e:
            print(f"[Correlation] Error: {e}", flush=True)
            traceback.print_exc()
            update_status(clean, 'correlation', False)

    # ---------- Macro data (X11) ----------
    def fetch_macro_data(self, symbol):
        if not X11_AVAILABLE:
            print("[Macro] X11 module not available, skipping", flush=True)
            return
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        # Delete old macro file first
        filepath = os.path.join(self.data_dir, "symbols", f"{clean.lower()}_macro.tsv")
        if os.path.exists(filepath):
            os.remove(filepath)
            print(f"[Macro] Deleted old file: {filepath}", flush=True)
        print("[Macro] Starting macro data fetch (X11)", flush=True)
        try:
            fetcher = MacroDataFetcher()
            fetcher.fetch_and_save_all(clean)  # saves to symbol_macro.tsv
            # Check if file was created
            if os.path.exists(filepath):
                update_status(clean, 'macro', True)
                print("[Macro] Macro data saved", flush=True)
            else:
                update_status(clean, 'macro', False)
                print("[Macro] Failed to save macro data", flush=True)
        except Exception as e:
            print(f"[Macro] Error: {e}", flush=True)
            traceback.print_exc()
            update_status(clean, 'macro', False)

    # ---------- X13 Liquidations ----------
    def run_x13_liquidations(self, symbol):
        if not X13_AVAILABLE:
            print("[X13] Module not available, skipping", flush=True)
            return
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        filepath = os.path.join(self.data_dir, "symbols", f"{clean.lower()}_liquidations.tsv")
        if os.path.exists(filepath):
            os.remove(filepath)
            print(f"[X13] Deleted old file: {filepath}", flush=True)
        print("[X13] Starting liquidation data fetch", flush=True)
        try:
            liq = LiquidationDataREST()
            liq.collect_and_save(clean)
            if os.path.exists(filepath):
                update_status(clean, 'liquidations', True)
            else:
                update_status(clean, 'liquidations', False)
        except Exception as e:
            print(f"[X13] Error: {e}", flush=True)
            traceback.print_exc()
            update_status(clean, 'liquidations', False)

    # ---------- X15 Session ----------
    def run_x15_session(self, symbol):
        if not X15_AVAILABLE:
            print("[X15] Module not available, skipping", flush=True)
            return
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        filepath = os.path.join(self.data_dir, "symbols", f"{clean.lower()}_sessions.tsv")
        if os.path.exists(filepath):
            os.remove(filepath)
            print(f"[X15] Deleted old file: {filepath}", flush=True)
        print("[X15] Starting session analysis", flush=True)
        try:
            session_collect(clean)
            if os.path.exists(filepath):
                update_status(clean, 'sessions', True)
            else:
                update_status(clean, 'sessions', False)
        except Exception as e:
            print(f"[X15] Error: {e}", flush=True)
            traceback.print_exc()
            update_status(clean, 'sessions', False)

    # ---------- X17 Sentiment ----------
    def run_x17_sentiment(self, symbol):
        if not X17_AVAILABLE:
            print("[X17] Module not available, skipping", flush=True)
            return
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        filepath = os.path.join(self.data_dir, "symbols", f"{clean.lower()}_sentiment.tsv")
        if os.path.exists(filepath):
            os.remove(filepath)
            print(f"[X17] Deleted old file: {filepath}", flush=True)
        print("[X17] Starting sentiment data fetch", flush=True)
        try:
            sent = SentimentData()
            sent.collect_and_save(clean)
            if os.path.exists(filepath):
                update_status(clean, 'sentiment', True)
            else:
                update_status(clean, 'sentiment', False)
        except Exception as e:
            print(f"[X17] Error: {e}", flush=True)
            traceback.print_exc()
            update_status(clean, 'sentiment', False)

    # ---------- X19 Volume Profile ----------
    def run_x19_volprofile(self, symbol):
        if not X19_AVAILABLE:
            print("[X19] Module not available, skipping", flush=True)
            return
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        filepath = os.path.join(self.data_dir, "symbols", f"{clean.lower()}_volProfile.tsv")
        if os.path.exists(filepath):
            os.remove(filepath)
            print(f"[X19] Deleted old file: {filepath}", flush=True)
        print("[X19] Starting volume profile analysis", flush=True)
        try:
            vp = VolumeProfile()
            vp.collect_and_save(clean)
            if os.path.exists(filepath):
                update_status(clean, 'volProfile', True)
            else:
                update_status(clean, 'volProfile', False)
        except Exception as e:
            print(f"[X19] Error: {e}", flush=True)
            traceback.print_exc()
            update_status(clean, 'volProfile', False)

    # ---------- X21 Market Structure ----------
    def run_x21_mstructure(self, symbol):
        if not X21_AVAILABLE:
            print("[X21] Module not available, skipping", flush=True)
            return
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        filepath = os.path.join(self.data_dir, "symbols", f"{clean.lower()}_mstructure.tsv")
        if os.path.exists(filepath):
            os.remove(filepath)
            print(f"[X21] Deleted old file: {filepath}", flush=True)
        print("[X21] Starting market structure analysis", flush=True)
        try:
            ms = MarketStructure()
            ms.collect_and_save(clean)
            if os.path.exists(filepath):
                update_status(clean, 'mstructure', True)
            else:
                update_status(clean, 'mstructure', False)
        except Exception as e:
            print(f"[X21] Error: {e}", flush=True)
            traceback.print_exc()
            update_status(clean, 'mstructure', False)

    # ---------- X23 On‑chain ----------
    def run_x23_onchain(self, symbol):
        if not X23_AVAILABLE:
            print("[X23] Module not available, skipping", flush=True)
            return
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        filepath = os.path.join(self.data_dir, "symbols", f"{clean.lower()}_onchain.tsv")
        if os.path.exists(filepath):
            os.remove(filepath)
            print(f"[X23] Deleted old file: {filepath}", flush=True)
        print("[X23] Starting on‑chain data fetch", flush=True)
        try:
            onchain_collect(clean)
            if os.path.exists(filepath):
                update_status(clean, 'onchain', True)
            else:
                update_status(clean, 'onchain', False)
        except Exception as e:
            print(f"[X23] Error: {e}", flush=True)
            traceback.print_exc()
            update_status(clean, 'onchain', False)

    # ---------- X25 Tick ----------
    def run_x25_tick(self, symbol):
        if not X25_AVAILABLE:
            print("[X25] Module not available, skipping", flush=True)
            return
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        filepath = os.path.join(self.data_dir, "symbols", f"{clean.lower()}_tick.tsv")
        if os.path.exists(filepath):
            os.remove(filepath)
            print(f"[X25] Deleted old file: {filepath}", flush=True)
        print("[X25] Starting tick & volume profile fetch", flush=True)
        try:
            tick_collect(clean)
            if os.path.exists(filepath):
                update_status(clean, 'tick', True)
            else:
                update_status(clean, 'tick', False)
        except Exception as e:
            print(f"[X25] Error: {e}", flush=True)
            traceback.print_exc()
            update_status(clean, 'tick', False)

    # ---------- fill_single (parallel execution) ----------
    def fill_single(self, symbol, minutes=120):
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        print(f"[Fill] Starting fill for {clean}", flush=True)
        if is_crypto_symbol(clean):
            # 1. Candles using X01 (direct REST + WebSocket update)
            print("[Fill] Step 1: Candles (via X01)", flush=True)
            try:
                from Groups.group_x.X01_klines_rest import fetch_and_update_ws
                success = fetch_and_update_ws(clean, self.binance_ws)
                if success:
                    update_status(clean, 'candles', True)
                    print(f"[Fill] Added 1m candles for {clean} to WebSocket", flush=True)
                else:
                    update_status(clean, 'candles', False)
            except Exception as e:
                print(f"[Fill] X01 error: {e}", flush=True)
                traceback.print_exc()
                update_status(clean, 'candles', False)

            # 2. Macro data (X11) – background
            print("[Fill] Step 2: Macro data (X11) in background", flush=True)
            t_macro = threading.Thread(target=self.fetch_macro_data, args=(clean,), daemon=True)
            t_macro.start()

            # 3. Depth (fast, sequential)
            print("[Fill] Step 3: Depth", flush=True)
            try:
                from Groups.group_x.X06_depth_rest import fetch_depth_snapshot
                snapshot = fetch_depth_snapshot(clean, limit=500)
                if snapshot:
                    symbols_dir = os.path.join(self.data_dir, "symbols")
                    os.makedirs(symbols_dir, exist_ok=True)
                    filepath = os.path.join(symbols_dir, f"{clean.lower()}_depth.tsv")
                    with open(filepath, 'w') as f:
                        f.write("type\tprice\tquantity\n")
                        for p, q in snapshot['bids'][:200]:
                            f.write(f"bid\t{p}\t{q}\n")
                        for p, q in snapshot['asks'][:200]:
                            f.write(f"ask\t{p}\t{q}\n")
                    if os.path.exists(filepath):
                        update_status(clean, 'depth', True)
                        print(f"[Depth] Saved snapshot for {clean}", flush=True)
                    else:
                        update_status(clean, 'depth', False)
                else:
                    update_status(clean, 'depth', False)
            except Exception as e:
                print(f"[Depth] Error: {e}", flush=True)
                traceback.print_exc()
                update_status(clean, 'depth', False)

            # 4. Start all remaining background threads (CVD, Derivative, Correlation, and optional X modules)
            print("[Fill] Step 4: Starting all background tasks", flush=True)
            
            def run_cvd():
                try:
                    self.compute_and_save_cvd(clean)
                except Exception as e:
                    print(f"[CVD] Thread error: {e}", flush=True)
                    traceback.print_exc()
                    update_status(clean, 'cvd', False)
            
            def run_derivative():
                try:
                    self.activate_derivative_for_symbol(clean)
                except Exception as e:
                    print(f"[Derivative] Thread error: {e}", flush=True)
                    traceback.print_exc()
                    update_status(clean, 'derivative', False)
            
            def run_correlation():
                try:
                    self.activate_correlation_for_symbol(clean)
                except Exception as e:
                    print(f"[Correlation] Thread error: {e}", flush=True)
                    traceback.print_exc()
                    update_status(clean, 'correlation', False)
            
            t_cvd = threading.Thread(target=run_cvd, daemon=True)
            t_deriv = threading.Thread(target=run_derivative, daemon=True)
            t_corr = threading.Thread(target=run_correlation, daemon=True)
            t_x13 = threading.Thread(target=self.run_x13_liquidations, args=(clean,), daemon=True)
            t_x15 = threading.Thread(target=self.run_x15_session, args=(clean,), daemon=True)
            t_x17 = threading.Thread(target=self.run_x17_sentiment, args=(clean,), daemon=True)
            t_x19 = threading.Thread(target=self.run_x19_volprofile, args=(clean,), daemon=True)
            t_x21 = threading.Thread(target=self.run_x21_mstructure, args=(clean,), daemon=True)
            t_x23 = threading.Thread(target=self.run_x23_onchain, args=(clean,), daemon=True)
            t_x25 = threading.Thread(target=self.run_x25_tick, args=(clean,), daemon=True)
            
            t_cvd.start()
            t_deriv.start()
            t_corr.start()
            t_x13.start()
            t_x15.start()
            t_x17.start()
            t_x19.start()
            t_x21.start()
            t_x23.start()
            t_x25.start()
            
            print("[Fill] All background tasks started", flush=True)
        else:
            # Non-crypto – only candles
            print(f"[Fill] Non-crypto {clean}, only candles", flush=True)
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
                print(f"[Fill] Non-crypto error: {e}", flush=True)
                update_status(clean, 'candles', False)
            update_status(clean, 'cvd', True)
            update_status(clean, 'depth', True)
            update_status(clean, 'derivative', True)
            update_status(clean, 'correlation', True)
        
        print(f"[Fill] fill_single main thread finished (background tasks running)", flush=True)

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
    def fill_single(self, symbol, minutes=60):
        print(f"[IQOptionHandler] Manual fill not implemented for {symbol}", flush=True)
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        update_status(clean, 'candles', True)
        update_status(clean, 'cvd', True)
        update_status(clean, 'depth', True)
        update_status(clean, 'derivative', True)
        update_status(clean, 'correlation', True)
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
    def fill_single(self, symbol, minutes=60):
        print(f"[QuotexHandler] Manual fill not implemented for {symbol}", flush=True)
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        update_status(clean, 'candles', True)
        update_status(clean, 'cvd', True)
        update_status(clean, 'depth', True)
        update_status(clean, 'derivative', True)
        update_status(clean, 'correlation', True)
    def get_candle_count(self, symbol):
        return 0

# ── SysData main class ──────────────────────────────────────────────────────
_current_scores = {}
_score_lock = threading.Lock()

class SysData:
    def __init__(self):
        print("[SysData] Init...", flush=True)
        self.engine = get_engine()
        self._platform = None
        self._current_platform_name = None
        self._interval = "5m"
        self._market = ""
        self._pairs = []
        print("[SysData] Ready", flush=True)

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

    def _feel(self, symbol: str, interval: str) -> dict:
        if not self._platform:
            return {"steps": 0, "pct": 0, "color": "red"}
        candles = self._platform.get_candles(symbol, "1m", 20)
        now = int(time.time() * 1000)
        cutoff = now - (20 * 60 * 1000)
        recent_closed = {c['timestamp'] for c in candles if c['timestamp'] > cutoff}
        live = self._platform.get_live_candle(symbol)
        live_ts = None
        if live and live.get('timestamp') and live['timestamp'] > cutoff:
            live_ts = live['timestamp']
        minutes_covered = set(recent_closed)
        if live_ts:
            minutes_covered.add(live_ts)
        steps = len(minutes_covered)
        steps = min(steps, 20)
        pct = int(steps / 20 * 100)
        if pct >= 40:
            color = "green"
        elif pct >= 5:
            color = "orange"
        else:
            color = "red"
        return {"steps": steps, "pct": pct, "color": color}

    def get_feel(self, symbol: str) -> dict:
        if not self._platform:
            return {"steps": 0, "pct": 0, "color": "red"}
        return self._feel(symbol, self._interval)

    def _get_rows(self, symbol: str, interval: str, limit=100):
        if not self._platform:
            return []
        return self._platform.get_candles(symbol, interval, limit)

    def scan(self, market: str, pairs: list, src: str = "real",
             interval: str = "5m", iq_email="", iq_pwd="") -> list:
        global _current_scores
        print(f"[SysData] Scanning {len(pairs)} pairs | market={market} tf={interval} src={src}", flush=True)
        self._interval = interval
        self._market = market
        self._pairs = pairs
        platform = self._ensure_platform(src, pairs, iq_email, iq_pwd)
        results = []
        for pair in pairs:
            try:
                sym = pair.upper().replace("/", "").replace(" (OTC)", "")
                rows = self._get_rows(sym, interval, 100)
                if rows and len(rows) >= 10:
                    result = self.engine.get_z_score(sym, market, interval, rows)
                else:
                    result = {"score": "NA", "signal": "WAIT", "trend": "FLAT",
                              "sr_position": "—", "reason": "No data",
                              "regime": "—", "quality": "LOW"}
                result["pair"] = pair
                result["feel"] = self._feel(sym, interval)
                results.append(result)
            except Exception as e:
                results.append({
                    "pair": pair, "score": "NA", "signal": "WAIT", "trend": "FLAT",
                    "sr_position": "—", "reason": f"Error: {str(e)[:30]}",
                    "feel": {"steps":0,"pct":0,"color":"red"}
                })
        results.sort(key=lambda x: x.get("score", 0) if isinstance(x.get("score"), (int, float)) else -1, reverse=True)
        with _score_lock:
            _current_scores = {}
            for r in results:
                pair = r["pair"]
                _current_scores[pair] = {
                    "score": r.get("score", 0),
                    "signal": r.get("signal", "WAIT"),
                    "trend": r.get("trend", "FLAT"),
                    "reason": r.get("reason", ""),
                    "sr_position": r.get("sr_position", "—"),
                    "quality": r.get("quality", "LOW"),
                    "feel_pct": r.get("feel", {}).get("pct", 0)
                }
        print(f"[SysData] Scan complete, {len(results)} results stored", flush=True)
        return results

    def refresh_scores(self) -> dict:
        global _current_scores
        if not self._platform or not self._market or not self._pairs:
            return {}
        results = []
        for pair in self._pairs:
            try:
                sym = pair.upper().replace("/", "").replace(" (OTC)", "")
                rows = self._get_rows(sym, self._interval, 100)
                if rows and len(rows) >= 10:
                    result = self.engine.get_z_score(sym, self._market, self._interval, rows)
                else:
                    result = {"score": "NA", "signal": "WAIT", "trend": "FLAT",
                              "sr_position": "—", "reason": "No data",
                              "regime": "—", "quality": "LOW"}
                result["pair"] = pair
                result["feel"] = self._feel(sym, self._interval)
                results.append(result)
            except Exception as e:
                results.append({
                    "pair": pair, "score": "NA", "signal": "WAIT", "trend": "FLAT",
                    "sr_position": "—", "reason": f"Error: {str(e)[:30]}",
                    "feel": {"steps":0,"pct":0,"color":"red"}
                })
        with _score_lock:
            _current_scores = {}
            for r in results:
                pair = r["pair"]
                _current_scores[pair] = {
                    "score": r.get("score", 0),
                    "signal": r.get("signal", "WAIT"),
                    "trend": r.get("trend", "FLAT"),
                    "reason": r.get("reason", ""),
                    "sr_position": r.get("sr_position", "—"),
                    "quality": r.get("quality", "LOW"),
                    "feel_pct": r.get("feel", {}).get("pct", 0)
                }
        return _current_scores

    def go(self, symbol: str, market: str, interval: str = "") -> dict:
        if not interval:
            interval = self._interval
        try:
            rows = self._get_rows(symbol, interval, 100)
            if not rows or len(rows) < 10:
                return {"error": "Not enough data for analysis"}
            z_result = self.engine.get_z_score(symbol, market, interval, rows)
            a_result = self.engine.get_a_score(symbol, market, interval, rows, z_result)
            a_signal = a_result.get("a_signal", "WAIT")
            a_result["requires_deep"] = False   # Group D removed
            a_result["forecast"] = a_result.get("forecast", {})
            a_result["z_score"] = z_result.get("score", "NA")
            a_result["regime"] = z_result.get("regime", "—")
            a_result["quality"] = a_result.get("quality", "MED")
            return a_result
        except Exception as e:
            return {"error": f"GO error: {str(e)}"}

    # deep() method removed – Group D no longer exists

    def get_current_scores(self) -> dict:
        with _score_lock:
            return _current_scores

    def get_call_stats(self) -> dict:
        from data_sources.binance_rest import BinanceREST
        from data_sources.finnhub_rest import FinnhubREST
        return {
            "Binance": BinanceREST.get_total_calls(),
            "Finnhub": FinnhubREST.get_total_calls(),
        }

    def fill_single(self, symbol: str):
        if not self._platform:
            raise RuntimeError("No platform active")
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        with _status_lock:
            _fill_status[clean] = {'cvd': False, 'depth': False, 'derivative': False, 'correlation': False, 'candles': False}
        thread = threading.Thread(target=self._platform.fill_single, args=(symbol, 120), daemon=True)
        thread.start()
        print(f"[SysData] fill_single thread started for {clean}", flush=True)

    def get_fill_status(self, symbol: str) -> dict:
        clean = symbol.upper().replace("/", "").replace(" (OTC)", "")
        return get_fill_status(clean)

    def get_candle_count(self, symbol: str) -> int:
        if not self._platform:
            return 0
        return self._platform.get_candle_count(symbol)

print("✅ [SysData] Loaded OK", flush=True)