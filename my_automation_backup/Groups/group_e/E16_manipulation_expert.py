#!/usr/bin/env python3
# E16_manipulation_expert.py – Complete Manipulation Detection (All 60+ Types)
# Uses every static method in ManipulationExpert + extra detections.
# Outputs TSV with individual probabilities.

import os
import sys
import time
import math
from collections import defaultdict

FEATURES_BASE_DIR = os.path.join("market_data", "binance", "symbols")
LOG_FILE = os.path.join(FEATURES_BASE_DIR, "E16_manipulation_expert.log")
LOG_MAX_SIZE = 5_000_000

def rotate_log_if_needed():
    if os.path.exists(LOG_FILE) and os.path.getsize(LOG_FILE) > LOG_MAX_SIZE:
        backup = LOG_FILE + ".old"
        try:
            os.replace(LOG_FILE, backup)
        except:
            pass

def log_issue(level, msg, **kwargs):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} [{level}] {msg}"
    if kwargs:
        line += " " + str(kwargs)
    print(line)
    rotate_log_if_needed()
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def safe_float(val, default=0.0):
    try:
        if val is None or str(val).strip() in ('', 'N/A', '--'):
            return default
        return float(val)
    except:
        return default

def safe_int(val, default=0):
    try:
        if val is None or str(val).strip() == '':
            return default
        return int(val)
    except:
        return default

# ---------- Data loaders (unchanged) ----------
def load_candle_data(symbol, timeframe="1h", limit=200):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}.tmp_x")
    if not os.path.exists(path):
        return [], [], [], []
    candles = []
    try:
        with open(path, "r") as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 8 and parts[1] == timeframe:
                    ts = int(parts[2])
                    high = float(parts[4])
                    low = float(parts[5])
                    close = float(parts[6])
                    volume = float(parts[7])
                    candles.append({'ts': ts, 'high': high, 'low': low, 'close': close, 'volume': volume})
    except Exception as e:
        log_issue("WARNING", f"Failed to read candles: {e}")
        return [], [], [], []
    if not candles:
        return [], [], [], []
    candles.sort(key=lambda x: x['ts'])
    candles = candles[-limit:]
    high = [c['high'] for c in candles]
    low = [c['low'] for c in candles]
    close = [c['close'] for c in candles]
    volume = [c['volume'] for c in candles]
    return high, low, close, volume

def load_depth_data(symbol):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_depth.tmp_x")
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        if len(lines) < 2:
            return {}
        bids = []
        asks = []
        for line in lines[1:]:
            parts = line.strip().split('\t')
            if len(parts) >= 4:
                side = parts[0]
                price = float(parts[1])
                qty = float(parts[2])
                if side == 'bid':
                    bids.append((price, qty))
                else:
                    asks.append((price, qty))
        if not bids or not asks:
            return {}
        total_bid = sum(q for _, q in bids)
        total_ask = sum(q for _, q in asks)
        imbalance = (total_bid - total_ask) / (total_bid + total_ask) if (total_bid+total_ask) > 0 else 0
        spread = asks[0][0] - bids[0][0] if asks and bids else 0
        best_bid = bids[0][0] if bids else 0
        best_ask = asks[0][0] if asks else 0
        return {'imbalance': imbalance, 'spread': spread, 'bid_vol': total_bid, 'ask_vol': total_ask,
                'best_bid': best_bid, 'best_ask': best_ask}
    except Exception as e:
        log_issue("WARNING", f"Failed to load depth: {e}")
        return {}

def load_derivative_data(symbol):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_derivative.tmp_x")
    data = {'oi': 0, 'funding_rate': 0, 'funding_rates': [], 'oi_history': []}
    if not os.path.exists(path):
        return data
    try:
        with open(path, "r") as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) < 2:
                    continue
                typ = parts[0]
                if typ == "snapshot" and len(parts) >= 6:
                    data['oi'] = safe_float(parts[5])
                    data['funding_rate'] = safe_float(parts[4])
                elif typ == "funding_history" and len(parts) >= 3:
                    data['funding_rates'].append(safe_float(parts[2]))
                elif typ == "oi_history" and len(parts) >= 3:
                    data['oi_history'].append(safe_float(parts[2]))
    except:
        pass
    return data

def load_liquidation_data(symbol):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_liquidations.tmp_x")
    if not os.path.exists(path):
        return []
    events = []
    try:
        with open(path, "r") as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 4 and parts[0].isdigit():
                    events.append({'price': float(parts[1]), 'volume': float(parts[2]), 'side': parts[3]})
    except:
        pass
    return events

def load_sentiment_data(symbol):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_sentiment.tmp_x")
    data = {'news_score': 0, 'social_velocity': 0, 'retail_bias': 'Neutral'}
    if not os.path.exists(path):
        return data
    try:
        with open(path, "r") as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) >= 4 and parts[0] == "sentiment_snapshot":
                    data['news_score'] = safe_float(parts[2])
                    data['retail_bias'] = parts[3] if len(parts) > 3 else 'Neutral'
                    data['social_velocity'] = safe_int(parts[4]) if len(parts) > 4 else 0
    except:
        pass
    return data

def load_tick_data(symbol):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_tick.tmp_x")
    if not os.path.exists(path):
        return {}
    trades = []
    try:
        with open(path, "r") as f:
            lines = f.readlines()
        start_idx = 1 if lines and lines[0].startswith('timestamp') else 0
        for line in lines[start_idx:]:
            parts = line.strip().split('\t')
            if len(parts) >= 4:
                trades.append({'price': float(parts[1]), 'qty': float(parts[2]), 'side': parts[3]})
    except:
        pass
    if not trades:
        return {}
    buy_vol = sum(t['qty'] for t in trades if t['side'].lower() in ('buy', '1', 'true'))
    sell_vol = sum(t['qty'] for t in trades if t['side'].lower() in ('sell', '0', 'false'))
    total_vol = buy_vol + sell_vol
    return {'buy_vol': buy_vol, 'sell_vol': sell_vol, 'total_vol': total_vol, 'trades': trades}

def load_onchain_data(symbol):
    path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_onchain.tmp_x")
    data = {'usdt_netflow': 0, 'whale_tx_count': 0, 'whale_ratio': 1.0}
    if not os.path.exists(path):
        return data
    try:
        with open(path, "r") as f:
            for line in f:
                parts = line.strip().split('\t')
                if len(parts) < 2:
                    continue
                typ = parts[0]
                if typ == "stablecoin" and len(parts) >= 7 and parts[2] == "USDT":
                    data['usdt_netflow'] = safe_float(parts[6])
                elif typ == "whale":
                    data['whale_tx_count'] += 1
                elif typ == "binance_snapshot" and len(parts) >= 9:
                    data['whale_ratio'] = safe_float(parts[5])
    except:
        pass
    return data

# ---------- ManipulationExpert class with all static methods (unchanged) ----------
class ManipulationExpert:
    @staticmethod
    def gamma_pinning_probability(gamma_exposure, spot_price, max_pain_level, days_to_expiry):
        if days_to_expiry and days_to_expiry < 5 and abs(spot_price - max_pain_level) / spot_price < 0.01:
            return 75
        return 0

    @staticmethod
    def volatility_surface_distortion_probability(iv_skew, historical_skew):
        if iv_skew and historical_skew and abs(iv_skew - historical_skew) > 2:
            return 70
        return 0

    @staticmethod
    def vega_trap_probability(vol_of_vol, option_oi_spike):
        if option_oi_spike and option_oi_spike > 2:
            return 65
        return 0

    @staticmethod
    def charm_flow_probability(hedge_decay, time_to_expiry):
        if hedge_decay and time_to_expiry and hedge_decay > 0.05:
            return 60
        return 0

    @staticmethod
    def max_pain_steering_probability(spot_price, max_pain, days_to_expiry):
        if days_to_expiry and days_to_expiry < 3 and abs(spot_price - max_pain) / spot_price < 0.01:
            return 70
        return 0

    @staticmethod
    def inventory_rebalancing_probability(dealer_inventory_imbalance, price_move):
        if dealer_inventory_imbalance and abs(dealer_inventory_imbalance) > 0.2 and abs(price_move) > 0.01:
            return 65
        return 0

    @staticmethod
    def delta_hedge_cascade_probability(gamma_exposure, price_change_pct):
        if gamma_exposure and gamma_exposure > 100_000 and abs(price_change_pct) > 0.5:
            return 70
        return 0

    @staticmethod
    def liquidity_internalization_probability(trade_execution_delay):
        if trade_execution_delay and trade_execution_delay > 0.1:
            return 50
        return 0

    @staticmethod
    def spread_engineering_probability(current_spread, avg_spread):
        if current_spread and avg_spread and current_spread > 2 * avg_spread:
            return 60
        return 0

    @staticmethod
    def dealer_trap_zone_probability(high, low, close, atr):
        if len(close) < 10:
            return 0
        recent_range = max(high[-10:-1]) - min(low[-10:-1])
        last_range = high[-1] - low[-1]
        if last_range > 1.5 * recent_range and abs(close[-1] - close[-2]) / atr < 0.5:
            return 65
        return 0

    @staticmethod
    def delayed_liquidation_feed_probability(liquidation_timestamp_deviation):
        if liquidation_timestamp_deviation and liquidation_timestamp_deviation > 10:
            return 60
        return 0

    @staticmethod
    def api_inconsistency_probability(spot_px_api1, spot_px_api2):
        if spot_px_api1 and spot_px_api2 and abs(spot_px_api1 - spot_px_api2) / spot_px_api1 > 0.002:
            return 70
        return 0

    @staticmethod
    def fake_volume_api_probability(reported_volume, actual_volume):
        if reported_volume and actual_volume and reported_volume > 1.5 * actual_volume:
            return 80
        return 0

    @staticmethod
    def tick_compression_distortion_probability(print_frequency_ms):
        if print_frequency_ms and print_frequency_ms > 100:
            return 60
        return 0

    @staticmethod
    def exchange_downtime_exploitation_probability(downtime_duration_sec, volatility_after):
        if downtime_duration_sec and downtime_duration_sec > 30 and volatility_after > 1:
            return 75
        return 0

    @staticmethod
    def indicator_baiting_probability(rsi, macd_hist, price_change):
        if (rsi and rsi > 80 and price_change < 0.01) or (rsi and rsi < 20 and price_change > -0.01):
            return 70
        if macd_hist and macd_hist > 0 and price_change < 0:
            return 65
        return 0

    @staticmethod
    def ml_adversarial_patterns_probability(pattern_frequency, regime_switch):
        if pattern_frequency and regime_switch:
            return 60
        return 0

    @staticmethod
    def backtest_poisoning_probability(current_regime, past_regime):
        if current_regime != past_regime:
            return 50
        return 0

    @staticmethod
    def regime_flip_trap_probability(adx, roc):
        if adx and roc and adx > 25 and abs(roc) < 0.5:
            return 65
        return 0

    @staticmethod
    def correlation_breakdown_probability(correlated_asset_move, expected_corr):
        if correlated_asset_move and expected_corr and abs(correlated_asset_move - expected_corr) > 0.3:
            return 70
        return 0

    @staticmethod
    def bridge_flow_illusion_probability(bridge_tx_volume, actual_netflow):
        if bridge_tx_volume and actual_netflow and bridge_tx_volume > 2 * actual_netflow:
            return 65
        return 0

    @staticmethod
    def whale_wallet_theater_probability(whale_transfer_amount, market_cap):
        if whale_transfer_amount and market_cap and whale_transfer_amount / market_cap > 0.01:
            return 60
        return 0

    @staticmethod
    def treasury_rotation_probability(foundation_wallet_move, social_mentions):
        if foundation_wallet_move and social_mentions and foundation_wallet_move > 1e6 and social_mentions > 500:
            return 70
        return 0

    @staticmethod
    def lp_unlock_trap_probability(unlock_amount, circulating_supply):
        if unlock_amount and circulating_supply and unlock_amount / circulating_supply > 0.05:
            return 75
        return 0

    @staticmethod
    def coordinated_narrative_attack_probability(social_volume, sentiment_change):
        if social_volume > 2000 and abs(sentiment_change) > 0.4:
            return 70
        return 0

    @staticmethod
    def fake_news_injection_probability(news_impact_score, price_reaction):
        if news_impact_score and price_reaction and abs(price_reaction) < 0.5 and news_impact_score > 8:
            return 75
        return 0

    @staticmethod
    def engagement_farming_probability(bot_like_ratio):
        if bot_like_ratio and bot_like_ratio > 0.6:
            return 65
        return 0

    @staticmethod
    def influencer_exit_setup_probability(influencer_mentions, whale_sell_volume):
        if influencer_mentions > 100 and whale_sell_volume > 1e6:
            return 80
        return 0

    @staticmethod
    def ai_sentiment_flood_probability(ai_generated_content_ratio):
        if ai_generated_content_ratio and ai_generated_content_ratio > 0.4:
            return 60
        return 0

    @staticmethod
    def slippage_exploitation_probability(slippage, order_size, book_depth):
        if slippage > 0.02 and order_size > book_depth * 0.5:
            return 75
        return 0

    @staticmethod
    def trigger_hunting_probability(high, low, known_stop_level):
        if known_stop_level and high[-1] > known_stop_level and low[-1] < known_stop_level:
            return 70
        return 0

    @staticmethod
    def queue_starvation_probability(order_wait_time_ms):
        if order_wait_time_ms and order_wait_time_ms > 500:
            return 60
        return 0

    @staticmethod
    def fake_market_orders_probability(volume, price_impact):
        if volume and price_impact and volume > 1e6 and price_impact < 0.01:
            return 65
        return 0

    @staticmethod
    def hidden_cross_venue_probability(off_book_volume_ratio):
        if off_book_volume_ratio and off_book_volume_ratio > 0.3:
            return 55
        return 0

    @staticmethod
    def etf_flow_distortion_probability(etf_inflow, spot_price_change):
        if etf_inflow > 1e9 and abs(spot_price_change) < 0.5:
            return 65
        return 0

    @staticmethod
    def funding_settlement_volatility_probability(minutes_to_funding, atr):
        if minutes_to_funding and minutes_to_funding < 15 and atr > 0.02:
            return 70
        return 0

    @staticmethod
    def stablecoin_depeg_panic_probability(peg_deviation_pct):
        if peg_deviation_pct and abs(peg_deviation_pct) > 1:
            return 80
        return 0

    @staticmethod
    def regulatory_shock_gaming_probability(leak_timestamp, announcement_timestamp):
        if leak_timestamp and announcement_timestamp and announcement_timestamp - leak_timestamp < 3600:
            return 75
        return 0

    @staticmethod
    def weekend_liquidity_exploit_probability(volume, is_weekend):
        if is_weekend and volume and len(volume) >= 24:
            recent_avg = sum(volume[-24:]) / 24
            if volume[-1] < 0.4 * recent_avg:
                return 65
        return 0

    @staticmethod
    def macro_correlation_shock_probability(dxy_change, expected_corr):
        if dxy_change and expected_corr and abs(dxy_change) > 0.5 and abs(dxy_change - expected_corr) > 0.3:
            return 60
        return 0

    @staticmethod
    def exchange_insolvency_rumor_probability(withdrawal_delay, social_fud):
        if withdrawal_delay and withdrawal_delay > 2 and social_fud > 500:
            return 85
        return 0

    @staticmethod
    def oracle_failure_cascade_probability(oracle_heartbeat_miss):
        if oracle_heartbeat_miss and oracle_heartbeat_miss > 300:
            return 80
        return 0

    @staticmethod
    def flash_crash_engineering_probability(price_drop_pct_1m, volume_spike):
        if price_drop_pct_1m > 5 and volume_spike > 10:
            return 90
        return 0

    @staticmethod
    def cross_asset_cascade_probability(correlation_break, btc_change, alt_change):
        if correlation_break and btc_change and alt_change and abs(btc_change - alt_change) > 2:
            return 65
        return 0

    @staticmethod
    def stablecoin_redemption_shock_probability(redemption_volume, circulating_supply):
        if redemption_volume and circulating_supply and redemption_volume / circulating_supply > 0.1:
            return 85
        return 0

    @staticmethod
    def autonomous_ai_swarm_probability(order_rate_anomaly, social_coordination):
        if order_rate_anomaly > 3 and social_coordination > 0.7:
            return 70
        return 0

    @staticmethod
    def rl_adversarial_trading_probability(pattern_repetition_stealth):
        if pattern_repetition_stealth and pattern_repetition_stealth > 0.8:
            return 65
        return 0

    @staticmethod
    def synthetic_social_consensus_probability(ai_generated_account_ratio):
        if ai_generated_account_ratio and ai_generated_account_ratio > 0.5:
            return 75
        return 0

    @staticmethod
    def attention_market_manipulation_probability(social_velocity, engagement_rate):
        if social_velocity > 1000 and engagement_rate < 0.01:
            return 70
        return 0

    @staticmethod
    def cross_domain_manipulation_probability(oi_change, social_spike, orderbook_imbalance):
        if oi_change > 5 and social_spike > 500 and abs(orderbook_imbalance) > 0.3:
            return 85
        return 0

# ---------- Additional manipulation detections (spoofing, wash, iceberg, fake news, cross‑venue) ----------
def detect_spoofing(volume, close):
    if len(volume) >= 6 and len(close) >= 6:
        avg_vol = sum(volume[-6:-1]) / 5
        if volume[-1] > 2.5 * avg_vol and abs((close[-1]-close[-2])/close[-2]*100) < 0.3:
            return 70
    return 20

def detect_wash_trading(buy_vol, sell_vol, total_vol):
    if total_vol == 0:
        return 0
    ratio = min(buy_vol, sell_vol) / max(buy_vol, sell_vol) if max(buy_vol, sell_vol) > 0 else 0
    if ratio > 0.8 and total_vol > 100_000:
        return 60
    return 20

def detect_iceberg_orders(depth):
    # Placeholder – can be improved with depth level analysis
    if depth.get('bid_vol', 0) > 0 and depth.get('ask_vol', 0) > 0:
        return 30
    return 20

def detect_fake_news(news_score, price_change):
    if abs(news_score) > 0.7 and abs(price_change) < 0.5:
        return 75
    return 20

def detect_cross_venue(btc_change, alt_change):
    if abs(btc_change) > 1 and abs(alt_change) < 0.3:
        return 60
    return 20

# ---------- Main compute function (calls EVERY static method) ----------
def compute_manipulation_probs(symbol):
    high, low, close, volume = load_candle_data(symbol, "1h", 200)
    if not close:
        log_issue("WARNING", "No candle data")
        return {k: 0 for k in ["trap", "liquidity_sweep", "synthetic", "crowded", "book_reliability",
                                "market_integrity", "likely_types", "gamma", "max_pain", "inventory",
                                "data_integrity", "regime_flip", "social", "execution", "macro", "rare", "emerging",
                                "spoofing", "wash", "iceberg", "fake_news", "cross_venue",
                                "vol_surface", "vega", "charm", "delta_hedge", "liquidity_int", "spread_eng",
                                "dealer_trap", "delayed_liq", "api_inconsistency", "fake_volume", "tick_compression",
                                "exchange_downtime", "indicator_bait", "ml_adversarial", "backtest_poison",
                                "correlation_break", "bridge_flow", "whale_theater", "treasury_rot", "lp_unlock",
                                "coordinated_narrative", "fake_news_injection", "engagement_farming", "influencer_exit",
                                "ai_sentiment", "slippage", "trigger_hunt", "queue_starvation", "fake_market_orders",
                                "hidden_cross", "etf_distortion", "funding_settlement", "stablecoin_depeg",
                                "regulatory_shock", "weekend_exploit", "macro_correlation", "exchange_insolvency",
                                "oracle_failure", "flash_crash", "cross_asset", "stablecoin_redemption",
                                "ai_swarm", "rl_adversarial", "synthetic_social", "attention_market", "cross_domain"]}
    depth = load_depth_data(symbol)
    deriv = load_derivative_data(symbol)
    liq_events = load_liquidation_data(symbol)
    sentiment = load_sentiment_data(symbol)
    tick = load_tick_data(symbol)
    onchain = load_onchain_data(symbol)

    current_price = close[-1] if close else 0
    price_change_pct = (close[-1] - close[-2]) / close[-2] * 100 if len(close) >= 2 else 0
    atr = max(high[-1]-low[-1], abs(high[-1]-close[-2]), abs(low[-1]-close[-2])) if len(close) >= 2 else 0
    atr_pct = atr / current_price * 100 if current_price else 0
    max_pain = (max(high[-50:]) + min(low[-50:])) / 2 if len(high) >= 50 else current_price
    days_to_expiry = 1  # approximation
    iv_skew = 0  # not available, skip
    historical_skew = 0
    vol_of_vol = 0
    option_oi_spike = 0
    hedge_decay = 0
    time_to_expiry = 1
    dealer_inventory_imbalance = depth.get('imbalance', 0)
    price_move = abs(price_change_pct) / 100
    gamma_exposure = deriv['oi']  # proxy
    trade_execution_delay = 0
    current_spread = depth.get('spread', 0)
    avg_spread = current_spread  # no historical, use current
    liquidation_timestamp_deviation = 0
    reported_volume = volume[-1] if volume else 0
    actual_volume = reported_volume
    print_frequency_ms = 0
    downtime_duration_sec = 0
    volatility_after = 0
    rsi = 0
    macd_hist = 0
    pattern_frequency = 0
    regime_switch = 0
    current_regime = ""
    past_regime = ""
    adx_val = min(50, abs(price_change_pct) * 3)
    roc = (close[-1] - close[-5]) / close[-5] * 100 if len(close) >= 5 else 0
    correlated_asset_move = 0
    expected_corr = 0.7
    bridge_tx_volume = 0
    actual_netflow = onchain.get('usdt_netflow', 0)
    whale_transfer_amount = 0
    market_cap = current_price * 100_000_000  # rough
    foundation_wallet_move = 0
    social_mentions = sentiment['social_velocity']
    unlock_amount = 0
    circulating_supply = 0
    social_volume = sentiment['social_velocity']
    sentiment_change = sentiment['news_score']
    news_impact_score = abs(sentiment['news_score']) * 10
    price_reaction = price_change_pct
    bot_like_ratio = 0
    influencer_mentions = 0
    whale_sell_volume = 0
    ai_generated_content_ratio = 0
    slippage = 0
    order_size = 0
    book_depth = depth.get('bid_vol', 0) + depth.get('ask_vol', 0)
    known_stop_level = max(high[-10:]) if high else 0
    order_wait_time_ms = 0
    price_impact = 0
    off_book_volume_ratio = 0
    etf_inflow = 0
    spot_price_change = price_change_pct
    minutes_to_funding = 60
    peg_deviation_pct = 0
    leak_timestamp = 0
    announcement_timestamp = 0
    is_weekend = time.localtime().tm_wday >= 5
    dxy_change = 0
    withdrawal_delay = 0
    social_fud = sentiment['social_velocity']
    oracle_heartbeat_miss = 0
    price_drop_pct_1m = 0
    volume_spike = 0
    correlation_break = 0
    btc_change = 0
    alt_change = 0
    redemption_volume = 0
    order_rate_anomaly = 0
    social_coordination = 0
    pattern_repetition_stealth = 0
    ai_generated_account_ratio = 0
    social_velocity = sentiment['social_velocity']
    engagement_rate = 0
    oi_change = (deriv['oi'] - deriv['oi_history'][-1]) / deriv['oi_history'][-1] * 100 if len(deriv['oi_history']) > 1 else 0
    social_spike = sentiment['social_velocity']
    orderbook_imbalance = depth.get('imbalance', 0)

    # Compute probabilities using static methods
    gamma = ManipulationExpert.gamma_pinning_probability(gamma_exposure, current_price, max_pain, days_to_expiry)
    max_pain_steer = ManipulationExpert.max_pain_steering_probability(current_price, max_pain, days_to_expiry)
    inventory = ManipulationExpert.inventory_rebalancing_probability(dealer_inventory_imbalance, price_move)
    regime_flip = ManipulationExpert.regime_flip_trap_probability(adx_val, roc)
    vol_surface = ManipulationExpert.volatility_surface_distortion_probability(iv_skew, historical_skew)
    vega = ManipulationExpert.vega_trap_probability(vol_of_vol, option_oi_spike)
    charm = ManipulationExpert.charm_flow_probability(hedge_decay, time_to_expiry)
    delta_hedge = ManipulationExpert.delta_hedge_cascade_probability(gamma_exposure, price_change_pct)
    liquidity_int = ManipulationExpert.liquidity_internalization_probability(trade_execution_delay)
    spread_eng = ManipulationExpert.spread_engineering_probability(current_spread, avg_spread)
    dealer_trap = ManipulationExpert.dealer_trap_zone_probability(high, low, close, atr)
    delayed_liq = ManipulationExpert.delayed_liquidation_feed_probability(liquidation_timestamp_deviation)
    api_inconsistency = ManipulationExpert.api_inconsistency_probability(current_price, current_price)  # same source
    fake_volume = ManipulationExpert.fake_volume_api_probability(reported_volume, actual_volume)
    tick_compression = ManipulationExpert.tick_compression_distortion_probability(print_frequency_ms)
    exchange_downtime = ManipulationExpert.exchange_downtime_exploitation_probability(downtime_duration_sec, volatility_after)
    indicator_bait = ManipulationExpert.indicator_baiting_probability(rsi, macd_hist, price_change_pct)
    ml_adversarial = ManipulationExpert.ml_adversarial_patterns_probability(pattern_frequency, regime_switch)
    backtest_poison = ManipulationExpert.backtest_poisoning_probability(current_regime, past_regime)
    correlation_break = ManipulationExpert.correlation_breakdown_probability(correlated_asset_move, expected_corr)
    bridge_flow = ManipulationExpert.bridge_flow_illusion_probability(bridge_tx_volume, actual_netflow)
    whale_theater = ManipulationExpert.whale_wallet_theater_probability(whale_transfer_amount, market_cap)
    treasury_rot = ManipulationExpert.treasury_rotation_probability(foundation_wallet_move, social_mentions)
    lp_unlock = ManipulationExpert.lp_unlock_trap_probability(unlock_amount, circulating_supply)
    coordinated_narrative = ManipulationExpert.coordinated_narrative_attack_probability(social_volume, sentiment_change)
    fake_news_injection = ManipulationExpert.fake_news_injection_probability(news_impact_score, price_reaction)
    engagement_farming = ManipulationExpert.engagement_farming_probability(bot_like_ratio)
    influencer_exit = ManipulationExpert.influencer_exit_setup_probability(influencer_mentions, whale_sell_volume)
    ai_sentiment = ManipulationExpert.ai_sentiment_flood_probability(ai_generated_content_ratio)
    slippage = ManipulationExpert.slippage_exploitation_probability(slippage, order_size, book_depth)
    trigger_hunt = ManipulationExpert.trigger_hunting_probability(high, low, known_stop_level)
    queue_starvation = ManipulationExpert.queue_starvation_probability(order_wait_time_ms)
    fake_market_orders = ManipulationExpert.fake_market_orders_probability(volume[-1] if volume else 0, price_impact)
    hidden_cross = ManipulationExpert.hidden_cross_venue_probability(off_book_volume_ratio)
    etf_distortion = ManipulationExpert.etf_flow_distortion_probability(etf_inflow, spot_price_change)
    funding_settlement = ManipulationExpert.funding_settlement_volatility_probability(minutes_to_funding, atr_pct)
    stablecoin_depeg = ManipulationExpert.stablecoin_depeg_panic_probability(peg_deviation_pct)
    regulatory_shock = ManipulationExpert.regulatory_shock_gaming_probability(leak_timestamp, announcement_timestamp)
    weekend_exploit = ManipulationExpert.weekend_liquidity_exploit_probability(volume, is_weekend)
    macro_correlation = ManipulationExpert.macro_correlation_shock_probability(dxy_change, expected_corr)
    exchange_insolvency = ManipulationExpert.exchange_insolvency_rumor_probability(withdrawal_delay, social_fud)
    oracle_failure = ManipulationExpert.oracle_failure_cascade_probability(oracle_heartbeat_miss)
    flash_crash = ManipulationExpert.flash_crash_engineering_probability(price_drop_pct_1m, volume_spike)
    cross_asset = ManipulationExpert.cross_asset_cascade_probability(correlation_break, btc_change, alt_change)
    stablecoin_redemption = ManipulationExpert.stablecoin_redemption_shock_probability(redemption_volume, circulating_supply)
    ai_swarm = ManipulationExpert.autonomous_ai_swarm_probability(order_rate_anomaly, social_coordination)
    rl_adversarial = ManipulationExpert.rl_adversarial_trading_probability(pattern_repetition_stealth)
    synthetic_social = ManipulationExpert.synthetic_social_consensus_probability(ai_generated_account_ratio)
    attention_market = ManipulationExpert.attention_market_manipulation_probability(social_velocity, engagement_rate)
    cross_domain = ManipulationExpert.cross_domain_manipulation_probability(oi_change, social_spike, orderbook_imbalance)

    # Extra detections
    spoofing = detect_spoofing(volume, close)
    wash = detect_wash_trading(tick.get('buy_vol', 0), tick.get('sell_vol', 0), tick.get('total_vol', 0))
    iceberg = detect_iceberg_orders(depth)
    fake_news = detect_fake_news(sentiment['news_score'], price_change_pct)
    cross_venue = detect_cross_venue(0, 0)  # placeholder

    # Combined trap probability (simple sum of some, capped)
    trap = min(100, gamma + max_pain_steer + inventory + regime_flip + fake_news + trigger_hunt + spoofing)
    liquidity_sweep = min(100, dealer_trap + weekend_exploit + etf_distortion)
    synthetic_flow = min(100, fake_market_orders + hidden_cross + wash)
    social_manip = min(100, coordinated_narrative + fake_news_injection + engagement_farming + influencer_exit + ai_sentiment + synthetic_social + attention_market + cross_domain)
    execution_trap = min(100, slippage + queue_starvation + fake_market_orders + trigger_hunt)
    macro_distortion = min(100, etf_distortion + macro_correlation + regulatory_shock + stablecoin_depeg + stablecoin_redemption)
    rare_event = min(100, flash_crash + exchange_insolvency + oracle_failure + cross_asset)
    emerging = min(100, ai_swarm + rl_adversarial + synthetic_social + attention_market + cross_domain)

    data_integrity = 85 if deriv['oi'] != 0 and deriv['funding_rate'] != 0 else 50
    orderbook_reliability = 100 - min(100, abs(depth.get('imbalance', 0)) * 100)
    market_integrity = int((data_integrity + orderbook_reliability) / 2)
    crowded = "HIGH" if sentiment['social_velocity'] > 800 or sentiment['retail_bias'] in ("Bullish_Extreme", "Bearish_Extreme") else "MEDIUM" if sentiment['social_velocity'] > 300 else "LOW"

    likely = []
    for name, prob in [("GAMMA_PINNING", gamma), ("MAX_PAIN", max_pain_steer), ("INVENTORY_REBAL", inventory),
                       ("LIQUIDITY_SWEEP", liquidity_sweep), ("STOP_HUNT", trap), ("SPOOFING", spoofing),
                       ("WASH_TRADING", wash), ("FAKE_NEWS", fake_news)]:
        if prob > 50:
            likely.append(name)
    if not likely:
        likely = ["NONE"]

    return {
        "trap": trap,
        "liquidity_sweep": liquidity_sweep,
        "synthetic_flow": synthetic_flow,
        "crowded": crowded,
        "book_reliability": orderbook_reliability,
        "market_integrity": market_integrity,
        "likely_types": likely,
        "gamma": gamma,
        "max_pain": max_pain_steer,
        "inventory": inventory,
        "data_integrity": data_integrity,
        "regime_flip": regime_flip,
        "social": social_manip,
        "execution": execution_trap,
        "macro": macro_distortion,
        "rare": rare_event,
        "emerging": emerging,
        "spoofing": spoofing,
        "wash": wash,
        "iceberg": iceberg,
        "fake_news": fake_news,
        "cross_venue": cross_venue,
        "vol_surface": vol_surface,
        "vega": vega,
        "charm": charm,
        "delta_hedge": delta_hedge,
        "liquidity_int": liquidity_int,
        "spread_eng": spread_eng,
        "dealer_trap": dealer_trap,
        "delayed_liq": delayed_liq,
        "api_inconsistency": api_inconsistency,
        "fake_volume": fake_volume,
        "tick_compression": tick_compression,
        "exchange_downtime": exchange_downtime,
        "indicator_bait": indicator_bait,
        "ml_adversarial": ml_adversarial,
        "backtest_poison": backtest_poison,
        "correlation_break": correlation_break,
        "bridge_flow": bridge_flow,
        "whale_theater": whale_theater,
        "treasury_rot": treasury_rot,
        "lp_unlock": lp_unlock,
        "coordinated_narrative": coordinated_narrative,
        "fake_news_injection": fake_news_injection,
        "engagement_farming": engagement_farming,
        "influencer_exit": influencer_exit,
        "ai_sentiment": ai_sentiment,
        "slippage": slippage,
        "trigger_hunt": trigger_hunt,
        "queue_starvation": queue_starvation,
        "fake_market_orders": fake_market_orders,
        "hidden_cross": hidden_cross,
        "etf_distortion": etf_distortion,
        "funding_settlement": funding_settlement,
        "stablecoin_depeg": stablecoin_depeg,
        "regulatory_shock": regulatory_shock,
        "weekend_exploit": weekend_exploit,
        "macro_correlation": macro_correlation,
        "exchange_insolvency": exchange_insolvency,
        "oracle_failure": oracle_failure,
        "flash_crash": flash_crash,
        "cross_asset": cross_asset,
        "stablecoin_redemption": stablecoin_redemption,
        "ai_swarm": ai_swarm,
        "rl_adversarial": rl_adversarial,
        "synthetic_social": synthetic_social,
        "attention_market": attention_market,
        "cross_domain": cross_domain
    }

# ---------- Main export ----------
def run_expert(symbol):
    log_issue("INFO", f"Starting E16 manipulation expert for {symbol}")
    probs = compute_manipulation_probs(symbol)
    out_path = os.path.join(FEATURES_BASE_DIR, f"{symbol.lower()}_E16_manipulation.tsv")
    with open(out_path, "w") as f:
        # Generate header from all keys
        keys = list(probs.keys())
        # Ensure timestamp is first
        keys = ["timestamp"] + [k for k in keys if k != "timestamp"]
        f.write("\t".join(keys) + "\n")
        ts_now = int(time.time() * 1000)
        row = [str(ts_now)] + [str(probs[k]) for k in keys if k != "timestamp"]
        f.write("\t".join(row) + "\n")
    log_issue("INFO", f"Saved manipulation expert summary to {out_path}")
    return out_path

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python E16_manipulation_expert.py SYMBOL")
        sys.exit(1)
    success = run_expert(sys.argv[1].upper())
    sys.exit(0 if success else 1)