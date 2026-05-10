#!/usr/bin/env python3
# E01_candles_expert.py – Pattern detection + compression (no TSV output)
# Returns data for X01 to write final TSV.

import sys
import math
from collections import defaultdict

# ========================== PATTERN CODES ==========================
PATTERN_CODES = {
    "Doji": "DOJ", "Dragonfly Doji": "DDO", "Gravestone Doji": "GDO", "Long-Legged Doji": "LDO",
    "Spinning Top": "SPT", "White Marubozu": "WHM", "Black Marubozu": "BLM",
    "Hammer (or Hanging)": "HAM", "Inverted Hammer (or Shooting Star)": "INV",
    "Long White Candle": "LWC", "Long Black Candle": "LBC", "High Wave Candle": "HWC",
    "Bullish Engulfing": "BUE", "Bearish Engulfing": "BRE",
    "Bullish Harami": "BUH", "Bearish Harami": "BRH",
    "Piercing Line": "PIE", "Dark Cloud Cover": "DCC",
    "Tweezer Top": "TZT", "Tweezer Bottom": "TZB",
    "Morning Star": "MRS", "Evening Star": "EVS",
    "Three White Soldiers": "3WS", "Three Black Crows": "3BC",
    "Four White Soldiers": "4WS", "Four Black Crows": "4BC",
    "Heikin-Ashi Hollow Bullish": "HAB", "Heikin-Ashi Filled Bullish": "HAF",
    "Heikin-Ashi Hollow Bearish": "HBB", "Heikin-Ashi Filled Bearish": "HBF",
    "Heikin-Ashi Bullish Reversal": "HBR", "Heikin-Ashi Bearish Reversal": "HBE",
    "Heikin-Ashi Consolidation": "HAC",
    "Renko UP Brick": "RUP", "Renko DOWN Brick": "RDN",
}
def encode_pattern(p): return PATTERN_CODES.get(p, p[:5].upper())
def encode_patterns(s): return '|'.join(encode_pattern(x.strip()) for x in s.split('|')) if s else ""

# ========================== CANDLE HELPERS ==========================
def is_bullish(c): return c['close'] > c['open']
def is_bearish(c): return c['close'] < c['open']
def body_length(c): return abs(c['close'] - c['open'])
def upper_wick(c): return c['high'] - max(c['open'], c['close'])
def lower_wick(c): return min(c['open'], c['close']) - c['low']
def total_range(c): return c['high'] - c['low']
def avg_body(candles, window=20):
    if len(candles) < window: return sum(body_length(c) for c in candles)/len(candles)
    recent = candles[-window:]
    return sum(body_length(c) for c in recent)/window

# ========================== HEIKIN-ASHI ==========================
def heikin_ashi(candles):
    if not candles: return [], []
    ha = []
    for i, c in enumerate(candles):
        if i==0:
            ha_close = (c['open']+c['high']+c['low']+c['close'])/4.0
            ha_open = (c['open']+c['close'])/2.0
        else:
            ha_close = (c['open']+c['high']+c['low']+c['close'])/4.0
            ha_open = (ha[-1]['ha_open']+ha[-1]['ha_close'])/2.0
        ha_high = max(c['high'], ha_open, ha_close)
        ha_low = min(c['low'], ha_open, ha_close)
        ha_bullish = ha_close > ha_open
        ha_body = abs(ha_close - ha_open)
        ha.append({'ha_open':ha_open,'ha_close':ha_close,'ha_high':ha_high,'ha_low':ha_low,'ha_bullish':ha_bullish,'ha_body':ha_body})
    patterns = []
    for i in range(1,len(ha)):
        if ha[i]['ha_bullish']:
            patterns.append(("Heikin-Ashi Hollow Bullish" if ha[i]['ha_close']>ha[i]['ha_open'] else "Heikin-Ashi Filled Bullish", i))
        else:
            patterns.append(("Heikin-Ashi Hollow Bearish" if ha[i]['ha_close']>ha[i]['ha_open'] else "Heikin-Ashi Filled Bearish", i))
        if ha[i]['ha_bullish'] and not ha[i-1]['ha_bullish']: patterns.append(("Heikin-Ashi Bullish Reversal", i))
        elif not ha[i]['ha_bullish'] and ha[i-1]['ha_bullish']: patterns.append(("Heikin-Ashi Bearish Reversal", i))
        body_ma = sum(h['ha_body'] for h in ha[max(0,i-9):i+1])/min(10,i+1)
        if ha[i]['ha_body'] < 0.3*body_ma: patterns.append(("Heikin-Ashi Consolidation", i))
    return patterns, ha

def renko_blocks(candles, brick_size=None):
    if not candles: return []
    if brick_size is None: brick_size = candles[-1]['close']*0.001
    bricks = []
    current = {'high':candles[0]['close'],'low':candles[0]['close'],'direction':None}
    for c in candles:
        price = c['close']
        if current['direction'] is None:
            current['direction'] = 'up' if price>current['high'] else 'down'
            current['high'] = max(current['high'],price); current['low']=min(current['low'],price)
        elif current['direction']=='up':
            if price >= current['high']+brick_size:
                bricks.append({'direction':'up','high':current['high'],'low':current['low']})
                current = {'high':price,'low':price,'direction':'up'}
            else:
                current['high'] = max(current['high'],price); current['low']=min(current['low'],price)
        else:
            if price <= current['low']-brick_size:
                bricks.append({'direction':'down','high':current['high'],'low':current['low']})
                current = {'high':price,'low':price,'direction':'down'}
            else:
                current['high'] = max(current['high'],price); current['low']=min(current['low'],price)
    return bricks

# ========================== PATTERN DETECTION (ALL 100+) ==========================
def detect_patterns_for_timeframe(candles):
    if len(candles)<5: return []
    for i,c in enumerate(candles):
        c['body'] = body_length(c); c['upper_wick']=upper_wick(c); c['lower_wick']=lower_wick(c); c['range']=total_range(c)
    avg_body_len = avg_body(candles,20)
    ha_patterns,_ = heikin_ashi(candles)
    ha_dict = defaultdict(list)
    for pat,idx in ha_patterns: ha_dict[idx].append(pat)
    renko_blocks_list = renko_blocks(candles)
    renko_current = renko_blocks_list[-1]['direction'] if renko_blocks_list else None

    results = []
    for i,c in enumerate(candles):
        patterns = []
        # --- single ---
        if c['body'] <= 0.05*avg_body_len:
            patterns.append("Doji")
            if c['lower_wick']>2*c['body'] and c['upper_wick']<0.5*c['body']: patterns.append("Dragonfly Doji")
            elif c['upper_wick']>2*c['body'] and c['lower_wick']<0.5*c['body']: patterns.append("Gravestone Doji")
            elif c['upper_wick']>c['body'] and c['lower_wick']>c['body']: patterns.append("Long-Legged Doji")
        if c['body']<0.2*avg_body_len and c['upper_wick']>0 and c['lower_wick']>0: patterns.append("Spinning Top")
        if c['upper_wick']<0.05*avg_body_len and c['lower_wick']<0.05*avg_body_len:
            patterns.append("White Marubozu" if is_bullish(c) else "Black Marubozu")
        if c['lower_wick']>2*c['body'] and c['body']<0.4*c['range']: patterns.append("Hammer (or Hanging)")
        if c['upper_wick']>2*c['body'] and c['body']<0.4*c['range']: patterns.append("Inverted Hammer (or Shooting Star)")
        if c['body']>2*avg_body_len: patterns.append("Long White Candle" if is_bullish(c) else "Long Black Candle")
        if c['range']>2*c['body'] and not (c['body']<=0.05*avg_body_len): patterns.append("High Wave Candle")
        # --- double ---
        if i>0:
            prev = candles[i-1]
            if is_bearish(prev) and is_bullish(c) and c['open']<prev['close'] and c['close']>prev['open']: patterns.append("Bullish Engulfing")
            if is_bullish(prev) and is_bearish(c) and c['open']>prev['close'] and c['close']<prev['open']: patterns.append("Bearish Engulfing")
            if is_bullish(prev) and is_bullish(c) and c['open']>prev['open'] and c['close']<prev['close']: patterns.append("Bullish Harami")
            if is_bearish(prev) and is_bearish(c) and c['open']<prev['open'] and c['close']>prev['close']: patterns.append("Bearish Harami")
            if is_bearish(prev) and is_bullish(c) and c['close']>(prev['open']+prev['close'])/2 and c['open']<prev['close']: patterns.append("Piercing Line")
            if is_bullish(prev) and is_bearish(c) and c['close']<(prev['open']+prev['close'])/2 and c['open']>prev['close']: patterns.append("Dark Cloud Cover")
            if c['high']==prev['high'] and is_bearish(c) and is_bullish(prev): patterns.append("Tweezer Top")
            if c['low']==prev['low'] and is_bullish(c) and is_bearish(prev): patterns.append("Tweezer Bottom")
        # --- triple ---
        if i>1:
            p1,p2 = candles[i-1],candles[i-2]
            if is_bearish(p2) and (candles[i-1]['body']<=0.05*avg_body_len) and is_bullish(c) and c['close']>(p2['open']+p2['close'])/2: patterns.append("Morning Star")
            if is_bullish(p2) and (candles[i-1]['body']<=0.05*avg_body_len) and is_bearish(c) and c['close']<(p2['open']+p2['close'])/2: patterns.append("Evening Star")
            if is_bullish(p2) and is_bullish(p1) and is_bullish(c) and p2['close']<p1['close']<c['close']: patterns.append("Three White Soldiers")
            if is_bearish(p2) and is_bearish(p1) and is_bearish(c) and p2['close']>p1['close']>c['close']: patterns.append("Three Black Crows")
        # --- four ---
        if i>2:
            p1,p2,p3 = candles[i-3],candles[i-2],candles[i-1]
            if is_bullish(p1) and is_bullish(p2) and is_bullish(p3) and is_bullish(c): patterns.append("Four White Soldiers")
            if is_bearish(p1) and is_bearish(p2) and is_bearish(p3) and is_bearish(c): patterns.append("Four Black Crows")
        # Heikin-Ashi
        if i in ha_dict: patterns.extend(ha_dict[i])
        # Renko hint
        if i==len(candles)-1 and renko_current: patterns.append(f"Renko {renko_current.upper()} Brick")
        if patterns:
            results.append({'index':i,'timestamp':c['timestamp'],'patterns':'|'.join(patterns)})
    return results

# ========================== COMPRESSION (identical patterns only) ==========================
def compress_patterns(patterns_list, keep_last_n):
    """patterns_list: list of (timestamp, pattern_string) sorted ascending.
       keep_last_n: number of newest candles to keep as raw rows.
       Returns (raw_rows, events) where raw_rows are (ts, pattern) and events are merged identical blocks.
    """
    if keep_last_n > 0 and len(patterns_list) > keep_last_n:
        raw = patterns_list[-keep_last_n:]
        compress = patterns_list[:-keep_last_n]
    else:
        raw = patterns_list[:]
        compress = []
    events = []
    if not compress:
        return raw, events
    curr_pat = compress[0][1]
    start_ts = compress[0][0]
    cnt = 1
    for i in range(1, len(compress)):
        ts, pat = compress[i]
        if pat == curr_pat:
            cnt += 1
        else:
            events.append({
                'start_ts': start_ts,
                'end_ts': compress[i-1][0],
                'duration': cnt,
                'pattern': curr_pat,
                'strength': cnt
            })
            curr_pat = pat
            start_ts = ts
            cnt = 1
    events.append({
        'start_ts': start_ts,
        'end_ts': compress[-1][0],
        'duration': cnt,
        'pattern': curr_pat,
        'strength': cnt
    })
    return raw, events

# ========================== STRUCTURE, FLOW, SCENARIO (same as before) ==========================
def find_swing_points(candles, lookback=2):
    highs = []
    lows = []
    n = len(candles)
    for i in range(lookback, n - lookback):
        is_high = all(candles[i]['high'] > candles[i-j]['high'] for j in range(1, lookback+1)) and \
                  all(candles[i]['high'] > candles[i+j]['high'] for j in range(1, lookback+1))
        if is_high:
            highs.append((candles[i]['timestamp'], candles[i]['high']))
        is_low = all(candles[i]['low'] < candles[i-j]['low'] for j in range(1, lookback+1)) and \
                 all(candles[i]['low'] < candles[i+j]['low'] for j in range(1, lookback+1))
        if is_low:
            lows.append((candles[i]['timestamp'], candles[i]['low']))
    return highs, lows

def detect_market_structure(candles):
    if len(candles) < 20:
        return "RANGE"
    highs, lows = find_swing_points(candles, lookback=2)
    if len(highs) < 3 or len(lows) < 3:
        return "RANGE"
    last_highs = highs[-3:]
    last_lows = lows[-3:]
    bullish_highs = all(last_highs[i][1] > last_highs[i-1][1] for i in range(1, len(last_highs)))
    bullish_lows = all(last_lows[i][1] > last_lows[i-1][1] for i in range(1, len(last_lows)))
    if bullish_highs and bullish_lows:
        return "HH/HL"
    bearish_highs = all(last_highs[i][1] < last_highs[i-1][1] for i in range(1, len(last_highs)))
    bearish_lows = all(last_lows[i][1] < last_lows[i-1][1] for i in range(1, len(last_lows)))
    if bearish_highs and bearish_lows:
        return "LH/LL"
    return "RANGE"

def get_atr_ratio(candles, period=14):
    if len(candles) < period+1:
        return 0.015
    tr = []
    for i in range(1, len(candles)):
        tr.append(max(candles[i]['high']-candles[i]['low'],
                      abs(candles[i]['high']-candles[i-1]['close']),
                      abs(candles[i]['low']-candles[i-1]['close'])))
    atr = sum(tr[-period:]) / period
    return atr / candles[-1]['close'] * 100

def volatility_regime(atr_pct):
    if atr_pct < 1.0: return "low_vol"
    if atr_pct > 2.0: return "high_vol"
    return "normal_vol"

def compute_continuous_flow(candles, period=5):
    if len(candles) < period+1:
        return 0
    changes = [candles[i]['close'] - candles[i-1]['close'] for i in range(-period, 0)]
    atr = compute_atr(candles, 14)
    if atr == 0:
        return 0
    avg_change = sum(changes) / period
    gradient = avg_change / atr
    return max(-1.0, min(1.0, gradient))

def compute_atr(candles, period=14):
    if len(candles) < period+1:
        return candles[-1]['close'] * 0.015
    tr = []
    for i in range(1, len(candles)):
        tr.append(max(candles[i]['high']-candles[i]['low'],
                      abs(candles[i]['high']-candles[i-1]['close']),
                      abs(candles[i]['low']-candles[i-1]['close'])))
    return sum(tr[-period:]) / period

def compute_pos(candles, lookback=20):
    if not candles or len(candles) < lookback:
        return 0.5
    recent_high = max(c['high'] for c in candles[-lookback:])
    recent_low = min(c['low'] for c in candles[-lookback:])
    if recent_high == recent_low: return 0.5
    return max(0.0, min(1.0, (candles[-1]['close'] - recent_low) / (recent_high - recent_low)))

PATTERN_BASE_WEIGHTS = {
    "3WS": 3, "4WS": 3, "MRS": 3, "BUE": 2.5, "HAB": 2, "HBR": 2, "PIE": 2, "TZB": 2,
    "3BC": -3, "4BC": -3, "EVS": -3, "BRE": -2.5, "HBB": -2, "HBE": -2, "DCC": -2, "TZT": -2,
    "HAF": 1.5, "HBF": -1.5, "WHM": 1.5, "LWC": 1.5, "BLM": -1.5, "LBC": -1.5,
    "HAM": 1, "INV": 1, "BUH": 1, "BRH": -1,
    "RUP": 2, "RDN": -2,
    "HWC": 0, "SPT": 0, "DOJ": 0, "HAC": 0
}
def contextual_weight(pattern_code, structure, momentum, pos):
    base = PATTERN_BASE_WEIGHTS.get(pattern_code, 0)
    if base == 0: return 0
    if structure == "HH/HL" and base > 0:
        if pos < 0.4: base *= 2
        else: base *= 1.2
    elif structure == "LH/LL" and base < 0:
        if pos > 0.6: base *= 2
        else: base *= 1.2
    elif structure == "RANGE":
        base *= 0.5
    if (base > 0 and momentum < -0.2) or (base < 0 and momentum > 0.2):
        base *= 0.5
    elif (base > 0 and momentum > 0.5) or (base < 0 and momentum < -0.5):
        base *= 1.5
    return base

def compute_pattern_score_with_decay(patterns_with_time, current_time, structure, momentum, pos):
    total = 0.0
    for ts, pat in patterns_with_time:
        age_hours = (current_time - ts) / (3600 * 1000)
        decay = math.exp(-age_hours / 8.0)
        w = contextual_weight(pat, structure, momentum, pos)
        total += w * decay
    return total

PROB_MAP = [(-100,-60,25), (-60,-30,35), (-30,-10,45), (-10,10,50), (10,30,60), (30,60,70), (60,100,80)]
def score_to_probability(score):
    for lo, hi, prob in PROB_MAP:
        if lo <= score <= hi:
            return min(85, prob)
    return 50

def compute_high_prob_scenario(data_by_tf, raw_1m, events_by_tf):
    current_ts = 0
    for tf in ['1m','5m','15m','1h','4h']:
        if data_by_tf.get(tf):
            current_ts = max(current_ts, data_by_tf[tf][-1]['timestamp'])
    tf_4h = data_by_tf.get('4h')
    tf_1h = data_by_tf.get('1h')
    if not tf_4h or not tf_1h:
        return "No clear signal", 0, "Insufficient data"
    structure = detect_market_structure(tf_4h)
    atr_pct = get_atr_ratio(tf_4h)
    vol_reg = volatility_regime(atr_pct)
    flow_4h = compute_continuous_flow(tf_4h)
    flow_1h = compute_continuous_flow(tf_1h)
    momentum = compute_continuous_flow(tf_1h, period=3)
    pos = compute_pos(tf_1h)
    pattern_list = []
    for r in raw_1m:
        for pat in r['patterns'].split('|'):
            if pat: pattern_list.append((r['timestamp'], pat))
    for evs in events_by_tf.values():
        for ev in evs:
            for pat in ev['pattern'].split('|'):
                if pat: pattern_list.append((ev['start_ts'], pat))
    pattern_total = compute_pattern_score_with_decay(pattern_list, current_ts, structure, momentum, pos)
    pattern_score = min(50, max(-50, pattern_total * 2.5))
    structure_score = 20 if structure=="HH/HL" else (-20 if structure=="LH/LL" else 0)
    momentum_score = momentum * 30
    alignment = 15 if flow_4h*flow_1h>0 else (-15 if abs(flow_4h)>0.3 and abs(flow_1h)>0.3 else 0)
    vol_score = 10 if vol_reg=="high_vol" and abs(momentum)>0.3 else (-10 if vol_reg=="low_vol" else 0)
    penalty = 15 if (pattern_score>10 and momentum_score<-15) or (pattern_score<-10 and momentum_score>15) else 0
    total_score = pattern_score + structure_score + momentum_score + alignment + vol_score - penalty
    raw_score_clip = max(-100, min(100, total_score))
    probability = score_to_probability(raw_score_clip)
    if raw_score_clip > 15:
        dir_text = "Probably UP" if probability>=70 else "Maybe UP"
    elif raw_score_clip < -15:
        dir_text = "Probably DOWN" if probability>=70 else "Maybe DOWN"
    else:
        dir_text = "No clear signal"
        probability = 0
    reason = (f"Structure:{structure}|Vol:{vol_reg}|Momentum:{momentum:.2f}|Score:{raw_score_clip:.0f}")
    return dir_text, probability, reason

# ========================== MAIN EXPORT FUNCTIONS FOR X01 ==========================
def load_and_prepare(input_tsv):
    with open(input_tsv,'r') as f:
        lines = f.readlines()
    data_by_tf = {tf:[] for tf in ['1m','5m','15m','1h','4h']}
    for line in lines:
        line=line.strip()
        if not line: continue
        parts=line.split('\t')
        if len(parts)<8: continue
        symbol, tf, ts, o, h, l, c, v = parts
        if tf not in data_by_tf: continue
        data_by_tf[tf].append({'symbol':symbol,'timeframe':tf,'timestamp':int(ts),'open':float(o),'high':float(h),'low':float(l),'close':float(c),'volume':float(v)})
    for tf in data_by_tf:
        data_by_tf[tf].sort(key=lambda x:x['timestamp'])
    return data_by_tf, None

def compute_all_patterns_and_events(data_by_tf):
    """Returns raw_rows (per timeframe?) and events_by_tf."""
    # We'll keep raw rows per timeframe as needed, but X01 wants raw_1m and events_by_tf.
    # For other timeframes, we also keep raw rows for last 14 candles.
    raw_rows_by_tf = {}
    events_by_tf = {}
    keep_raw = {'1m':9, '5m':14, '15m':14, '1h':14, '4h':14}
    for tf, candles in data_by_tf.items():
        if not candles:
            raw_rows_by_tf[tf] = []
            events_by_tf[tf] = []
            continue
        pat_dicts = detect_patterns_for_timeframe(candles)
        pat_tuples = [(p['timestamp'], p['patterns']) for p in pat_dicts]
        # Compress with keep_last_n
        raw, events = compress_patterns(pat_tuples, keep_raw.get(tf, 0))
        # raw is list of (timestamp, pattern_string) – still need to encode patterns
        raw_list = [{'timestamp':ts, 'patterns':encode_patterns(pat)} for ts,pat in raw]
        # events also need encoded patterns
        for ev in events:
            ev['pattern'] = encode_patterns(ev['pattern'])
        raw_rows_by_tf[tf] = raw_list
        events_by_tf[tf] = events
    # Keep raw_1m separately for compatibility (X01 expects raw_1m)
    raw_1m = raw_rows_by_tf.get('1m', [])
    return raw_1m, events_by_tf, raw_rows_by_tf

# compute_scenario is not used; X01 will call compute_high_prob_scenario directly