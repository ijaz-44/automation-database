# test_cvd_direct2.py
import sys
import os
import json

# Add project root
sys.path.insert(0, os.path.dirname(__file__))

print("=" * 60)
print("TEST CVD USING X03_cvd_rest DIRECTLY")
print("=" * 60)

try:
    from Groups.group_x.X03_cvd_rest import backfill_cvd
    print("✅ backfill_cvd imported")
except Exception as e:
    print(f"❌ Import error: {e}")
    sys.exit(1)

symbol = "BCHUSDT"
print(f"\nCalling backfill_cvd for {symbol} (60 min, up to 10000 trades)...")
result = backfill_cvd(symbol, minutes=60, max_trades=10000, max_cluster_levels=50)

if "error" in result:
    print(f"❌ Error: {result['error']}")
else:
    print(f"✅ CVD computed: {result.get('cvd')}")
    print(f"   Trade count: {result.get('trade_count')}")
    print(f"   Footprint entries: {len(result.get('footprint', {}))}")
    print(f"   Imbalance events: {len(result.get('imbalance_events', []))}")
    print(f"   Absorption events: {len(result.get('absorption_events', []))}")

    # Save to the expected location
    symbols_dir = os.path.join(os.path.dirname(__file__), "market_data", "binance", "symbols")
    os.makedirs(symbols_dir, exist_ok=True)
    filepath = os.path.join(symbols_dir, f"{symbol.lower()}_cvd.json")
    try:
        with open(filepath, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"\n✅ File saved to: {filepath}")
    except Exception as e:
        print(f"❌ Save error: {e}")

print("\n" + "=" * 60)