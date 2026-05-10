import sys
import os

# Add project root to path (assuming this script is inside 'My Automation' folder)
sys.path.insert(0, "/storage/emulated/0/Android/data/org.qpython.qpy3/My Automation")

from Groups.group_x.X50_master import generate_master

symbol = "BTCUSDT"
print(f"Generating master JSONL for {symbol} ...")
result = generate_master(symbol)

if result is None:
    print("ERROR: generate_master returned None")
elif isinstance(result, str) and result.startswith("Error"):
    print(f"ERROR: {result}")
else:
    print(f"SUCCESS! Got {len(result)} characters.")
    print("First 500 characters:")
    print(result[:500])
    print("...")
    # Optionally, save to file for inspection
    with open("/storage/emulated/0/Android/data/org.qpython.qpy3/My Automation/test_master_output.jsonl", "w") as f:
        f.write(result)
    print("Full output saved to test_master_output.jsonl")