#!/usr/bin/env python3
"""
Complete Diagnostic Script for VolProfile Button Issue
Run this script ONCE. It will analyze all relevant files and configuration.
No button pressing required.
"""

import os
import re
import sys
import json
import sqlite3
import requests
from pathlib import Path

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SYMBOLS_DIR = os.path.join(BASE_DIR, "market_data", "binance", "symbols")
MAIN_PY = os.path.join(BASE_DIR, "main.py")
SYS_DATA_PY = os.path.join(BASE_DIR, "sys_data.py")
X19_PY = os.path.join(BASE_DIR, "Groups", "group_x", "X19_volProfile_rest.py")
CONFIG_PY = os.path.join(BASE_DIR, "config.py")
HTML_TEMPLATE = None

# Load HTML template from config.py if possible
try:
    from config import HTML_TEMPLATE
except ImportError:
    HTML_TEMPLATE = ""
    print("⚠️ Could not import HTML_TEMPLATE from config.py")

print("=" * 80)
print("VOLPROFILE BUTTON ISSUE DIAGNOSTIC")
print("=" * 80)

# 1. Check if volProfile database exists and has data
print("\n[1] Checking volProfile SQLite database...")
db_path = os.path.join(SYMBOLS_DIR, "btcusdt_volProfile.db")
if not os.path.exists(db_path):
    print(f"❌ Database not found: {db_path}")
    print("   Run X19_volProfile_rest.py BTCUSDT to create it.")
else:
    size = os.path.getsize(db_path)
    print(f"✅ Database exists: {db_path} ({size} bytes)")
    try:
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cur.fetchall()]
        print(f"   Tables: {', '.join(tables)}")
        for table in ['developing_poc', 'daily_profiles', 'untested_pocs', 'prediction_context']:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"   {table}: {count} rows")
        conn.close()
    except Exception as e:
        print(f"❌ Error reading DB: {e}")

# 2. Check main.py route for volprofile
print("\n[2] Checking Flask route in main.py...")
if not os.path.exists(MAIN_PY):
    print(f"❌ main.py not found at {MAIN_PY}")
else:
    with open(MAIN_PY, 'r', encoding='utf-8') as f:
        main_content = f.read()
    # Find the view_data route
    route_pattern = r"@app\.route\('/data/<symbol>/<data_type>'\)\s+def view_data\(symbol,\s*data_type\):.*?(?=@app\.route|if __name__)"
    match = re.search(route_pattern, main_content, re.DOTALL)
    if match:
        route_code = match.group(0)
        if "data_type == 'volprofile'" in route_code:
            print("✅ Route handler includes 'volprofile' branch.")
            # Check what it does
            if "daily_profiles" in route_code:
                print("   👍 It reads from daily_profiles table.")
            else:
                print("   ⚠️ It does NOT read from daily_profiles.")
            if "return \"\n\".join(lines)" in route_code:
                print("   👍 Returns JSONL with newline separator.")
            else:
                print("   ⚠️ Return format may be incorrect.")
        else:
            print("❌ No 'volprofile' branch found in view_data route.")
    else:
        print("❌ Could not find view_data route in main.py")

# 3. Check frontend JavaScript (HTML_TEMPLATE) for button handler
print("\n[3] Checking frontend JavaScript for VOLP button...")
if HTML_TEMPLATE:
    # Look for copyData function call with 'volprofile'
    if 'copyData' in HTML_TEMPLATE:
        # Find the button that calls volprofile
        volprofile_buttons = re.findall(r"copyData\([^,]+,\s*'volprofile'\)", HTML_TEMPLATE)
        if volprofile_buttons:
            print(f"✅ Found {len(volprofile_buttons)} button(s) calling copyData(..., 'volprofile')")
        else:
            # Also check for 'volProfile' with capital P
            volprofile_buttons = re.findall(r"copyData\([^,]+,\s*'volProfile'\)", HTML_TEMPLATE)
            if volprofile_buttons:
                print("⚠️ Buttons call copyData with 'volProfile' (capital P).")
                print("   The Flask endpoint expects 'volprofile' (lowercase).")
                print("   This is likely the CAUSE of the issue!")
            else:
                print("❌ No button found that calls copyData with 'volprofile'.")
    else:
        print("❌ copyData function not found in HTML_TEMPLATE.")
else:
    print("⚠️ HTML_TEMPLATE not loaded; cannot check frontend.")

# 4. Check X19_volProfile_rest.py for proper SQLite writing
print("\n[4] Checking X19_volProfile_rest.py...")
if os.path.exists(X19_PY):
    with open(X19_PY, 'r', encoding='utf-8') as f:
        x19_content = f.read()
    # Check atomic_write_db function
    if "def atomic_write_db" in x19_content:
        print("✅ X19 has atomic_write_db function.")
    else:
        print("❌ X19 missing atomic_write_db function.")
    # Check if it creates daily_profiles table
    if "daily_profiles" in x19_content:
        print("✅ X19 creates daily_profiles table.")
    else:
        print("❌ X19 does not create daily_profiles table.")
    # Check last line of log to see if it's been run recently
    log_path = os.path.join(BASE_DIR, "x19_volprofile.log")
    if os.path.exists(log_path):
        with open(log_path, 'r') as f:
            last_lines = f.readlines()[-5:]
        print("   Last few log lines:")
        for line in last_lines:
            print(f"     {line.strip()}")
    else:
        print("   No log file found; X19 may not have been run.")
else:
    print(f"❌ X19_volProfile_rest.py not found at {X19_PY}")

# 5. Check if Flask server is running (optional, but helpful)
print("\n[5] Checking Flask server status...")
try:
    r = requests.get("http://127.0.0.1:5000/health", timeout=2)
    if r.status_code == 200:
        print("✅ Flask server is running.")
        # Also try to fetch the volprofile endpoint
        resp = requests.get("http://127.0.0.1:5000/data/BTCUSDT/volprofile", timeout=5)
        if resp.status_code == 200:
            content = resp.text
            if content and content.strip():
                lines = content.splitlines()
                print(f"   Endpoint returns {len(lines)} JSONL lines.")
                if len(lines) > 0:
                    print("   Sample line:")
                    print(f"     {lines[0][:150]}...")
            else:
                print("   ⚠️ Endpoint returns empty response.")
        else:
            print(f"   Endpoint returned status {resp.status_code}")
    else:
        print(f"⚠️ Flask responded with status {r.status_code}")
except requests.exceptions.ConnectionError:
    print("❌ Flask server is NOT running. Start main.py and try again.")
except Exception as e:
    print(f"❌ Error connecting to Flask: {e}")

# 6. Summary and likely cause
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

issues = []
if not os.path.exists(db_path):
    issues.append("Missing volProfile database (run X19_volProfile_rest.py BTCUSDT)")
else:
    # Check if daily_profiles has rows
    try:
        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM daily_profiles").fetchone()[0]
        if count == 0:
            issues.append("daily_profiles table is empty (database exists but no data)")
        conn.close()
    except:
        pass

if HTML_TEMPLATE:
    if not re.search(r"copyData\([^,]+,\s*'volprofile'\)", HTML_TEMPLATE):
        if re.search(r"copyData\([^,]+,\s*'volProfile'\)", HTML_TEMPLATE):
            issues.append("Frontend calls 'volProfile' (capital P) but endpoint expects 'volprofile' (lowercase)")
        else:
            issues.append("Frontend button does not call copyData with 'volprofile'")

# Check route in main.py
if os.path.exists(MAIN_PY):
    with open(MAIN_PY, 'r') as f:
        if "data_type == 'volprofile'" not in f.read():
            issues.append("No 'volprofile' route handler in main.py")

if not issues:
    print("✅ No issues detected. The button should work if Flask server is running.")
    print("   If it still doesn't, ensure the frontend calls copyData(symbol, 'volprofile').")
else:
    print("❌ The following issue(s) were found:")
    for issue in issues:
        print(f"   - {issue}")
    print("\n💡 To fix:")
    if "volProfile" in str(issues):
        print("   - Edit HTML template (config.py) and change 'volProfile' to 'volprofile' in the button's onclick.")
    if "No 'volprofile' route handler" in str(issues):
        print("   - Add a proper route in main.py for volprofile (as provided earlier).")
    if "daily_profiles table is empty" in str(issues):
        print("   - Run X19_volProfile_rest.py BTCUSDT manually to populate data.")

print("\n" + "=" * 80)