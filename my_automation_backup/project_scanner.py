import os
import json
import datetime

TARGET_DIR  = '/storage/emulated/0/Android/data/org.qpython.qpy3/My Automation'
OUTPUT_FILE = os.path.join(TARGET_DIR, 'project_summary_compact.txt')

SKIP_DIRS  = {'__pycache__', '.git', 'node_modules', '.idea', 'logs'}
SKIP_FILES = {'project_summary.txt', 'project_short_logic.txt', 'project_summary_full.txt', 'project_summary_compact.txt'}
MAX_LINES_PER_FILE = 50
MAX_JSON_CONFIGS = 5

def fmt_size(size):
    if size > 1024*1024:
        return f"{round(size/(1024*1024),1)} MB"
    if size > 1024:
        return f"{round(size/1024,1)} KB"
    return f"{size} B"

def extract_py_summary(lines):
    """Extract only essential info from Python file."""
    classes = [l.strip() for l in lines if l.strip().startswith('class ')][:5]
    funcs = [l.strip() for l in lines if l.strip().startswith('def ')][:5]
    imports = [l.strip() for l in lines if l.strip().startswith(('import ', 'from '))][:5]
    logic = []
    for l in lines:
        s = l.strip()
        if any(k in s.lower() for k in ['rsi', 'ema', 'macd', 'score', 'signal', 'buy', 'sell']):
            logic.append(s[:80])
            if len(logic) >= 5:
                break
    return classes, funcs, imports, logic

def scan():
    out = []
    out.append("="*80)
    out.append("COMPACT PROJECT SCAN")
    out.append(f"Time: {datetime.datetime.now()}")
    out.append("="*80)

    # Folder structure (only important folders, limited files)
    out.append("\n[FOLDER STRUCTURE]")
    total_files = 0
    for root, dirs, files in os.walk(TARGET_DIR):
        dirs[:] = sorted([d for d in dirs if d not in SKIP_DIRS and not d.startswith('.')])
        level = root.replace(TARGET_DIR, '').count(os.sep)
        indent = "  " * level
        folder = os.path.basename(root) or 'My Automation'
        out.append(indent + folder + "/")
        sub = "  " * (level + 1)
        # Show at most 20 files per folder
        file_list = sorted([f for f in files if f not in SKIP_FILES])[:20]
        for f in file_list:
            fpath = os.path.join(root, f)
            size = os.path.getsize(fpath)
            out.append(sub + f + f"  [{fmt_size(size)}]")
            total_files += 1
        if len(files) > 20:
            out.append(sub + f"... and {len(files)-20} more files")
    out.append(f"\nTotal files shown: {total_files}")

    # Architecture (shortened)
    out.append("\n[ARCHITECTURE]")
    out.append("""
LAYERS: Z (fast scan) → A (single pair) → D (deep)
PLATFORMS: Binance, Finnhub, IQ Option, Quotex
FLOW: WS connect → 1hr data → resampling (1m→4h)
CONFIGS: group_c/configs/<market>/<pair>.json
""")

    # File details (only Python and JSON, limited)
    out.append("\n[FILE DETAILS]")
    for root, dirs, files in os.walk(TARGET_DIR):
        dirs[:] = sorted([d for d in dirs if d not in SKIP_DIRS])
        for fname in files:
            if fname in SKIP_FILES or not fname.endswith(('.py', '.json')):
                continue
            fpath = os.path.join(root, fname)
            rel = os.path.relpath(fpath, TARGET_DIR)
            out.append(f"\n--- {rel} ---")
            try:
                with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                    lines = f.readlines()[:MAX_LINES_PER_FILE]
                if fname.endswith('.py'):
                    classes, funcs, imports, logic = extract_py_summary(lines)
                    if classes: out.append("Classes: " + ", ".join(classes))
                    if funcs: out.append("Functions: " + ", ".join(funcs))
                    if imports: out.append("Imports: " + ", ".join(imports))
                    if logic: out.append("Key Logic: " + " | ".join(logic))
                else:  # JSON
                    data = json.loads(''.join(lines))
                    out.append(json.dumps(data, indent=2)[:800])
                    if len(json.dumps(data)) > 800:
                        out.append("... (truncated)")
            except Exception as e:
                out.append(f"Error: {e}")

    # Configs (limited to 5 examples)
    out.append("\n[CONFIG EXAMPLES]")
    cfg_root = os.path.join(TARGET_DIR, 'Groups', 'group_c', 'configs')
    if os.path.exists(cfg_root):
        examples = []
        for mkt in os.listdir(cfg_root):
            mkt_path = os.path.join(cfg_root, mkt)
            if os.path.isdir(mkt_path):
                for cfg in os.listdir(mkt_path)[:2]:  # up to 2 per market
                    examples.append(os.path.join(mkt_path, cfg))
                if len(examples) >= MAX_JSON_CONFIGS:
                    break
        for ex in examples:
            try:
                with open(ex) as f:
                    data = json.load(f)
                out.append(f"\n{os.path.basename(ex)}:")
                out.append(json.dumps(data, indent=2)[:500])
            except:
                pass
    else:
        out.append("Configs not found")

    # Market data summary
    out.append("\n[MARKET DATA]")
    md = os.path.join(TARGET_DIR, 'market_data')
    if os.path.exists(md):
        for plat in os.listdir(md):
            plat_path = os.path.join(md, plat)
            if os.path.isdir(plat_path):
                count = len(os.listdir(plat_path))
                out.append(f"{plat}: {count} files")
    else:
        out.append("Not created yet")

    # Save
    text = "\n".join(out)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(text)
    print(f"Compact summary saved: {OUTPUT_FILE} ({len(text)//1024} KB)")

if __name__ == "__main__":
    scan()