import os
import csv
import datetime
from collections import defaultdict

TARGET_DIR = '/storage/emulated/0/Android/data/org.qpython.qpy3/My Automation'
OUTPUT_TSV = os.path.join(TARGET_DIR, 'balanced_summary.tsv')

SKIP_DIRS = {'__pycache__', '.git', 'node_modules', '.idea', 'logs'}
SKIP_FILES = {'balanced_summary.tsv'}

# Detailed limits for Python
MAX_PY_LINES = 200
MAX_CLASSES = 10
MAX_FUNCS = 10
MAX_IMPORTS = 8
MAX_LOGIC = 10

# Limits for TSV preview (non-market_data)
MAX_TSV_PREVIEW_ROWS = 3
MAX_TSV_COLS = 6

# Limits for market_data summary
MARKET_DATA_SUMMARY_FILES = 5   # example files per market

def fmt_size(size):
    if size > 1024*1024:
        return f"{round(size/(1024*1024),1)} MB"
    return f"{round(size/1024,1)} KB"

def extract_py_details(lines):
    classes, funcs, imports, logic = [], [], [], []
    for line in lines:
        s = line.strip()
        if s.startswith('class ') and len(classes) < MAX_CLASSES:
            classes.append(s.split('(')[0].replace('class ', ''))
        elif s.startswith('def ') and len(funcs) < MAX_FUNCS:
            funcs.append(s.split('(')[0].replace('def ', ''))
        elif s.startswith(('import ', 'from ')) and len(imports) < MAX_IMPORTS:
            imports.append(s[:60])
        elif any(k in s.lower() for k in ['rsi','ema','macd','signal','buy','sell','trend','strategy']) and len(logic) < MAX_LOGIC:
            logic.append(s[:80])
    return '|'.join(classes), '|'.join(funcs), '|'.join(imports), '|'.join(logic)

def tsv_preview(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f, delimiter='\t')
            header = next(reader)[:MAX_TSV_COLS]
            sample_rows = []
            for i, row in enumerate(reader):
                if i < MAX_TSV_PREVIEW_ROWS:
                    sample_rows.append(';'.join(row[:3]))
                else:
                    break
            return f"{len(header)} cols: {','.join(header)} | sample: {' | '.join(sample_rows)}"[:150]
    except:
        return ""

def log_preview(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            last5 = ' '.join(l.strip()[:50] for l in lines[-5:])
            errors = [l.strip()[:60] for l in lines if 'error' in l.lower()]
            return f"last: {last5[:100]} | err: {';'.join(errors[:2])}"[:120]
    except:
        return ""

def txt_preview(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            first3 = [f.readline().strip()[:60] for _ in range(3)]
            return ' | '.join(first3)[:120]
    except:
        return ""

def summarize_market_data(market_path):
    """Returns list of TSV rows summarizing market_data subfolders."""
    rows = []
    if not os.path.exists(market_path):
        return rows
    for sub in sorted(os.listdir(market_path)):
        sub_path = os.path.join(market_path, sub)
        if not os.path.isdir(sub_path):
            continue
        files = [f for f in os.listdir(sub_path) if f.endswith('.tsv')]
        if not files:
            continue
        total_size = sum(os.path.getsize(os.path.join(sub_path, f)) for f in files)
        sample_files = files[:MARKET_DATA_SUMMARY_FILES]
        rows.append([
            f"market_data/{sub}",
            "DIR_SUMMARY",
            fmt_size(total_size),
            f"{len(files)} TSV files",
            "", "", "", "",  # py fields empty
            f"examples: {', '.join(sample_files)}",
            "", ""
        ])
    return rows

def scan():
    all_rows = []
    market_rows = []
    other_rows = []
    market_data_path = os.path.join(TARGET_DIR, 'market_data')

    # First, summarize market_data folder (no per-file rows)
    if os.path.exists(market_data_path):
        market_rows = summarize_market_data(market_data_path)

    # Now walk the rest, but skip market_data folder entirely to avoid repetition
    for root, dirs, files in os.walk(TARGET_DIR):
        # Skip market_data folder and other skip dirs
        if root == market_data_path or market_data_path in root:
            continue
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS and not d.startswith('.')]
        for fname in files:
            if fname in SKIP_FILES:
                continue
            fpath = os.path.join(root, fname)
            rel = os.path.relpath(fpath, TARGET_DIR)
            ext = os.path.splitext(fname)[1].lower()
            size = os.path.getsize(fpath)
            mtime = datetime.datetime.fromtimestamp(os.path.getmtime(fpath)).strftime('%Y-%m-%d %H:%M')

            # Defaults
            classes=funcs=imports=logic=''
            tsv_info=log_info=txt_info=''

            if ext == '.py':
                try:
                    with open(fpath, 'r', encoding='utf-8', errors='ignore') as f:
                        lines = f.readlines()[:MAX_PY_LINES]
                    classes, funcs, imports, logic = extract_py_details(lines)
                except:
                    pass
            elif ext == '.tsv':
                tsv_info = tsv_preview(fpath)
            elif ext == '.log':
                log_info = log_preview(fpath)
            elif ext == '.txt':
                txt_info = txt_preview(fpath)

            other_rows.append([
                rel, ext[1:] if ext else 'txt', fmt_size(size), mtime,
                classes, funcs, imports, logic,
                tsv_info, log_info, txt_info
            ])

    # Limit other files to 100 rows (to keep output small)
    if len(other_rows) > 100:
        other_rows = other_rows[:100]

    # Combine: market summary rows first, then other files
    all_rows = market_rows + other_rows

    # Write TSV
    with open(OUTPUT_TSV, 'w', encoding='utf-8', newline='') as tsvfile:
        writer = csv.writer(tsvfile, delimiter='\t')
        writer.writerow(['FilePath', 'Type', 'Size', 'Modified',
                         'Classes', 'Functions', 'Imports', 'KeyLogic',
                         'TSV_Preview', 'Log_Preview', 'TXT_Preview'])

        for row in all_rows:
            writer.writerow(row)

    print(f"✅ Balanced summary saved: {OUTPUT_TSV} ({os.path.getsize(OUTPUT_TSV)//1024} KB) - {len(all_rows)} rows (market summarized)")

if __name__ == "__main__":
    scan()