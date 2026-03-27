import os

# --- CONFIG ---
TARGET_DIR = '/storage/emulated/0/Android/data/org.qpython.qpy3/My Automation'
OUTPUT_FILE = os.path.join(TARGET_DIR, 'project_short_logic.txt')

# In cheezon ko bilkul ignore karna hai (Size bachane ke liye)
IGNORE_KEYWORDS = ['<!DOCTYPE', '<style>', '<script>', 'color:', 'padding:', 'margin:']

def super_summarize():
    if not os.path.exists(TARGET_DIR):
        print("❌ Path missing!")
        return

    output = ["=== CORE LOGIC & ARCHITECTURE SUMMARY ===\n"]

    for root, dirs, files in os.walk(TARGET_DIR):
        dirs[:] = [d for d in dirs if not d.startswith('.') and d != '__pycache__']
        
        for file in files:
            # Sirf Python aur JSON files
            if file.endswith(('.py', '.json')) and 'result' not in file:
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, TARGET_DIR)
                
                output.append(f"\n📂 FILE: {rel_path}")
                output.append("-" * 25)
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                        
                        for line in lines:
                            clean_line = line.strip()
                            
                            # 1. Skip UI/HTML/CSS lines
                            if any(k in clean_line for k in IGNORE_KEYWORDS):
                                continue
                            
                            # 2. Capture Structure (Classes/Functions/Imports)
                            if clean_line.startswith(('def ', 'class ', 'import ', 'from ')):
                                output.append(clean_line)
                            
                            # 3. Capture Trading Logic (Indicators & Scoring)
                            # Ye lines apke strategy ka nichor (essence) hain
                            logic_keys = ['rsi', 'ema', 'sma', 'wma', 'macd', 'bollinger', 
                                         'upper_band', 'lower_band', 'score', 'signal', 
                                         'binary', 'spot', 'market', 'trend', 'cross']
                            
                            if any(k in clean_line.lower() for k in logic_keys):
                                if '=' in clean_line and not clean_line.startswith('#'):
                                    # Sirf kaam ki logic line (max 100 chars)
                                    output.append(f"  >> {clean_line[:100]}")

                except Exception as e:
                    output.append(f"⚠️ Error: {str(e)[:30]}")
                
                output.append("="*15)

    # Save to file
    final_text = "\n".join(output)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as out_f:
        out_f.write(final_text)
    
    print(f"✅ Summary Done! Size: {len(final_text)} characters.")
    print(f"📂 Location: {OUTPUT_FILE}")
    print("\n Copy and Paste Data.")

if __name__ == "__main__":
    super_summarize()