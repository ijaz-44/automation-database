#!/usr/bin/env python3
"""
list_e_modules.py – List all E modules (group_e) in the project.
Run: python list_e_modules.py
"""

import os
import re

def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    group_e_dir = os.path.join(base_dir, "Groups", "group_e")
    
    if not os.path.exists(group_e_dir):
        print(f"[ERROR] group_e directory not found at: {group_e_dir}")
        return
    
    # Pattern for E module files: E<two digits>_<anything>.py
    pattern = re.compile(r'^(E\d{2}_.+)\.py$')
    modules = []
    
    for filename in os.listdir(group_e_dir):
        m = pattern.match(filename)
        if m:
            module_name = m.group(1)  # e.g., "E01_candles_expert"
            modules.append(module_name)
    
    modules.sort()
    
    print("\n" + "=" * 50)
    print("E Modules Found in group_e/")
    print("=" * 50)
    for idx, mod in enumerate(modules, 1):
        print(f"{idx:2}. {mod}")
    print("=" * 50)
    print(f"Total: {len(modules)} modules")

if __name__ == "__main__":
    main()