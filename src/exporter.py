# src/exporter.py

import json
from config.settings import OUTPUT_FILE

def save_to_json(data):
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
