#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Genera un HTML standalone con los datos embebidos para compartir.
"""

import json
import os
import re

HTML_PATH = r'c:\Users\carlo\Carlos Ribas Cursor Projects\chatters-dashboard\index.html'
JSON_PATH = r'c:\Users\carlo\Carlos Ribas Cursor Projects\chatters-dashboard\dashboard_data.json'
OUTPUT_PATH = r'c:\Users\carlo\Carlos Ribas Cursor Projects\chatters-dashboard\Chatters_Dashboard_Feb12_2026.html'

with open(JSON_PATH, 'r', encoding='utf-8') as f:
    data = json.load(f)

json_minified = json.dumps(data, ensure_ascii=False, separators=(',', ':'))

with open(HTML_PATH, 'r', encoding='utf-8') as f:
    html = f.read()

# Replace loadData() function regardless of its current content
# Match: async function loadData() { ... render(); \n}
pattern = r'async function loadData\(\)\s*\{.*?render\(\);\s*\}'
replacement = "async function loadData() {\n  D = " + json_minified + ";\n  render();\n}"

new_html, count = re.subn(pattern, replacement, html, count=1, flags=re.DOTALL)

if count == 0:
    print("ERROR: No se encontro la funcion loadData() en index.html")
    exit(1)

with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
    f.write(new_html)

size_kb = os.path.getsize(OUTPUT_PATH) / 1024
print("Archivo generado: %s" % OUTPUT_PATH)
print("Tamano: %d KB" % size_kb)

# Verify account_type is in the output
at_count = new_html.count('account_type')
print("Verificacion: 'account_type' aparece %d veces en el HTML" % at_count)
