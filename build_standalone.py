#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Genera un HTML standalone con los datos embebidos para compartir.
"""

import json
import os

HTML_PATH = r'c:\Users\carlo\Carlos Ribas Cursor Projects\chatters-dashboard\index.html'
JSON_PATH = r'c:\Users\carlo\Carlos Ribas Cursor Projects\chatters-dashboard\dashboard_data.json'
OUTPUT_PATH = r'c:\Users\carlo\Carlos Ribas Cursor Projects\chatters-dashboard\Chatters_Dashboard_Feb12_2026.html'

with open(JSON_PATH, 'r', encoding='utf-8') as f:
    data = json.load(f)

json_minified = json.dumps(data, ensure_ascii=False, separators=(',', ':'))

with open(HTML_PATH, 'r', encoding='utf-8') as f:
    html = f.read()

# New HTML uses: fetch('dashboard_data.json') with fallback to window.__EMBEDDED_DATA__
# Replace the loadData function to use embedded data directly
old_load = """async function loadData() {
  try {
    const r = await fetch('dashboard_data.json');
    D = await r.json();
  } catch(e) {
    D = window.__EMBEDDED_DATA__;
  }
  if (!D) {
    document.querySelector('.container').innerHTML = '<div style="padding:40px;text-align:center;color:var(--accent-red)">Error cargando datos</div>';
    return;
  }
  render();
}"""

new_load = "async function loadData() {\n  D = " + json_minified + ";\n  render();\n}"

html = html.replace(old_load, new_load)

with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
    f.write(html)

size_kb = os.path.getsize(OUTPUT_PATH) / 1024
print("Archivo generado: %s" % OUTPUT_PATH)
print("Tamano: %d KB" % size_kb)
