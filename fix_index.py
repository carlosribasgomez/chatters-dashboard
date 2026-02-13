#!/usr/bin/env python3
import re

path = r'c:\Users\carlo\Carlos Ribas Cursor Projects\chatters-dashboard\index.html'
with open(path, 'r', encoding='utf-8') as f:
    html = f.read()

# Replace loadData with fetch version
pattern = r'async function loadData\(\)\s*\{.*?render\(\);\s*\}'
replacement = """async function loadData() {
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

new_html, count = re.subn(pattern, replacement, html, count=1, flags=re.DOTALL)
print('Replacements made:', count)

with open(path, 'w', encoding='utf-8') as f:
    f.write(new_html)
print('index.html restored to fetch version')
