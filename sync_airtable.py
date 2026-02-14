#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sincroniza modelos Live desde Airtable y actualiza airtable_model_types.json.
Se ejecuta via GitHub Action diariamente o manualmente.

Requiere variable de entorno: AIRTABLE_PAT
"""

import json
import os
import sys

import requests

# Airtable config
BASE_ID = 'appA44xNGmua0JMoZ'
TBL_MODELO = 'tblbb6vMPQLNzqWdJ'
FIELDS = ['Nombre Artístico', 'Tipo de Página', 'Estado']

# Output path (relative to repo root)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(SCRIPT_DIR, 'airtable_model_types.json')


def fetch_airtable_models(pat):
    """Fetch all records from the Modelo table in Airtable."""
    headers = {'Authorization': 'Bearer ' + pat}
    url = 'https://api.airtable.com/v0/%s/%s' % (BASE_ID, TBL_MODELO)
    params = {'fields[]': FIELDS}
    
    records = []
    while True:
        resp = requests.get(url, headers=headers, params=params)
        if resp.status_code != 200:
            print("Error Airtable API: %d %s" % (resp.status_code, resp.text[:200]))
            sys.exit(1)
        data = resp.json()
        records.extend(data.get('records', []))
        offset = data.get('offset')
        if not offset:
            break
        params['offset'] = offset
    
    return records


def classify_models(records):
    """Filter Live models and classify by account type."""
    live_records = [r for r in records if r.get('fields', {}).get('Estado') == 'Live']
    print('Modelos totales en Airtable: %d' % len(records))
    print('Modelos LIVE: %d' % len(live_records))
    
    output = {}
    for r in live_records:
        f = r.get('fields', {})
        nombre = f.get('Nombre Artístico', '')
        tipo_pagina = f.get('Tipo de Página', '')
        
        if not nombre:
            continue
        
        if tipo_pagina:
            tp_lower = tipo_pagina.lower()
            if 'gratu' in tp_lower or 'free' in tp_lower:
                account_type = 'free'
            elif 'pago' in tp_lower or 'paid' in tp_lower:
                account_type = 'paid'
            elif 'mixta' in tp_lower or 'mix' in tp_lower:
                account_type = 'mixta'
            else:
                account_type = 'unknown'
        else:
            account_type = 'unknown'
        
        output[nombre] = account_type
    
    return output


def main():
    pat = os.environ.get('AIRTABLE_PAT')
    if not pat:
        print("ERROR: AIRTABLE_PAT no definido en variables de entorno")
        sys.exit(1)
    
    print("Conectando a Airtable...")
    records = fetch_airtable_models(pat)
    model_types = classify_models(records)
    
    # Check for changes
    old_data = {}
    if os.path.exists(OUTPUT_PATH):
        with open(OUTPUT_PATH, 'r', encoding='utf-8') as f:
            old_data = json.load(f)
    
    if model_types == old_data:
        print("Sin cambios. El archivo esta actualizado.")
        return False
    
    # Show diff
    added = set(model_types.keys()) - set(old_data.keys())
    removed = set(old_data.keys()) - set(model_types.keys())
    changed = {k for k in model_types if k in old_data and model_types[k] != old_data[k]}
    
    if added:
        print("\n+++ Modelos nuevos: %s" % ', '.join(sorted(added)))
    if removed:
        print("\n--- Modelos eliminados: %s" % ', '.join(sorted(removed)))
    if changed:
        print("\n~~~ Modelos cambiados:")
        for k in sorted(changed):
            print("    %s: %s -> %s" % (k, old_data[k], model_types[k]))
    
    # Write updated file
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(model_types, f, ensure_ascii=False, indent=2)
    
    # Summary
    for t in ['free', 'paid', 'mixta', 'unknown']:
        count = len([v for v in model_types.values() if v == t])
        if count > 0:
            print("  %s: %d" % (t, count))
    
    print("\nActualizado: %s (%d modelos)" % (OUTPUT_PATH, len(model_types)))
    return True


if __name__ == '__main__':
    changed = main()
    # Set output for GitHub Actions
    github_output = os.environ.get('GITHUB_OUTPUT')
    if github_output:
        with open(github_output, 'a') as f:
            f.write("changed=%s\n" % ('true' if changed else 'false'))
