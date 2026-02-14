#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sincroniza horas trabajadas desde Hubstaff API v2.
Genera hubstaff_hours.json con horas reales por chatter.

Requiere: HUBSTAFF_REFRESH_TOKEN en env o hubstaff_token.json
"""

import json
import os
import sys
from datetime import datetime

import requests

# Config
ORG_ID = 580385  # Chatting Wizard ESP
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TOKEN_PATH = os.path.join(SCRIPT_DIR, 'hubstaff_token.json')
OUTPUT_PATH = os.path.join(SCRIPT_DIR, 'hubstaff_hours.json')

# Name mapping: Hubstaff name -> Inflow/Dashboard name
# (Some names differ between platforms)
NAME_MAP = {
    # Nombres que difieren significativamente
    'Moises Anzola': 'Aaron Moises Anzola',
    'brayli abel acevedo lora': 'Brayli',
    'Saul Gutierrez': 'Eli Saul Gutierrez',
    'Eli Gutierrez': 'Eli Jose',
    'jose figueroa': 'Jose figueroa',
    'Wilmer Marquinez': 'Wilmer Jose',
    'José Parada': 'Jose Parada',
    'José Romero': 'Jose Romero',
    'Jose Loepz': 'Jose Lopez',
    'Omaris Marte': 'Omaris',
    'Omar Daniel Vallenilla Gonzalez': 'Omar Vallenilla',
    'Gabriel Alejandro Antunez villasmil': 'Gabriel Antunez',
    'Albert Arapé': 'Albert  Arape',
    'Breiner Pérez': 'Breiner Perez',
    'carlos mistre': 'Carlos Mistre Millan',
    'Diego Castillo': 'Diego Castillo Garcia',
    'jeremy yepez': 'Jeremy Adrian Yepez',
    'Jean Meléndez': 'Jean Melendez',
    'Jesús López': 'Jesus Lopez',
    'Jesus Alberto Acuña Arias': 'Jesus Alberto Acuna',
    'luna chacon': 'Luna Chacon',
    'marcelo payares': 'Marcelo Payares',
    'carolina veron': 'Carolina Veron',
    'Neyker Porras': 'Neyker',
    'Sebastián González': 'Sebastian Manuel Gonzalez',
    'Gustavo Angel': 'Gustavo Maldonado',
    'Humberto Enrique Balza Ajunta': 'Enrique  Balza',
    'Frangel Lopez': 'Frangel',
    'Freinman Vizcaya': 'Freinman',
    'Yudersis Bello': 'Yudersis',
    'Ricardo Silva': 'Ricardo Silva Moron',
    'Luis Gonzalez': 'Luis Guillermo Gonzalez Dubin',
    'Jesus Andara': 'Jesus',
    'Kevin Chaves': 'Kevin Chaves',
    'Cristian Folasco': 'Christian Silva',
    'Andrew Marcano': 'Andrew Marcano',
    # Nombres que coinciden directamente (sin acentos/tildes)
    'Hilary Molina': 'Hilary Molina',
    'Cristian Da Silveira': 'Cristian Da Silveira',
    'Dioskelvid Alvarado': 'Dioskelvid Alvarado',
    'Leonel Calderon': 'Leonel Calderon',
    'Herickson Perez': 'Herickson Perez',
    'Neyber Chacon': 'Neyber Chacon',
    'Darwins Rodriguez': 'Darwins Rodriguez',
}


def get_access_token(refresh_token):
    """Exchange refresh token for access token via OpenID Connect."""
    disc = requests.get('https://account.hubstaff.com/.well-known/openid-configuration').json()
    token_endpoint = disc['token_endpoint']

    resp = requests.post(token_endpoint, data={
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
    })

    if resp.status_code != 200:
        print("ERROR: Token exchange failed: %d %s" % (resp.status_code, resp.text[:200]))
        sys.exit(1)

    data = resp.json()
    return data['access_token'], data.get('refresh_token', refresh_token)


def load_refresh_token():
    """Load refresh token from env or file."""
    rt = os.environ.get('HUBSTAFF_REFRESH_TOKEN')
    if rt:
        return rt

    if os.path.exists(TOKEN_PATH):
        with open(TOKEN_PATH, 'r') as f:
            data = json.load(f)
            return data.get('refresh_token', '')

    print("ERROR: No refresh token found. Set HUBSTAFF_REFRESH_TOKEN env var or create hubstaff_token.json")
    sys.exit(1)


def save_refresh_token(new_rt):
    """Save updated refresh token for next use."""
    with open(TOKEN_PATH, 'w') as f:
        json.dump({'refresh_token': new_rt, 'updated_at': datetime.now().isoformat()}, f, indent=2)


def get_org_members(headers):
    """Get all active members in the organization."""
    members = []
    page = None
    while True:
        params = {'page_limit': 100}
        if page:
            params['page_start_id'] = page
        r = requests.get('https://api.hubstaff.com/v2/organizations/%d/members' % ORG_ID,
                         headers=headers, params=params)
        data = r.json()
        members.extend(data.get('members', []))
        page = data.get('pagination', {}).get('next_page_start_id')
        if not page:
            break
    return [m for m in members if m.get('membership_status') == 'active']


def get_user_details(headers, user_ids):
    """Get user name and email for each user_id."""
    users = {}
    for uid in user_ids:
        r = requests.get('https://api.hubstaff.com/v2/users/%d' % uid, headers=headers)
        if r.status_code == 200:
            u = r.json().get('user', {})
            users[uid] = {
                'name': u.get('name', ''),
                'email': u.get('email', ''),
            }
    return users


def get_daily_activities(headers, start_date, end_date):
    """Get daily activities (tracked seconds) for all members in date range."""
    activities = []
    page = None
    while True:
        params = {
            'date[start]': start_date,
            'date[stop]': end_date,
            'page_limit': 100,
        }
        if page:
            params['page_start_id'] = page
        r = requests.get('https://api.hubstaff.com/v2/organizations/%d/activities/daily' % ORG_ID,
                         headers=headers, params=params)
        if r.status_code != 200:
            print("ERROR: Activities API: %d %s" % (r.status_code, r.text[:200]))
            break
        data = r.json()
        activities.extend(data.get('daily_activities', []))
        page = data.get('pagination', {}).get('next_page_start_id')
        if not page:
            break
    return activities


def map_hubstaff_to_inflow(hubstaff_name):
    """Map Hubstaff user name to Inflow chatter name."""
    if hubstaff_name in NAME_MAP:
        return NAME_MAP[hubstaff_name]
    # Try case-insensitive match
    for k, v in NAME_MAP.items():
        if k.lower() == hubstaff_name.lower():
            return v
    # Default: return as-is (might match directly)
    return hubstaff_name


def main(start_date='2026-02-01', end_date='2026-02-13'):
    print("Hubstaff Sync: %s to %s" % (start_date, end_date))

    # Auth
    refresh_token = load_refresh_token()
    access_token, new_refresh_token = get_access_token(refresh_token)
    save_refresh_token(new_refresh_token)
    headers = {'Authorization': 'Bearer ' + access_token}
    print("  Autenticado OK")

    # Get members
    members = get_org_members(headers)
    user_ids = [m['user_id'] for m in members]
    print("  Miembros activos: %d" % len(user_ids))

    # Get user details
    print("  Obteniendo nombres de usuarios...")
    users = get_user_details(headers, user_ids)
    print("  Usuarios cargados: %d" % len(users))

    # Get daily activities
    print("  Descargando actividades diarias...")
    activities = get_daily_activities(headers, start_date, end_date)
    print("  Actividades: %d entradas" % len(activities))

    # Aggregate: total tracked seconds per user
    user_hours = {}
    user_daily = {}
    for act in activities:
        uid = act['user_id']
        tracked = act.get('tracked', 0)  # seconds
        date = act.get('date', '')

        if uid not in user_hours:
            user_hours[uid] = 0
            user_daily[uid] = {}
        user_hours[uid] += tracked
        user_daily[uid][date] = user_daily[uid].get(date, 0) + tracked

    # Build output: map to Inflow names
    output = {
        'period': {'start': start_date, 'end': end_date},
        'generated_at': datetime.now().isoformat(),
        'chatters': {},
    }

    unmatched = []
    for uid, total_seconds in sorted(user_hours.items(), key=lambda x: x[1], reverse=True):
        user = users.get(uid, {})
        hubstaff_name = user.get('name', 'Unknown')
        inflow_name = map_hubstaff_to_inflow(hubstaff_name)
        total_minutes = round(total_seconds / 60, 1)
        total_hours = round(total_seconds / 3600, 2)

        daily = {}
        for date, secs in sorted(user_daily.get(uid, {}).items()):
            daily[date] = round(secs / 60, 1)  # minutes per day

        output['chatters'][inflow_name] = {
            'hubstaff_name': hubstaff_name,
            'hubstaff_user_id': uid,
            'email': user.get('email', ''),
            'total_minutes': total_minutes,
            'total_hours': total_hours,
            'daily_minutes': daily,
        }

        if hubstaff_name == inflow_name and hubstaff_name not in NAME_MAP.values():
            unmatched.append(hubstaff_name)

    # Write output
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # Summary
    print("\n=== RESUMEN ===")
    for name, data in sorted(output['chatters'].items(), key=lambda x: x[1]['total_hours'], reverse=True)[:15]:
        print("  %-25s %6.1fh (%s)" % (name[:25], data['total_hours'], data['hubstaff_name'][:25]))

    if unmatched:
        print("\n=== SIN MAPEO (nombres Hubstaff sin equivalente en NAME_MAP) ===")
        for n in sorted(unmatched):
            print("  - %s" % n)

    print("\nExportado: %s (%d chatters)" % (OUTPUT_PATH, len(output['chatters'])))
    return True


if __name__ == '__main__':
    main()
