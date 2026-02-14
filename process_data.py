#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Procesa múltiples archivos Excel (Feb 1-13, 2026) y genera un JSON completo para el dashboard.
Fuentes (en pares: Feb 1-10 + Feb 11-13):
  1. Message Dashboard: mensajes individuales, tiempos, PPVs enviados/comprados
  2. Detailed Breakdown: ventas REALES por chatter x modelo (incluye PPVs de dias anteriores)
  3. Sales Record: transacciones individuales de venta (messages, subs, tips)
  4. Creator Statistics: stats de cada modelo (subs, new fans, LTV, etc.)
"""

import json
import re
import sys
from collections import defaultdict
from datetime import datetime

import pandas as pd

# ================================================================
# FILE PATHS - Multiple files per type (Feb 1-10 + Feb 11-13)
# ================================================================
MSG_DASHBOARDS = [
    r'c:\Users\carlo\Downloads\(Chatting_Wizard_ESP)Message_Dashboard_Report_20260214105148.xlsx',  # Feb 1-10
    r'c:\Users\carlo\Downloads\(Chatting_Wizard_ESP)Message_Dashboard_Report_20260214105609.xlsx',  # Feb 11-13
]
DETAILED_BREAKDOWNS = [
    r'c:\Users\carlo\Downloads\ae6f32e3-09ba-481f-b46b-36c02a2cf38b.xlsx',  # Feb 1-10
    r'c:\Users\carlo\Downloads\f991d2bc-3add-445e-801d-74598641ae9f.xlsx',  # Feb 11-13
]
SALES_RECORDS = [
    r'c:\Users\carlo\Downloads\1003a8dd-a1a5-4fa6-8e62-a303f693d75c.xlsx',  # Feb 1-10
    r'c:\Users\carlo\Downloads\df02fd11-3319-4dcb-92b9-a6f748cd15f6.xlsx',  # Feb 11-13
]
CREATOR_STATS_FILES = [
    r'c:\Users\carlo\Downloads\35b5ab04-d615-4827-85b5-708e8aa279e3.xlsx',  # Feb 1-10
    r'c:\Users\carlo\Downloads\5c62220a-3446-476e-8167-9cf49cfe3cda.xlsx',  # Feb 11-13
]

OUTPUT_PATH = r'c:\Users\carlo\Carlos Ribas Cursor Projects\chatters-dashboard\dashboard_data.json'
AIRTABLE_TYPES_PATH = r'c:\Users\carlo\Carlos Ribas Cursor Projects\chatters-dashboard\airtable_model_types.json'

REPORT_START = 'Feb 1, 2026'
REPORT_END = 'Feb 13, 2026'


# ================================================================
# UTILITY FUNCTIONS
# ================================================================
def parse_dollar(v):
    if not v or v == '-':
        return 0.0
    return float(str(v).replace('$', '').replace(',', ''))


def parse_pct(v):
    if not v or v == '-':
        return 0.0
    return float(str(v).replace('%', '').replace(',', ''))


def parse_replay_seconds(rt_str):
    if not rt_str or not isinstance(rt_str, str) or rt_str.strip() == '' or rt_str.strip() == '-':
        return None
    total = 0
    h = re.search(r'(\d+)h', rt_str)
    m = re.search(r'(\d+)m', rt_str)
    s = re.search(r'(\d+)s', rt_str)
    if h:
        total += int(h.group(1)) * 3600
    if m:
        total += int(m.group(1)) * 60
    if s:
        total += int(s.group(1))
    return total if total > 0 or (m or s or h) else None


def fmt_time(seconds):
    if seconds is None or (isinstance(seconds, float) and (seconds != seconds or seconds == 0)):
        return "N/A"
    try:
        secs = int(seconds)
    except (ValueError, TypeError):
        return "N/A"
    if secs == 0:
        return "0s"
    m, s = divmod(secs, 60)
    h, m = divmod(m, 60)
    if h > 0:
        return "%dh %dm %ds" % (h, m, s)
    return "%dm %ds" % (m, s)


def parse_hours_minutes(v):
    """Parse '7h 3min' or '0min' to minutes."""
    if not v or v == '-' or str(v).strip() == '0min':
        return 0
    s = str(v)
    total = 0
    h = re.search(r'(\d+)h', s)
    m = re.search(r'(\d+)min', s)
    if h:
        total += int(h.group(1)) * 60
    if m:
        total += int(m.group(1))
    return total


def get_shift(hour):
    """Return shift name based on hour (0-23)."""
    if 0 <= hour < 8:
        return 'turno1'  # 12 AM - 8 AM
    elif 8 <= hour < 16:
        return 'turno2'  # 8 AM - 4 PM
    else:
        return 'turno3'  # 4 PM - 11:59 PM


SHIFT_LABELS = {
    'turno1': '12:00 AM - 8:00 AM',
    'turno2': '8:00 AM - 4:00 PM',
    'turno3': '4:00 PM - 11:59 PM',
}


# ================================================================
# MULTI-FILE LOADING WITH DEDUPLICATION
# ================================================================
def load_and_concat(file_list, sheet_name, label, dedup_cols=None):
    """Load multiple Excel files, concatenate, and deduplicate."""
    frames = []
    for path in file_list:
        df = pd.read_excel(path, sheet_name=sheet_name)
        print("   %s: %d filas (%s)" % (label, len(df), path.split('\\')[-1][:40]))
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    total_before = len(combined)

    if dedup_cols:
        combined = combined.drop_duplicates(subset=dedup_cols, keep='first')

    total_after = len(combined)
    dupes = total_before - total_after
    if dupes > 0:
        print("   -> Duplicados eliminados: %d (de %d a %d)" % (dupes, total_before, total_after))
    else:
        print("   -> Total combinado: %d filas (0 duplicados)" % total_after)

    return combined


def load_creator_stats(file_list):
    """Load Creator Statistics from multiple files and combine.
    Revenue fields are summed. Snapshot fields use the latest file's values.
    """
    all_summary = []
    all_detail = []
    for path in file_list:
        df_summary = pd.read_excel(path, sheet_name='Creator Statistics')
        print("   CreatorStats summary: %d modelos (%s)" % (len(df_summary), path.split('\\')[-1][:40]))
        all_summary.append(df_summary)
        try:
            df_detail = pd.read_excel(path, sheet_name='Creator Statistics Detail')
            all_detail.append(df_detail)
            print("   CreatorStats detail: %d filas" % len(df_detail))
        except Exception:
            pass

    # Combine summary data: sum revenue fields, take latest for snapshot fields
    cs_data = {}
    # Process files in order (earliest first, latest last)
    for df in all_summary:
        for _, row in df.iterrows():
            name = row['Creator']
            if name not in cs_data:
                cs_data[name] = {
                    # Revenue fields (will be summed)
                    'subscription_net': 0, 'new_subs_net': 0, 'recurring_subs_net': 0,
                    'tips_net': 0, 'total_earnings_net': 0, 'message_net': 0,
                    # Snapshot fields (will be overwritten by latest)
                    'contribution_pct': 0, 'of_ranking': 0, 'following': 0,
                    'fans_renew_on': 0, 'renew_on_pct': 0, 'new_fans': 0,
                    'active_fans': 0, 'expired_fans_change': 0, 'group': '',
                    'avg_spend_per_spender': 0, 'avg_spend_per_tx': 0,
                    'avg_earnings_per_fan': 0, 'avg_sub_length': 'N/A',
                }
            cs = cs_data[name]
            # SUM revenue fields across periods
            cs['subscription_net'] += parse_dollar(row['Subscription Net'])
            cs['new_subs_net'] += parse_dollar(row['New subscriptions Net'])
            cs['recurring_subs_net'] += parse_dollar(row['Recurring subscriptions Net'])
            cs['tips_net'] += parse_dollar(row['Tips Net'])
            cs['total_earnings_net'] += parse_dollar(row['Total earnings Net'])
            cs['message_net'] += parse_dollar(row['Message Net'])
            # SUM fan movement fields
            cs['new_fans'] += int(row['New fans']) if pd.notna(row['New fans']) else 0
            cs['expired_fans_change'] += int(row['Change in expired fan count']) if pd.notna(row['Change in expired fan count']) else 0
            # LATEST snapshot fields (overwrite each iteration -> last file wins)
            cs['contribution_pct'] = parse_pct(row['Contribution %']) if pd.notna(row['Contribution %']) else cs['contribution_pct']
            cs['of_ranking'] = parse_pct(row['OF ranking']) if pd.notna(row['OF ranking']) else cs['of_ranking']
            cs['following'] = int(row['Following']) if pd.notna(row['Following']) else cs['following']
            cs['fans_renew_on'] = int(row['Fans with renew on']) if pd.notna(row['Fans with renew on']) else cs['fans_renew_on']
            cs['renew_on_pct'] = parse_pct(row['Renew on %']) if pd.notna(row['Renew on %']) else cs['renew_on_pct']
            cs['active_fans'] = int(row['Active fans']) if pd.notna(row['Active fans']) else cs['active_fans']
            cs['group'] = str(row['Creator group']) if pd.notna(row['Creator group']) else cs['group']
            cs['avg_spend_per_spender'] = parse_dollar(row['Avg spend per spender Net'])
            cs['avg_spend_per_tx'] = parse_dollar(row['Avg spend per transaction Net'])
            cs['avg_earnings_per_fan'] = parse_dollar(row['Avg earnings per fan Net'])
            cs['avg_sub_length'] = str(row['Avg subscription length']) if pd.notna(row['Avg subscription length']) else cs['avg_sub_length']

    # Round revenue fields
    for name in cs_data:
        for k in ['subscription_net', 'new_subs_net', 'recurring_subs_net', 'tips_net', 'total_earnings_net', 'message_net']:
            cs_data[name][k] = round(cs_data[name][k], 2)

    return cs_data, all_detail


# ================================================================
# MAIN
# ================================================================
def main():
    # Load Airtable model types (free/paid/mixta classification)
    with open(AIRTABLE_TYPES_PATH, 'r', encoding='utf-8') as f:
        airtable_types = json.load(f)
    print("Airtable types loaded: %d modelos" % len(airtable_types))

    # ================================================================
    # 1. LOAD MESSAGE DASHBOARDS (Feb 1-10 + Feb 11-13)
    # ================================================================
    print("\n1/4 Leyendo Message Dashboards...")
    df_msg = load_and_concat(
        MSG_DASHBOARDS, 'Message Dashboard', 'MsgDash',
        dedup_cols=['Sender', 'Creator', 'Sent time', 'Sent date', 'Price', 'Source']
    )

    df_msg['Price_num'] = pd.to_numeric(df_msg['Price'], errors='coerce').fillna(0)
    df_msg['is_ppv'] = df_msg['Price_num'] > 0
    df_msg['is_purchased'] = df_msg['Purchased'].astype(str).str.lower() == 'yes'
    df_msg['Hour'] = df_msg['Sent time'].apply(
        lambda x: int(str(x).split(':')[0]) if x and ':' in str(x) else None
    )
    df_msg['Shift'] = df_msg['Hour'].apply(lambda h: get_shift(h) if pd.notna(h) else None)
    df_msg['Replay_seconds'] = df_msg['Replay time'].apply(parse_replay_seconds)

    # Parse date for daily breakdown
    df_msg['Date'] = pd.to_datetime(df_msg['Sent date'], errors='coerce')

    # ================================================================
    # 2. LOAD DETAILED BREAKDOWNS (Feb 1-10 + Feb 11-13)
    # ================================================================
    print("\n2/4 Leyendo Detailed Breakdowns...")
    df_db = load_and_concat(
        DETAILED_BREAKDOWNS, 'Detailed breakdown', 'DetailBrkdn',
        dedup_cols=['Date/Time Africa/Monrovia', 'Employees', 'Creators']
    )

    df_db['Sales_num'] = df_db['Sales'].apply(parse_dollar)
    df_db['PPVs_sent'] = pd.to_numeric(df_db['Direct PPVs sent'], errors='coerce').fillna(0).astype(int)
    df_db['PPVs_unlocked'] = pd.to_numeric(df_db['PPVs unlocked'], errors='coerce').fillna(0).astype(int)
    df_db['Msgs_sent'] = pd.to_numeric(df_db['Direct messages sent'], errors='coerce').fillna(0).astype(int)
    df_db['GR_pct'] = df_db['Golden ratio'].apply(parse_pct)
    df_db['UR_pct'] = df_db['Unlock rate'].apply(parse_pct)
    df_db['Fans_chatted'] = pd.to_numeric(df_db['Fans chatted'], errors='coerce').fillna(0).astype(int)
    df_db['Fans_spent'] = pd.to_numeric(df_db['Fans who spent money'], errors='coerce').fillna(0).astype(int)
    df_db['Fan_CVR'] = df_db['Fan CVR'].apply(parse_pct)

    # Handle column name variations for Response time
    resp_col = None
    for col_name in df_db.columns:
        if 'response time' in col_name.lower():
            resp_col = col_name
            break
    df_db['Resp_time_str'] = df_db[resp_col].fillna('') if resp_col else ''
    df_db['Resp_seconds'] = df_db['Resp_time_str'].apply(parse_replay_seconds)

    # Handle Clocked hours column
    clocked_col = None
    for col_name in df_db.columns:
        if 'clocked' in col_name.lower() or 'scheduled' in col_name.lower():
            clocked_col = col_name
            break
    df_db['Clocked_min'] = df_db[clocked_col].apply(parse_hours_minutes) if clocked_col else 0

    df_db['Sales_per_hour'] = df_db['Sales per hour'].apply(parse_dollar)
    df_db['Msgs_per_hour'] = pd.to_numeric(df_db['Messages sent per hour'], errors='coerce').fillna(0)
    df_db['Char_count'] = pd.to_numeric(df_db['Character count'], errors='coerce').fillna(0).astype(int)
    df_db['Avg_earn_per_spender'] = df_db['Avg earnings per fan who spent money'].apply(parse_dollar)

    # Parse date
    date_col_db = [c for c in df_db.columns if 'date' in c.lower() and 'time' in c.lower()]
    df_db['Date'] = pd.to_datetime(df_db[date_col_db[0]], errors='coerce') if date_col_db else pd.NaT

    # ================================================================
    # 3. LOAD SALES RECORDS (Feb 1-10 + Feb 11-13)
    # ================================================================
    print("\n3/4 Leyendo Sales Records...")
    df_sales = load_and_concat(
        SALES_RECORDS, 'Sales record', 'SalesRec',
        dedup_cols=None  # Each transaction is unique
    )

    # Rename columns
    date_col = [c for c in df_sales.columns if 'date' in c.lower() and 'time' in c.lower()]
    if date_col:
        df_sales.rename(columns={date_col[0]: 'DateTime'}, inplace=True)

    df_sales['Earnings'] = df_sales['Earnings'].apply(parse_dollar) if 'Earnings' in df_sales.columns else 0
    df_sales['Gross'] = df_sales['Gross revenue'].apply(parse_dollar) if 'Gross revenue' in df_sales.columns else 0
    df_sales['Net'] = df_sales['Net revenue'].apply(parse_dollar) if 'Net revenue' in df_sales.columns else 0
    df_sales['Hour'] = pd.to_datetime(df_sales['DateTime'], errors='coerce').dt.hour
    df_sales['Shift'] = df_sales['Hour'].apply(lambda h: get_shift(h) if pd.notna(h) else None)
    df_sales['Date'] = pd.to_datetime(df_sales['DateTime'], errors='coerce').dt.date

    # Deduplicate sales by all identifying columns
    sales_before = len(df_sales)
    df_sales = df_sales.drop_duplicates(subset=['DateTime', 'Employee', 'Creator', 'Fan', 'Net revenue', 'Type'], keep='first')
    sales_dupes = sales_before - len(df_sales)
    if sales_dupes > 0:
        print("   -> Sales duplicados eliminados: %d" % sales_dupes)
    print("   -> Total transacciones: %d" % len(df_sales))

    # Filter out reverses for revenue calculations
    df_sales_valid = df_sales[df_sales['Status'] != 'Reverse'].copy()

    # ================================================================
    # 4. LOAD CREATOR STATISTICS (Feb 1-10 + Feb 11-13, combined)
    # ================================================================
    print("\n4/4 Leyendo Creator Statistics...")
    cs_data, cs_details = load_creator_stats(CREATOR_STATS_FILES)
    print("   -> %d modelos combinados" % len(cs_data))

    # ================================================================
    # COMPUTE: General KPIs
    # ================================================================
    print("\nCalculando metricas (Feb 1-13, 2026)...")

    total_messages = len(df_msg)
    total_ppv_sent_msg = int(df_msg['is_ppv'].sum())
    total_ppv_purchased_msg = int((df_msg['is_ppv'] & df_msg['is_purchased']).sum())

    # Number of unique days in data
    unique_dates = df_msg['Date'].dt.date.dropna().nunique()
    print("   Dias en el rango: %d" % unique_dates)

    # REAL revenue from sales record
    total_net_revenue = round(df_sales_valid['Net'].sum(), 2)
    msg_revenue = round(df_sales_valid[df_sales_valid['Type'] == 'Messages']['Net'].sum(), 2)
    sub_revenue = round(df_sales_valid[df_sales_valid['Type'] == 'Subscription']['Net'].sum(), 2)
    tips_revenue = round(df_sales_valid[df_sales_valid['Type'].astype(str).str.startswith('Tips')]['Net'].sum(), 2)

    # From detailed breakdown (chatter-attributed sales)
    total_chatter_sales = round(df_db['Sales_num'].sum(), 2)
    total_ppv_sent_db = int(df_db['PPVs_sent'].sum())
    total_ppv_unlocked_db = int(df_db['PPVs_unlocked'].sum())
    total_fans_chatted = int(df_db['Fans_chatted'].sum())

    # From creator stats
    total_new_fans = sum(cs['new_fans'] for cs in cs_data.values() if cs['new_fans'] > 0)
    total_new_subs_revenue = round(sum(cs['new_subs_net'] for cs in cs_data.values()), 2)
    total_rec_subs_revenue = round(sum(cs['recurring_subs_net'] for cs in cs_data.values()), 2)
    total_active_fans = sum(cs['active_fans'] for cs in cs_data.values())

    # Response time from message dashboard
    rt_vals = df_msg['Replay_seconds'].dropna()
    avg_rt = round(float(rt_vals.mean()), 1) if len(rt_vals) > 0 else 0
    median_rt = round(float(rt_vals.median()), 1) if len(rt_vals) > 0 else 0

    # Golden/Unlock from detailed breakdown
    overall_gr = round(total_ppv_sent_db / total_messages * 100, 2) if total_messages > 0 else 0
    overall_ur = round(total_ppv_unlocked_db / total_ppv_sent_db * 100, 2) if total_ppv_sent_db > 0 else 0

    general = {
        'total_messages': total_messages,
        'total_net_revenue': total_net_revenue,
        'msg_revenue': msg_revenue,
        'sub_revenue': sub_revenue,
        'tips_revenue': tips_revenue,
        'new_subs_revenue': total_new_subs_revenue,
        'recurring_subs_revenue': total_rec_subs_revenue,
        'chatter_attributed_sales': total_chatter_sales,
        'total_ppv_sent': total_ppv_sent_db,
        'total_ppv_unlocked': total_ppv_unlocked_db,
        'total_fans_chatted': total_fans_chatted,
        'total_new_fans': total_new_fans,
        'total_active_fans': total_active_fans,
        'golden_ratio': overall_gr,
        'unlock_ratio': overall_ur,
        'avg_replay_seconds': avg_rt,
        'avg_replay_formatted': fmt_time(avg_rt),
        'median_replay_seconds': median_rt,
        'median_replay_formatted': fmt_time(median_rt),
        'total_chatters': df_db['Employees'].nunique(),
        'total_models': df_db['Creators'].nunique(),
        'days_in_range': unique_dates,
    }

    # ================================================================
    # COMPUTE: Hourly data (from message dashboard + sales record)
    # ================================================================
    hourly_data = []
    for hour in range(24):
        msg_h = df_msg[df_msg['Hour'] == hour]
        sales_h = df_sales_valid[df_sales_valid['Hour'] == hour]

        hourly_data.append({
            'hour': hour,
            'hour_label': '%02d:00' % hour,
            'shift': get_shift(hour),
            'messages': len(msg_h),
            'ppv_sent': int(msg_h['is_ppv'].sum()),
            'sales_net': round(float(sales_h['Net'].sum()), 2),
            'msg_sales_net': round(float(sales_h[sales_h['Type'] == 'Messages']['Net'].sum()), 2),
            'sub_sales_net': round(float(sales_h[sales_h['Type'] == 'Subscription']['Net'].sum()), 2),
            'tips_net': round(float(sales_h[sales_h['Type'].astype(str).str.startswith('Tips')]['Net'].sum()), 2),
            'transactions': len(sales_h),
        })

    peak_traffic = max(hourly_data, key=lambda x: x['messages'])
    peak_sales = max(hourly_data, key=lambda x: x['sales_net'])

    # ================================================================
    # COMPUTE: Daily data
    # ================================================================
    daily_data = []
    for date in sorted(df_msg['Date'].dt.date.dropna().unique()):
        msg_d = df_msg[df_msg['Date'].dt.date == date]
        sales_d = df_sales_valid[df_sales_valid['Date'] == date]
        rt_d = msg_d['Replay_seconds'].dropna()

        daily_data.append({
            'date': date.isoformat(),
            'date_label': date.strftime('%b %d'),
            'messages': len(msg_d),
            'sales_net': round(float(sales_d['Net'].sum()), 2),
            'msg_sales': round(float(sales_d[sales_d['Type'] == 'Messages']['Net'].sum()), 2),
            'sub_sales': round(float(sales_d[sales_d['Type'] == 'Subscription']['Net'].sum()), 2),
            'tips': round(float(sales_d[sales_d['Type'].astype(str).str.startswith('Tips')]['Net'].sum()), 2),
            'transactions': len(sales_d),
            'ppv_sent': int(msg_d['is_ppv'].sum()),
            'avg_replay_seconds': round(float(rt_d.mean()), 1) if len(rt_d) > 0 else 0,
        })

    # ================================================================
    # COMPUTE: Shift data
    # ================================================================
    shifts_data = {}
    for shift_key, shift_label in SHIFT_LABELS.items():
        msg_s = df_msg[df_msg['Shift'] == shift_key]
        sales_s = df_sales_valid[df_sales_valid['Shift'] == shift_key]
        rt_s = msg_s['Replay_seconds'].dropna()

        shift_hours = [h for h in hourly_data if h['shift'] == shift_key]

        # Top models in this shift by sales
        shift_model_sales = sales_s.groupby('Creator')['Net'].sum().sort_values(ascending=False).head(10)
        top_models_shift = [{'name': n, 'revenue': round(v, 2)} for n, v in shift_model_sales.items()]

        # Top chatters in this shift by sales
        shift_chatter_sales = sales_s[sales_s['Employee'].fillna('').astype(str).str.strip() != ''].groupby('Employee')['Net'].sum().sort_values(ascending=False).head(10)
        top_chatters_shift = [{'name': n, 'revenue': round(v, 2)} for n, v in shift_chatter_sales.items()]

        shifts_data[shift_key] = {
            'label': shift_label,
            'messages': len(msg_s),
            'sales_net': round(float(sales_s['Net'].sum()), 2),
            'msg_sales': round(float(sales_s[sales_s['Type'] == 'Messages']['Net'].sum()), 2),
            'sub_sales': round(float(sales_s[sales_s['Type'] == 'Subscription']['Net'].sum()), 2),
            'tips_sales': round(float(sales_s[sales_s['Type'].astype(str).str.startswith('Tips')]['Net'].sum()), 2),
            'transactions': len(sales_s),
            'ppv_sent': int(msg_s['is_ppv'].sum()),
            'avg_replay_seconds': round(float(rt_s.mean()), 1) if len(rt_s) > 0 else 0,
            'avg_replay_formatted': fmt_time(rt_s.mean() if len(rt_s) > 0 else None),
            'hourly': shift_hours,
            'top_models': top_models_shift,
            'top_chatters': top_chatters_shift,
        }

    # ================================================================
    # COMPUTE: Per Model (combining all sources)
    # ================================================================
    models_data = []
    all_creators = set(df_db['Creators'].unique()) | set(cs_data.keys())

    for creator in sorted(all_creators):
        db_rows = df_db[df_db['Creators'] == creator]
        cs = cs_data.get(creator, {})
        msg_rows = df_msg[df_msg['Creator'] == creator]
        sales_rows = df_sales_valid[df_sales_valid['Creator'] == creator]

        # From detailed breakdown (chatter-level) - aggregate across all days
        db_sales = round(float(db_rows['Sales_num'].sum()), 2)
        db_ppv_sent = int(db_rows['PPVs_sent'].sum())
        db_ppv_unlocked = int(db_rows['PPVs_unlocked'].sum())
        db_msgs_sent = int(db_rows['Msgs_sent'].sum())
        db_fans_chatted = int(db_rows['Fans_chatted'].sum())
        db_fans_spent = int(db_rows['Fans_spent'].sum())

        # From creator stats (already summed across periods)
        total_earnings = cs.get('total_earnings_net', db_sales)
        new_subs = cs.get('new_subs_net', 0)
        rec_subs = cs.get('recurring_subs_net', 0)
        sub_total = cs.get('subscription_net', 0)
        tips = cs.get('tips_net', 0)
        message_net = cs.get('message_net', db_sales)
        new_fans = cs.get('new_fans', 0)
        active_fans = cs.get('active_fans', 0)
        following = cs.get('following', 0)
        fans_renew = cs.get('fans_renew_on', 0)
        renew_pct = cs.get('renew_on_pct', 0)
        avg_sub_len = cs.get('avg_sub_length', 'N/A')
        avg_spend_spender = cs.get('avg_spend_per_spender', 0)
        avg_spend_tx = cs.get('avg_spend_per_tx', 0)
        avg_earn_fan = cs.get('avg_earnings_per_fan', 0)
        expired_change = cs.get('expired_fans_change', 0)
        contribution = cs.get('contribution_pct', 0)
        of_ranking = cs.get('of_ranking', 0)

        # LTV = Total Earnings / Active Fans
        ltv = round(total_earnings / active_fans, 2) if active_fans > 0 else 0

        # Avg sub length in days
        avg_sub_days = 0
        if avg_sub_len and avg_sub_len != 'N/A':
            days_match = re.search(r'(\d+)', str(avg_sub_len))
            if days_match:
                avg_sub_days = int(days_match.group(1))

        # Free vs Paid vs Mixta classification from Airtable
        account_type = airtable_types.get(creator, 'unknown')
        if account_type == 'unknown':
            creator_clean = creator.strip().lower()
            for at_name, at_type in airtable_types.items():
                if at_name.strip().lower() == creator_clean:
                    account_type = at_type
                    break
            if account_type == 'unknown':
                for at_name, at_type in airtable_types.items():
                    if creator_clean in at_name.strip().lower() or at_name.strip().lower() in creator_clean:
                        account_type = at_type
                        break

        # Golden/Unlock from DB
        gr = round(db_ppv_sent / db_msgs_sent * 100, 2) if db_msgs_sent > 0 else 0
        ur = round(db_ppv_unlocked / db_ppv_sent * 100, 2) if db_ppv_sent > 0 else 0
        fan_cvr = round(db_fans_spent / db_fans_chatted * 100, 2) if db_fans_chatted > 0 else 0

        # Response time from message dashboard
        rt = msg_rows['Replay_seconds'].dropna()
        avg_resp = round(float(rt.mean()), 1) if len(rt) > 0 else 0
        median_resp = round(float(rt.median()), 1) if len(rt) > 0 else 0

        # Hourly for this model
        model_hourly = []
        for hour in range(24):
            mh = msg_rows[msg_rows['Hour'] == hour]
            sh = sales_rows[sales_rows['Hour'] == hour]
            if len(mh) > 0 or len(sh) > 0:
                model_hourly.append({
                    'hour': hour,
                    'hour_label': '%02d:00' % hour,
                    'messages': len(mh),
                    'ppv_sent': int(mh['is_ppv'].sum()),
                    'sales_net': round(float(sh['Net'].sum()), 2),
                })

        # Peak hours for this model
        peak_traffic_h = max(model_hourly, key=lambda x: x['messages'])['hour_label'] if model_hourly else 'N/A'
        peak_sales_h = max(model_hourly, key=lambda x: x['sales_net'])['hour_label'] if model_hourly and total_earnings > 0 else 'N/A'

        # Chatters working this model - aggregate per chatter across all days
        chatter_agg = {}
        for _, r in db_rows.iterrows():
            emp = str(r['Employees']).strip()
            if not emp or emp == '' or emp == 'nan':
                continue
            if emp not in chatter_agg:
                chatter_agg[emp] = {
                    'name': emp,
                    'group': str(r['Group']) if pd.notna(r['Group']) else '',
                    'sales': 0, 'messages_sent': 0, 'ppv_sent': 0, 'ppv_unlocked': 0,
                    'fans_chatted': 0, 'fans_spent': 0, 'char_count': 0, 'clocked_min': 0,
                    'resp_seconds_list': [], 'days_worked': 0,
                }
            ca = chatter_agg[emp]
            ca['sales'] += float(r['Sales_num'])
            ca['messages_sent'] += int(r['Msgs_sent'])
            ca['ppv_sent'] += int(r['PPVs_sent'])
            ca['ppv_unlocked'] += int(r['PPVs_unlocked'])
            ca['fans_chatted'] += int(r['Fans_chatted'])
            ca['fans_spent'] += int(r['Fans_spent'])
            ca['char_count'] += int(r['Char_count'])
            ca['clocked_min'] += int(r.get('Clocked_min', 0)) if pd.notna(r.get('Clocked_min', 0)) else 0
            if pd.notna(r['Resp_seconds']):
                ca['resp_seconds_list'].append(float(r['Resp_seconds']))
            ca['days_worked'] += 1

        model_chatters = []
        for emp, ca in chatter_agg.items():
            ms = ca['messages_sent']
            ps = ca['ppv_sent']
            fc = ca['fans_chatted']
            fs = ca['fans_spent']
            rl = ca['resp_seconds_list']
            cm = ca['clocked_min']
            model_chatters.append({
                'name': ca['name'],
                'group': ca['group'],
                'sales': round(ca['sales'], 2),
                'messages_sent': ms,
                'ppv_sent': ps,
                'ppv_unlocked': ca['ppv_unlocked'],
                'golden_ratio': round(ps / ms * 100, 2) if ms > 0 else 0,
                'unlock_ratio': round(ca['ppv_unlocked'] / ps * 100, 2) if ps > 0 else 0,
                'fans_chatted': fc,
                'fans_spent': fs,
                'fan_cvr': round(fs / fc * 100, 2) if fc > 0 else 0,
                'response_time': fmt_time(sum(rl) / len(rl)) if rl else 'N/A',
                'response_seconds': round(sum(rl) / len(rl), 1) if rl else 0,
                'clocked_minutes': cm,
                'sales_per_hour': round(ca['sales'] / (cm / 60), 2) if cm > 0 else 0,
                'msgs_per_hour': round(ms / (cm / 60), 2) if cm > 0 else 0,
                'char_count': ca['char_count'],
                'days_worked': ca['days_worked'],
            })
        model_chatters.sort(key=lambda x: x['sales'], reverse=True)

        models_data.append({
            'name': creator,
            'group': cs.get('group', ''),
            'account_type': account_type,
            'ltv': ltv,
            # Revenue
            'total_earnings': round(total_earnings, 2),
            'message_revenue': round(message_net, 2),
            'subscription_revenue': round(sub_total, 2),
            'new_subs_revenue': round(new_subs, 2),
            'recurring_subs_revenue': round(rec_subs, 2),
            'tips_revenue': round(tips, 2),
            'chatter_sales': db_sales,
            # Activity
            'messages_sent': db_msgs_sent,
            'ppv_sent': db_ppv_sent,
            'ppv_unlocked': db_ppv_unlocked,
            'golden_ratio': gr,
            'unlock_ratio': ur,
            'fans_chatted': db_fans_chatted,
            'fans_spent': db_fans_spent,
            'fan_cvr': fan_cvr,
            # Fans & Subs
            'new_fans': new_fans,
            'active_fans': active_fans,
            'following': following,
            'fans_renew_on': fans_renew,
            'renew_on_pct': renew_pct,
            'expired_fans_change': expired_change,
            'avg_sub_length': avg_sub_len,
            'avg_sub_days': avg_sub_days,
            'contribution_pct': contribution,
            'of_ranking': of_ranking,
            # Averages
            'avg_spend_per_spender': avg_spend_spender,
            'avg_spend_per_tx': avg_spend_tx,
            'avg_earnings_per_fan': avg_earn_fan,
            # Response
            'avg_replay_seconds': avg_resp,
            'avg_replay_formatted': fmt_time(avg_resp),
            'median_replay_seconds': median_resp,
            'median_replay_formatted': fmt_time(median_resp),
            # Peaks
            'peak_traffic_hour': peak_traffic_h,
            'peak_sales_hour': peak_sales_h,
            # Detail
            'hourly': model_hourly,
            'chatters': model_chatters,
        })

    models_data.sort(key=lambda x: x['total_earnings'], reverse=True)

    # ================================================================
    # COMPUTE: Per Chatter (combining all sources, aggregated across days)
    # ================================================================
    chatters_data = []
    chatter_names = df_db['Employees'].dropna().unique()

    for emp in sorted(chatter_names, key=str):
        if not emp or str(emp).strip() == '' or str(emp) == 'nan':
            continue

        db_rows = df_db[df_db['Employees'] == emp]
        msg_rows = df_msg[df_msg['Sender'] == emp]
        sales_rows = df_sales_valid[df_sales_valid['Employee'] == emp]

        total_sales = round(float(db_rows['Sales_num'].sum()), 2)
        total_msgs = int(db_rows['Msgs_sent'].sum())
        ppv_sent = int(db_rows['PPVs_sent'].sum())
        ppv_unlocked = int(db_rows['PPVs_unlocked'].sum())
        fans_chatted = int(db_rows['Fans_chatted'].sum())
        fans_spent = int(db_rows['Fans_spent'].sum())
        char_count = int(db_rows['Char_count'].sum())
        clocked_min = int(db_rows['Clocked_min'].sum())
        days_worked = db_rows['Date'].dt.date.nunique()

        gr = round(ppv_sent / total_msgs * 100, 2) if total_msgs > 0 else 0
        ur = round(ppv_unlocked / ppv_sent * 100, 2) if ppv_sent > 0 else 0
        fan_cvr = round(fans_spent / fans_chatted * 100, 2) if fans_chatted > 0 else 0
        sales_per_hour = round(total_sales / (clocked_min / 60), 2) if clocked_min > 0 else 0

        # Response time from message dashboard
        rt = msg_rows['Replay_seconds'].dropna()
        avg_resp = round(float(rt.mean()), 1) if len(rt) > 0 else 0
        median_resp = round(float(rt.median()), 1) if len(rt) > 0 else 0

        # Response time buckets
        under_2m = int((rt <= 120).sum())
        btwn_2_5 = int(((rt > 120) & (rt <= 300)).sum())
        btwn_5_10 = int(((rt > 300) & (rt <= 600)).sum())
        over_10m = int((rt > 600).sum())

        # Group from DB
        groups_list = db_rows['Group'].dropna().unique()
        group = str(groups_list[0]) if len(groups_list) > 0 else ''

        # Models this chatter works - aggregate per model across all days
        model_agg = {}
        for _, r in db_rows.iterrows():
            model_name = str(r['Creators'])
            if model_name not in model_agg:
                model_agg[model_name] = {
                    'name': model_name, 'sales': 0, 'messages_sent': 0,
                    'ppv_sent': 0, 'ppv_unlocked': 0, 'fans_chatted': 0,
                    'fans_spent': 0, 'resp_list': [], 'sph': 0, 'days': 0,
                }
            ma = model_agg[model_name]
            ma['sales'] += float(r['Sales_num'])
            ma['messages_sent'] += int(r['Msgs_sent'])
            ma['ppv_sent'] += int(r['PPVs_sent'])
            ma['ppv_unlocked'] += int(r['PPVs_unlocked'])
            ma['fans_chatted'] += int(r['Fans_chatted'])
            ma['fans_spent'] += int(r['Fans_spent'])
            if pd.notna(r['Resp_seconds']):
                ma['resp_list'].append(float(r['Resp_seconds']))
            ma['sph'] += float(r['Sales_per_hour'])
            ma['days'] += 1

        chatter_models = []
        for mn, ma in model_agg.items():
            ms = ma['messages_sent']
            ps = ma['ppv_sent']
            fc = ma['fans_chatted']
            fs = ma['fans_spent']
            rl = ma['resp_list']
            chatter_models.append({
                'name': mn,
                'sales': round(ma['sales'], 2),
                'messages_sent': ms,
                'ppv_sent': ps,
                'ppv_unlocked': ma['ppv_unlocked'],
                'golden_ratio': round(ps / ms * 100, 2) if ms > 0 else 0,
                'unlock_ratio': round(ma['ppv_unlocked'] / ps * 100, 2) if ps > 0 else 0,
                'fans_chatted': fc,
                'fans_spent': fs,
                'fan_cvr': round(fs / fc * 100, 2) if fc > 0 else 0,
                'response_time': fmt_time(sum(rl) / len(rl)) if rl else 'N/A',
                'response_seconds': round(sum(rl) / len(rl), 1) if rl else 0,
                'sales_per_hour': round(ma['sph'] / ma['days'], 2) if ma['days'] > 0 else 0,
            })
        chatter_models.sort(key=lambda x: x['sales'], reverse=True)

        # Hourly for this chatter
        chatter_hourly = []
        for hour in range(24):
            mh = msg_rows[msg_rows['Hour'] == hour]
            sh = sales_rows[sales_rows['Hour'] == hour]
            if len(mh) > 0 or len(sh) > 0:
                chatter_hourly.append({
                    'hour': hour,
                    'hour_label': '%02d:00' % hour,
                    'messages': len(mh),
                    'sales_net': round(float(sh['Net'].sum()), 2),
                })

        chatters_data.append({
            'name': str(emp),
            'group': group,
            'total_sales': total_sales,
            'total_messages': total_msgs,
            'ppv_sent': ppv_sent,
            'ppv_unlocked': ppv_unlocked,
            'golden_ratio': gr,
            'unlock_ratio': ur,
            'fans_chatted': fans_chatted,
            'fans_spent': fans_spent,
            'fan_cvr': fan_cvr,
            'models_count': len(chatter_models),
            'clocked_minutes': clocked_min,
            'clocked_hours_formatted': '%dh %dm' % (clocked_min // 60, clocked_min % 60) if clocked_min > 0 else 'N/A',
            'sales_per_hour': sales_per_hour,
            'char_count': char_count,
            'days_worked': days_worked,
            'avg_replay_seconds': avg_resp,
            'avg_replay_formatted': fmt_time(avg_resp),
            'median_replay_seconds': median_resp,
            'median_replay_formatted': fmt_time(median_resp),
            'response_buckets': {
                'under_2m': under_2m,
                'btwn_2_5m': btwn_2_5,
                'btwn_5_10m': btwn_5_10,
                'over_10m': over_10m,
            },
            'models': chatter_models,
            'hourly': chatter_hourly,
        })

    chatters_data.sort(key=lambda x: x['total_sales'], reverse=True)

    # ================================================================
    # ASSEMBLE JSON
    # ================================================================
    dashboard = {
        'report_date': '%s - %s' % (REPORT_START, REPORT_END),
        'generated_at': datetime.now().strftime('%b %d, %Y %H:%M'),
        'general': general,
        'peak_traffic_hour': {
            'hour_label': peak_traffic['hour_label'],
            'messages': peak_traffic['messages'],
        },
        'peak_sales_hour': {
            'hour_label': peak_sales['hour_label'],
            'revenue': peak_sales['sales_net'],
        },
        'hourly': hourly_data,
        'daily': daily_data,
        'shifts': shifts_data,
        'models': models_data,
        'chatters': chatters_data,
    }

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(dashboard, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 60)
    print("JSON generado: %s" % OUTPUT_PATH)
    print("Periodo: %s a %s (%d dias)" % (REPORT_START, REPORT_END, unique_dates))
    print("=" * 60)
    print("Revenue total (Net): $%.2f" % total_net_revenue)
    print("  Messages: $%.2f" % msg_revenue)
    print("  Subscriptions: $%.2f (New: $%.2f | Rec: $%.2f)" % (sub_revenue, total_new_subs_revenue, total_rec_subs_revenue))
    print("  Tips: $%.2f" % tips_revenue)
    print("Chatter-attributed sales: $%.2f" % total_chatter_sales)
    print("New fans: %d" % total_new_fans)
    print("Modelos: %d | Chatters: %d" % (len(models_data), len(chatters_data)))
    print("Total mensajes procesados: %s" % f"{total_messages:,}")


if __name__ == '__main__':
    main()
