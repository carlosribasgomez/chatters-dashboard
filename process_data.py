#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Procesa los 4 archivos Excel y genera un JSON completo para el dashboard.
Fuentes:
  1. Message Dashboard: mensajes individuales, tiempos, PPVs enviados/comprados ese dia
  2. Detailed Breakdown: ventas REALES por chatter x modelo (incluye PPVs de dias anteriores)
  3. Sales Record: transacciones individuales de venta (messages, subs, tips)
  4. Creator Statistics: stats de cada modelo (subs, new fans, LTV, etc.)
"""

import json
import re
import sys
from collections import defaultdict

import pandas as pd

# Paths
MSG_DASHBOARD = r'c:\Users\carlo\Downloads\(Chatting_Wizard_ESP)Message_Dashboard_Report_20260213202203.xlsx'
DETAILED_BREAKDOWN = r'c:\Users\carlo\Downloads\e35191b3-3310-4138-a1d5-89da57631ba5.xlsx'
SALES_RECORD = r'c:\Users\carlo\Downloads\6789ef81-6aa2-4a1e-b713-091ad5e622c5.xlsx'
CREATOR_STATS = r'c:\Users\carlo\Downloads\255e8800-c5ac-41dc-98f6-051a5137ec55.xlsx'
OUTPUT_PATH = r'c:\Users\carlo\Carlos Ribas Cursor Projects\chatters-dashboard\dashboard_data.json'


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


AIRTABLE_TYPES_PATH = r'c:\Users\carlo\Carlos Ribas Cursor Projects\chatters-dashboard\airtable_model_types.json'


def main():
    # Load Airtable model types (free/paid classification)
    with open(AIRTABLE_TYPES_PATH, 'r', encoding='utf-8') as f:
        airtable_types = json.load(f)
    print("Airtable types loaded: %d modelos" % len(airtable_types))

    # ================================================================
    # 1. LOAD MESSAGE DASHBOARD (for hourly traffic, response times, PPV detail)
    # ================================================================
    print("1/4 Leyendo Message Dashboard...")
    df_msg = pd.read_excel(MSG_DASHBOARD, sheet_name='Message Dashboard')
    print("   %d mensajes" % len(df_msg))

    df_msg['Price_num'] = pd.to_numeric(df_msg['Price'], errors='coerce').fillna(0)
    df_msg['is_ppv'] = df_msg['Price_num'] > 0
    df_msg['is_purchased'] = df_msg['Purchased'].str.lower() == 'yes'
    df_msg['Hour'] = df_msg['Sent time'].apply(
        lambda x: int(str(x).split(':')[0]) if x and ':' in str(x) else None
    )
    df_msg['Shift'] = df_msg['Hour'].apply(lambda h: get_shift(h) if pd.notna(h) else None)
    df_msg['Replay_seconds'] = df_msg['Replay time'].apply(parse_replay_seconds)

    # ================================================================
    # 2. LOAD DETAILED BREAKDOWN (real sales per chatter x model)
    # ================================================================
    print("2/4 Leyendo Detailed Breakdown...")
    df_db = pd.read_excel(DETAILED_BREAKDOWN, sheet_name='Detailed breakdown')
    print("   %d filas chatter x modelo" % len(df_db))

    df_db['Sales_num'] = df_db['Sales'].apply(parse_dollar)
    df_db['PPVs_sent'] = pd.to_numeric(df_db['Direct PPVs sent'], errors='coerce').fillna(0).astype(int)
    df_db['PPVs_unlocked'] = pd.to_numeric(df_db['PPVs unlocked'], errors='coerce').fillna(0).astype(int)
    df_db['Msgs_sent'] = pd.to_numeric(df_db['Direct messages sent'], errors='coerce').fillna(0).astype(int)
    df_db['GR_pct'] = df_db['Golden ratio'].apply(parse_pct)
    df_db['UR_pct'] = df_db['Unlock rate'].apply(parse_pct)
    df_db['Fans_chatted'] = pd.to_numeric(df_db['Fans chatted'], errors='coerce').fillna(0).astype(int)
    df_db['Fans_spent'] = pd.to_numeric(df_db['Fans who spent money'], errors='coerce').fillna(0).astype(int)
    df_db['Fan_CVR'] = df_db['Fan CVR'].apply(parse_pct)
    df_db['Resp_time_str'] = df_db['Response time (based on clocked hours)'].fillna('')
    df_db['Resp_seconds'] = df_db['Resp_time_str'].apply(parse_replay_seconds)
    df_db['Clocked_min'] = df_db['Clocked hours'].apply(parse_hours_minutes)
    df_db['Sales_per_hour'] = df_db['Sales per hour'].apply(parse_dollar)
    df_db['Msgs_per_hour'] = pd.to_numeric(df_db['Messages sent per hour'], errors='coerce').fillna(0)
    df_db['Char_count'] = pd.to_numeric(df_db['Character count'], errors='coerce').fillna(0).astype(int)
    df_db['Avg_earn_per_spender'] = df_db['Avg earnings per fan who spent money'].apply(parse_dollar)

    # ================================================================
    # 3. LOAD SALES RECORD (individual transactions)
    # ================================================================
    print("3/4 Leyendo Sales Record...")
    df_sales = pd.read_excel(SALES_RECORD, sheet_name='Sales record')
    print("   %d transacciones" % len(df_sales))

    df_sales.rename(columns={
        'Date & time Africa/Monrovia': 'DateTime',
        'Employee': 'Employee',
        'Creator': 'Creator',
        'Fan': 'Fan',
        'Earnings': 'Earnings_str',
        'Gross revenue': 'Gross_str',
        'Net revenue': 'Net_str',
        'Type': 'Type',
        'Rule': 'Rule',
        'Status': 'Status',
    }, inplace=True)

    df_sales['Earnings'] = df_sales['Earnings_str'].apply(parse_dollar)
    df_sales['Gross'] = df_sales['Gross_str'].apply(parse_dollar)
    df_sales['Net'] = df_sales['Net_str'].apply(parse_dollar)
    df_sales['Hour'] = pd.to_datetime(df_sales['DateTime']).dt.hour
    df_sales['Shift'] = df_sales['Hour'].apply(get_shift)

    # Filter out reverses for revenue calculations
    df_sales_valid = df_sales[df_sales['Status'] != 'Reverse'].copy()

    # ================================================================
    # 4. LOAD CREATOR STATISTICS (model-level: subs, new fans, LTV)
    # ================================================================
    print("4/4 Leyendo Creator Statistics...")
    df_cs = pd.read_excel(CREATOR_STATS, sheet_name='Creator Statistics')
    print("   %d modelos" % len(df_cs))

    cs_data = {}
    for _, row in df_cs.iterrows():
        name = row['Creator']
        cs_data[name] = {
            'subscription_net': parse_dollar(row['Subscription Net']),
            'new_subs_net': parse_dollar(row['New subscriptions Net']),
            'recurring_subs_net': parse_dollar(row['Recurring subscriptions Net']),
            'tips_net': parse_dollar(row['Tips Net']),
            'total_earnings_net': parse_dollar(row['Total earnings Net']),
            'contribution_pct': parse_pct(row['Contribution %']) if row['Contribution %'] else 0,
            'of_ranking': parse_pct(row['OF ranking']) if row['OF ranking'] else 0,
            'following': int(row['Following']) if row['Following'] else 0,
            'fans_renew_on': int(row['Fans with renew on']) if row['Fans with renew on'] else 0,
            'renew_on_pct': parse_pct(row['Renew on %']) if row['Renew on %'] else 0,
            'new_fans': int(row['New fans']) if row['New fans'] else 0,
            'active_fans': int(row['Active fans']) if row['Active fans'] else 0,
            'expired_fans_change': int(row['Change in expired fan count']) if row['Change in expired fan count'] else 0,
            'message_net': parse_dollar(row['Message Net']),
            'group': str(row['Creator group']) if row['Creator group'] else '',
            'avg_spend_per_spender': parse_dollar(row['Avg spend per spender Net']),
            'avg_spend_per_tx': parse_dollar(row['Avg spend per transaction Net']),
            'avg_earnings_per_fan': parse_dollar(row['Avg earnings per fan Net']),
            'avg_sub_length': str(row['Avg subscription length']) if row['Avg subscription length'] else 'N/A',
        }

    # ================================================================
    # COMPUTE: General KPIs
    # ================================================================
    print("\nCalculando metricas...")

    total_messages = len(df_msg)
    total_ppv_sent_msg = int(df_msg['is_ppv'].sum())
    total_ppv_purchased_msg = int((df_msg['is_ppv'] & df_msg['is_purchased']).sum())

    # REAL revenue from sales record
    total_net_revenue = round(df_sales_valid['Net'].sum(), 2)
    msg_revenue = round(df_sales_valid[df_sales_valid['Type'] == 'Messages']['Net'].sum(), 2)
    sub_revenue = round(df_sales_valid[df_sales_valid['Type'] == 'Subscription']['Net'].sum(), 2)
    tips_revenue = round(df_sales_valid[df_sales_valid['Type'].str.startswith('Tips', na=False)]['Net'].sum(), 2)

    # From detailed breakdown (chatter-attributed sales)
    total_chatter_sales = round(df_db['Sales_num'].sum(), 2)
    total_ppv_sent_db = int(df_db['PPVs_sent'].sum())
    total_ppv_unlocked_db = int(df_db['PPVs_unlocked'].sum())
    total_fans_chatted = int(df_db['Fans_chatted'].sum())  # note: may have overlaps across chatters

    # From creator stats
    total_new_fans = sum(cs['new_fans'] for cs in cs_data.values() if cs['new_fans'] > 0)
    total_new_subs_revenue = round(sum(cs['new_subs_net'] for cs in cs_data.values()), 2)
    total_rec_subs_revenue = round(sum(cs['recurring_subs_net'] for cs in cs_data.values()), 2)
    total_active_fans = sum(cs['active_fans'] for cs in cs_data.values())

    # Response time from message dashboard
    rt_vals = df_msg['Replay_seconds'].dropna()
    avg_rt = round(float(rt_vals.mean()), 1) if len(rt_vals) > 0 else 0
    median_rt = round(float(rt_vals.median()), 1) if len(rt_vals) > 0 else 0

    # Golden/Unlock from detailed breakdown (more accurate)
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
            'tips_net': round(float(sales_h[sales_h['Type'].str.startswith('Tips', na=False)]['Net'].sum()), 2),
            'transactions': len(sales_h),
        })

    peak_traffic = max(hourly_data, key=lambda x: x['messages'])
    peak_sales = max(hourly_data, key=lambda x: x['sales_net'])

    # ================================================================
    # COMPUTE: Shift data
    # ================================================================
    shifts_data = {}
    for shift_key, shift_label in SHIFT_LABELS.items():
        msg_s = df_msg[df_msg['Shift'] == shift_key]
        sales_s = df_sales_valid[df_sales_valid['Shift'] == shift_key]
        rt_s = msg_s['Replay_seconds'].dropna()

        # Hourly within shift
        shift_hours = [h for h in hourly_data if h['shift'] == shift_key]

        # Top models in this shift by sales
        shift_model_sales = sales_s.groupby('Creator')['Net'].sum().sort_values(ascending=False).head(10)
        top_models_shift = [{'name': n, 'revenue': round(v, 2)} for n, v in shift_model_sales.items()]

        # Top chatters in this shift by sales
        shift_chatter_sales = sales_s[sales_s['Employee'].fillna('').str.strip() != ''].groupby('Employee')['Net'].sum().sort_values(ascending=False).head(10)
        top_chatters_shift = [{'name': n, 'revenue': round(v, 2)} for n, v in shift_chatter_sales.items()]

        shifts_data[shift_key] = {
            'label': shift_label,
            'messages': len(msg_s),
            'sales_net': round(float(sales_s['Net'].sum()), 2),
            'msg_sales': round(float(sales_s[sales_s['Type'] == 'Messages']['Net'].sum()), 2),
            'sub_sales': round(float(sales_s[sales_s['Type'] == 'Subscription']['Net'].sum()), 2),
            'tips_sales': round(float(sales_s[sales_s['Type'].str.startswith('Tips', na=False)]['Net'].sum()), 2),
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

        # From detailed breakdown (chatter-level)
        db_sales = round(float(db_rows['Sales_num'].sum()), 2)
        db_ppv_sent = int(db_rows['PPVs_sent'].sum())
        db_ppv_unlocked = int(db_rows['PPVs_unlocked'].sum())
        db_msgs_sent = int(db_rows['Msgs_sent'].sum())
        db_fans_chatted = int(db_rows['Fans_chatted'].sum())
        db_fans_spent = int(db_rows['Fans_spent'].sum())

        # From creator stats
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

        # LTV = Total Earnings / Active Fans (total subscribed fans)
        ltv = round(total_earnings / active_fans, 2) if active_fans > 0 else 0

        # Avg sub length in days
        avg_sub_days = 0
        if avg_sub_len and avg_sub_len != 'N/A':
            days_match = re.search(r'(\d+)', str(avg_sub_len))
            if days_match:
                avg_sub_days = int(days_match.group(1))

        # Free vs Paid classification from Airtable
        account_type = airtable_types.get(creator, 'unknown')
        # Try case-insensitive match with strip if exact match fails
        if account_type == 'unknown':
            creator_clean = creator.strip().lower()
            for at_name, at_type in airtable_types.items():
                if at_name.strip().lower() == creator_clean:
                    account_type = at_type
                    break
            # Try partial/contains match as last resort
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
                    'sales_net': round(float(sh['Net'].sum()), 2),
                })

        # Peak hours for this model
        peak_traffic_h = max(model_hourly, key=lambda x: x['messages'])['hour_label'] if model_hourly else 'N/A'
        peak_sales_h = max(model_hourly, key=lambda x: x['sales_net'])['hour_label'] if model_hourly and total_earnings > 0 else 'N/A'

        # Chatters working this model (from detailed breakdown)
        model_chatters = []
        for _, r in db_rows.iterrows():
            emp = r['Employees']
            if not emp or str(emp).strip() == '':
                continue
            # Response time from msg dashboard for this chatter+model
            cm_msgs = msg_rows[msg_rows['Sender'] == emp]
            cm_rt = cm_msgs['Replay_seconds'].dropna()

            model_chatters.append({
                'name': str(emp),
                'group': str(r['Group']) if r['Group'] else '',
                'sales': round(float(r['Sales_num']), 2),
                'messages_sent': int(r['Msgs_sent']),
                'ppv_sent': int(r['PPVs_sent']),
                'ppv_unlocked': int(r['PPVs_unlocked']),
                'golden_ratio': round(float(r['GR_pct']), 2),
                'unlock_ratio': round(float(r['UR_pct']), 2),
                'fans_chatted': int(r['Fans_chatted']),
                'fans_spent': int(r['Fans_spent']),
                'fan_cvr': round(float(r['Fan_CVR']), 2),
                'response_time': fmt_time(r['Resp_seconds']),
                'response_seconds': round(float(r['Resp_seconds']), 1) if pd.notna(r['Resp_seconds']) else 0,
                'clocked_minutes': int(r['Clocked_min']),
                'sales_per_hour': round(float(r['Sales_per_hour']), 2),
                'msgs_per_hour': round(float(r['Msgs_per_hour']), 2),
                'char_count': int(r['Char_count']),
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
    # COMPUTE: Per Chatter (combining all sources)
    # ================================================================
    chatters_data = []
    chatter_names = df_db['Employees'].dropna().unique()

    for emp in sorted(chatter_names, key=str):
        if not emp or str(emp).strip() == '':
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

        # Models this chatter works
        chatter_models = []
        for _, r in db_rows.iterrows():
            chatter_models.append({
                'name': str(r['Creators']),
                'sales': round(float(r['Sales_num']), 2),
                'messages_sent': int(r['Msgs_sent']),
                'ppv_sent': int(r['PPVs_sent']),
                'ppv_unlocked': int(r['PPVs_unlocked']),
                'golden_ratio': round(float(r['GR_pct']), 2),
                'unlock_ratio': round(float(r['UR_pct']), 2),
                'fans_chatted': int(r['Fans_chatted']),
                'fans_spent': int(r['Fans_spent']),
                'fan_cvr': round(float(r['Fan_CVR']), 2),
                'response_time': fmt_time(r['Resp_seconds']),
                'response_seconds': round(float(r['Resp_seconds']), 1) if pd.notna(r['Resp_seconds']) else 0,
                'sales_per_hour': round(float(r['Sales_per_hour']), 2),
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
        'report_date': 'Feb 12, 2026',
        'generated_at': 'Feb 13, 2026',
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
        'shifts': shifts_data,
        'models': models_data,
        'chatters': chatters_data,
    }

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(dashboard, f, ensure_ascii=False, indent=2)

    print("\nJSON generado: %s" % OUTPUT_PATH)
    print("Revenue total (Net): $%.2f" % total_net_revenue)
    print("  Messages: $%.2f" % msg_revenue)
    print("  Subscriptions: $%.2f (New: $%.2f | Rec: $%.2f)" % (sub_revenue, total_new_subs_revenue, total_rec_subs_revenue))
    print("  Tips: $%.2f" % tips_revenue)
    print("Chatter-attributed sales: $%.2f" % total_chatter_sales)
    print("New fans: %d" % total_new_fans)
    print("Modelos: %d | Chatters: %d" % (len(models_data), len(chatters_data)))


if __name__ == '__main__':
    main()
