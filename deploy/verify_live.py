import json
import urllib.request
import urllib.parse
from datetime import datetime
import calendar

BASE = 'http://localhost:8000'

def request_json(path, params=None, method='GET', body=None, headers=None, timeout=120):
    if params:
        qs = urllib.parse.urlencode(params)
        url = f'{BASE}{path}?{qs}'
    else:
        url = f'{BASE}{path}'
    data = None
    h = {'Accept': 'application/json'}
    if headers:
        h.update(headers)
    if body is not None:
        data = json.dumps(body).encode('utf-8')
        h['Content-Type'] = 'application/json'
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, json.loads(resp.read().decode('utf-8'))

def parse_month(value):
    s = str(value).strip()
    if not s:
        return None
    for fmt in ('%b-%y', '%b %y', '%Y-%m-%d', '%Y-%m-%d %H:%M:%S'):
        try:
            dt = datetime.strptime(s, fmt)
            if fmt in ('%b-%y', '%b %y'):
                dt = dt.replace(day=1)
            return dt
        except Exception:
            pass
    try:
        return datetime.fromisoformat(s.replace('Z', ''))
    except Exception:
        return None

hs, health = request_json('/health')
print('HEALTH', hs, health)

_, login = request_json(
    '/auth/login',
    method='POST',
    body={'email': 'admin.user@zopper.com', 'password': 'admin123', 'role': 'admin'},
)
auth = {'Authorization': f"Bearer {login['access_token']}"}
print('LOGIN', login.get('email'), login.get('role'))

sources = ['samsung', 'reliance', 'godrej']
for src in sources:
    _, bounds = request_json('/analytics/date-bounds', {'source': src, 'dataset_type': 'claims'}, headers=auth)
    print('\nSOURCE=', src, 'BOUNDS=', bounds)
    mn = bounds.get('min_date')
    mx = bounds.get('max_date')
    if not mn or not mx:
        print('  missing bounds, skip')
        continue

    _, month_rows = request_json(
        '/analytics/by-dimension',
        {
            'source': src,
            'dataset_type': 'claims',
            'dimension': 'month',
            'metric': 'claims',
            'from_date': mn,
            'to_date': mx,
        },
        headers=auth,
    )
    _, month_rows_net = request_json(
        '/analytics/by-dimension',
        {
            'source': src,
            'dataset_type': 'claims',
            'dimension': 'month',
            'metric': 'net_claims',
            'from_date': mn,
            'to_date': mx,
        },
        headers=auth,
    )

    claims_len = len(month_rows) if isinstance(month_rows, list) else -1
    net_len = len(month_rows_net) if isinstance(month_rows_net, list) else -1
    claims_total = sum(float((r.get('claims') or 0)) for r in month_rows) if isinstance(month_rows, list) else 0.0
    net_total = sum(float((r.get('net_claims') or 0)) for r in month_rows_net) if isinstance(month_rows_net, list) else 0.0
    print('  since_inception claims_rows=', claims_len, 'claims_total=', round(claims_total, 2),
          'net_rows=', net_len, 'net_total=', round(net_total, 2))

    if not isinstance(month_rows, list) or not month_rows:
        continue

    sampled = month_rows[-3:]
    for row in sampled:
        mval = row.get('month')
        dt = parse_month(mval)
        if not dt:
            print('   month parse failed for', mval)
            continue
        day = min(19, calendar.monthrange(dt.year, dt.month)[1])
        day_str = dt.replace(day=day).strftime('%Y-%m-%d')

        _, one_day_claims = request_json(
            '/analytics/by-dimension',
            {
                'source': src,
                'dataset_type': 'claims',
                'dimension': 'month',
                'metric': 'claims',
                'from_date': day_str,
                'to_date': day_str,
            },
            headers=auth,
        )
        _, one_day_net = request_json(
            '/analytics/by-dimension',
            {
                'source': src,
                'dataset_type': 'claims',
                'dimension': 'month',
                'metric': 'net_claims',
                'from_date': day_str,
                'to_date': day_str,
            },
            headers=auth,
        )

        c_rows = len(one_day_claims) if isinstance(one_day_claims, list) else -1
        n_rows = len(one_day_net) if isinstance(one_day_net, list) else -1
        c_total = sum(float((r.get('claims') or 0)) for r in one_day_claims) if isinstance(one_day_claims, list) else 0.0
        n_total = sum(float((r.get('net_claims') or 0)) for r in one_day_net) if isinstance(one_day_net, list) else 0.0

        print(
            '   filter_day=', day_str,
            'month_bucket=', mval,
            'claims_rows=', c_rows,
            'claims_total=', round(c_total, 2),
            'net_rows=', n_rows,
            'net_total=', round(n_total, 2),
        )
