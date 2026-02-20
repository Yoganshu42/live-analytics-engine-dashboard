import json
import urllib.request
import urllib.parse

BASE = 'http://localhost:8000'

def req(path, params=None, method='GET', body=None, headers=None):
    if params:
        path = path + '?' + urllib.parse.urlencode(params)
    url = BASE + path
    data = None
    hdr = {'Accept': 'application/json'}
    if headers:
        hdr.update(headers)
    if body is not None:
        data = json.dumps(body).encode('utf-8')
        hdr['Content-Type'] = 'application/json'
    r = urllib.request.Request(url, data=data, method=method, headers=hdr)
    with urllib.request.urlopen(r, timeout=120) as resp:
        return resp.status, json.loads(resp.read().decode())

_, login = req('/auth/login', method='POST', body={'email':'admin.user@zopper.com','password':'admin123','role':'admin'})
auth = {'Authorization': 'Bearer ' + login['access_token']}

checks = [
    {'source':'reliance','dataset_type':'claims','from_date':'2026-02-19','to_date':'2026-02-19'},
    {'source':'reliance','dataset_type':'claims','from_date':'2026-01-19','to_date':'2026-01-19'},
    {'source':'reliance','dataset_type':'claims','from_date':'2025-12-19','to_date':'2025-12-19'},
]

for p in checks:
    s1, summary = req('/analytics/summary', p, headers=auth)
    s2, lu = req('/analytics/last-updated', p, headers=auth)
    print('\\nCHECK', p)
    print('  summary_status', s1, 'summary', summary)
    print('  last_updated_status', s2, 'last_updated', lu)
