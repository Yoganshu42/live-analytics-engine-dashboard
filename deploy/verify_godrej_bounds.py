import json
import urllib.request
import urllib.parse

BASE = 'http://localhost:8000'

def req(path, params=None, method='GET', body=None, headers=None):
    if params:
        path = path + '?' + urllib.parse.urlencode(params)
    data = None
    hdr = {'Accept':'application/json'}
    if headers:
        hdr.update(headers)
    if body is not None:
        data = json.dumps(body).encode('utf-8')
        hdr['Content-Type'] = 'application/json'
    r = urllib.request.Request(BASE + path, data=data, method=method, headers=hdr)
    with urllib.request.urlopen(r, timeout=120) as resp:
        return resp.status, json.loads(resp.read().decode())

_, login = req('/auth/login', method='POST', body={'email':'admin.user@zopper.com','password':'admin123','role':'admin'})
auth = {'Authorization':'Bearer ' + login['access_token']}

for ds in ['sales','claims']:
    s, b = req('/analytics/date-bounds', {'source':'godrej','dataset_type':ds}, headers=auth)
    print('no_job', ds, s, b)

# fetch godrej job ids from admin/files
s, files = req('/admin/files?source=godrej', headers=auth)
items = files.get('items', []) if isinstance(files, dict) else []
print('files_status', s, 'count', len(items))
seen = []
for it in items:
    jid = (it.get('job_id') or '').strip()
    if jid and jid not in seen:
        seen.append(jid)

print('job_ids', seen[:10])
for jid in seen[:5]:
    for ds in ['sales','claims']:
        s, b = req('/analytics/date-bounds', {'source':'godrej','dataset_type':ds,'job_id':jid}, headers=auth)
        print('job', jid, ds, s, b)
