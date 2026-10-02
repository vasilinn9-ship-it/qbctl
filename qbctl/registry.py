"""Metainfo reconciliation. Never reads handed-off payloads."""
from pathlib import Path
import json, shutil
from .common import Fault, guarded, norm, now

def aliases(x):
    values=set()
    for key in ('id','hash','v1','v2','infohash_v1','infohash_v2'):
        h=x.get(key)
        if not h: continue
        if not isinstance(h,str) or len(h) not in (40,64) or any(c not in '0123456789abcdefABCDEF' for c in h):
            raise Fault('FORMAT_UNSUPPORTED','Некорректный торрент-хеш.',field=key)
        values.add(h.lower())
    return values

def reconcile(ctl,entries,rows):
    parent={}
    def find(h):
        parent.setdefault(h,h)
        if parent[h]!=h: parent[h]=find(parent[h])
        return parent[h]
    def join(values):
        values=sorted(values)
        for h in values: find(h)
        for h in values[1:]: parent[find(h)]=find(values[0])
    records=ctl.store.registry()
    by_hash={r['hash']:r for r in records}; changed_rows=0
    for x in entries+rows: join(aliases(x))
    for r in records: join(set(r.get('aliases',[r['hash']])))
    groups={}
    for e in entries:
        a=aliases(e); key=find(next(iter(a)))
        groups.setdefault(key,[]).append(e)
    alias_groups={}; client_groups={}; record_groups={}
    for h in parent: alias_groups.setdefault(find(h),set()).add(h)
    for r in rows: client_groups.setdefault(find(next(iter(aliases(r)))),[]).append(r)
    for r in records: record_groups.setdefault(find(next(iter(r.get('aliases',[r['hash']])))),[]).append(r)
    incoming_groups={key for key,items in groups.items() if any(e['role']=='incoming' for e in items)}
    conflicts=[]; unverified=[]; blocked=set()
    pending={o['hash'] for o in ctl.store.pending()}
    for key,items in groups.items():
        ctl.budget.check()
        all_aliases=alias_groups[key]
        client=client_groups.get(key,[])
        old=record_groups.get(key,[])
        done=next((r for r in old if r['state']=='finished'),None)
        archives=[e for e in items if e['role']=='archive']; incoming=[e for e in items if e['role']=='incoming']
        if len(incoming)>1 or len(archives)>1 or (incoming and archives):
            conflicts.append({'code':'TORRENT_DUPLICATE','hash':key,'paths':[e['path'] for e in items],'roles':[e['role'] for e in items]}); blocked.update(all_aliases)
        if len(client)>1:
            conflicts.append({'code':'CLIENT_DUPLICATE','hash':key,'ids':[r['hash'] for r in client]}); blocked.update(all_aliases)
        if incoming and done:
            conflicts.append({'code':'ALREADY_PROCESSED_INPUT','hash':key,'paths':[e['path'] for e in incoming]}); blocked.update(all_aliases)
        state='finished' if done else ('processing' if any(r['hash'] in pending for r in client) else ('historical_unverified' if archives else 'incoming'))
        if state=='historical_unverified': unverified.append(key)
        for r in client:
            if not incoming and r['hash'] not in pending and not done:
                conflicts.append({'code':'CLIENT_SOURCE_MISSING','hash':r['hash']}); blocked.update(all_aliases)
        r=dict(done or (old[0] if old else {}))
        r.update(hash=r.get('hash',client[0]['hash'] if client else items[0]['id']),aliases=sorted(all_aliases),state=state,sources=[{'path':e['path'],'role':e['role'],'sha256':e['sha256']} for e in items])
        previous=by_hash.get(r['hash'])
        if previous is None or {k:v for k,v in r.items() if k!='indexed_at'}!={k:v for k,v in previous.items() if k!='indexed_at'}:
            r['indexed_at']=now()
            ctl.store.db.execute('INSERT OR REPLACE INTO registry VALUES(?,?)',(r['hash'],json.dumps(r))); changed_rows+=1
    for r in records:
        if r['state']=='finished' and find(next(iter(r.get('aliases',[r['hash']])))) not in groups:
            conflicts.append({'code':'ARCHIVE_MISSING','hash':r['hash'],'message':'Квитанция сохранена; данные k не проверяются, повторное добавление запрещено.'})
    for t in rows:
        if t['hash'] not in pending and find(next(iter(aliases(t)))) not in incoming_groups:
            if not any(i.get('hash')==t['hash'] and i['code']=='CLIENT_SOURCE_MISSING' for i in conflicts):
                conflicts.append({'code':'CLIENT_SOURCE_MISSING','hash':t['hash'],'message':'У записи клиента нет исходного torrent в корне t.'})
            blocked.update(aliases(t))
    ctl.store.db.commit()
    return {'complete':True,'verified_at':now(),'changed_rows':changed_rows,'indexed':len(entries),'conflicts':conflicts,'historical_unverified':unverified,'blocked_hashes':sorted(blocked),'completed_data_audited':False}

def finished_aliases(store):
    return {a for r in store.registry() if r['state']=='finished' for a in r.get('aliases',[r['hash']])}

def disk_admission(ctl,size=0,exclude=None):
    root=guarded(ctl.c['paths']['working']); free=shutil.disk_usage(root).free
    devices={norm(root):root.stat().st_dev}
    outstanding=0
    for t in ctl.api.torrents():
        if t['hash']==exclude or t.get('amount_left',0)<=0: continue
        key=norm(t['save_path'])
        if key not in devices: devices[key]=guarded(t['save_path']).stat().st_dev
        if devices[key]==devices[norm(root)]: outstanding+=int(t['amount_left'])
    required=outstanding+int(size)+1024**3
    return {'free_bytes':free,'reserved_bytes':outstanding,'candidate_bytes':size,'required_bytes':required,'ok':free>=required,'conservative':True}
