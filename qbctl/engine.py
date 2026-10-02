from __future__ import annotations
import contextlib, json, os, time, uuid, functools
from collections import Counter
from pathlib import Path
from .common import ROOT, Budget, Fault, digest, fingerprint, guarded, lexical, move_no_replace, norm, now, samefile_identity, save_config, sha_file, stream_sha_file, prefix_sha_file, copy_prefix
from .registry import aliases, reconcile, finished_aliases, disk_admission
from .ownership import claims, conflicts as path_conflicts, overlaps, scope

def timed(phase):
    def decorate(func):
        @functools.wraps(func)
        def wrapped(self,*args,**kwargs):
            start=time.monotonic()
            try: return func(self,*args,**kwargs)
            finally:
                item=self.timings.setdefault(phase,{'calls':0,'seconds':0})
                item['calls']+=1; item['seconds']=round(item['seconds']+time.monotonic()-start,6)
        return wrapped
    return decorate

def read_phase(func):
    @functools.wraps(func)
    def wrapped(self, *args, **kwargs):
        phase = getattr(self.api, 'file_phase', None)
        with phase() if phase else contextlib.nullcontext():
            return func(self, *args, **kwargs)
    return wrapped

def stopped(t): return t['state'].startswith(('stopped','paused'))
def speed_limit_matches(actual, requested):
    # qBittorrent stores global rates in whole KiB/s. Accept only the
    # requested value or its exact downward conversion, never a tolerance.
    normalized = 0 if requested <= 0 else max(1024, requested // 1024 * 1024)
    return actual == requested or actual == normalized

def preference_matches(key, actual, requested):
    return speed_limit_matches(actual, requested) if key in ('dl_limit','up_limit') else actual == requested

def checking(t): return 'checking' in t['state'].lower()
def complete(t):
    return t.get('progress')==1 and t.get('amount_left')==0 and not checking(t) and t['state'] not in ('moving','error','missingFiles','unknown')
def summary(t):
    return {k:t.get(k) for k in ('hash','name','state','progress','amount_left','save_path','dlspeed','upspeed','num_seeds','num_leechs','availability','auto_tmm','force_start')}

class Controller:
    def __init__(self,c,api,store,budget):
        self.c,self.api,self.store,self.budget=c,api,store,budget
        self.actions=[]; self.issues=[]
        self.request_id=None
        self.timings={}; self._index_memory={}
        self.service_mode=False; self.service_waits={}; self.initial_completions=None
        self.nonblocking=False
    def headroom(self,phase,seconds=3):
        if self.service_mode and self.budget.left()<seconds:
            raise Fault('SERVICE_DEFERRED','Новая операция отложена; время оставлено на итоговый статус.','partial',phase=phase,remaining_seconds=round(self.budget.left(),3))
    def wait_no_checks(self):
        rows=self.api.torrents()
        while any(checking(t) for t in rows):
            if self.nonblocking:
                raise Fault('CHECK_PENDING','Проверка продолжается; состояние сохранено, исполнитель свободен.','partial',checking_hashes=[t['hash'] for t in rows if checking(t)])
            if self.budget.left()<3:
                raise Fault('SERVICE_WAITING','Проверка диска продолжается; проход завершён с явным ожиданием.','partial',checking_hashes=[t['hash'] for t in rows if checking(t)])
            time.sleep(min(0.2,self.budget.left())); rows=self.api.torrents()
        return rows
    def request_stop(self,o,h,holder=None,prefix='stop'):
        holder=o if holder is None else holder
        sent,accepted=prefix+'_sent',prefix+'_accepted'
        if holder.get(sent):
            if not holder.get(accepted): raise Fault('STOP_UNCONFIRMED','Исход запроса остановки неизвестен; повтор не отправлен.','unknown',hash=h)
            return
        holder[sent]=True; self.store.put_op(o)
        try: self.api.stop(h)
        except Fault as e:
            if e.details.get('request_sent') is False: holder[sent]=False; self.store.put_op(o)
            raise
        holder[accepted]=True; self.store.put_op(o)
    def request_remove(self,o,h):
        if o.get('remove_sent'):
            if not o.get('remove_accepted'): raise Fault('REMOVE_UNCONFIRMED','Исход удаления записи неизвестен; повтор не отправлен.','unknown',hash=h)
            return
        if o['stage']=='remove_requested':
            raise Fault('REMOVE_UNCONFIRMED','Старое намерение удаления без подтверждения; повтор не отправлен.','unknown',hash=h)
        o['remove_sent']=True; self.stage(o,'remove_requested')
        try: self.api.remove(h)
        except Fault as e:
            if e.details.get('request_sent') is False:
                o['remove_sent']=False; self.stage(o,'data_verified' if o['kind']=='complete' else 'stopped')
            raise
        o['remove_accepted']=True; self.store.put_op(o)
    def wait_state(self,h,predicate,seconds=2):
        if self.nonblocking: return self.api.get(h)
        end=min(time.monotonic()+seconds,self.budget.end)
        while True:
            t=self.api.get(h)
            if t and predicate(t): return t
            if time.monotonic()>=end: return t
            time.sleep(min(0.1,max(0,end-time.monotonic())))
    def roots(self):
        roots={k:(lexical(v,'') if k=='completed' else guarded(v)) for k,v in self.c['paths'].items()}
        for k,p in roots.items():
            if k=='completed': continue  # Completed payload is outside ongoing responsibility.
            if not p.is_dir(): raise Fault('PATH_UNAVAILABLE','Рабочий каталог недоступен.',role=k,path=str(p))
        return roots
    def snapshot(self,include_torrents=False):
        observed_at=now(); rows=self.api.torrents(); prefs=self.api.prefs(); transfer=self.api.call('transfer/info')
        rechecks=self.observe_rechecks(rows,observed_at)
        pending=self.store.pending()
        progress=[]
        for o in pending:
            item={'id':o['id'],'hash':o['hash'],'kind':o['kind'],'stage':o['stage'],'last_file':o.get('moving_file')}
            if o['kind']=='complete':
                item.update(files_total=len(o['files']),files_handed_off=sum(f.get('handoff')=='handed_off' for f in o['files']))
                item['files_remaining']=item['files_total']-item['files_handed_off']
            row=self.store.db.execute('SELECT updated FROM operations WHERE id=?',(o['id'],)).fetchone()
            item['last_activity_at']=row[0] if row else None; progress.append(item)
        effective=min(self.c['policy']['download_slots'],self.store.setting('effective_slots',self.c['policy']['download_slots']))
        watch=dict(self.store.setting('watch',{}))
        from datetime import datetime, timezone
        try: age=(datetime.now(timezone.utc)-datetime.fromisoformat(watch['heartbeat'])).total_seconds()
        except (KeyError,ValueError): age=float('inf')
        watch['heartbeat_fresh']=0<=age<=max(15,self.c['resources'].get('sample_seconds',5)*3)
        watch['alive']=False
        if os.name=='nt' and watch.get('pid') and watch['heartbeat_fresh']:
            import ctypes
            k=ctypes.WinDLL('kernel32',use_last_error=True); k.OpenProcess.restype=ctypes.c_void_p
            handle=k.OpenProcess(0x1000,False,watch['pid'])
            if handle:
                code=ctypes.c_ulong(); watch['alive']=bool(k.GetExitCodeProcess(ctypes.c_void_p(handle),ctypes.byref(code)) and code.value==259); k.CloseHandle(ctypes.c_void_p(handle))
        watch['heartbeat_age_seconds']=round(age,1) if age!=float('inf') else None
        from .presentation import diagnosis
        reasons=dict(self.store.db.execute('SELECT hash,reason FROM stops'))
        diagnostics=[diagnosis(t,reasons.get(t['hash']),pending,self.valid_path(t),done=complete(t),in_check=checking(t),is_stopped=stopped(t)) for t in rows]
        from .executor import runtime_status
        result={'snapshot_at':now(),'policy_revision':self.c['revision'],'executor':runtime_status(self.store),'versions':{'qBittorrent':self.api.version,'api':self.api.api_version},'desired':self.c['policy'],
            'observed':{'client_count':len(rows),'allowed_downloads':sum(not stopped(t) and not complete(t) for t in rows),'transferring_downloads':sum(t.get('dlspeed',0)>0 for t in rows),'completed':sum(complete(t) for t in rows),'completion_basis':'client_progress_only; file manifest not audited by status','checking':sum(checking(t) for t in rows),'states':dict(Counter(t['state'] for t in rows)),
            'native_caps':{k:prefs.get(k) for k in ('max_active_downloads','max_active_torrents','max_active_uploads','queueing_enabled','dont_count_slow_torrents')},'speed_caps':{'down_bps':prefs.get('dl_limit'),'up_bps':prefs.get('up_limit')},'transfer':{k:transfer.get(k) for k in ('dl_info_speed','up_info_speed','connection_status','dht_nodes')},'effective_download_slots':effective,'resource_monitor_enabled':self.c['resources']['enabled'],'continuous_monitor':watch,'last_metrics':self.store.setting('last_metrics',{}),'completed_data_audited':False,
            'drift':{'native_downloads':prefs.get('max_active_downloads')!=max(1,self.c['policy']['download_slots']),'native_total':prefs.get('max_active_torrents')!=max(1,self.c['policy']['download_slots']),'down_bps':not speed_limit_matches(prefs.get('dl_limit'),self.c['policy']['down_bps']),'up_bps':not speed_limit_matches(prefs.get('up_limit'),self.c['policy']['up_bps']),'slots_exceeded':sum(not stopped(t) and not complete(t) for t in rows)>effective},'unexpected_paths':[summary(t) for t in rows if not self.valid_path(t)],'legacy_paths':[summary(t) for t in rows if t['hash'] in self.c['policy']['legacy_k'] and norm(t['save_path'])==norm(self.c['paths']['completed'])]},'pending_operations':[{'id':o['id'],'hash':o['hash'],'kind':o['kind'],'stage':o['stage']} for o in pending],'operation_progress':progress,'rechecks':rechecks,'recheck_basis':'saved receipts; read-only status does not update lifecycle' if getattr(self.store,'readonly',False) else 'executor observations persisted','diagnostics':diagnostics}
        if include_torrents: result['torrents']=[summary(t) for t in rows]
        return result
    def valid_path(self,t):
        p=self.c['paths']
        working=norm(t['save_path'])==norm(p['working'])
        download=t.get('download_path')
        return (working and (not download or norm(download)==norm(p['working'])) and not t.get('auto_tmm')) or (t['hash'] in self.c['policy']['legacy_k'] and norm(t['save_path'])==norm(p['completed']) and not t.get('auto_tmm'))
    @timed('registry')
    def reconcile_index(self,entries,rows):
        return reconcile(self,entries,rows)
    def add_preconditions(self):
        p=self.api.prefs(); roots=self.roots()
        if norm(p.get('save_path',''))!=norm(roots['working']) or p.get('temp_path_enabled') or p.get('auto_tmm_enabled'):
            raise Fault('CLIENT_PATH_POLICY','Настройки путей клиента не соответствуют новым загрузкам в m.',save_path=p.get('save_path'),temp_path_enabled=p.get('temp_path_enabled'),auto_tmm_enabled=p.get('auto_tmm_enabled'))
    def pending_scopes(self,ignore=None):
        result=[]
        for o in self.store.pending():
            if o['id']==ignore: continue
            s=scope(o)
            if o['kind']=='isolate_shared':
                s['paths'].extend(str(lexical(self.c['paths']['working'],o[n])) for n in ('old_relative','new_relative','temp_relative'))
                s['paths'].append(str(lexical(self.c['paths']['working'],Path(o['new_relative']).parts[0])))
            if o['kind']=='add' and not s['paths']:
                try: s['paths']=[x['file'] for x in claims(self.c['paths']['working'],self.metadata(o['path'])['files'],'path')]
                except (Fault,OSError): s['global']=True
            result.append({'operation_id':o['id'],**s})
        return result
    def global_pending(self): return any(s['global'] for s in self.pending_scopes())
    def pending_hashes(self): return set().union(*(s['hashes'] for s in self.pending_scopes())) if self.store.pending() else set()
    def guard_pending(self,h,paths=(),ignore=None):
        for s in self.pending_scopes(ignore):
            if s['global'] or h in s['hashes'] or any(overlaps(p,q) for p in paths for q in s['paths']):
                raise Fault('TASK_RECOVERY_REQUIRED','Задача зависит от незавершённой операции.','partial',hash=h,operation_id=s['operation_id'],global_scope=s['global'])
    def owned_paths(self,rows=None):
        rows=self.api.torrents() if rows is None else rows
        def read(t):
            self.budget.check()
            return t['hash'],claims(t['save_path'],self.api.files(t['hash']))
        if len(rows)<2: return dict(read(t) for t in rows)
        # Workers only read API and validate lexical names; SQLite mutations
        # and filesystem handoffs remain on the coordinator thread.
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            return dict(pool.map(read,rows))
    def ensure_exclusive(self,h,owned,rows=None,ignore=None,owners=None):
        self.guard_pending(h,[x['file'] for x in owned],ignore)
        if owners is None:
            rows=self.api.torrents() if rows is None else rows
            others=self.owned_paths([t for t in rows if t['hash']!=h])
        else: others=owners
        collisions=path_conflicts(owned,[x for owner,items in others.items() if owner!=h for x in items])
        if collisions: raise Fault('SHARED_FILES','Пути загрузки пересекаются с другой раздачей.',hash=h,paths=collisions)
    def path_findings(self,rows=None):
        rows=self.api.torrents() if rows is None else rows; owners=self.owned_paths(rows); result=[]
        for h,items in owners.items():
            collisions=path_conflicts(items,[x for other,paths in owners.items() if other!=h for x in paths])
            if collisions: result.append({'code':'SHARED_FILES','hash':h,'message':'Пересечение путей раздач; запуск запрещён.','paths':collisions})
        return result
    def observe_rechecks(self,rows,observed_at=None):
        current={t['hash']:t for t in rows}; observed_at=observed_at or now()
        def update(ledger):
            for h,item in ledger.items():
                t=current.get(h)
                if not t or observed_at<item['requested_at']: continue
                if checking(t) and not item.get('completion_observed'):
                    item.update(phase='checking',start_observed=True,started_at=item.get('started_at',observed_at))
                elif item.get('start_observed') and item.get('phase')=='checking':
                    item.update(phase='finished',completion_observed=True,finished_at=observed_at,final_state=t['state'],progress=t.get('progress'),amount_left=t.get('amount_left'),fully_downloaded=complete(t))
            return ledger
        ledger=(self.store.setting('rechecks',{}) if getattr(self.store,'readonly',False)
                else self.store.update_setting('rechecks',update,{}))
        return [{'hash':h,**item} for h,item in ledger.items()]
    def perform_recheck(self,o,task):
        h=task['hash']; t=self.api.get(h)
        if not t or not self.valid_path(t) or norm(t['save_path'])!=norm(self.c['paths']['working']):
            raise Fault('COMPLETED_DATA_OUTSIDE_SCOPE','Проверка допустима только для данных в m.')
        samples=[]
        if not task.get('sent'):
            if any(checking(x) for x in self.api.torrents()): raise Fault('CHECK_PENDING','Уже идёт другая проверка.','partial')
            import threading
            stop=threading.Event()
            def observer():
                while not stop.is_set():
                    try:
                        row=self.api.get(h)
                        if row: samples.append({'at':now(),'torrent':summary(row)})
                    except Fault: return
                    stop.wait(0.05)
            task.update(sent=True,sent_at=now(),baseline=summary(t)); self.store.put_op(o)
            thread=threading.Thread(target=observer,name='qbctl-recheck-observer',daemon=True); thread.start()
            try:
                self.api.call('torrents/recheck',{'hashes':h},text=True)
                task['accepted']=True; self.store.put_op(o)
                t=self.wait_state(h,checking)
            finally:
                stop.set(); thread.join(timeout=5)
        else:
            t=self.api.get(h)
        seen=next((s for s in samples if checking(s['torrent'])),None)
        if t and checking(t) and not seen: seen={'at':now(),'torrent':summary(t)}
        if not task.get('accepted') and not seen:
            raise Fault('RECHECK_UNCONFIRMED','Ответ на запрос не подтверждён; автоматического повтора нет.','unknown',hash=h)
        item={'operation_id':o['id'],'phase':'checking' if seen else 'unobserved','request_accepted':bool(task.get('accepted')),'start_observed':bool(seen),'completion_observed':False,'requested_at':task.get('sent_at',now())}
        if seen:
            item['started_at']=seen['at']
            finished=next((s for s in reversed(samples) if s['at']>=seen['at'] and not checking(s['torrent'])),None)
            if finished:
                final=finished['torrent']; item.update(phase='finished',completion_observed=True,finished_at=finished['at'],final_state=final['state'],progress=final.get('progress'),amount_left=final.get('amount_left'),fully_downloaded=complete(final))
        def update(ledger): ledger[h]=item; return ledger
        self.store.update_setting('rechecks',update,{}); task['recheck_receipt']=item
        if not seen: self.issues.append({'code':'RECHECK_OBSERVATION_MISSED','hash':h,'message':'API принял запрос; начало и окончание не наблюдались. Это не подтверждение полной готовности.','retryable':False})
        return t
    def metadata(self,p):
        self.budget.check(); p=Path(p); fp=fingerprint(p)
        r=self.store.db.execute('SELECT fingerprint,metadata FROM cache WHERE path=?',(str(p),)).fetchone()
        if r and json.loads(r[0])==fp: return json.loads(r[1])
        data=p.read_bytes()
        if len(data)>16*1024*1024: raise Fault('FORMAT_UNSUPPORTED','Torrent превышает допустимые 16 MiB.',path=str(p))
        m=self.api.metadata(data); m['sha256']=__import__('hashlib').sha256(data).hexdigest()
        self.store.db.execute('INSERT OR REPLACE INTO cache VALUES(?,?,?)',(str(p),json.dumps(fp),json.dumps(m))); self.store.db.commit()
        return m
    @timed('index')
    def index(self):
        # Batch SQLite commits and bound parallel local metainfo requests.
        # This does not add torrents or read payload data.
        import concurrent.futures, hashlib, threading, stat
        from .directory import scan_torrents
        roots=self.roots(); ordered=[]; cached={}; todo=[]; observed={}; cache_rows=None
        root_identity={role:fingerprint(roots[role]) for role in ('archive','incoming')}
        for role in ('archive','incoming'):
            for p,fp in scan_torrents(roots[role],self.budget):
                self.budget.check()
                # Parents are validated once before/after this read-only scan.
                # Every selected mutation still uses guarded() and its SHA-256.
                ordered.append((role,p))
                observed[str(p)]=fp
                memory=self._index_memory.get(str(p))
                if memory and memory[0]==fp:
                    cached[str(p)]=memory[1]; continue
                if cache_rows is None:
                    cache_rows={r[0]:r[1:] for r in self.store.db.execute('SELECT path,fingerprint,metadata FROM cache')}
                row=cache_rows.get(str(p))
                if row and json.loads(row[0])==fp: cached[str(p)]=json.loads(row[1])
                else: todo.append((p,fp))
        local=threading.local()
        def parse(item):
            self.budget.check()
            p,fp=item; data=p.read_bytes()
            if len(data)>16*1024*1024: raise Fault('FORMAT_UNSUPPORTED','Torrent превышает 16 MiB.',path=str(p))
            if not hasattr(local,'api'):
                from .api import API
                local.api=API(self.api.config,deadline=getattr(self.api,'deadline',None),metrics=self.api.metrics,metrics_lock=self.api._metrics_lock) if isinstance(self.api,API) else self.api
            m=local.api.metadata(data); m['sha256']=hashlib.sha256(data).hexdigest()
            if fingerprint(p)!=fp: raise Fault('PLAN_STALE','Torrent изменён при индексировании.',path=str(p))
            return p,fp,m
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            for offset in range(0,len(todo),32):
                self.budget.check()
                for p,fp,m in pool.map(parse,todo[offset:offset+32]):
                    cached[str(p)]=m
                    self.store.db.execute('INSERT OR REPLACE INTO cache VALUES(?,?,?)',(str(p),json.dumps(fp),json.dumps(m)))
                self.store.db.commit()
        self.roots()
        for role in ('archive','incoming'):
            if fingerprint(roots[role])!=root_identity[role]:
                raise Fault('INDEX_CHANGED','Корень изменён при индексировании.','partial',role=role)
            final={str(p):fp for p,fp in scan_torrents(roots[role],self.budget)}
            if set(final)!={str(p) for r,p in ordered if r==role}:
                raise Fault('INDEX_CHANGED','Состав папки изменился при индексировании.','partial')
            for path,fp in final.items():
                if fp!=observed[path]: raise Fault('INDEX_CHANGED','Torrent изменён во время индексирования.','partial',path=path)
        self._index_memory={str(p):(observed[str(p)],cached[str(p)]) for role,p in ordered}
        return [{'path':str(p),'role':role,**cached[str(p)]} for role,p in ordered]
    @timed('preflight')
    def manifest(self,t,source,entries,all_rows,allow_archive=False,queued_claims=None):
        if not complete(t): raise Fault('NOT_COMPLETE','Задача не завершена.',hash=t['hash'])
        if not self.valid_path(t) or t.get('auto_tmm'):
            raise Fault('CLIENT_PATH_POLICY','Непредусмотренный путь или автоматическое управление.',hash=t['hash'],save_path=t['save_path'])
        roots=self.roots(); files=self.api.files(t['hash'])
        legacy=t['hash'] in self.c['policy']['legacy_k'] and norm(t['save_path'])==norm(roots['completed'])
        if not files: raise Fault('METADATA_MISSING','Нет файловой ведомости.',hash=t['hash'])
        mf=[]; names=set()
        for f in files:
            if f.get('is_padding') or f.get('padding'): continue
            if f.get('priority',0)==0 or f.get('progress')!=1:
                raise Fault('PARTIAL_SELECTION','Не все файлы выбраны и скачаны.',hash=t['hash'],file=f.get('name'))
            s=lexical(t['save_path'],f['name']) if legacy else guarded(t['save_path'],f['name'])
            d=lexical(roots['completed'],f['name']) if legacy else guarded(roots['completed'],f['name'])
            if norm(s) in names: raise Fault('SHARED_FILES','Повторяющиеся пути файлов.',path=str(s))
            names.add(norm(s))
            if legacy:
                mf.append({'relative':f['name'],'source':str(s),'destination':str(d),'size':f['size'],'handoff':'handed_off','evidence':'authorized_legacy_client_completion'})
                continue
            if not s.is_file() or s.stat().st_size!=f['size']: raise Fault('FILE_MISMATCH','Исходный файл отсутствует или неверного размера.',path=str(s),expected_size=f['size'])
            if norm(s)!=norm(d) and d.exists(): raise Fault('DESTINATION_EXISTS','Готовое назначение уже занято.',path=str(d))
            if s.stat().st_nlink>1: raise Fault('SHARED_FILES','Исходный файл имеет несколько жёстких ссылок.',path=str(s))
            if s.stat().st_dev!=roots['completed'].stat().st_dev: raise Fault('CROSS_VOLUME','Разные тома; перенос запрещён.',path=str(s))
            mf.append({'relative':f['name'],'source':str(s),'destination':str(d),'size':f['size'],'identity':fingerprint(s),'handoff':'prepared'})
        # Client manifest must cover the complete metadata, including rename-safe size count.
        metadata_files=[f for f in source.get('files',[]) if not (f.get('is_padding') or f.get('padding') or 'p' in f.get('attr',''))]
        if not metadata_files or len(metadata_files)!=len(mf) or [f.get('length') for f in metadata_files]!=[f['size'] for f in mf]:
            raise Fault('MANIFEST_MISMATCH','Файловая ведомость клиента не совпадает с полным metainfo.',hash=t['hash'])
        others=set()
        for other in all_rows:
            self.budget.check()
            if other['hash']==t['hash']: continue
            for f in self.api.files(other['hash']):
                others.add(norm(lexical(other['save_path'],f['name'])))
        overlap=(names|{norm(f['destination']) for f in mf}) & others
        if overlap: raise Fault('SHARED_FILES','Исходные или целевые файлы используются другой задачей.',hash=t['hash'],paths=sorted(overlap))
        if queued_claims is None:
            trees={Path(f['relative']).parts[0].casefold() for f in mf if Path(f['relative']).parts}
            queued_claims=self.incoming_file_claims(entries,roots['working'],trees)
        # An unlisted file is safe to leave in m when an incoming torrent in t
        # explicitly claims that exact path and its declared size matches.
        # Payload handoff renames only manifest files; it never moves a tree.
        for first in (set() if legacy else {Path(f['source']).relative_to(Path(t['save_path'])).parts[0] for f in mf}):
            top=guarded(t['save_path'],first)
            if top.is_dir():
                target_top=guarded(roots['completed'],first)
                if norm(top)!=norm(target_top) and target_top.exists(): raise Fault('DESTINATION_EXISTS','Дерево назначения уже существует; слияние запрещено.',path=str(target_top))
                for base,dirs,fnames in os.walk(top,followlinks=False):
                    self.budget.check()
                    for name in dirs+fnames: guarded(t['save_path'],str((Path(base)/name).relative_to(Path(t['save_path']))))
                    for name in fnames:
                        p=Path(base)/name
                        if norm(p) not in names:
                            sizes=queued_claims.get(norm(p),set())
                            if len(sizes)==1 and p.stat().st_size==next(iter(sizes)): continue
                            raise Fault('FOREIGN_FILES','В дереве есть файлы вне ведомости или очереди.',path=str(p))
        archive=guarded(roots['archive'],Path(source['path']).name)
        if archive.exists() and not allow_archive: raise Fault('DESTINATION_EXISTS','Архивное имя занято.',path=str(archive))
        if Path(source['path']).stat().st_dev!=roots['archive'].stat().st_dev: raise Fault('CROSS_VOLUME','Архив находится на другом томе.')
        return {'id':uuid.uuid4().hex,'kind':'complete','hash':t['hash'],'stage':'prepared','source_torrent':source['path'],'archive_torrent':str(archive),'torrent_sha256':source['sha256'],'torrent_identity':fingerprint(source['path']),'files':mf,'save_path':t['save_path'],'created_at':now(),'verification_level':'manifest_verified'}
    def incoming_file_claims(self,entries,root,top_components):
        claims_by_path={}
        for entry in entries:
            if entry.get('role')!='incoming': continue
            for item in entry.get('files',[]):
                if item.get('is_padding') or item.get('padding') or 'p' in item.get('attr',''): continue
                relative=item.get('path')
                length=item.get('length')
                if not isinstance(relative,str) or not relative or not isinstance(length,int) or length<0: continue
                if not Path(relative).parts or Path(relative).parts[0].casefold() not in top_components: continue
                # This is only a path-ownership index; the path is never opened.
                # lexical() avoids an exists/lstat round trip per metadata entry.
                try: path=lexical(root,relative)
                except Fault: continue
                claims_by_path.setdefault(norm(path),set()).add(length)
        return claims_by_path
    @timed('planning')
    @read_phase
    def make_plan(self,max_complete=5,max_add=5,selected_hash=None,defer_manifests=False):
        self.api.compatible(); rows=self.api.torrents(); entries=self.index()
        registry=self.reconcile_index(entries,rows)
        pending=self.store.pending(); blocked_ids=self.pending_hashes()
        operations=[]; issues=list(registry['conflicts']); conflicts=set(registry['blocked_hashes'])
        selected_id=self.select(rows,selected_hash)['hash'] if selected_hash else None
        for t in sorted(rows,key=lambda t:t['hash']):
            if selected_id and t['hash']!=selected_id: continue
            if self.initial_completions is not None and t['hash'] not in self.initial_completions: continue
            self.budget.check()
            if not complete(t) or t['hash'] in blocked_ids or aliases(t)&conflicts: continue
            if len(operations)>=max_complete: break
            matches=[e for e in entries if e['role']=='incoming' and aliases(e)&aliases(t)]
            if len(matches)!=1:
                issues.append({'code':'SOURCE_MISSING' if not matches else 'SOURCE_AMBIGUOUS','message':'Нет одного исходного torrent в корне t.','hash':t['hash']}); continue
            try:
                if defer_manifests:
                    operations.append({'id':uuid.uuid4().hex,'kind':'complete','hash':t['hash'],'source_torrent':matches[0]['path'],'torrent_sha256':matches[0]['sha256'],'preflight_deferred':True})
                else: operations.append(self.manifest(t,matches[0],entries,rows))
            except Fault as e: issues.append(e.issue())
        excluded={a for t in rows for a in aliases(t)}|{o['hash'] for o in pending}|{a for e in entries if e['role']=='archive' for a in aliases(e)}|finished_aliases(self.store)|conflicts
        candidates=[]
        present={t['hash'] for t in rows}
        reserved_adds=sum(o['kind']=='add' and o['hash'] not in present for o in pending)
        wanted=max(0,self.c['policy']['target_client_count']-len(rows)-reserved_adds+len(operations))
        owners=self.owned_paths(rows) if max_add and wanted else {}
        for e in entries:
            if e['role']!='incoming' or aliases(e)&excluded: continue
            if len(candidates)>=min(max_add,wanted): break
            owned=claims(self.c['paths']['working'],e['files'],'path')
            try: self.ensure_exclusive(e['id'],owned,rows,owners=owners)
            except Fault as fault:
                issues.append(fault.issue()); excluded.update(aliases(e)); continue
            excluded.update(aliases(e)); candidates.append({'id':uuid.uuid4().hex,'hash':e['id'],'path':e['path'],'sha256':e['sha256']})
            owners[e['id']]=owned
        plan={'schema_version':1,'id':uuid.uuid4().hex,'created_at':now(),'policy_revision':self.c['revision'],'policy_digest':digest(self.c),'client_ids':sorted(t['hash'] for t in rows),'completion_scope':'single_snapshot','completion_candidates':[t['hash'] for t in rows if complete(t)],'operations':operations,'additions':candidates,'recover':[o['id'] for o in pending],'issues':issues,'registry':registry}
        folder=ROOT/'plans'; folder.mkdir(exist_ok=True)
        (folder/(plan['id']+'.json')).write_text(json.dumps(plan,ensure_ascii=False,indent=2),encoding='utf-8')
        return plan
    def stage(self,o,s):
        if o['kind']=='complete':
            stages=('prepared','stop_requested','stopped','archive_requested','archived','moving','data_verified','remove_requested','removed','finished')
            current=o.get('stage')
            # Only a proven unsent remove may roll back for a new request.
            rollback=current=='remove_requested' and s=='data_verified' and o.get('remove_sent') is False
            if current in stages and s in stages and not rollback and stages.index(s)<stages.index(current): return
        o['stage']=s; self.store.put_op(o)
    def verify_target(self,f):
        guarded(self.c['paths']['completed'],f['relative'])
        d=Path(f['destination'])
        if not d.is_file() or not samefile_identity(d,f['identity']): raise Fault('FILE_MISMATCH','Назначение не подтверждено по файловой идентичности.',path=str(d))
        if norm(f['source'])!=norm(d) and Path(f['source']).exists(): raise Fault('FILE_MISMATCH','Исходный файл ещё существует.',path=f['source'])
    @timed('handoff')
    def recover_complete(self,o):
        self.api.compatible(); self.roots(); self.budget.check()
        h=o['hash']; t=self.api.get(h)
        if t and o['stage']=='remove_requested' and not o.get('remove_sent'):
            raise Fault('REMOVE_UNCONFIRMED','Старое намерение удаления без подтверждения; повтор не отправлен.','unknown',hash=h)
        # Guard persisted journal paths; never trust a modified journal as free-form file instructions.
        s=guarded(self.c['paths']['incoming'],Path(o['source_torrent']).name)
        d=guarded(self.c['paths']['archive'],Path(o['archive_torrent']).name)
        if norm(s)!=norm(o['source_torrent']) or norm(d)!=norm(o['archive_torrent']): raise Fault('PATH_OUTSIDE_ROOT','Журнал содержит неверные torrent-пути.')
        for f in o['files']:
            if norm(lexical(o['save_path'],f['relative']))!=norm(f['source']) or norm(lexical(self.c['paths']['completed'],f['relative']))!=norm(f['destination']):
                raise Fault('PATH_OUTSIDE_ROOT','Журнал содержит неверный путь данных.')
        if norm(o['save_path']) not in (norm(self.c['paths']['working']),norm(self.c['paths']['completed'])): raise Fault('PATH_OUTSIDE_ROOT','Неверный рабочий корень в журнале.')
        if t:
            if norm(t['save_path'])!=norm(o['save_path']) or t.get('auto_tmm'): raise Fault('PLAN_STALE','Путь/autoTMM изменён.',hash=h)
            current=self.api.files(h)
            if sorted((x['name'],x['size']) for x in current)!=sorted((x['relative'],x['size']) for x in o['files']): raise Fault('PLAN_STALE','Файловая ведомость изменилась.',hash=h)
            if not complete(t) and not d.exists(): raise Fault('NOT_COMPLETE','Клиент больше не подтверждает завершение до архивирования.',hash=h)
            if not stopped(t):
                if o['stage']!='stop_requested': self.stage(o,'stop_requested')
                self.store.set_stop(h,'completion'); self.request_stop(o,h)
                t=self.wait_state(h,stopped)
                if not t or not stopped(t): raise Fault('STOP_PENDING','Остановка ещё не подтверждена.','partial',hash=h)
            if not d.exists() and not complete(t): raise Fault('NOT_COMPLETE','Завершение не подтверждено после остановки.',hash=h)
            self.stage(o,'stopped')
        if s.exists() and d.exists(): raise Fault('DESTINATION_EXISTS','Torrent существует и в источнике, и в архиве.',path=str(d))
        if s.exists():
            if not t: raise Fault('RECOVERY_AMBIGUOUS','Клиент отсутствует до архивирования.')
            if sha_file(s)!=o['torrent_sha256']: raise Fault('PLAN_STALE','Исходный torrent изменён.')
            current=self.api.files(h)
            if any(f.get('priority',0)==0 or f.get('progress')!=1 for f in current if not (f.get('is_padding') or f.get('padding'))): raise Fault('PARTIAL_SELECTION','Выбор/завершение файлов изменились перед архивированием.')
            # Recheck all no-overwrite destinations before archiving.
            for f in o['files']:
                if f.get('handoff')=='handed_off': continue
                guarded(o['save_path'],f['relative']); guarded(self.c['paths']['completed'],f['relative'])
                if norm(f['source'])!=norm(f['destination']) and Path(f['destination']).exists(): raise Fault('DESTINATION_EXISTS','Назначение занято до архивирования.',path=f['destination'])
                if not Path(f['source']).is_file() or not samefile_identity(f['source'],f['identity']): raise Fault('FILE_MISMATCH','Источник изменён.',path=f['source'])
            self.stage(o,'archive_requested'); move_no_replace(s,d)
        if not d.is_file() or sha_file(d)!=o['torrent_sha256'] or s.exists(): raise Fault('ARCHIVE_MISMATCH','Архивирование не подтверждено.')
        self.stage(o,'archived')
        # Persist all remaining move intentions before any rename. Each handoff
        # still receives its own FULL transaction; no confirmed file is reread.
        if any(f.get('handoff') not in ('move_requested','handed_off') for f in o['files']):
            for f in o['files']:
                if f.get('handoff')!='handed_off': f['handoff']='move_requested'
            self.stage(o,'moving')
        if any(f.get('handoff')!='handed_off' for f in o['files']): self.stage(o,'moving')
        for ordinal,f in enumerate(o['files']):
            if f.get('handoff')=='handed_off': continue
            self.budget.check(); src,dst=Path(f['source']),Path(f['destination'])
            guarded(o['save_path'],f['relative']); guarded(self.c['paths']['completed'],f['relative'])
            if norm(src)==norm(dst): raise Fault('LEGACY_EVIDENCE_MISSING','Нет подтверждения разрешённого исключения.')
            if src.exists() and dst.exists(): raise Fault('DESTINATION_EXISTS','Оба пути данных существуют.',source=str(src),destination=str(dst))
            if src.exists():
                if not t: raise Fault('RECOVERY_AMBIGUOUS','Запись исчезла до переноса всех файлов.',hash=h)
                if not samefile_identity(src,f['identity']): raise Fault('FILE_MISMATCH','Источник изменён.',path=str(src))
                guarded(self.c['paths']['completed'],f['relative']); dst.parent.mkdir(parents=True,exist_ok=True)
                guarded(self.c['paths']['completed'],f['relative'])
                o['moving_file']=f['relative']; move_no_replace(src,dst)
                f['evidence']='windows_rename_write_through'
            else:
                if f.get('handoff')!='move_requested': raise Fault('RECOVERY_AMBIGUOUS','Источник исчез до записанного намерения переноса.',path=str(src))
                self.verify_target(f)
                f['evidence']='recovered_file_identity'
            f['moved']=True; f['handoff']='handed_off'; f['handed_off_at']=now(); self.store.put_op(o,file_ordinal=ordinal)
        if not all(f.get('handoff')=='handed_off' for f in o['files']): raise Fault('HANDOFF_INCOMPLETE','Не все файлы переданы.')
        self.stage(o,'data_verified')
        if t:
            # A last check catches manual resume/path changes before removing a record.
            latest=self.api.get(h)
            if not latest or not stopped(latest) or norm(latest['save_path'])!=norm(o['save_path']): raise Fault('PLAN_STALE','Запись изменена перед удалением.',hash=h)
            self.request_remove(o,h)
        if self.api.get(h): raise Fault('REMOVE_PENDING','Клиент принял удаление записи; ожидается отсутствие в снимке.','partial',hash=h,request_accepted=bool(o.get('remove_accepted')))
        self.stage(o,'removed')
        if s.exists() or not d.is_file() or sha_file(d)!=o['torrent_sha256']: raise Fault('ARCHIVE_MISMATCH','Итоговый архив не подтверждён.')
        # Empty ancestors only; never recursive deletion.
        for f in o['files']:
            if norm(f['source'])==norm(f['destination']): continue
            parent=Path(f['source']).parent
            while norm(parent)!=norm(o['save_path']) and parent.is_relative_to(Path(o['save_path'])):
                try: parent.rmdir()
                except OSError: break
                parent=parent.parent
        self.store.set_stop(h,None); self.stage(o,'finished')
        moved=[f for f in o['files'] if f.get('evidence') in ('windows_rename_write_through','recovered_file_identity')]
        self.actions.append({'action':'complete','hash':h,'result':'verified','postconditions':{'torrent_archived':True,'client_removed':True,'delete_files':False,'data_handed_off':True,'completed_data_audited':False,'files':len(o['files']),'bytes':sum(f['size'] for f in o['files']),'files_moved':len(moved),'bytes_moved':sum(f['size'] for f in moved),'files_retained_legacy':sum(f.get('evidence')=='authorized_legacy_client_completion' for f in o['files']),'verification_level':'recorded_handoff'}})
    @timed('refill')
    def execute_add(self,o):
        self.api.compatible(); self.budget.check()
        h=o['hash']; t=self.api.get(h)
        if t and checking(t):
            if not self.valid_path(t) or norm(t['save_path'])!=norm(self.c['paths']['working']): raise Fault('CLIENT_PATH_POLICY','Проверяемая задача имеет неверный путь.',hash=h)
            if o['stage']!='checking': self.stage(o,'checking')
            raise Fault('CHECK_PENDING','Существующие данные проверяются; продолжение отложено.','partial',hash=h)
        if not t:
            self.add_preconditions()
            if o['stage']!='prepared':
                accepted=bool(o.get('add_accepted'))
                raise Fault('ADD_PENDING','Запись добавления ещё не найдена; запрос не повторяется.','partial' if accepted else 'unknown',hash=h,operation_id=o['id'],request_accepted=accepted)
            rows=self.api.torrents()
            if len(rows)>=self.c['policy']['target_client_count']: raise Fault('QUEUE_FULL','Целевое число уже достигнуто; кандидат не добавлен.','partial')
            present={t['hash'] for t in rows}
            reserved=sum(other['kind']=='add' and other['id']!=o['id'] and other['hash'] not in present for other in self.store.pending())
            if len(rows)+reserved>=self.c['policy']['target_client_count']:
                raise Fault('ADD_ADMISSION_WAIT','Место зарезервировано ещё не видимым добавлением; новый запрос отложен.','partial')
            if any(checking(t) for t in rows): raise Fault('CHECK_PENDING','Уже идёт проверка; добавление отложено.','partial')
            p=guarded(self.c['paths']['incoming'],Path(o['path']).name)
            if norm(p)!=norm(o['path']) or sha_file(p)!=o['sha256'] or self.api.metadata(p.read_bytes())['id']!=h: raise Fault('PLAN_STALE','Входной torrent изменился.')
            entries=self.index(); reg=self.reconcile_index(entries,rows); meta=self.metadata(p)
            matching=[e for e in entries if e['role']=='incoming' and aliases(e)&aliases(meta)]
            if len(matching)!=1 or aliases(meta)&set(reg['blocked_hashes']): raise Fault('SOURCE_AMBIGUOUS','Нет уникального входного torrent.',hash=h)
            if aliases(meta)&finished_aliases(self.store) or any(e['role']=='archive' and aliases(e)&aliases(meta) for e in entries): raise Fault('ARCHIVED_HASH','Хеш уже обработан или архивирован.',hash=h)
            owned=claims(self.c['paths']['working'],meta['files'],'path')
            self.ensure_exclusive(h,owned,rows,ignore=o['id'])
            o['payload_paths']=[x['file'] for x in owned]; self.store.put_op(o)
            space=disk_admission(self,sum(f['length'] for f in meta['files']))
            if not space['ok']: raise Fault('DISK_RESERVATION','Недостаточно места с учётом резерва и незавершённых загрузок.',**space)
            self.stage(o,'add_requested')
            try: self.api.add(p.read_bytes(),self.c['paths']['working'])
            except Fault as e:
                if e.details.get('request_sent') is False: self.stage(o,'prepared')
                raise
            o['add_accepted']=True; self.store.put_op(o)
            t=self.wait_state(h,lambda x:True)
            if not t: raise Fault('ADD_PENDING','Клиент принял запрос; ожидается появление записи.','partial',hash=h,request_accepted=True)
        if not self.valid_path(t) or norm(t['save_path'])!=norm(self.c['paths']['working']):
            if not stopped(t): self.request_stop(o,h,prefix='add_stop')
            raise Fault('CLIENT_PATH_POLICY','Добавленная задача имеет неправильный путь/autoTMM и остановлена.',hash=h)
        if checking(t):
            if o['stage']!='checking': self.stage(o,'checking')
            raise Fault('CHECK_PENDING','Существующие данные проверяются; продолжение отложено.','partial',hash=h)
        try: self.ensure_exclusive(h,claims(t['save_path'],self.api.files(h)),ignore=o['id'])
        except Fault:
            if not stopped(t): self.request_stop(o,h,prefix='add_stop')
            raise
        if not stopped(t):
            self.request_stop(o,h,prefix='add_stop'); t=self.wait_state(h,stopped)
            if not t or not stopped(t): raise Fault('STOP_PENDING','Добавленная задача ещё не остановлена.','partial',hash=h)
        self.store.set_stop(h,'slots')
        self.stage(o,'finished')
        self.actions.append({'action':'add','hash':h,'result':'verified','postconditions':{'present':True,'save_path':t['save_path'],'auto_tmm':False,'state':t['state']}})
    @timed('control')
    def control(self,o):
        self.api.compatible(); self.budget.check()
        changes=o.get('changes',{})
        if changes:
            expected=o.get('policy_revision')
            applied=o.get('applied_revision')
            if expected is not None and self.c['revision'] not in (expected,applied):
                raise Fault('POLICY_SUPERSEDED','Политика изменена после команды; старые настройки не применены.')
            for section,vals in changes.items(): self.c[section].update(vals)
            if self.store.setting('control_config:'+o['id']) is not True:
                if applied is None:
                    o['applied_revision']=self.c['revision']+1; self.store.put_op(o)
                if self.c['revision']!=o['applied_revision']: save_config(self.c)
                self.store.set_setting('control_config:'+o['id'],True)
            if 'download_slots' in changes.get('policy',{}) or changes.get('resources',{}).get('enabled') is False: self.store.set_setting('effective_slots',self.c['policy']['download_slots'])
        prefs=o.get('preferences',{})
        if prefs:
            actual=self.api.prefs()
            if any(not preference_matches(k,actual.get(k),v) for k,v in prefs.items()):
                if o.get('preferences_sent'):
                    raise Fault('PREFERENCES_UNCONFIRMED','Настройки не подтверждены после отправки; повтор не выполнен.','unknown',expected=prefs)
                o['preferences_sent']=True; self.stage(o,'preferences_requested')
                try: self.api.call('app/setPreferences',{'json':json.dumps(prefs)},text=True)
                except Fault as e:
                    if e.details.get('request_sent') is False: o['preferences_sent']=False; self.store.put_op(o)
                    raise
                o['preferences_accepted']=True; self.store.put_op(o); actual=self.api.prefs()
            if any(not preference_matches(k,actual.get(k),v) for k,v in prefs.items()): raise Fault('PREFERENCES_UNCONFIRMED','Настройки API не совпали с запрошенными.','unknown',expected=prefs)
            o['preferences_observed']=True; self.store.put_op(o)
        for task in o.get('tasks',[]):
            if task.get('verified'): continue
            self.budget.check(); h=task['hash']; t=self.api.get(h)
            if not t: raise Fault('PLAN_STALE','Задача управления исчезла.',hash=h)
            if any(x['hash']==h and x['kind']=='complete' for x in self.store.pending()): raise Fault('RECOVERY_REQUIRED','Задача находится в незавершённом переносе.',hash=h)
            if o['stage']!='task_requested': self.stage(o,'task_requested')
            if task['action']=='pause':
                if not stopped(t):
                    self.request_stop(o,h,task,'pause')
                t=self.wait_state(h,stopped)
                if not t or not stopped(t): raise Fault('STOP_PENDING','Остановка ещё не подтверждена.','partial',hash=h)
                self.store.set_stop(h,task['reason'])
            elif task['action']=='resume':
                if not self.valid_path(t): raise Fault('CLIENT_PATH_POLICY','Неверный путь перед запуском.',hash=h)
                if task.get('start_sent'):
                    if stopped(t) and not task.get('start_accepted'): raise Fault('START_UNCONFIRMED','Исход запроса запуска неизвестен; повтор не отправлен.','unknown',hash=h)
                else:
                    if checking(t): raise Fault('CHECK_PENDING','Проверка ещё выполняется.','partial',hash=h)
                    self.ensure_exclusive(h,claims(t['save_path'],self.api.files(h)),ignore=o['id'])
                    self.add_preconditions()
                    live=self.api.torrents(); limit=min(self.c['policy']['download_slots'],self.store.setting('effective_slots',self.c['policy']['download_slots']))
                    if stopped(t) and not complete(t) and sum(not stopped(x) and not complete(x) for x in live)>=limit: raise Fault('SLOTS_EXCEEDED','Нет свободного места в лимите загрузок.')
                    space=disk_admission(self)
                    if not space['ok'] and not complete(t): raise Fault('DISK_RESERVATION','Недостаточно места для возобновления.',**space)
                    if stopped(t):
                        self.headroom('start_request',1)
                        task['start_sent']=True; self.store.put_op(o)
                        try: self.api.start(h)
                        except Fault as e:
                            if e.details.get('request_sent') is False: task['start_sent']=False; self.store.put_op(o)
                            raise
                        task['start_accepted']=True; self.store.put_op(o)
                t=self.wait_state(h,lambda x:not stopped(x) and x['state'] not in ('error','missingFiles','unknown','moving'))
                if t and t['state'] in ('error','missingFiles','unknown','moving'): raise Fault('START_FAILED','Клиент не подтверждает рабочее состояние задачи.',hash=h,state=t['state'])
                if not t or stopped(t): raise Fault('START_PENDING','Запуск не подтверждён.','partial',hash=h)
                self.store.set_stop(h,None)
            elif task['action']=='recheck':
                t=self.perform_recheck(o,task)
            task['verified']=True; self.store.put_op(o)
            self.actions.append({'action':task['action'],'hash':h,'result':'verified','postconditions':{'state':t['state'] if t else 'missing','recheck':task.get('recheck_receipt')}})
        self.stage(o,'finished')
        if changes: self.actions.append({'action':'policy','result':'verified','changes':changes})
        if prefs: self.actions.append({'action':'preferences','result':'verified','postconditions':prefs})
    def dispatch(self,o):
        existing=self.store.db.execute('SELECT stage FROM operations WHERE id=?',(o['id'],)).fetchone()
        if not existing: self.headroom(o['kind'])
        while True:
            try: return self.dispatch_once(o)
            except Fault as e:
                if not self.service_mode or e.code not in ('CHECK_PENDING','START_PENDING','STOP_PENDING'): raise
                item=self.service_waits.setdefault(o['id'],{'kind':o['kind'],'hash':o['hash'],'polls':0})
                item.update(reason=e.code); item['polls']+=1
                if self.nonblocking: raise
                if self.budget.left()<3: raise Fault('SERVICE_WAITING','Операция ещё ожидает подтверждения; повтор запроса не отправлен.','partial',operation_id=o['id'],hash=o['hash'],reason=e.code)
                time.sleep(min(0.2,self.budget.left()))
    def dispatch_once(self,o):
        # Fresh file lists for every journalled action; never reuse across actions.
        invalidate=getattr(self.api,'invalidate_files',None)
        if invalidate: invalidate()
        existing=self.store.db.execute('SELECT stage,body FROM operations WHERE id=?',(o['id'],)).fetchone()
        if existing and existing[0]=='finished':
            recorded=self.store.decode_op(existing[1])
            self.actions.append({'action':o['kind'],'hash':o['hash'],'result':'already_verified','postconditions':{'recorded_finished':True,'evidence':'historical_receipt','completed_data_audited':False}}); return
        if existing: o=self.store.decode_op(existing[1])
        attach=self.request_id and not o.get('request_id')
        if attach: o['request_id']=self.request_id
        if not existing or attach: self.store.put_op(o)
        from .repair import recover_prefix
        from .queue import recover_release
        {'complete':self.recover_complete,'add':self.execute_add,'control':self.control,'dedupe':self.recover_dedupe,'duplicate_cleanup':self.recover_duplicate_cleanup,'isolate_shared':self.recover_isolate_shared,'repair_prefix':lambda item:recover_prefix(self,item),'release':lambda item:recover_release(self,item)}[o['kind']](o)
    def recover(self):
        for o in self.store.pending():
            self.budget.check()
            try: self.dispatch(o)
            except Fault as e:
                self.issues.append({**e.issue(),'operation_id':o['id'],'affected_hashes':sorted(scope(o)['hashes'])})
                if scope(o)['global'] or e.code in ('BUDGET_EXHAUSTED','API_UNAVAILABLE','STATE_WRITE_FAILED','PATH_UNAVAILABLE'): raise
    @read_phase
    def enforce_slots(self,start=True):
        self.budget.check()
        pending_ops=self.store.pending()
        if self.global_pending(): raise Fault('RECOVERY_REQUIRED','Незавершённое глобальное управление блокирует планировщик.','partial')
        rows=self.api.torrents(); pending=self.pending_hashes()
        findings=self.path_findings(rows); collision_hashes={f['hash'] for f in findings}
        self.issues.extend(findings)
        for t in rows:
            if t['hash'] in collision_hashes and not stopped(t) and t['hash'] not in pending:
                self.dispatch({'id':uuid.uuid4().hex,'kind':'control','hash':t['hash'],'stage':'prepared','tasks':[{'hash':t['hash'],'action':'pause','reason':'path_collision'}]})
        rows=self.api.torrents()
        for t in rows:
            if t['hash'] in pending or self.valid_path(t): continue
            if not stopped(t):
                self.dispatch({'id':uuid.uuid4().hex,'kind':'control','hash':t['hash'],'stage':'prepared','tasks':[{'hash':t['hash'],'action':'pause','reason':'path_policy'}]})
            self.issues.append({'code':'CLIENT_PATH_POLICY','hash':t['hash'],'message':'Непредусмотренный путь; задача остановлена и автоматически не возобновляется.'})
        rows=self.api.torrents()
        space=disk_admission(self)
        low=space['free_bytes']<1024**3
        self.store.set_setting('disk_space',space)
        reserved=sum(not stopped(t) and not complete(t) and t['hash'] in pending for t in rows)
        slots=min(self.c['policy']['download_slots'],self.store.setting('effective_slots',self.c['policy']['download_slots']))
        slots=max(0,slots-reserved)
        if low: slots=0
        eligible=[t for t in rows if not complete(t) and t['hash'] not in pending and t['hash'] not in collision_hashes]
        active=sorted([t for t in eligible if not stopped(t)],key=lambda t:(t.get('priority',9999) if t.get('priority',-1)>=0 else 9999,t.get('added_on',0),t['hash']))
        reason='disk' if low else ('resources' if slots<self.c['policy']['download_slots'] else 'slots')
        for t in reversed(active[slots:]):
            self.budget.check(); self.dispatch({'id':uuid.uuid4().hex,'kind':'control','hash':t['hash'],'stage':'prepared','tasks':[{'hash':t['hash'],'action':'pause','reason':reason}]})
        free=max(0,slots-min(slots,len(active)))
        if free and not space['ok']:
            self.issues.append({'code':'DISK_RESERVATION','message':'Возобновление отложено: консервативный резерв места не обеспечен.',**space})
        if not start:
            self.verify_slots(); return
        if free and any(checking(t) for t in rows):
            if self.service_mode: self.wait_no_checks()
            else: self.verify_slots(); return
        for t in sorted(eligible,key=lambda t:(t.get('added_on',0),t['hash'])):
            if free<=0: break
            if stopped(t) and self.valid_path(t) and t['state'] not in ('error','missingFiles','unknown') and space['ok'] and self.store.stop_reason(t['hash']) in ('slots','resources','disk'):
                if self.service_mode: self.wait_no_checks()
                self.dispatch({'id':uuid.uuid4().hex,'kind':'control','hash':t['hash'],'stage':'prepared','tasks':[{'hash':t['hash'],'action':'resume'}]}); free-=1
        self.verify_slots()

    def verify_slots(self):
        actual=sum(not stopped(t) and not complete(t) for t in self.api.torrents())
        limit=min(self.c['policy']['download_slots'],self.store.setting('effective_slots',self.c['policy']['download_slots']))
        if actual>limit: raise Fault('SLOTS_UNCONFIRMED','Фактический предел ещё не соблюдён.','partial',actual=actual,limit=limit)
    def preflight_completions(self,operations):
        """Validate the whole frozen completion batch before its first stop."""
        if not operations: return []
        invalidate=getattr(self.api,'invalidate_files',None)
        if invalidate: invalidate()
        entries=self.index()
        rows=self.api.torrents()
        by_hash={t['hash']:t for t in rows}
        prepared=[]
        phase=getattr(self.api,'file_phase',None)
        with phase() if phase else contextlib.nullcontext():
            for original in operations:
                self.budget.check()
                try: self.guard_pending(original['hash'])
                except Fault as e:
                    self.issues.append(e.issue()); continue
                t=by_hash.get(original['hash'])
                if not t: raise Fault('PLAN_STALE','Готовая запись исчезла.',hash=original['hash'])
                source={'path':original['source_torrent'],**self.metadata(Path(original['source_torrent']))}
                try:
                    fresh=self.manifest(t,source,entries,rows)
                except Fault as e:
                    if not original.get('preflight_deferred') or e.result in ('unknown','error') or e.code=='BUDGET_EXHAUSTED': raise
                    self.issues.append(e.issue()); continue
                if fresh['torrent_sha256']!=original['torrent_sha256'] or (not original.get('preflight_deferred') and digest(fresh['files'])!=digest(original['files'])):
                    raise Fault('PLAN_STALE','Манифест изменился.',hash=original['hash'])
                deferred=original.get('preflight_deferred',False)
                if deferred: fresh['id']=original['id']
                else: fresh=original
                prepared.append((fresh,deferred))
        return prepared
    def apply_plan(self,plan):
        self.api.compatible()
        if plan['policy_digest']!=digest(self.c) or plan['policy_revision']!=self.c['revision']: raise Fault('PLAN_STALE','Политика изменилась после плана.')
        if sorted(t['hash'] for t in self.api.torrents())!=plan['client_ids']: raise Fault('PLAN_STALE','Состав клиента изменился после плана.')
        self.issues.extend(plan.get('issues',[]))
        if plan.get('control'):
            for task in plan['control'].get('tasks',[]): self.guard_pending(task['hash'])
            self.dispatch(plan['control'])
            from .queue import trim_queue
            if 'target_client_count' in plan['control'].get('changes',{}).get('policy',{}): trim_queue(self)
            self.enforce_slots(); return
        if not self.service_mode: self.recover()
        if self.global_pending(): raise Fault('RECOVERY_REQUIRED','Есть незавершённое глобальное управление.','partial')
        prepared=self.preflight_completions(plan['operations'])
        for o,deferred in prepared:
            self.budget.check()
            try: self.dispatch(o)
            except Fault as e:
                self.issues.append(e.issue())
                if self.service_mode and e.code in ('SERVICE_DEFERRED','SERVICE_WAITING'): break
                if not deferred or e.code in ('BUDGET_EXHAUSTED','API_UNAVAILABLE','STATE_WRITE_FAILED','PATH_UNAVAILABLE'): raise
        if self.global_pending(): return
        from .queue import trim_queue
        trim_queue(self)
        for candidate in plan['additions']:
            self.budget.check()
            if len(self.api.torrents())>=self.c['policy']['target_client_count']: break
            o={'id':candidate.get('id',plan['id']+candidate['hash']),'kind':'add','hash':candidate['hash'],'stage':'prepared','path':candidate['path'],'sha256':candidate['sha256']}
            deferred=False
            while True:
                try:
                    # Keep disk checks serial; continue the same journaled add
                    # once its check ends, within this command's deadline.
                    if any(checking(t) for t in self.api.torrents()):
                        raise Fault('CHECK_PENDING','Проверка диска ещё выполняется.','partial')
                    self.dispatch(o); break
                except Fault as e:
                    if e.code=='CHECK_PENDING' and not self.nonblocking and self.budget.left()>4:
                        time.sleep(min(0.2,self.budget.left())); continue
                    self.issues.append(e.issue())
                    if e.code in ('CHECK_PENDING','ADD_PENDING','ADD_ADMISSION_WAIT','BUDGET_EXHAUSTED','API_UNAVAILABLE','STATE_WRITE_FAILED','SERVICE_DEFERRED','SERVICE_WAITING'):
                        deferred=True
                    break
            if deferred: break
            if self.service_mode:
                # Start the replacement before consuming the remaining budget
                # adding other stopped records. Safety checks stay fresh.
                rows=self.api.torrents(); t=next((x for x in rows if x['hash']==o['hash']),None)
                active=sum(not stopped(x) and not complete(x) for x in rows)
                limit=min(self.c['policy']['download_slots'],self.store.setting('effective_slots',self.c['policy']['download_slots']))
                if t and stopped(t) and not complete(t) and active<limit:
                    self.wait_no_checks()
                    self.dispatch({'id':uuid.uuid4().hex,'kind':'control','hash':t['hash'],'stage':'prepared','tasks':[{'hash':t['hash'],'action':'resume'}]})
        self.enforce_slots()
        count=len(self.api.torrents())
        if count<self.c['policy']['target_client_count']:
            self.issues.append({'code':'QUEUE_BELOW_TARGET','message':'Очередь ниже цели; продолжите проход или проверьте кандидатов.','actual':count,'target':self.c['policy']['target_client_count']})
    def audit(self,deep_hash=None):
        rows=self.api.torrents(); entries=self.index(); registry=self.reconcile_index(entries,rows); findings=list(registry['conflicts'])
        findings.extend(self.path_findings(rows))
        archive={e['id'] for e in entries if e['role']=='archive'}
        pending={o['hash'] for o in self.store.pending()}
        for t in rows:
            if not self.valid_path(t): findings.append({'code':'CLIENT_PATH_POLICY',**summary(t)})
            if t['hash'] in archive and t['hash'] not in pending: findings.append({'code':'ARCHIVED_IN_CLIENT','hash':t['hash'],'message':'Требуется отдельная сверка; автоматически не удаляется.'})
        result={'indexed':len(entries),'findings':findings,'registry':registry,'completed_data_audited':False,'completed':[summary(t) for t in rows if complete(t)]}
        if deep_hash:
            t=self.select(rows,deep_hash)
            matches=[e for e in entries if e['id']==t['hash'] and e['role']=='incoming']
            if len(matches)!=1: raise Fault('SOURCE_MISSING','Не найден один исходный torrent для сверки.')
            result['manifest']=self.manifest(t,matches[0],entries,rows)
        return result

    @timed('duplicate_cleanup')
    def clean_duplicates(self,apply=False):
        entries=self.index(); archives={}; operations=[]; skipped=[]
        for e in entries:
            if e['role']=='archive':
                for a in aliases(e): archives.setdefault(a,e)
        pending=self.store.pending()
        for e in entries:
            self.budget.check()
            if e['role']!='incoming': continue
            keeper=next((archives[a] for a in sorted(aliases(e)) if a in archives),None)
            if not keeper: continue
            if any(aliases(e)&(set(o.get('aliases',[]))|{o['hash']}) for o in pending):
                skipped.append({'hash':e['id'],'path':e['path'],'reason':'pending_operation'}); continue
            operations.append({'id':uuid.uuid4().hex,'kind':'duplicate_cleanup','hash':e['id'],'aliases':sorted(aliases(e)),
                'stage':'prepared','source':e['path'],'source_sha256':e['sha256'],'source_identity':fingerprint(e['path']),
                'archive':keeper['path'],'archive_sha256':keeper['sha256'],'archive_identity':fingerprint(keeper['path']),
                'matching_aliases':sorted(aliases(e)&aliases(keeper))})
        result={'operations':operations,'skipped':skipped,'candidate_count':len(operations),'completed_data_audited':False}
        if apply:
            for o in operations:
                self.budget.check(); self.dispatch(o)
            if skipped: self.issues.append({'code':'DUPLICATE_CLEANUP_DEFERRED','message':'Копии с незавершённой операцией сохранены; сначала recover.','count':len(skipped)})
        return result

    def recover_duplicate_cleanup(self,o):
        self.budget.check()
        src=guarded(self.c['paths']['incoming'],Path(o['source']).name)
        archive=guarded(self.c['paths']['archive'],Path(o['archive']).name)
        if norm(src)!=norm(o['source']) or norm(archive)!=norm(o['archive']) or src.suffix.lower()!='.torrent' or archive.suffix.lower()!='.torrent':
            raise Fault('PATH_OUTSIDE_ROOT','Удаление разрешено только для корневого torrent с архивной копией.')
        def verify_archive():
            if not archive.is_file() or not samefile_identity(archive,o['archive_identity']) or sha_file(archive)!=o['archive_sha256']:
                raise Fault('PLAN_STALE','Архивная копия исчезла или изменилась; удаление заблокировано.')
            # Fresh metadata parsing binds the proof to the bytes just hashed.
            data=archive.read_bytes()
            if __import__('hashlib').sha256(data).hexdigest()!=o['archive_sha256'] or not set(o['matching_aliases'])&aliases(self.api.metadata(data)):
                raise Fault('PLAN_STALE','Info-hash архивной копии не подтверждён.')
        verify_archive()
        if src.exists():
            if not src.is_file() or not samefile_identity(src,o['source_identity']) or sha_file(src)!=o['source_sha256']:
                raise Fault('PLAN_STALE','Корневая копия изменилась; удаление заблокировано.')
            data=src.read_bytes()
            if __import__('hashlib').sha256(data).hexdigest()!=o['source_sha256'] or not set(o['matching_aliases'])&aliases(self.api.metadata(data)):
                raise Fault('PLAN_STALE','Info-hash корневой копии не подтверждён.')
            self.stage(o,'delete_requested'); self.budget.check()
            guarded(self.c['paths']['incoming'],src.name)
            verify_archive()
            if not samefile_identity(src,o['source_identity']) or sha_file(src)!=o['source_sha256']:
                raise Fault('PLAN_STALE','Корневая копия изменена перед удалением.')
            src.unlink()
        elif o['stage']!='delete_requested':
            raise Fault('RECOVERY_AMBIGUOUS','Копия исчезла до записанного намерения удаления.')
        verify_archive()
        if src.exists(): raise Fault('RECOVERY_AMBIGUOUS','Корневое имя снова занято; повторное удаление запрещено.')
        self.stage(o,'finished')
        self.actions.append({'action':'duplicate_cleanup','hash':o['hash'],'result':'verified','source':str(src),'archive':str(archive),
            'postconditions':{'source_absent':True,'archive_unchanged':True,'completed_data_audited':False}})

    def dedupe(self,h,keep,apply=False,purge_quarantine=None):
        entries=self.index(); reg=reconcile(self,entries,self.api.torrents())
        selected=[e for e in entries if any(a.startswith(h.lower()) for a in aliases(e))]
        if not selected: raise Fault('SOURCE_MISSING','Хеш не найден в t/d.')
        aset=set().union(*(aliases(e) for e in selected))
        selected=[e for e in entries if aliases(e)&aset]
        if len({e['id'] for e in selected})>1 and not all(aliases(e)&aliases(selected[0]) for e in selected): raise Fault('HASH_AMBIGUOUS','Неоднозначный префикс.')
        keeper=next((e for e in selected if norm(e['path'])==norm(keep)),None)
        if not keeper: raise Fault('SOURCE_MISSING','--keep должен указывать на одну из копий.')
        if any(o['hash'] in aset for o in self.store.pending()): raise Fault('RECOVERY_REQUIRED','Сначала закончите операцию этой раздачи.')
        if keeper['role']=='incoming' and (any(e['role']=='archive' for e in selected) or aset&finished_aliases(self.store)):
            raise Fault('ARCHIVE_KEEP_REQUIRED','Для обработанной раздачи нужно сохранить архивную копию.')
        oid=uuid.uuid4().hex
        op={'id':oid,'kind':'dedupe','hash':keeper['id'],'stage':'prepared','keep':keeper['path'],'keep_sha256':keeper['sha256'],'files':[{'source':e['path'],'role':e['role'],'sha256':e['sha256'],'identity':fingerprint(e['path']),'handoff':'prepared'} for e in selected if e is not keeper]}
        op['strategy']='delete_identical'; op['keep_identity']=fingerprint(keeper['path'])
        if purge_quarantine:
            if len(purge_quarantine)!=32 or any(c not in '0123456789abcdef' for c in purge_quarantine):
                raise Fault('ARGUMENT_INVALID','Неверный ID прежней dedupe-операции.','error')
            row=self.store.db.execute('SELECT body FROM operations WHERE id=?',(purge_quarantine,)).fetchone()
            old=self.store.decode_op(row[0]) if row else None
            if not old or old['kind']!='dedupe' or old['stage']!='finished' or old['hash'] not in aset or old.get('strategy')=='delete_identical':
                raise Fault('PURGE_PRECONDITION','Нет законченной операции карантина для этого хеша.')
            for previous in old['files']:
                src=guarded(ROOT,'quarantine/'+old['id']+'/'+previous['role']+'/'+Path(previous['source']).name)
                if norm(src)!=norm(previous['destination']) or not src.is_file() or not samefile_identity(src,previous['identity']):
                    raise Fault('PLAN_STALE','Карантинная копия отсутствует или изменилась.')
                op['files'].append({'source':str(src),'role':'quarantine','quarantine_id':old['id'],'original_role':previous['role'],
                                    'sha256':sha_file(src),'identity':fingerprint(src),'handoff':'prepared'})
        if any(f['sha256']!=op['keep_sha256'] for f in op['files']):
            raise Fault('DUPLICATE_CONTENT_DIFFERS','Info-hash совпадает, но содержимое torrent различается; удаление не выполнено.')
        if apply: self.dispatch(op)
        return op

    def recover_dedupe(self,o):
        self.api.compatible()
        if o.get('strategy')=='delete_identical': return self.recover_delete_identical(o)
        if not Path(o['keep']).is_file() or sha_file(o['keep'])!=o['keep_sha256']: raise Fault('PLAN_STALE','Сохраняемая копия изменена.')
        for f in o['files']:
            self.budget.check()
            src=guarded(self.c['paths'][f['role']],Path(f['source']).name)
            dst=guarded(ROOT,'quarantine/'+o['id']+'/'+f['role']+'/'+Path(f['source']).name)
            if norm(src)!=norm(f['source']) or norm(dst)!=norm(f['destination']): raise Fault('PATH_OUTSIDE_ROOT','Неверные пути карантина.')
            if f['handoff']=='handed_off': continue
            if src.exists():
                if not samefile_identity(src,f['identity']) or sha_file(src)!=f['sha256']: raise Fault('PLAN_STALE','Копия изменилась.')
                dst.parent.mkdir(parents=True,exist_ok=True)
                f['handoff']='move_requested'; self.store.put_op(o); move_no_replace(src,dst)
            elif f['handoff']!='move_requested' or not dst.is_file() or not samefile_identity(dst,f['identity']): raise Fault('RECOVERY_AMBIGUOUS','Не подтверждён перенос копии.')
            f['handoff']='handed_off'; self.store.put_op(o)
        self.stage(o,'finished'); self.actions.append({'action':'dedupe','hash':o['hash'],'result':'verified','quarantined':len(o['files'])})

    def recover_delete_identical(self,o):
        keep=Path(o['keep'])
        role=next((r for r in ('incoming','archive') if norm(keep.parent)==norm(self.c['paths'][r])),None)
        if not role or keep.suffix.lower()!='.torrent': raise Fault('PATH_OUTSIDE_ROOT','Сохраняемый torrent должен быть в корне t или d.')
        keep=guarded(self.c['paths'][role],keep.name)
        def verify_keep():
            if not keep.is_file() or not samefile_identity(keep,o['keep_identity']) or sha_file(keep)!=o['keep_sha256']:
                raise Fault('PLAN_STALE','Сохраняемый оригинал отсутствует или изменился.')
        for f in o['files']:
            self.budget.check(); verify_keep()
            if f['role']=='quarantine':
                if f['original_role'] not in ('incoming','archive'): raise Fault('PATH_OUTSIDE_ROOT','Неверная роль карантина.')
                src=guarded(ROOT,'quarantine/'+f['quarantine_id']+'/'+f['original_role']+'/'+Path(f['source']).name)
            elif f['role'] in ('incoming','archive'):
                src=guarded(self.c['paths'][f['role']],Path(f['source']).name)
            else: raise Fault('PATH_OUTSIDE_ROOT','Неверный источник удаления.')
            if norm(src)!=norm(f['source']) or norm(src)==norm(keep) or src.suffix.lower()!='.torrent':
                raise Fault('PATH_OUTSIDE_ROOT','Удаление допустимо только для отдельной torrent-копии.')
            if f['handoff']=='deleted':
                if src.exists(): raise Fault('RECOVERY_AMBIGUOUS','Удалённое имя вновь занято; новый файл не удалён.')
                continue
            if src.exists():
                if not samefile_identity(src,f['identity']) or sha_file(src)!=o['keep_sha256'] or f['sha256']!=o['keep_sha256']:
                    raise Fault('PLAN_STALE','Копия не совпадает с оригиналом или изменилась.')
                f['handoff']='delete_requested'; self.store.put_op(o)
                verify_keep(); src.unlink()
            elif f['handoff']!='delete_requested': raise Fault('RECOVERY_AMBIGUOUS','Копия исчезла до намерения удаления.')
            verify_keep()
            if src.exists(): raise Fault('RECOVERY_AMBIGUOUS','Имя вновь занято; повторное удаление запрещено.')
            f['handoff']='deleted'; self.store.put_op(o)
        self.stage(o,'finished')
        self.actions.append({'action':'dedupe','hash':o['hash'],'result':'verified','deleted':len(o['files']),
                             'postconditions':{'keeper_unchanged':True,'copies_absent':True,'payload_untouched':True}})

    def isolate_shared(self,h,other,apply=False):
        self.api.compatible(); self.budget.check(); rows=self.api.torrents()
        a=self.select(rows,h); b=self.select(rows,other)
        if a['hash']==b['hash']: raise Fault('ARGUMENT_INVALID','Нужны две разные раздачи.')
        if self.store.pending(): raise Fault('RECOVERY_REQUIRED','Сначала восстановите текущую операцию.')
        for t in (a,b):
            if not stopped(t) or not self.valid_path(t) or norm(t['save_path'])!=norm(self.c['paths']['working']):
                raise Fault('ISOLATION_PRECONDITION','Обе задачи должны быть остановлены с данными в m.')
        fa,fb=self.api.files(a['hash']),self.api.files(b['hash'])
        if len(fa)!=1 or len(fb)!=1 or fa[0]['name']!=fb[0]['name']:
            raise Fault('FORMAT_UNSUPPORTED','Изоляция поддерживает две однофайловые раздачи с общим путём.')
        root=self.c['paths']['working']; old=fa[0]['name']; new=f'_isolated_{a["hash"]}/{Path(old).name}'
        suffix=''
        if self.api.prefs().get('incomplete_files_ext') and not guarded(root,old).exists() and guarded(root,old+'.!qB').is_file(): suffix='.!qB'
        src,dst=guarded(root,old+suffix),guarded(root,new+suffix)
        if suffix and guarded(root,new).exists(): raise Fault('DESTINATION_EXISTS','Имя назначения без расширения уже занято.')
        if not src.is_file() or src.stat().st_size!=fa[0]['size'] or src.stat().st_nlink!=1 or dst.exists():
            raise Fault('ISOLATION_PRECONDITION','Выберите раздачу, чей размер равен исходному файлу; назначение должно быть свободно.')
        for t in rows:
            if t['hash'] in (a['hash'],b['hash']): continue
            if any(norm(lexical(t['save_path'],f['name'])) in (norm(lexical(root,old)),norm(lexical(root,new))) for f in self.api.files(t['hash'])):
                raise Fault('SHARED_FILES','Файл используется третьей задачей.')
        import shutil
        if shutil.disk_usage(src.parent).free<src.stat().st_size+1024**3: raise Fault('DISK_RESERVATION','Недостаточно места для отдельной копии и резерва.')
        identity=fingerprint(src); checksum=stream_sha_file(src,self.budget)
        if fingerprint(src)!=identity: raise Fault('PLAN_STALE','Исходный файл изменён при чтении.')
        copy_bytes=min(identity['size'],fb[0]['size'])
        copy_checksum=prefix_sha_file(src,copy_bytes,self.budget)
        if fingerprint(src)!=identity: raise Fault('PLAN_STALE','Источник изменился перед изоляцией.')
        oid=uuid.uuid4().hex
        o={'id':oid,'kind':'isolate_shared','hash':a['hash'],'other_hash':b['hash'],'stage':'prepared','old_relative':old,'new_relative':new,
            'temp_relative':f'_isolated_{a["hash"]}/.clone-{oid}.part','actual_suffix':suffix,'identity':identity,'sha256':checksum,'sizes':[fa[0]['size'],fb[0]['size']], 'copy_bytes':copy_bytes,'copy_sha256':copy_checksum}
        if apply: self.dispatch(o)
        return o

    @timed('isolation')
    def recover_isolate_shared(self,o):
        self.api.compatible(); self.budget.check()
        root=self.c['paths']['working']; suffix=o.get('actual_suffix','')
        if suffix not in ('','.!qB') or (suffix and not self.api.prefs().get('incomplete_files_ext')): raise Fault('PLAN_STALE','Политика временного расширения изменилась.')
        src=guarded(root,o['old_relative']+suffix); dst=guarded(root,o['new_relative']+suffix); tmp=guarded(root,o['temp_relative'])
        expected={f'_isolated/{o["hash"]}/{Path(o["old_relative"]).name}',f'_isolated_{o["hash"]}/{Path(o["old_relative"]).name}'}
        expected_temp=(Path(o['new_relative']).parent/f'.clone-{o["id"]}.part').as_posix()
        if o['new_relative'] not in expected or o['temp_relative']!=expected_temp:
            raise Fault('PATH_OUTSIDE_ROOT','Неверные пути изоляции в журнале.')
        def stopped_pair():
            for h in (o['hash'],o['other_hash']):
                t=self.api.get(h)
                if not t or not stopped(t) or not self.valid_path(t) or norm(t['save_path'])!=norm(root):
                    raise Fault('ISOLATION_PRECONDITION','Задачи изменены или запущены; изоляция остановлена.')
        stopped_pair()
        if o['stage']=='copy_retention_requested':
            self.retain_isolation_copy(o)
        if o['stage']=='prepared':
            if dst.exists() or tmp.exists() or not src.is_file() or not samefile_identity(src,o['identity']) or stream_sha_file(src,self.budget)!=o['sha256']:
                raise Fault('PLAN_STALE','Источник или назначение изменены.')
            dst.parent.mkdir(parents=True,exist_ok=True); guarded(root,o['new_relative'])
            self.stage(o,'rename_requested')
            self.api.call('torrents/renameFile',{'hash':o['hash'],'oldPath':o['old_relative'],'newPath':o['new_relative']},text=True)
        if o['stage']=='rename_requested':
            end=min(self.budget.end,time.monotonic()+4)
            while True:
                files=self.api.files(o['hash'])
                if len(files)==1 and files[0]['name'].replace('\\','/')==o['new_relative'] and dst.is_file() and not src.exists(): break
                if time.monotonic()>=end: raise Fault('RENAME_PENDING','Изменение пути пока не подтверждено; запрос повторно не отправлен.','partial')
                time.sleep(0.1)
            if not samefile_identity(dst,o['identity']): raise Fault('FILE_MISMATCH','Переименованный источник не подтверждён.')
            self.stage(o,'renamed')
        stopped_pair()
        if not dst.is_file() or not samefile_identity(dst,o['identity']): raise Fault('FILE_MISMATCH','Основной файл изменён.')
        if o['stage'] in ('renamed','copy_requested'):
            if src.exists(): raise Fault('DESTINATION_EXISTS','Старый путь занят до установки отдельной копии.')
            if o['stage']=='renamed':
                if tmp.exists(): raise Fault('DESTINATION_EXISTS','Временное имя занято.')
                self.stage(o,'copy_requested')
            if not tmp.exists():
                copy_prefix(dst,tmp,o.get('copy_bytes',o['identity']['size']),self.budget)
            if not tmp.is_file() or tmp.stat().st_size!=o.get('copy_bytes',o['identity']['size']) or stream_sha_file(tmp,self.budget)!=o.get('copy_sha256',o['sha256']):
                raise Fault('COPY_INCOMPLETE','Копия не подтверждена; источник сохранён, перезапись запрещена.')
            if stream_sha_file(dst,self.budget)!=o['sha256'] or not samefile_identity(dst,o['identity']): raise Fault('FILE_MISMATCH','Источник изменён при копировании.')
            o['copy_identity']=fingerprint(tmp); self.stage(o,'install_requested')
        if o['stage']=='install_requested':
            stopped_pair()
            if tmp.exists():
                if not samefile_identity(tmp,o['copy_identity']): raise Fault('FILE_MISMATCH','Временная копия изменена.')
                move_no_replace(tmp,src)
            if not src.is_file() or not samefile_identity(src,o['copy_identity']) or tmp.exists(): raise Fault('RECOVERY_AMBIGUOUS','Установка отдельной копии не подтверждена.')
            self.stage(o,'installed')
        stopped_pair()
        fa,fb=self.api.files(o['hash']),self.api.files(o['other_hash'])
        if len(fa)!=1 or len(fb)!=1 or fa[0]['name'].replace('\\','/')!=o['new_relative'] or fb[0]['name']!=o['old_relative']:
            raise Fault('PLAN_STALE','Пути клиента не соответствуют двум независимым файлам.')
        if not src.is_file() or not samefile_identity(src,o['copy_identity']) or not samefile_identity(dst,o['identity']):
            raise Fault('FILE_MISMATCH','Независимость файлов не подтверждена.')
        if (o['copy_identity']['dev'],o['copy_identity']['ino'])==(o['identity']['dev'],o['identity']['ino']): raise Fault('SHARED_FILES','Копия не должна быть жёсткой ссылкой.')
        self.stage(o,'finished')
        self.actions.append({'action':'isolate_shared','hash':o['hash'],'other_hash':o['other_hash'],'result':'verified','postconditions':{'paths':[str(dst),str(src)],'independent_files':True,'copy_sha256_verified':True,'both_stopped':True,'needs_piece_recheck':True}})

    def retain_isolation_copy(self,o):
        r=o['retention']; root=self.c['paths']['working']
        src=guarded(root,o['temp_relative']); dst=guarded(root,r['relative'])
        expected=Path('_recovery')/o['id']
        if Path(r['relative']).parent!=expected or Path(r['relative']).suffix!='.part': raise Fault('PATH_OUTSIDE_ROOT','Неверный путь сохранения неполной копии.')
        if src.exists():
            if not samefile_identity(src,r['identity']): raise Fault('FILE_MISMATCH','Неполная копия изменена.')
            dst.parent.mkdir(parents=True,exist_ok=True); guarded(root,r['relative']); move_no_replace(src,dst)
        if not dst.is_file() or not samefile_identity(dst,r['identity']) or src.exists(): raise Fault('RECOVERY_AMBIGUOUS','Сохранение неполной копии не подтверждено.')
        o.setdefault('retained_copies',[]).append(r); o.pop('retention'); self.stage(o,'renamed')

    def resolve_isolation(self,oid,decision,apply=False):
        self.budget.check(); self.api.compatible()
        o=next((x for x in self.store.pending() if x['id']==oid and x['kind']=='isolate_shared'),None)
        if not o: raise Fault('OPERATION_MISSING','Нет ожидающей операции изоляции.')
        result={'operation_id':oid,'decision':decision,'stage':o['stage'],'payload_deleted':False}
        if decision=='cancel-untouched':
            if o['stage']!='prepared': raise Fault('CANCEL_UNSAFE','Отмена допустима только до первого намерения rename.')
            if apply: o['resolution']='cancelled_before_mutation'; self.stage(o,'finished')
            return result
        if decision=='retry-rename':
            if o['stage']!='rename_requested': raise Fault('RESOLUTION_INVALID','Повтор rename относится только к ожидающему rename.')
            root=self.c['paths']['working']; src=guarded(root,o['old_relative']+o.get('actual_suffix','')); dst=guarded(root,o['new_relative']+o.get('actual_suffix',''))
            for h in (o['hash'],o['other_hash']):
                t=self.api.get(h)
                if not t or not stopped(t) or not self.valid_path(t): raise Fault('ISOLATION_PRECONDITION','Обе задачи должны оставаться остановленными.')
            if self.api.files(o['hash'])[0]['name']!=o['old_relative'] or dst.exists() or not src.is_file() or not samefile_identity(src,o['identity']) or stream_sha_file(src,self.budget)!=o['sha256']:
                raise Fault('PLAN_STALE','Предпосылки явного повтора не подтверждены.')
            result['warning']='Это явное решение повторить неизвестный запрос; автоматического повтора нет.'
            if apply:
                o['explicit_retry_at']=now(); self.store.put_op(o)
                self.api.call('torrents/renameFile',{'hash':o['hash'],'oldPath':o['old_relative'],'newPath':o['new_relative']},text=True)
                self.recover_isolate_shared(o)
            return result
        if decision!='retry-copy' or o['stage'] not in ('copy_requested','copy_retention_requested'): raise Fault('RESOLUTION_INVALID','Повтор copy допустим для незавершённой копии.')
        if apply:
            tmp=guarded(self.c['paths']['working'],o['temp_relative'])
            if o['stage']=='copy_requested' and tmp.exists():
                o['retention']={'relative':f'_recovery/{o["id"]}/{uuid.uuid4().hex}.part','identity':fingerprint(tmp)}
                self.stage(o,'copy_retention_requested')
            self.recover_isolate_shared(o)
        return result

    def confirm_recheck(self,oid,report_path,apply=False):
        self.budget.check(); self.api.compatible()
        o=next((x for x in self.store.pending() if x['id']==oid),None)
        if not o or o['kind']!='control' or len(o.get('tasks',[]))!=1:
            raise Fault('OPERATION_MISSING','Нужна незавершённая проверка одной задачи.')
        task=o['tasks'][0]
        if task['action']!='recheck' or not task.get('sent') or task.get('verified'):
            raise Fault('OBSERVATION_INVALID','Операция не ожидает подтверждения отправленного recheck.')
        p=Path(report_path).absolute()
        try: relative=p.relative_to(ROOT)
        except ValueError: raise Fault('PATH_OUTSIDE_ROOT','Отчёт должен находиться в каталоге CLI.')
        p=guarded(ROOT,str(relative))
        if p.suffix.lower()!='.json' or not p.is_file() or p.stat().st_size>32*1024*1024:
            raise Fault('OBSERVATION_INVALID','Нужен обычный JSON-отчёт CLI.')
        data=p.read_bytes(); report=json.loads(data.decode('utf-8-sig')); t=report.get('torrent',{})
        if report.get('schema_version')!=1 or report.get('command')!='explain' or report.get('result')!='ok' or t.get('hash')!=task['hash'] or not checking(t):
            raise Fault('OBSERVATION_INVALID','Отчёт не подтверждает состояние checking нужной задачи.')
        if not any(x.get('id')==oid and x.get('kind')=='control' and x.get('hash')==task['hash'] for x in report.get('pending_operations',[])):
            raise Fault('OBSERVATION_INVALID','Отчёт не связан с ожидающей операцией.')
        from datetime import datetime
        first=self.store.db.execute('SELECT MIN(time) FROM events WHERE operation_id=?',(oid,)).fetchone()[0]
        if not first or datetime.fromisoformat(report['started_at'])<datetime.fromisoformat(first):
            raise Fault('OBSERVATION_INVALID','Наблюдение предшествует операции.')
        current=self.api.get(task['hash'])
        if not current or not self.valid_path(current) or not self.valid_path(t) or norm(current['save_path'])!=norm(t['save_path']):
            raise Fault('PLAN_STALE','Запись или путь задачи изменились.')
        evidence={'report':str(p),'sha256':__import__('hashlib').sha256(data).hexdigest(),'observed_state':t['state'],'observed_at':report['snapshot_at']}
        result={'operation_id':oid,'hash':task['hash'],'evidence':evidence,'current_state':current['state'],'confirms':'recheck_started_only; not successful completion'}
        if apply:
            task['verified']=True; task['observation']=evidence; self.stage(o,'finished')
            item={'operation_id':oid,'phase':'checking' if checking(current) else 'finished','request_accepted':bool(task.get('accepted')),'start_observed':True,'completion_observed':not checking(current),'requested_at':first,'started_at':report['snapshot_at']}
            if not checking(current): item.update(finished_at=now(),final_state=current['state'],fully_downloaded=complete(current))
            def update(ledger): ledger[task['hash']]=item; return ledger
            self.store.update_setting('rechecks',update,{})
            self.actions.append({'action':'recheck','hash':task['hash'],'result':'verified','evidence':evidence,'postconditions':{'start_observed':True,'current_state':current['state'],'data_completion_confirmed':False}})
        return result

    def resolve_add(self,oid,decision,apply=False):
        self.api.compatible()
        row=self.store.db.execute("SELECT body FROM operations WHERE id=? AND kind='add' AND stage!='finished'",(oid,)).fetchone()
        if not row: raise Fault('OPERATION_MISSING','Нет незавершённого добавления.')
        o=json.loads(row[0])
        if self.api.get(o['hash']): raise Fault('ADD_PRESENT','Раздача обнаружена; используйте recover.')
        result={'operation_id':oid,'decision':decision,'client_absent':True,'warning':'Отсутствие сейчас не доказывает, что прошлый запрос не выполнялся.'}
        if apply:
            if decision=='retry':
                o['stage']='prepared'; o['explicit_retry']=now(); self.store.put_op(o); self.execute_add(o)
            else:
                o['resolution']='cancelled_absent'; self.stage(o,'finished')
                self.actions.append({'action':'resolve-add','hash':o['hash'],'result':'verified','resolution':'cancelled_absent'})
        return result
    @staticmethod
    def select(rows,h):
        matched=[t for t in rows if t['hash'].lower().startswith(h.lower())]
        if len(matched)!=1: raise Fault('HASH_AMBIGUOUS','Нужен один однозначный хеш.',matches=len(matched))
        return matched[0]

    def adopt(self,h):
        rows=self.api.torrents(); t=self.select(rows,h)
        if any(o['hash']==t['hash'] for o in self.store.pending()): raise Fault('RECOVERY_REQUIRED','Для задачи уже существует журнал; используйте обычный recover.')
        entries=self.index()
        sources=[e for e in entries if e['id']==t['hash'] and e['role']=='incoming']
        archives=[e for e in entries if e['id']==t['hash'] and e['role']=='archive']
        if sources or len(archives)!=1: raise Fault('SOURCE_AMBIGUOUS','Принятие требует одного архивного torrent и отсутствия исходного в корне.')
        entry=archives[0]; op=self.manifest(t,entry,entries,rows,allow_archive=True)
        op['source_torrent']=str(guarded(self.c['paths']['incoming'],Path(entry['path']).name))
        if Path(op['source_torrent']).exists(): raise Fault('DESTINATION_EXISTS','Корневое имя занято другим файлом.')
        op['adopted']=True; op['stage']='archived'
        return op
