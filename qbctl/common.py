from __future__ import annotations
import contextlib, ctypes, hashlib, json, os, sqlite3, stat, threading, time, tomllib, uuid
from datetime import datetime, timezone
from pathlib import Path, PureWindowsPath

ROOT = Path(__file__).resolve().parent.parent

def now():
    return datetime.now(timezone.utc).isoformat()

def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True).encode()).hexdigest()

class Fault(Exception):
    def __init__(self, code, message, result='blocked', **details):
        super().__init__(message)
        self.code, self.message, self.result, self.details = code, message, result, details
    def issue(self):
        return {'code':self.code,'message':self.message,'retryable':self.result in ('unknown','partial'),**self.details}

class Budget:
    def __init__(self, seconds): self.end = time.monotonic() + seconds
    def check(self):
        if time.monotonic() >= self.end:
            raise Fault('BUDGET_EXHAUSTED','Бюджет прохода исчерпан; продолжите следующим запуском.','partial')
    def left(self): return max(0, self.end-time.monotonic())

def norm(p): return os.path.normcase(os.path.abspath(p)).rstrip('\\/')

def lexical(root, relative):
    """Validate journal path syntax without touching already handed-off files."""
    rel=PureWindowsPath(relative)
    if rel.is_absolute() or rel.drive or any(x in ('..','.') for x in rel.parts):
        raise Fault('PATH_OUTSIDE_ROOT','Недопустимый относительный путь.')
    for x in rel.parts:
        if ':' in x or x.endswith((' ','.')) or x.split('.')[0].upper() in {'CON','PRN','AUX','NUL',*(f'COM{i}' for i in range(1,10)),*(f'LPT{i}' for i in range(1,10))}:
            raise Fault('PATH_OUTSIDE_ROOT','Недопустимое имя Windows.')
    return Path(root).absolute().joinpath(*rel.parts)

def validate_roots(paths, application=ROOT):
    roots={k:lexical(v,'') for k,v in paths.items()}
    for a,p in roots.items():
        for b,q in roots.items():
            if a>=b: continue
            if norm(p)==norm(q) or p.is_relative_to(q) or q.is_relative_to(p):
                if {a,b}=={'archive','incoming'} and roots['archive'].parent==roots['incoming']: continue
                raise Fault('CONFIG_INVALID','Корни пересекаются.',roles=[a,b])
        app=Path(application).absolute()
        if norm(app)==norm(p) or app.is_relative_to(p) or p.is_relative_to(app):
            raise Fault('CONFIG_INVALID','Приложение пересекается с данными.',role=a)
    return roots

def guarded(root, relative=''):
    root=Path(root).absolute()
    rel=PureWindowsPath(relative)
    if rel.is_absolute() or rel.drive or any(x in ('..','.') for x in rel.parts):
        raise Fault('PATH_OUTSIDE_ROOT','Недопустимый относительный путь.',path=str(relative))
    for x in rel.parts:
        if ':' in x or x.endswith((' ','.')) or x.split('.')[0].upper() in {'CON','PRN','AUX','NUL',*(f'COM{i}' for i in range(1,10)),*(f'LPT{i}' for i in range(1,10))}:
            raise Fault('PATH_OUTSIDE_ROOT','Недопустимое имя Windows.',path=str(relative))
    target=root.joinpath(*rel.parts)
    try: target.relative_to(root)
    except ValueError: raise Fault('PATH_OUTSIDE_ROOT','Выход за разрешённый корень.')
    for p in (target,*target.parents):
        if p.exists() or p.is_symlink():
            s=p.lstat()
            if p.is_symlink() or getattr(s,'st_file_attributes',0) & 0x400:
                raise Fault('PATH_OUTSIDE_ROOT','Ссылки и reparse points запрещены.',path=str(p))
    return target

def fingerprint(p):
    s=Path(p).stat()
    return {'size':s.st_size,'mtime_ns':s.st_mtime_ns,'dev':s.st_dev,'ino':s.st_ino}

def samefile_identity(p, expected):
    s=fingerprint(p)
    return all(s[k]==expected[k] for k in ('size','dev','ino','mtime_ns'))

def sha_file(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def stream_sha_file(p,budget=None):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        while data:=f.read(4*1024*1024):
            if budget: budget.check()
            h.update(data)
    return h.hexdigest()

def prefix_sha_file(p,size,budget=None):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        remaining=size
        while remaining:
            if budget: budget.check()
            data=f.read(min(4*1024*1024,remaining))
            if not data: raise Fault('FILE_MISMATCH','Недостаточно данных для копирования префикса.')
            h.update(data); remaining-=len(data)
    return h.hexdigest()

def copy_prefix(source,destination,size,budget=None):
    # Exclusive creation; keep a partial file on interruption, never overwrite.
    with Path(source).open('rb') as src, Path(destination).open('xb') as dst:
        remaining=size
        while remaining:
            if budget: budget.check()
            data=src.read(min(4*1024*1024,remaining))
            if not data: raise Fault('FILE_MISMATCH','Источник короче ожидаемого префикса.')
            dst.write(data); remaining-=len(data)
        dst.flush(); os.fsync(dst.fileno())

def move_no_replace(source, destination):
    source,destination=Path(source),Path(destination)
    if source.stat().st_dev != destination.parent.stat().st_dev:
        raise Fault('CROSS_VOLUME','Межтомный перенос не поддерживается.',source=str(source),destination=str(destination))
    if destination.exists(): raise Fault('DESTINATION_EXISTS','Назначение занято.',path=str(destination))
    if os.name != 'nt': raise Fault('PLATFORM_UNSUPPORTED','Перенос пользовательских данных поддержан только на Windows.')
    fn=ctypes.WinDLL('kernel32',use_last_error=True).MoveFileExW
    fn.argtypes=[ctypes.c_wchar_p,ctypes.c_wchar_p,ctypes.c_uint32]
    fn.restype=ctypes.c_int
    # WRITE_THROUGH, deliberately no REPLACE_EXISTING or COPY_ALLOWED.
    if not fn(str(source),str(destination),0x8):
        code=ctypes.get_last_error()
        raise Fault('MOVE_FAILED','Windows не выполнила перенос без замены.',winerror=code,source=str(source),destination=str(destination))

def load_config():
    try: c=tomllib.loads((ROOT/'config.toml').read_text(encoding='utf-8-sig'))
    except (OSError,ValueError): raise Fault('CONFIG_INVALID','Не удалось прочитать config.toml.','error')
    for k in ('target_client_count','download_slots','upload_slots','down_bps','up_bps'):
        v=c['policy'][k]
        if not isinstance(v,int) or isinstance(v,bool) or v<0: raise Fault('CONFIG_INVALID','Политика содержит неверное число.','error',field=k)
    roots=validate_roots(c['paths'],ROOT)
    if len(set(norm(v) for v in roots.values()))!=4: raise Fault('CONFIG_INVALID','Корни не должны совпадать.','error')
    if norm(roots['working']).startswith(norm(roots['completed'])+os.sep) or norm(roots['completed']).startswith(norm(roots['working'])+os.sep):
        raise Fault('CONFIG_INVALID','Рабочий и готовый корни не могут быть вложены.','error')
    r=c['resources']
    if not (0 <= r['resume_below'] < r['threshold'] <=100) or r['step']<1 or r['sample_seconds']<=0:
        raise Fault('CONFIG_INVALID','Неверная ресурсная политика.','error')
    if r['unknown_policy'] not in ('hold','pause'): raise Fault('CONFIG_INVALID','unknown_policy: hold или pause.','error')
    return c

def save_config(c):
    c['revision']+=1
    lines=[f"revision = {c['revision']}"]
    for section in ('paths','api','policy','resources'):
        lines.append(f'[{section}]')
        for k,v in c[section].items():
            val=json.dumps(v,ensure_ascii=False)
            # JSON scalar/array representation is also valid TOML for these fields.
            lines.append(f'{k} = {val}')
    tmp=ROOT/'config.toml.new'
    with tmp.open('w',encoding='utf-8',newline='\n') as f:
        f.write('\n'.join(lines)+'\n'); f.flush(); os.fsync(f.fileno())
    os.replace(tmp,ROOT/'config.toml')

_lock_owners = threading.local()

@contextlib.contextmanager
def writer_lock(filename="writer.lock"):
    if os.name!='nt': raise Fault('PLATFORM_UNSUPPORTED','Требуется Windows.')
    import msvcrt
    owners = getattr(_lock_owners, 'held', None)
    if owners is None:
        owners = _lock_owners.held = {}
    if filename in owners:
        owners[filename] += 1
        try: yield
        finally: owners[filename] -= 1
        return
    with (ROOT/filename).open('a+b') as f:
        if not f.seek(0,2): f.write(b'0'); f.flush()
        f.seek(0)
        try: msvcrt.locking(f.fileno(),msvcrt.LK_NBLCK,1)
        except OSError: raise Fault('WRITER_BUSY','Другой проход CLI выполняет изменения.','partial')
        owners[filename] = 1
        try: yield
        finally:
            owners.pop(filename, None)
            f.seek(0); msvcrt.locking(f.fileno(),msvcrt.LK_UNLCK,1)

class Store:
    def __init__(self, migrate=False, readonly=False):
        self.readonly = readonly
        self._ownership = None
        self.db = None
        if not readonly:
            self._ownership = writer_lock()
            self._ownership.__enter__()
        try:
            self._open(migrate, readonly)
        except BaseException:
            self.close()
            raise

    def _open(self, migrate, readonly):
        self.log_warnings=[]
        self._file_cache={}
        if readonly:
            self.db = sqlite3.connect((ROOT/'state.sqlite').as_uri()+'?mode=ro', uri=True, timeout=2)
            self.db.execute('PRAGMA query_only=ON')
            if self.db.execute('PRAGMA user_version').fetchone()[0] != 3:
                raise Fault('MIGRATION_REQUIRED', 'Чтение требует версии состояния 3.')
            return
        existed=(ROOT/'state.sqlite').exists()
        self.db=sqlite3.connect(ROOT/'state.sqlite',timeout=2)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS operations(id TEXT PRIMARY KEY, kind TEXT, hash TEXT, stage TEXT, body TEXT, updated TEXT);
        CREATE TABLE IF NOT EXISTS cache(path TEXT PRIMARY KEY, fingerprint TEXT, metadata TEXT);
        CREATE TABLE IF NOT EXISTS stops(hash TEXT PRIMARY KEY, reason TEXT);
        CREATE TABLE IF NOT EXISTS events(id INTEGER PRIMARY KEY, time TEXT, operation_id TEXT, body TEXT);
        CREATE TABLE IF NOT EXISTS requests(id TEXT PRIMARY KEY, signature TEXT, result TEXT);
        CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, body TEXT);
        CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY, request_key TEXT UNIQUE, signature TEXT NOT NULL, body TEXT NOT NULL, created TEXT NOT NULL, updated TEXT NOT NULL);
        ''')
        self.db.commit()
        version=self.db.execute('PRAGMA user_version').fetchone()[0]
        if version>3: raise Fault('STATE_VERSION_UNSUPPORTED','Версия состояния новее приложения.')
        if version<3:
            populated=existed and any(self.db.execute('SELECT count(*) FROM '+name).fetchone()[0] for name in ('operations','cache','requests','settings','stops','events'))
            if populated and not migrate:
                self.db.close(); raise Fault('MIGRATION_REQUIRED','Нужна команда migrate --apply; клиент не изменён.')
            if populated:
                folder=ROOT/'backups'; folder.mkdir(exist_ok=True)
                backup=sqlite3.connect(folder/(f'state-v{version}-'+uuid.uuid4().hex+'.sqlite'))
                try: self.db.backup(backup)
                finally: backup.close()
            self.db.execute('CREATE TABLE IF NOT EXISTS registry(hash TEXT PRIMARY KEY, body TEXT NOT NULL)')
            self.db.execute('CREATE TABLE IF NOT EXISTS operation_files(operation_id TEXT NOT NULL, ordinal INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY(operation_id,ordinal))')
            for (raw,) in self.db.execute("SELECT body FROM operations WHERE kind='complete'").fetchall():
                op=self.decode_op(raw)
                for f in op.get('files',[]):
                    if version<2 and (f.get('moved') or op['stage'] in ('data_verified','remove_requested','removed','finished')):
                        f['handoff']='handed_off'; f['evidence']='migrated_record'
                self._write_op(op)
            self.db.execute('PRAGMA user_version=3'); self.db.commit()
        self.db.execute('CREATE TABLE IF NOT EXISTS registry(hash TEXT PRIMARY KEY, body TEXT NOT NULL)'); self.db.commit()

    def decode_op(self,raw):
        op=json.loads(raw)
        if op.get('file_storage')=='rows':
            rows=self.db.execute('SELECT ordinal,body FROM operation_files WHERE operation_id=? ORDER BY ordinal',(op['id'],)).fetchall()
            if len(rows)!=op['file_count'] or [r[0] for r in rows]!=list(range(op['file_count'])):
                raise Fault('STATE_MANIFEST_INCOMPLETE','Неполная файловая ведомость в SQLite.',operation_id=op['id'])
            op['files']=[json.loads(r[1]) for r in rows]
            self._file_cache[op['id']]={r[0]:r[1] for r in rows}
        return op

    def _write_op(self,op,file_ordinal=None):
        body=dict(op)
        if op['kind']=='complete':
            if op['id'] not in self._file_cache:
                self._file_cache[op['id']]={r[0]:r[1] for r in self.db.execute('SELECT ordinal,body FROM operation_files WHERE operation_id=?',(op['id'],))}
            cached=self._file_cache[op['id']]
            indices=range(len(op['files'])) if file_ordinal is None else (file_ordinal,)
            handed_off=self.db.execute('SELECT body FROM registry WHERE hash=?',(op['hash'],)).fetchone() if file_ordinal is not None else None
            handoffs=json.loads(handed_off[0]).get('handoffs',0) if handed_off else 0
            for i in indices:
                f=op['files'][i]
                raw=json.dumps(f,ensure_ascii=False)
                if cached.get(i)!=raw:
                    if file_ordinal is not None:
                        if i not in cached: raise Fault('STATE_MANIFEST_INCOMPLETE','Нельзя адресно записать файл без исходной ведомости.')
                        handoffs+=int(f.get('handoff')=='handed_off')-int(json.loads(cached[i]).get('handoff')=='handed_off')
                    self.db.execute('INSERT INTO operation_files VALUES(?,?,?) ON CONFLICT(operation_id,ordinal) DO UPDATE SET body=excluded.body',(op['id'],i,raw))
                    cached[i]=raw
            body.pop('files'); body.update(file_storage='rows',file_count=len(op['files']))
        self.db.execute('INSERT INTO operations VALUES(?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET kind=excluded.kind,hash=excluded.hash,stage=excluded.stage,body=excluded.body,updated=excluded.updated',(op['id'],op['kind'],op['hash'],op['stage'],json.dumps(body,ensure_ascii=False),now()))
        if op['kind']=='complete':
            state='finished' if op['stage']=='finished' else 'processing'
            previous=self.db.execute('SELECT body FROM registry WHERE hash=?',(op['hash'],)).fetchone()
            r=json.loads(previous[0]) if previous else {'hash':op['hash'],'aliases':[op['hash']]}
            r.update(state=state,operation_id=op['id'],archive=op['archive_torrent'],torrent_sha256=op['torrent_sha256'],handoffs=(handoffs if file_ordinal is not None else sum(f.get('handoff')=='handed_off' for f in op['files'])),files=len(op['files']))
            self.db.execute('INSERT OR REPLACE INTO registry VALUES(?,?)',(op['hash'],json.dumps(r)))

    def registry(self):
        return [json.loads(r[0]) for r in self.db.execute('SELECT body FROM registry ORDER BY hash')]

    def log_event(self,oid,data):
        try:
            log=ROOT/'logs'; log.mkdir(exist_ok=True)
            with (log/'operations.jsonl').open('a',encoding='utf-8') as f:
                f.write(json.dumps({'time':now(),'operation_id':oid,**data},ensure_ascii=False)+'\n'); f.flush()
        except OSError:
            self.log_warnings.append({'code':'SECONDARY_LOG_UNAVAILABLE','message':'SQLite сохранён; дополнительный журнал недоступен.'})
    def event(self, oid, data):
        self.db.execute('INSERT INTO events(time,operation_id,body) VALUES(?,?,?)',(now(),oid,json.dumps(data,ensure_ascii=False))); self.db.commit()
        self.log_event(oid,data)
    def put_op(self, op, file_ordinal=None):
        data={'kind':op['kind'],'hash':op['hash'],'stage':op['stage']}
        try:
            with self.db:
                self._write_op(op,file_ordinal)
                self.db.execute('INSERT INTO events(time,operation_id,body) VALUES(?,?,?)',(now(),op['id'],json.dumps(data)))
        except BaseException:
            self._file_cache.clear(); raise
        self.log_event(op['id'],data)
    def pending(self):
        return [self.decode_op(r[0]) for r in self.db.execute("SELECT body FROM operations WHERE stage != 'finished' ORDER BY updated").fetchall()]
    def stop_reason(self,h):
        r=self.db.execute('SELECT reason FROM stops WHERE hash=?',(h,)).fetchone()
        return r[0] if r else None
    def set_stop(self,h,reason):
        if reason: self.db.execute('INSERT OR REPLACE INTO stops VALUES(?,?)',(h,reason))
        else: self.db.execute('DELETE FROM stops WHERE hash=?',(h,))
        self.db.commit()
    def setting(self,k,default=None):
        r=self.db.execute('SELECT body FROM settings WHERE key=?',(k,)).fetchone()
        return json.loads(r[0]) if r else default
    def set_setting(self,k,v):
        self.db.execute('INSERT OR REPLACE INTO settings VALUES(?,?)',(k,json.dumps(v))); self.db.commit()
    def update_setting(self,k,update,default=None):
        # Read/merge/write under one SQLite lock; read-only observers must not
        # overwrite a lifecycle receipt written by another CLI process.
        with self.db:
            self.db.execute('BEGIN IMMEDIATE')
            row=self.db.execute('SELECT body FROM settings WHERE key=?',(k,)).fetchone()
            before=row[0] if row else None; initial=before if row else json.dumps(default)
            value=json.loads(initial)
            value=update(value); raw=json.dumps(value)
            if initial!=raw:
                self.db.execute('INSERT INTO settings VALUES(?,?) ON CONFLICT(key) DO UPDATE SET body=excluded.body',(k,raw))
        return value
    def close(self):
        try:
            if self.db is not None:
                self.db.close(); self.db = None
        finally:
            if self._ownership is not None:
                self._ownership.__exit__(None, None, None); self._ownership = None


def sqlite_details(error):
    if not isinstance(error, sqlite3.Error): return {}
    code = getattr(error, 'sqlite_errorcode', None)
    return {'sqlite_errorcode': code,
            'sqlite_errorname': getattr(error, 'sqlite_errorname', None),
            'storage_failure': 'busy' if code is not None and (code & 255) in (5, 6) else 'storage_error'}
