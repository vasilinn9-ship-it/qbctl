"""Durable jobs and one asynchronous coordinator around the existing engine.

The coordinator never moves payload. Each worker call uses Controller and its
writer lock; SQLite connections are opened and closed in the owning thread.
"""
from __future__ import annotations
import argparse
import asyncio
import contextlib
import ctypes
import json
import os
import sqlite3
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from .common import ROOT, Fault, Store, digest, load_config, now, writer_lock, sqlite_details

ACTIVE = {'queued', 'running', 'waiting'}
TERMINAL = {'finished', 'paused', 'blocked', 'unknown', 'error'}
TRANSIENT = {'CHECK_PENDING', 'START_PENDING', 'STOP_PENDING', 'SERVICE_WAITING',
             'SERVICE_DEFERRED', 'BUDGET_EXHAUSTED', 'WRITER_BUSY', 'RECOVERY_REQUIRED',
             'TASK_RECOVERY_REQUIRED', 'QUEUE_BELOW_TARGET', 'FINAL_SNAPSHOT_UNAVAILABLE',
             'REQUEST_RESULT_RECOVERED', 'API_UNAVAILABLE', 'EXECUTOR_STOPPED', 'ADD_PENDING', 'REMOVE_PENDING', 'ADD_ADMISSION_WAIT'}
UNKNOWN = {'MUTATION_UNCERTAIN', 'REQUEST_INCOMPLETE', 'START_UNCONFIRMED',
           'STOP_UNCONFIRMED', 'PREFERENCES_UNCONFIRMED', 'REMOVE_UNCONFIRMED',
           'RECHECK_UNCONFIRMED', 'RECOVERY_AMBIGUOUS', 'RECHECK_UNOBSERVED'}
STEP_SECONDS = 30.0


@contextlib.contextmanager
def control_lock():
    # Job control and payload use the same exclusive writer ownership.
    # Same-thread nesting is reentrant; concurrent commands return WRITER_BUSY.
    until=time.monotonic()+2
    while True:
        lock=writer_lock()
        try:
            lock.__enter__(); break
        except Fault as fault:
            if fault.code!='WRITER_BUSY' or time.monotonic()>=until: raise
            time.sleep(0.01)
    try: yield
    finally: lock.__exit__(None,None,None)


@contextlib.contextmanager
def database(readonly=True):
    store = Store(readonly=readonly)
    try:
        yield store
    finally:
        store.close()


def process_alive(pid):
    if not pid or os.name != 'nt':
        return False
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.OpenProcess.restype = ctypes.c_void_p
    handle = kernel.OpenProcess(0x1000, False, int(pid))
    if not handle:
        return False
    try:
        code = ctypes.c_ulong()
        return bool(kernel.GetExitCodeProcess(ctypes.c_void_p(handle), ctypes.byref(code)) and code.value == 259)
    finally:
        kernel.CloseHandle(ctypes.c_void_p(handle))


def runtime_state():
    try:
        return json.loads((ROOT/'executor-runtime.json').read_text(encoding='utf-8'))
    except FileNotFoundError:
        return {}


def save_runtime(state):
    # Disposable process ownership metadata, never operation receipts.
    import tempfile
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=ROOT,
                                         prefix='.runtime-', suffix='.tmp', delete=False) as file:
            temporary = file.name
            json.dump(state, file); file.flush(); os.fsync(file.fileno())
        os.replace(temporary, ROOT/'executor-runtime.json'); temporary = None
    finally:
        if temporary:
            with contextlib.suppress(OSError): os.unlink(temporary)


def runtime_status(store=None):
    if store is None:
        with database() as opened:
            return runtime_status(opened)
    state = runtime_state()
    locked=False
    try:
        with writer_lock('executor.lock'): pass
    except Fault as fault:
        if fault.code!='WRITER_BUSY': raise
        locked=True
    alive = bool(locked and state.get('mode') in ('foreground','background') and process_alive(state.get('pid')))
    counts = {}
    for raw, in store.db.execute('SELECT body FROM jobs'):
        status = json.loads(raw)['state']
        counts[status] = counts.get(status, 0) + 1
    return {'alive': alive, 'mode': state.get('mode', 'off') if locked else 'off',
            'ownership_locked':locked,'foreground_job':state.get('foreground_job') if locked else None,
            'pid': state.get('pid') if locked else None, 'heartbeat_age_seconds': None, 'liveness_basis': 'ownership_lock_and_process',
            'stop_requested': bool(state.get('stop_requested')), 'job_counts': counts,
            'automatic_watch': False, 'restart_requires_explicit_start': True}


def read_job(store, identifier):
    if len(identifier) != 32 or any(c not in '0123456789abcdef' for c in identifier):
        raise Fault('ARGUMENT_INVALID', 'Неверный ID задания.', 'error')
    row = store.db.execute('SELECT body FROM jobs WHERE id=?', (identifier,)).fetchone()
    if not row:
        raise Fault('JOB_NOT_FOUND', 'Задание не найдено.', id=identifier)
    return json.loads(row[0])


def save_job(store, job):
    job['updated_at'] = now()
    store.db.execute('UPDATE jobs SET body=?,updated=? WHERE id=?',
                     (json.dumps(job, ensure_ascii=False), job['updated_at'], job['id']))
    store.db.commit()


def update_job(identifier, change):
    with control_lock(), database(readonly=False) as store:
        job = read_job(store, identifier)
        change(job, store)
        save_job(store, job)
        return job


def public_job(job):
    last = job.get('last_result') or {}
    return {key: job.get(key) for key in ('id', 'command', 'count', 'state', 'created_at',
            'updated_at', 'reason', 'step_number', 'pause_requested', 'policy_revision',
            'selected_completions', 'max_completions', 'max_additions')} | {
        'scope_frozen': bool(job.get('initialized')),
        'pending_operations': last.get('pending_operations', []),
        'operation_progress': last.get('operation_progress', []),
        'last_snapshot_at': last.get('snapshot_at'),
        'observed': last.get('observed'),
        'verified_actions': job.get('actions', []),
        'last_issue_codes': [i['code'] for i in last.get('issues', [])],
        'current_step_request_id': (job.get('current_step') or {}).get('request_id'),
        'wait_observation': job.get('wait_observation'),
        'check_wait_polls': job.get('check_wait_polls', 0),
        'warnings': job.get('warnings', []),
        'performance':dict(job.get('performance',{}),check_wait_live_seconds=round(max(0,time.time()-job['check_wait_since']),3) if job.get('check_wait_since') is not None else 0) if job.get('performance') else {},
        'no_overall_deadline': True,
    }


def submit(args):
    specification = {'command': args.command, 'count': getattr(args, 'count', None),
                     'hash': getattr(args, 'hash', None), 'max_completions': args.max_completions,
                     'max_additions': args.max_additions}
    if args.command == 'downloads' and not 0 <= args.count <= 100000:
        raise Fault('ARGUMENT_INVALID', 'Количество загрузок: 0..100000.', 'error')
    signature = digest(specification)
    with control_lock(), database(readonly=False) as store:
        key = getattr(args, 'request_id', None)
        if key:
            row = store.db.execute('SELECT signature,body FROM jobs WHERE request_key=?', (key,)).fetchone()
            if row:
                if row[0] != signature:
                    raise Fault('REQUEST_ID_CONFLICT', 'request-id задания уже занят другой командой.')
                return json.loads(row[1]), True
        stamp = now()
        job = {'id': uuid.uuid4().hex, **specification, 'state': 'queued', 'created_at': stamp,
               'updated_at': stamp, 'initialized': False, 'pause_requested': False,
               'step_number': 0, 'current_step': None, 'control_id': uuid.uuid4().hex,
               'actions': [], 'reason': {'code': 'QUEUED', 'message': 'Задание сохранено; выполнение только в явно запущенном исполнителе или --wait.'}}
        store.db.execute('INSERT INTO jobs VALUES(?,?,?,?,?,?)',
                         (job['id'], key, signature, json.dumps(job, ensure_ascii=False), stamp, stamp))
        store.db.commit()
        return job, False


def initialize(identifier):
    from .api import API
    from .engine import complete
    # Freeze the scope durably before any client mutation. Manual CLI mutations
    # use the same lock; the policy revision is checked again in every step.
    with writer_lock(), database(readonly=False) as store:
        job = read_job(store, identifier)
        if job['initialized'] or job['pause_requested']:
            return job
        config = load_config()
        api = API(config['api'])
        api.compatible()
        rows = api.torrents()
        if job.get('hash'):
            from .engine import Controller
            from .common import Budget
            rows = [Controller(config, api, store, Budget(STEP_SECONDS)).select(rows, job['hash'])]
        context = dict(initialized=True, policy_revision=config['revision'],
                       selected_completions=sorted(t['hash'] for t in rows if complete(t))[:job['max_completions']],
                       initial_pending=[o['id'] for o in store.pending()], next_poll_at=0,
                       performance={'steps':0,'api_metrics':api.metrics,'phase_timings':{},'step_seconds':0,
                                    'check_poll_count':0,'check_wait_seconds':0})
        def freeze(current, opened):
            if not current['initialized'] and not current['pause_requested']:
                current.update(context)
        return update_job(identifier, freeze)


def operation_counts(store, job):
    prefix = 'job:' + job['id'] + ':'
    operations = [json.loads(raw) for raw, in store.db.execute('SELECT body FROM operations')]
    owned = [o for o in operations if (o.get('request_id') or '').startswith(prefix)]
    complete_hashes = {o['hash'] for o in operations if o['kind'] == 'complete' and o['stage'] == 'finished'}
    added = sum(o['kind'] == 'add' for o in owned)
    policy = next((o for o in operations if o['id'] == job['control_id']), None)
    return added, complete_hashes, policy


def prepare_step(identifier):
    def prepare(job, store):
        if job['pause_requested'] or job['state'] not in ACTIVE or job.get('current_step'):
            return
        added, _, policy = operation_counts(store, job)
        command_name = job['command']
        if command_name == 'downloads' and policy and policy['stage'] == 'finished':
            command_name = 'run'
        revision = policy.get('applied_revision', job['policy_revision']) if policy else job['policy_revision']
        job['policy_revision'] = revision
        number = job['step_number'] + 1
        request = f"job:{job['id']}:{number}"
        args = {'command': command_name, 'verb': 'set', 'count': job.get('count'),
                'hash': None, 'apply': True, 'json': True, 'events_jsonl': False,
                'enqueue': False, 'wait': False, 'request_id': request, 'deadline': STEP_SECONDS,
                # Preflight and journal one completion per bounded worker step.
                # Re-validating a multi-item frozen batch after each step budget
                # expired caused the entire manifest batch to be rescanned forever.
                'max_completions': min(1, job['max_completions']),
                'max_additions': max(0, job['max_additions'] - added),
                '_nonblocking': True, '_completion_scope': job['selected_completions'],
                '_control_id': job['control_id'], '_policy_revision': revision}
        job.update(step_number=number, current_step={'request_id': request, 'args': args}, state='running', wait_observation=None,
                   reason={'code': 'RUNNING', 'message': 'Выполняется шаг существующего журнала.'})
    return update_job(identifier, prepare)


def record_step(identifier, result):
    def record(job, store):
        performance=job.setdefault('performance',{'steps':0,'api_metrics':{},'phase_timings':{},'step_seconds':0})
        performance['steps']+=1
        performance['step_seconds']=round(performance['step_seconds']+result.get('elapsed_seconds',0),3)
        for category in ('api_metrics','phase_timings'):
            for name,item in result.get(category,{}).items():
                total=performance[category].setdefault(name,{'calls':0,'seconds':0})
                total['calls']+=item['calls']; total['seconds']=round(total['seconds']+item['seconds'],6)
        performance['timing_note']='API times may overlap; phase times may be nested; not wall clock.'
        job['last_result'] = result
        actions = job.get('actions', [])
        seen = {digest(a) for a in actions}
        for action in result.get('actions', []):
            if action.get('result') == 'verified' and digest(action) not in seen:
                actions.append(action); seen.add(digest(action))
        job['actions'] = actions
        job['current_step'] = None
        codes = {i['code'] for i in result.get('issues', [])}
        added, completed, policy = operation_counts(store, job)
        if policy and policy.get('applied_revision') is not None:
            job['policy_revision'] = policy['applied_revision']
        obs = result.get('observed', {})
        remaining = set(job.get('selected_completions', [])) - completed
        # The registry already excludes every alias of a duplicate from plans.
        # This scoped finding must not interrupt independent journal recovery.
        scoped_duplicates=[i for i in result.get('issues',[]) if i['code']=='TORRENT_DUPLICATE'
                           and i.get('hash') and i.get('paths') and i.get('roles')]
        warnings=job.get('warnings',[])
        seen_warnings={digest(w) for w in warnings}
        for issue in scoped_duplicates:
            warning={**issue,'scope':'torrent','effect':'excluded_from_admission',
                     'message':'Обнаружены дубли torrent; этот хеш исключён из новых загрузок, независимые операции продолжаются.',
                     'next_safe_commands':['qbctl audit --json']}
            if digest(warning) not in seen_warnings:
                warnings.append(warning); seen_warnings.add(digest(warning))
        job['warnings']=warnings
        blocking={i['code'] for i in result.get('issues',[]) if i['code'] not in TRANSIENT and i not in scoped_duplicates}
        if job['pause_requested']:
            state, reason = 'paused', {'code': 'JOB_PAUSED', 'message': 'Остановлены дальнейшие шаги; данные и клиент сохранены.'}
        elif result.get('result') == 'unknown' or codes & UNKNOWN or any(i['code']=='ADD_PENDING' and not i.get('request_accepted') for i in result.get('issues', [])):
            state, reason = 'unknown', {'code': 'JOB_UNKNOWN', 'message': 'Неизвестный исход требует диагностики; запросы не повторяются.', 'causes': sorted(codes)}
        elif blocking:
            state, reason = ('error' if result.get('result') == 'error' else 'blocked'), {'code': 'JOB_BLOCKED', 'message': 'Нарушено условие безопасной обработки.', 'causes': sorted(blocking)}
        elif 'API_UNAVAILABLE' in codes or 'WRITER_BUSY' in codes:
            state, reason = 'waiting', {'code': 'API_UNAVAILABLE' if 'API_UNAVAILABLE' in codes else 'WRITER_BUSY', 'message': 'Ожидание клиента или освобождения писателя; новых запросов изменения нет.'}
        elif result.get('pending_operations') or obs.get('checking'):
            state, reason = 'waiting', {'code': 'OPERATION_WAIT', 'message': 'Ожидание наблюдения или продолжения сохранённого этапа.'}
            if 'STOP_PENDING' in codes:
                reason={'code':'STOP_PENDING','message':'Запрос остановки сохранён; ожидается подтверждение и автоматическое продолжение, без повторной отправки.'}
        elif not obs or codes & {'BUDGET_EXHAUSTED', 'SERVICE_DEFERRED', 'REQUEST_RESULT_RECOVERED', 'RECOVERY_REQUIRED'}:
            state, reason = 'queued', {'code': 'NEXT_STEP', 'message': 'Задание продолжается следующим шагом без общего таймаута.'}
        elif remaining:
            state, reason = 'blocked', {'code': 'COMPLETION_NOT_CONFIRMED', 'message': 'Остальные операции выполнены; первоначальные завершения не подтверждены квитанциями. Проверить указанные хеши и конфликты источников.', 'hashes': sorted(remaining)}
        elif any(d['code']=='DOWNLOAD_LIMIT' for d in result.get('diagnostics', [])) and obs.get('allowed_downloads',0)<obs.get('effective_download_slots',0):
            state, reason = 'queued', {'code': 'RESUME_PENDING', 'message': 'Остались собственные остановленные задачи; следующий шаг применит лимит.'}
        elif obs['client_count'] < result.get('desired', {}).get('target_client_count', obs['client_count']):
            if added >= job['max_additions']:
                state, reason = 'finished', {'code': 'ADDITION_LIMIT', 'message': 'Достигнут явно заданный предел добавлений; очередь ниже цели.'}
            else:
                state, reason = 'blocked', {'code': 'NO_ADMISSIBLE_CANDIDATES', 'message': 'Нет подтверждённого пополнения до цели; проверить план и условия допуска.'}
        else:
            state, reason = 'finished', {'code': 'FINISHED', 'message': 'Исходный проход завершён; новые завершения оставлены следующему заданию.'}
        job.update(state=state, reason=reason, next_poll_at=time.time() + (2 if state == 'waiting' else 0))
    return update_job(identifier, record)


def work_one(identifier):
    from .cli import execute
    try:
        job = initialize(identifier)
        if observe_check_wait(job):
            with database() as store: return read_job(store, identifier)
        job = prepare_step(identifier)
        if job['pause_requested'] or job['state'] not in ACTIVE:
            return job
        step = job['current_step']
        result = execute(argparse.Namespace(**step['args']))
        return record_step(identifier, result)
    except Fault as fault:
        def failed(job, store):
            transient = fault.code in ('WRITER_BUSY', 'API_UNAVAILABLE')
            job.update(state='waiting' if transient else ('unknown' if fault.result == 'unknown' else ('error' if fault.result=='error' else 'blocked')),
                       reason=fault.issue(), next_poll_at=time.time() + 2)
        return update_job(identifier, failed)


def observe_check_wait(job):
    # A client check needs no repeated root index, manifests or planning.
    # Only delay a previously waiting job with no durable in-flight step.
    # The ordinary engine revalidates everything when checking disappears.
    codes={issue['code'] for issue in (job.get('last_result') or {}).get('issues', [])}
    if job['state']!='waiting' or job.get('current_step') or job['pause_requested'] or 'CHECK_PENDING' not in codes:
        return False
    config=load_config()
    if config['revision']!=job['policy_revision']: return False
    from .api import API
    from .engine import checking
    api=API(config['api']); api.compatible()
    rows=api.torrents()
    checks=[{'hash':t['hash'],'state':t['state'],'progress':t.get('progress')}
            for t in rows if checking(t)]
    def account(current):
        perf=current.setdefault('performance',{'steps':0,'api_metrics':{},'phase_timings':{},'step_seconds':0})
        perf['check_poll_count']=perf.get('check_poll_count',0)+1
        for name,item in api.metrics.items():
            total=perf['api_metrics'].setdefault(name,{'calls':0,'seconds':0})
            total['calls']+=item['calls']; total['seconds']=round(total['seconds']+item['seconds'],6)
        if checks:
            current.setdefault('check_wait_since',time.time())
        elif current.get('check_wait_since') is not None:
            perf['check_wait_seconds']=round(perf.get('check_wait_seconds',0)+max(0,time.time()-current.pop('check_wait_since')),3)
    if not checks:
        update_job(job['id'], lambda current,store: account(current))
        return False
    def observed(current, store):
        if current['state'] in ACTIVE and not current['pause_requested']:
            account(current)
            current.update(state='waiting', next_poll_at=time.time()+2,
                           check_wait_polls=current.get('check_wait_polls',0)+1,
                           wait_observation={'at':now(),'checking':checks,'api_metrics':api.metrics},
                           reason={'code':'CHECK_PENDING','message':'Клиент проверяет данные; короткий опрос без повторного планирования.'})
    update_job(job['id'], observed)
    return True


def next_job(only=None):
    with database() as store:
        jobs = [json.loads(raw) for raw, in store.db.execute('SELECT body FROM jobs ORDER BY updated,created')]
    return next((j for j in jobs if j['state'] in ACTIVE and not j['pause_requested']
                 and (only is None or j['id'] == only) and j.get('next_poll_at', 0) <= time.time()), None)


def requested_stop(token):
    state = runtime_state()
    return state.get('token') != token or bool(state.get('stop_requested'))


def diagnostic_error(progress, error):
    """Presentation callbacks cannot replace a durable worker result."""
    if progress is None: return
    try: progress.note_error(error)
    except Exception:
        try:
            warnings = progress.warnings
            issue = {'code':'PROGRESS_UNAVAILABLE', 'exception_type':type(error).__name__,
                     'message':'Диагностика недоступна; результат определяется журналом.'}
            if issue not in warnings: warnings.append(issue)
        except Exception: pass


async def coordinate(token, mode, only=None, progress=None):
    stop = asyncio.Event()
    async def pulse():
        while not stop.is_set():
            if progress and only:
                try: await asyncio.to_thread(progress, {'id':only})
                except Exception as error:
                    diagnostic_error(progress, error)
            try: await asyncio.wait_for(stop.wait(), timeout=2)
            except TimeoutError: pass
    monitor = asyncio.create_task(pulse())
    inflight = None
    try:
        while not stop.is_set():
            if requested_stop(token): break
            if only:
                with database() as store:
                    target = read_job(store, only)
                if target['state'] in TERMINAL:
                    break
            job = next_job(only)
            if job is None:
                await asyncio.sleep(0.5); continue
            # There is exactly one payload worker. Shielding does not undo file
            # writes; on cancellation we join this step before closing runtime.
            inflight = asyncio.create_task(asyncio.to_thread(work_one, job['id']))
            await asyncio.shield(inflight)
            inflight = None
            await asyncio.sleep(0)
    except asyncio.CancelledError:
        if inflight:
            await inflight
        if only:
            update_job(only, lambda j, s: j.update(pause_requested=True, state='paused', reason={'code': 'INTERRUPTED', 'message': 'Текущий шаг сохранён; дальнейшие шаги остановлены.'}))
        raise
    finally:
        stop.set()
        # Presentation failures never replace durable operation results.
        try: await monitor
        except Exception as error: diagnostic_error(progress, error)
        if only:
            def suspend(job, store):
                if job['state'] in ACTIVE:
                    job.update(state='paused', pause_requested=True, reason={'code': 'EXECUTOR_STOPPED', 'message': 'Исполнитель остановлен после сохранения шага.'})
            with database() as store: active = read_job(store, only)['state'] in ACTIVE
            if active: update_job(only, suspend)


def serve(token=None, only=None, progress=None):
    mode = 'foreground' if only else 'background'
    token = token or uuid.uuid4().hex
    with executor_owner(token is not None and only is None), writer_lock('watch.lock'):
        with control_lock(), writer_lock('runtime.lock'), database(readonly=False) as store:
            old = runtime_state()
            state = old if old.get('token') == token else {'token': token, 'stop_requested': False}
            state.update(pid=os.getpid(), mode=mode, foreground_job=only, started_at=now())
            save_runtime(state)
            for raw, in store.db.execute('SELECT body FROM jobs').fetchall():
                job = json.loads(raw)
                if job['state'] in ACTIVE:
                    job['next_poll_at'] = 0; save_job(store, job)
        try:
            asyncio.run(coordinate(token, mode, only, progress))
        finally:
            try:
                with writer_lock('runtime.lock'):
                    state = runtime_state()
                    if state.get('token') == token:
                        state.update(pid=None, mode='off', stopped_at=now())
                        save_runtime(state)
            except Exception as error:
                if progress: diagnostic_error(progress, error)
                else:
                    try: print(json.dumps({'code':'RUNTIME_CLEANUP_FAILED','exception_type':type(error).__name__}), file=sys.stderr)
                    except Exception: pass



@contextlib.contextmanager
def executor_owner(starting=False):
    until=time.monotonic()+2
    while True:
        lock=writer_lock('executor.lock')
        try:
            lock.__enter__(); break
        except Fault as fault:
            if not starting or fault.code!='WRITER_BUSY' or time.monotonic()>=until: raise
            time.sleep(0.01)
    try: yield
    finally: lock.__exit__(None,None,None)


def start():
    with control_lock(), database(readonly=False) as store:
        status = runtime_status(store)
        if status['alive']:
            return status
        # A held ownership lock forbids a second owner; no heartbeat writes.
        with writer_lock('executor.lock'), writer_lock('watch.lock'), writer_lock('runtime.lock'):
            token = uuid.uuid4().hex
            save_runtime({'token': token, 'mode': 'starting', 'pid': None,
                          'started_at': now(), 'stop_requested': False})
            try:
                with (ROOT / 'executor.log').open('ab') as log:
                    process = subprocess.Popen([sys.executable, '-B', str(ROOT / 'qbctl_entry.py'),
                        'executor', 'serve', '--apply', '--token', token, '--json'],
                        cwd=str(ROOT), stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
                state = runtime_state()
                state['pid'] = process.pid
                save_runtime(state)
            except OSError:
                save_runtime({'mode': 'off', 'pid': None, 'token': token, 'stopped_at': now()})
                raise Fault('EXECUTOR_START_FAILED', 'Процесс исполнителя не запущен.', 'error')
            return {'alive': False, 'mode': 'starting', 'pid': process.pid, 'start_requested': True,
                    'message': 'Запуск запрошен; готовность подтверждается executor status.'}


def stop_executor():
    with writer_lock('runtime.lock'), database() as store:
        state = runtime_state()
        state['stop_requested'] = True
        save_runtime(state)
        return {**runtime_status(store), 'message': 'Остановка запрошена после текущего шага; клиент и payload не останавливаются.'}


def response(job, historical=True):
    from .cli import result_base
    result = result_base('job')
    result['job'] = public_job(job)
    with database() as store:
        related=[]
        for operation in store.pending():
            if (operation.get('request_id') or '').startswith('job:'+job['id']+':') or operation['id'] in job.get('initial_pending', []):
                item={k:operation[k] for k in ('id','hash','kind','stage')}
                if operation['kind']=='complete':
                    count=sum(f.get('handoff')=='handed_off' for f in operation['files'])
                    item.update(files_total=len(operation['files']),files_handed_off=count,files_remaining=len(operation['files'])-count)
                related.append(item)
        result['job']['live_journal']={'observed_at':now(),'operations':related}
    runtime_warning = None
    try: result['executor'] = runtime_status()
    except Exception as error:
        result['executor'] = {'alive':None, 'mode':'unknown'}
        runtime_warning = {'code':'RUNTIME_STATUS_UNAVAILABLE',
                           'message':'Состояние процесса не прочитано; результат задания сохранён в журнале.',
                           'exception_type':type(error).__name__, **sqlite_details(error)}
    last = job.get('last_result') or {}
    for key in ('snapshot_at', 'observed', 'desired', 'diagnostics', 'operation_progress', 'pending_operations'):
        if key in last:
            result[key] = last[key]
    result['actions'] = job.get('actions', [])
    result['warnings'] = list(job.get('warnings', []))
    if runtime_warning: result['warnings'].append(runtime_warning)
    result['evidence'] = 'job_journal; observed snapshot has its own timestamp'
    result['actions_scope'] = 'whole_saved_job'
    result['response_replayed'] = historical
    state = job['state']
    result['result'] = {'blocked': 'blocked', 'unknown': 'unknown', 'error': 'error', 'paused': 'partial'}.get(state, 'ok')
    if state == 'finished' and job.get('reason', {}).get('code') == 'ADDITION_LIMIT':
        result['result'] = 'partial'
    if state in ('blocked', 'unknown', 'error', 'paused') or result['result'] == 'partial':
        details=[i for i in last.get('issues', []) if i['code'] not in TRANSIENT and i['code']!='TORRENT_DUPLICATE']
        result['issues'] = details + [job['reason']]
        # For old blocked jobs, preserve original details even before a resume.
        if 'TORRENT_DUPLICATE' in job.get('reason',{}).get('causes',[]):
            result['issues'] = [i for i in last.get('issues', []) if i['code']=='TORRENT_DUPLICATE'] + result['issues']
    if state in ACTIVE:
        result['next_safe_commands'] = [f"qbctl job status {job['id']} --json", 'qbctl executor status --json']
    elif state != 'finished':
        result['next_safe_commands'] = [f"qbctl job status {job['id']} --json", 'qbctl status --json']
    elif last.get('observed', {}).get('completed', 0):
        result['next_safe_commands'] = ['qbctl run --apply --wait --json']
    for warning in result['warnings']:
        for safe_command in warning.get('next_safe_commands', []):
            if safe_command not in result['next_safe_commands']: result['next_safe_commands'].append(safe_command)
    result['finished_at'] = now()
    return result


def command(args):
    from .cli import result_base
    if args.command in ('run', 'downloads'):
        job, reused = submit(args)
        historical=reused
        from .output import Progress
        progress=Progress(getattr(args,'progress',False) or (getattr(args,'wait',False) and not args.json),args.json)
        progress(job)
        if getattr(args, 'wait', False) and job['state'] in ACTIVE:
            historical=False
            try:
                runtime = runtime_status()
                if runtime['alive']:
                    if runtime['mode']=='foreground' and runtime.get('foreground_job')!=job['id']:
                        raise Fault('FOREGROUND_BUSY','В foreground выполняется другое задание; новое сохранено в очереди.', 'partial', job_id=job['id'])
                    async def observe():
                        while True:
                            with database() as store:
                                current = read_job(store, job['id'])
                            progress(current)
                            if current['state'] in TERMINAL: return
                            if not runtime_status()['alive']:
                                raise Fault('EXECUTOR_STOPPED', 'Исполнитель остановлен; задание сохранено.', 'partial', job_id=job['id'])
                            await asyncio.sleep(1)
                    asyncio.run(observe())
                else:
                    serve(only=job['id'],progress=progress)
            except KeyboardInterrupt:
                with database() as store: job=read_job(store,job['id'])
                result=response(job,historical); result['result']='partial'
                result['issues'].append({'code':'INTERRUPTED','message':'Ожидание прервано; состояние задания сохранено.'})
                return result
            except Fault as fault:
                with database() as store: job=read_job(store,job['id'])
                result=response(job,historical); result['result']=fault.result; result['issues'].append(fault.issue())
                return result
            except Exception as error:
                # Read the authoritative result before reporting a coordinator error.
                with database() as store: job = read_job(store,job['id'])
                result = response(job,historical)
                issue = {'code':'COORDINATOR_ERROR','message':'Ошибка координатора; сохранённые квитанции не отменены.',
                         'exception_type':type(error).__name__, **sqlite_details(error)}
                if job['state'] == 'finished': result['warnings'].append(issue)
                else:
                    result['result']='unknown'; result['issues'].append(issue)
                    result['next_safe_commands']=[f"qbctl job status {job['id']} --json"]
                result['warnings'].extend(progress.warnings)
                return result
            with database() as store: job = read_job(store, job['id'])
        progress(job)
        result = response(job,historical)
        result['warnings'].extend(progress.warnings)
        return result
    if args.command == 'job':
        if args.verb == 'list':
            with database() as store:
                jobs = [public_job(json.loads(raw)) for raw, in store.db.execute('SELECT body FROM jobs ORDER BY created DESC LIMIT 100')]
            return {**result_base('job'), 'jobs': jobs, 'executor': runtime_status(), 'finished_at': now()}
        if args.verb in ('pause', 'resume'):
            if not args.apply:
                with database() as store:
                    job = read_job(store, args.id)
                return {**response(job), 'mode': 'plan', 'requested_action': args.verb}
            def change(job, store):
                if job['state'] == 'finished':
                    raise Fault('JOB_FINISHED', 'Завершённое задание нельзя переиграть; создайте новое.')
                job['pause_requested'] = args.verb == 'pause'
                if args.verb == 'resume':
                    job.update(state='queued', next_poll_at=0, reason={'code': 'RESUME_REQUESTED', 'message': 'Разрешено продолжение по журналу; неизвестные запросы не повторяются.'})
                elif job['state'] != 'running':
                    job.update(state='paused', reason={'code': 'JOB_PAUSED', 'message': 'Дальнейшие шаги приостановлены.'})
            return response(update_job(args.id, change))
        with database() as store:
            return response(read_job(store, args.id))
    if args.verb == 'status':
        return {**result_base('executor'), 'executor': runtime_status(), 'finished_at': now()}
    if not args.apply:
        return {**result_base('executor'), 'mode': 'plan', 'requested_action': args.verb, 'finished_at': now()}
    if args.verb == 'start':
        status = start()
    elif args.verb == 'stop':
        status = stop_executor()
    else:
        serve(args.token); status = runtime_status()
    return {**result_base('executor'), 'executor': status, 'finished_at': now()}
