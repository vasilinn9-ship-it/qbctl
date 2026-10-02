from __future__ import annotations
import argparse, contextlib, json, os, sqlite3, sys, time, traceback, uuid
from pathlib import Path
from . import __version__
from .api import API
from .common import ROOT, Budget, Fault, Store, digest, load_config, norm, now, writer_lock, sqlite_details
from .engine import Controller, complete, checking, stopped, summary, read_phase
from .resources import Metrics, regulate
from .ownership import claims
from .presentation import build_report, report_line

class Parser(argparse.ArgumentParser):
    def error(self,message): raise Fault('ARGUMENT_INVALID',message,'error')

def options(p):
    for flag in ('json','apply','events-jsonl'):
        p.add_argument('--'+flag,action='store_true',default=argparse.SUPPRESS)
    p.add_argument('--request-id',default=argparse.SUPPRESS)
    p.add_argument('--deadline',type=float,default=argparse.SUPPRESS)
    p.add_argument('--output',default=argparse.SUPPRESS,help='Сохранить полный JSON в reports/NAME.json.')
    p.add_argument('--summary',action='store_true',default=argparse.SUPPRESS,help='Краткий итог вместо подробного вывода.')
    p.add_argument('--progress',action='store_true',default=argparse.SUPPRESS,help='Прогресс --wait в stderr; с --json — JSONL.')
    group=p.add_mutually_exclusive_group()
    group.add_argument('--enqueue',action='store_true',default=argparse.SUPPRESS)
    group.add_argument('--wait',action='store_true',default=argparse.SUPPRESS)
    return p

def parser():
    p=options(Parser(prog='qbctl',description='Проверяемое управление локальным qBittorrent. Без --apply изменения не выполняются.'))
    p.add_argument('--version',action='version',version=__version__)
    subs=p.add_subparsers(dest='command',required=True,parser_class=Parser)
    executor=options(subs.add_parser('executor',help='Явный запуск/остановка исполнителя заданий, без watch.'))
    ex=executor.add_subparsers(dest='verb',required=True,parser_class=Parser)
    for verb in ('start','stop','status','serve'):
        ep=options(ex.add_parser(verb))
        if verb=='serve': ep.add_argument('--token',default=None,help=argparse.SUPPRESS)
    job=options(subs.add_parser('job',help='Задания и их сохранённые этапы.'))
    js=job.add_subparsers(dest='verb',required=True,parser_class=Parser)
    options(js.add_parser('list'))
    for verb in ('status','pause','resume'):
        jp=options(js.add_parser(verb)); jp.add_argument('id')
    status=options(subs.add_parser('status')); status.add_argument('--torrents',action='store_true')
    diagnose=options(subs.add_parser('diagnose',help='Объяснить состояние задач без проверки payload.')); diagnose.add_argument('--hash')
    downloads=options(subs.add_parser('downloads',help='Единое количество записей и разрешённых загрузок.'))
    count=options(downloads.add_subparsers(dest='verb',required=True,parser_class=Parser).add_parser('set'))
    count.add_argument('count',type=int)
    count.add_argument('--max-completions',type=int,default=5)
    count.add_argument('--max-additions',type=int,default=1000)
    options(subs.add_parser('doctor'))
    options(subs.add_parser('migrate'))
    options(subs.add_parser('clean-duplicates'))
    options(subs.add_parser('trim'))
    confirm=options(subs.add_parser('confirm-recheck')); confirm.add_argument('--operation',required=True); confirm.add_argument('--report',required=True)
    isolate=options(subs.add_parser('isolate-shared')); isolate.add_argument('--hash',required=True); isolate.add_argument('--other',required=True)
    prefix=options(subs.add_parser('repair-prefix')); prefix.add_argument('--hash',required=True)
    resolution=options(subs.add_parser('resolve-isolation')); resolution.add_argument('--operation',required=True); resolution.add_argument('--decision',required=True,choices=['retry-copy','retry-rename','cancel-untouched'])
    dedupe=options(subs.add_parser('dedupe')); dedupe.add_argument('--hash',required=True); dedupe.add_argument('--keep',required=True)
    dedupe.add_argument('--purge-quarantine',help='Удалить одинаковые копии из карантина прежней dedupe-операции ID.')
    resolve=options(subs.add_parser('resolve-add')); resolve.add_argument('--operation',required=True); resolve.add_argument('--decision',choices=['retry','cancel'],required=True)
    recover=options(subs.add_parser('recover')); recover.add_argument('--hash'); recover.add_argument('--adopt',action='store_true')
    audit=options(subs.add_parser('audit')); audit.add_argument('--deep',action='store_true'); audit.add_argument('--hash')
    explain=options(subs.add_parser('explain')); explain.add_argument('--hash',required=True)
    history=options(subs.add_parser('history')); history.add_argument('--since',default=''); history.add_argument('--limit',type=int,default=100)
    plan=options(subs.add_parser('plan')); plan.add_argument('--hash'); plan.add_argument('--action',choices=['reconcile'],default='reconcile'); plan.add_argument('--max-completions',type=int,default=5); plan.add_argument('--max-additions',type=int,default=5)
    apply=options(subs.add_parser('apply')); apply.add_argument('--plan',required=True)
    run=options(subs.add_parser('run')); run.add_argument('--hash'); run.add_argument('--max-completions',type=int,default=5); run.add_argument('--max-additions',type=int,default=1000)
    watch=options(subs.add_parser('watch')); watch.add_argument('--interval',default='15s'); watch.add_argument('--max-completions',type=int,default=5); watch.add_argument('--max-additions',type=int,default=1000)
    for name in ('limits','policy'):
        parent=options(subs.add_parser(name)); child=options(parent.add_subparsers(dest='verb',required=True,parser_class=Parser).add_parser('set'))
        if name=='limits':
            for field in ('downloads','uploads','down-bps','up-bps'): child.add_argument('--'+field,type=int)
        else:
            child.add_argument('--target-client-count',type=int)
            child.add_argument('--resource-guard',choices=['on','off'])
            child.add_argument('--network-baseline-bps',type=int)
            child.add_argument('--resource-threshold',type=float)
            child.add_argument('--resume-below',type=float)
            child.add_argument('--unknown-metrics',choices=['hold','pause'])
    for name in ('pause','resume','recheck'):
        child=options(subs.add_parser(name)); group=child.add_mutually_exclusive_group(required=True)
        group.add_argument('--hash'); group.add_argument('--all',action='store_true')
    return p

def result_base(command):
    return {'schema_version':1,'cli_version':__version__,'operation_id':uuid.uuid4().hex,'command':command,'started_at':now(),'result':'ok','actions':[],'issues':[],'pending_operations':[],'next_safe_commands':[]}

def control_plan(ctl,args):
    c=ctl.c; rows=ctl.api.torrents(); changes={}; prefs={}; tasks=[]
    if args.command=='downloads':
        if not 0<=args.count<=100000: raise Fault('ARGUMENT_INVALID','Количество загрузок: 0..100000.','error')
        changes['policy']={'download_slots':args.count,'target_client_count':args.count}
        prefs.update(max_active_downloads=max(1,args.count),max_active_torrents=max(1,args.count),queueing_enabled=True,dont_count_slow_torrents=False)
    elif args.command=='limits':
        vals={}
        for arg,key in [('downloads','download_slots'),('uploads','upload_slots'),('down_bps','down_bps'),('up_bps','up_bps')]:
            v=getattr(args,arg,None)
            if v is not None:
                if v<0 or v>2**31-1: raise Fault('ARGUMENT_INVALID','Число должно быть от 0 до 2147483647.','error',field=arg)
                if arg=='uploads' and v==0: raise Fault('ARGUMENT_INVALID','uploads должен быть положительным; ноль не используется как запрет отдачи.','error')
                vals[key]=v
        if not vals: raise Fault('ARGUMENT_INVALID','Не заданы ограничения.','error')
        changes['policy']=vals
        if 'download_slots' in vals:
            prefs.update(max_active_downloads=max(1,vals['download_slots']),max_active_torrents=max(1,vals['download_slots']),queueing_enabled=True,dont_count_slow_torrents=False)
        if 'upload_slots' in vals: prefs['max_active_uploads']=max(1,vals['upload_slots'])
        if 'down_bps' in vals: prefs['dl_limit']=vals['down_bps']
        if 'up_bps' in vals: prefs['up_limit']=vals['up_bps']
        if any(k in vals for k in ('down_bps','up_bps')) and ctl.api.call('transfer/speedLimitsMode'):
            raise Fault('ALT_LIMITS_ACTIVE','Включены альтернативные лимиты; изменение обычных лимитов не даёт ожидаемого эффекта.')
    elif args.command=='policy':
        if args.target_client_count is not None:
            if not 0<=args.target_client_count<=100000: raise Fault('ARGUMENT_INVALID','Недопустимый размер очереди.','error')
            changes['policy']={'target_client_count':args.target_client_count}
        resources={}
        for field,key in [('network_baseline_bps','network_baseline_bps'),('resource_threshold','threshold'),('resume_below','resume_below'),('unknown_metrics','unknown_policy')]:
            v=getattr(args,field)
            if v is not None: resources[key]=v
        if args.resource_guard: resources['enabled']=args.resource_guard=='on'
        if resources:
            r={**c['resources'],**resources}
            if not (0<=r['resume_below']<r['threshold']<=100) or r['network_baseline_bps']<0: raise Fault('ARGUMENT_INVALID','Неверный порог/сетевая база.','error')
            changes['resources']=resources
        if not changes: raise Fault('ARGUMENT_INVALID','Не задано изменение политики.','error')
    else:
        selected=rows if args.all else [ctl.select(rows,args.hash)]
        if args.command=='recheck' and len(selected)!=1: raise Fault('ARGUMENT_INVALID','Recheck разрешён только по одной задаче.','error')
        if args.command=='resume':
            for t in selected: ctl.ensure_exclusive(t['hash'],claims(t['save_path'],ctl.api.files(t['hash'])),rows)
            slots=min(c['policy']['download_slots'],ctl.store.setting('effective_slots',c['policy']['download_slots'])); already=sum(not stopped(t) and not complete(t) for t in rows)
            to_resume=sum(stopped(t) and not complete(t) for t in selected)
            if already+to_resume>slots: raise Fault('SLOTS_EXCEEDED','Для возобновления увеличьте slots или остановите другие задачи.')
        tasks=[{'hash':t['hash'],'action':args.command,'reason':'user'} for t in selected]
    plan={'schema_version':1,'id':uuid.uuid4().hex,'created_at':now(),'policy_revision':c['revision'],'policy_digest':digest(c),'client_ids':sorted(t['hash'] for t in rows),'operations':[],'additions':[],'issues':[],
        'control':{'id':uuid.uuid4().hex,'hash':'policy' if changes or prefs else (tasks[0]['hash'] if len(tasks)==1 else 'all'),'kind':'control','stage':'prepared','policy_revision':c['revision'],'changes':changes,'preferences':prefs,'tasks':tasks}}
    folder=ROOT/'plans'; folder.mkdir(exist_ok=True)
    (folder/(plan['id']+'.json')).write_text(json.dumps(plan,ensure_ascii=False,indent=2),encoding='utf-8')
    return plan

@read_phase
def service_pass(ctl,args,result):
    ctl.service_mode=True
    if ctl.initial_completions is None:
        frozen=getattr(args,'_completion_scope',None)
        ctl.initial_completions=set(frozen) if frozen is not None else {t['hash'] for t in ctl.api.torrents() if complete(t)}
    ctl.api.compatible(); ctl.recover()
    # Observe/recover accepted operations before rebuilding admission plans.
    waits={'CHECK_PENDING','START_PENDING','STOP_PENDING','ADD_PENDING','REMOVE_PENDING','ADD_ADMISSION_WAIT','SERVICE_WAITING','SERVICE_DEFERRED'}
    if ctl.nonblocking and ctl.store.pending() and ctl.issues and all(i['code'] in waits for i in ctl.issues):
        result['continuation']={'mode':'recover_only','reason':'accepted_operation_wait'}
        return
    if ctl.global_pending(): raise Fault('RECOVERY_REQUIRED','Незавершённое глобальное управление требует продолжения.','partial')
    result['duplicate_cleanup']=ctl.clean_duplicates(True)
    p=ctl.api.prefs(); n=max(1,ctl.c['policy']['download_slots'])
    desired={'max_active_downloads':n,'max_active_torrents':n,'queueing_enabled':True,'dont_count_slow_torrents':False}
    if any(p.get(k)!=v for k,v in desired.items()):
        ctl.dispatch({'id':uuid.uuid4().hex,'kind':'control','hash':'policy','stage':'prepared','preferences':desired,'tasks':[]})
    ctl.enforce_slots(start=False)
    plan=ctl.make_plan(args.max_completions,args.max_additions,getattr(args,'hash',None),defer_manifests=True)
    result['plan']=plan; ctl.apply_plan(plan)


def dispatch(ctl,args,result):
    if args.command=='status':
        result.update(ctl.snapshot(include_torrents=args.torrents))
        return
    if args.command=='diagnose':
        result.update(ctl.snapshot())
        if args.hash:
            selected=[d for d in result['diagnostics'] if d['hash']==args.hash]
            if not selected: raise Fault('TORRENT_NOT_FOUND','Задача не найдена.',hash=args.hash)
            result['diagnostics']=selected
        return
    if args.command=='doctor':
        result.update(ctl.snapshot()); checks=[]
        for name,func in [('compatibility',ctl.api.compatible),('roots',ctl.roots),('new_download_paths',ctl.add_preconditions)]:
            try: func(); checks.append({'check':name,'ok':True})
            except Fault as e: checks.append({'check':name,'ok':False,**e.issue()}); ctl.issues.append(e.issue())
        result['checks']=checks; result['features']={'watch':True,'resource_guard':True,'piece_recheck_serial':True,'cross_volume_move':False,'process_resource_attribution':False,'physical_disk_attribution':False}
    elif args.command=='explain':
        t=ctl.select(ctl.api.torrents(),args.hash); result['torrent']=summary(t); result['files']=ctl.api.files(t['hash']); result['stop_reason']=ctl.store.stop_reason(t['hash'])
        trackers=ctl.api.call('torrents/trackers',query={'hash':t['hash']})
        from urllib.parse import urlsplit
        result['trackers']=[{'host':urlsplit(x.get('url','')).hostname or 'DHT/PeX/LSD','status':x.get('status'),'seeds':x.get('num_seeds'),'peers':x.get('num_peers'),'has_error':bool(x.get('msg'))} for x in trackers]
        result['explanation']='Ожидание данных/соединений не доказывает отсутствия раздающих.' if t['state']=='stalledDL' else 'Состояние клиента; подробности в torrent и files.'
    elif args.command=='audit':
        if args.deep and not args.hash: raise Fault('ARGUMENT_INVALID','Для --deep нужен --hash.','error')
        result['audit']=ctl.audit(args.hash if args.deep else None)
        ctl.issues.extend(result['audit']['findings'])
    elif args.command in ('plan','run'):
        if args.command=='run' and args.apply:
            service_pass(ctl,args,result); return
        plan=ctl.make_plan(args.max_completions,args.max_additions,getattr(args,'hash',None),defer_manifests=args.command=='run'); result['plan']=plan
        ctl.issues.extend(plan['issues'])
    elif args.command=='downloads':
        plan=control_plan(ctl,args); result['control_plan']=plan
        if args.apply:
            ctl.service_mode=True
            frozen=getattr(args,'_completion_scope',None)
            ctl.initial_completions=set(frozen) if frozen is not None else {t['hash'] for t in ctl.api.torrents() if complete(t)}
            if getattr(args,'_control_id',None): plan['control']['id']=args._control_id
            ctl.dispatch(plan['control'])
            service_pass(ctl,args,result)
        else:
            result['mode']='plan'; result['requested_downloads']=args.count
    elif args.command=='apply':
        if not args.plan.isalnum() or len(args.plan)!=32: raise Fault('ARGUMENT_INVALID','Неверный ID плана.','error')
        p=ROOT/'plans'/(args.plan+'.json')
        if not p.is_file(): raise Fault('PLAN_MISSING','План не найден.')
        plan=json.loads(p.read_text(encoding='utf-8')); result['plan_id']=args.plan
        result['duplicate_cleanup']=ctl.clean_duplicates(True)
        ctl.apply_plan(plan)
    elif args.command=='recover':
        if args.adopt:
            if not args.hash: raise Fault('ARGUMENT_INVALID','Для --adopt нужен --hash.','error')
            operation=ctl.adopt(args.hash); result['adoption_plan']=operation
            if args.apply: ctl.dispatch(operation); ctl.enforce_slots()
        elif args.hash: raise Fault('ARGUMENT_INVALID','--hash используется с --adopt.','error')
        elif not args.apply: result['recovery_plan']=ctl.store.pending()
        else: ctl.recover(); ctl.enforce_slots()
    elif args.command in ('limits','policy','pause','resume','recheck'):
        plan=control_plan(ctl,args); result['plan']=plan
        if args.apply: ctl.apply_plan(plan)
    elif args.command=='clean-duplicates': result['duplicate_cleanup']=ctl.clean_duplicates(args.apply)
    elif args.command=='trim':
        if args.apply:
            from .queue import trim_queue
            trim_queue(ctl)
        else: result['trim_plan']={'current':len(ctl.api.torrents()),'target':ctl.c['policy']['target_client_count'],'delete_files':False}
    elif args.command=='confirm-recheck': result['recheck_confirmation']=ctl.confirm_recheck(args.operation,args.report,args.apply)
    elif args.command=='isolate-shared': result['isolation']=ctl.isolate_shared(args.hash,args.other,args.apply)
    elif args.command=='repair-prefix':
        from .repair import prepare_prefix
        result['repair']=prepare_prefix(ctl,args.hash,args.apply)
    elif args.command=='resolve-isolation': result['resolution']=ctl.resolve_isolation(args.operation,args.decision,args.apply)
    elif args.command=='dedupe': result['dedupe_plan']=ctl.dedupe(args.hash,args.keep,args.apply,getattr(args,'purge_quarantine',None))
    elif args.command=='resolve-add': result['resolution']=ctl.resolve_add(args.operation,args.decision,args.apply)
    else: raise Fault('ARGUMENT_INVALID','Неизвестная команда.','error')

def execute(args):
    if args.command in ('job','executor') or getattr(args,'enqueue',False) or getattr(args,'wait',False):
        from .executor import command
        return command(args)
    mutating=args.command=='apply' or (args.apply and args.command in ('downloads','run','recover','limits','policy','pause','resume','recheck','dedupe','clean-duplicates','trim','confirm-recheck','isolate-shared','repair-prefix','resolve-isolation','resolve-add','migrate'))
    # Final result and idempotency receipt are committed while holding the writer lock.
    try:
        with (writer_lock() if mutating else contextlib.nullcontext()):
            with (writer_lock('watch.lock') if args.command=='migrate' and args.apply else contextlib.nullcontext()):
                result=execute_locked(args,mutating)
                result['report']=build_report(result)
                return result
    except Fault as e:
        result=result_base(args.command); result['result']=e.result; result['issues']=[e.issue()]
        result['finished_at']=now(); result['next_safe_commands']=['qbctl status --json']
        result['report']=build_report(result)
        return result


def execute_locked(args,mutating):
    execution_start=time.monotonic()
    overall=Budget(args.deadline); overall.end=execution_start+args.deadline
    result=result_base(args.command); store=None; ctl=None; owned_request=False
    signature=digest({k:v for k,v in vars(args).items() if k not in ('json','events_jsonl','request_id','output','summary','progress')})
    try:
        c=load_config(); result['policy_revision']=c['revision']
        store=Store(migrate=args.command=='migrate' and args.apply,readonly=args.command in ('status','diagnose','history','explain'))
        if args.request_id and mutating:
            old=store.db.execute('SELECT signature,result FROM requests WHERE id=?',(args.request_id,)).fetchone()
            if old:
                if old[0]!=signature: raise Fault('REQUEST_ID_CONFLICT','request-id уже использован для другой команды.')
                if old[1]:
                    replay=json.loads(old[1]); replay['response_replayed']=True; replay.setdefault('evidence','historical_result')
                    return replay
                operations=[json.loads(r[0]) for r in store.db.execute('SELECT body FROM operations')]
                related=[o for o in operations if o.get('request_id')==args.request_id]
                if related and all(o['stage']=='finished' for o in related):
                    replay=result_base(args.command); replay.update(result='ok',response_replayed=True,evidence='recovered_operation_receipts',finished_at=now())
                    replay['actions']=[{'action':o['kind'],'hash':o['hash'],'result':'already_verified','operation_id':o['id']} for o in related]
                    replay['issues']=[{'code':'REQUEST_RESULT_RECOVERED','message':'Подтверждены записанные операции; исходная полная сводка запроса недоступна.'}]
                    replay['result']='partial'; replay['next_safe_commands']=['qbctl status --json']
                    store.db.execute('UPDATE requests SET result=? WHERE id=? AND signature=?',(json.dumps(replay,ensure_ascii=False),args.request_id,signature)); store.db.commit()
                    return replay
                raise Fault('REQUEST_INCOMPLETE','Запрос прерван; сначала recover --apply. Исходный request-id не повторяет мутации.','unknown')
            store.db.execute('INSERT INTO requests VALUES(?,?,NULL)',(args.request_id,signature)); store.db.commit(); owned_request=True
        expected=getattr(args,'_policy_revision',None)
        if expected is not None and c['revision']!=expected:
            raise Fault('POLICY_SUPERSEDED','Политика изменена другим действием; старое задание остановлено.',expected=expected,actual=c['revision'])
        if args.command=='migrate':
            result['state_version']=store.db.execute('PRAGMA user_version').fetchone()[0]
            result['migration_applied']=bool(args.apply)
        elif args.command=='history':
            if not 1<=args.limit<=10000: raise Fault('ARGUMENT_INVALID','limit: 1..10000.','error')
            result['events']=[{'time':r[0],'operation_id':r[1],**json.loads(r[2])} for r in store.db.execute('SELECT time,operation_id,body FROM events WHERE time>=? ORDER BY id DESC LIMIT ?',(args.since,args.limit))]
        else:
            api=API(c['api']); api.deadline=overall.end
            reserve=min(5.0,args.deadline*0.2) if args.command in ('run','downloads') and args.apply else 0
            work=Budget(args.deadline); work.end=overall.end-reserve
            ctl=Controller(c,api,store,work); ctl.report_reserve=reserve
            ctl.nonblocking=bool(getattr(args,'_nonblocking',False))
            ctl.request_id=args.request_id if owned_request else None
            if args.command=='downloads' and ctl.nonblocking:
                ctl.recover()
            if mutating and args.command not in ('run','recover','resolve-add','confirm-recheck','resolve-isolation') and ctl.global_pending():
                raise Fault('RECOVERY_REQUIRED','Есть незавершённое управление; сначала recover --apply.','partial')
            dispatch(ctl,args,result)
            if args.command not in ('status','diagnose'):
                ctl.budget=overall
                try: result.update(ctl.snapshot())
                except Fault as e: ctl.issues.append(e.issue()); result['result']='unknown' if mutating else 'error'
            if ctl.issues and result['result']=='ok': result['result']='partial' if ctl.actions or store.pending() or any(i.get('retryable') for i in ctl.issues) else 'blocked'
    except Fault as e:
        result['result']=e.result; result['issues'].append(e.issue())
        if e.code in ('BUDGET_EXHAUSTED','INDEX_CHANGED') and args.command in ('audit','plan','run'):
            result['registry']={'complete':False,'completed_data_audited':False,'reason':e.code}
    except KeyboardInterrupt:
        result['result']='partial'; result['issues'].append({'code':'INTERRUPTED','message':'Прервано; журнал сохранён, используйте recover.'})
    except (OSError,ValueError,KeyError,TypeError,sqlite3.Error) as e:
        result['result']='error'; result['issues'].append({'code':'LOCAL_ERROR','message':'Локальная ошибка; операция не объявлена успешной.','exception_type':type(e).__name__,**sqlite_details(e),'locations':[{'file':Path(f.filename).name,'line':f.lineno,'function':f.name} for f in traceback.extract_tb(e.__traceback__)[-5:]]})
    finally:
        # A replay must remain the saved result, without mixing in a new snapshot.
        if store and not ('replay' in locals()):
            if ctl:
                if args.command not in ('status','diagnose') and 'observed' not in result:
                    ctl.budget=overall
                    try: result.update(ctl.snapshot())
                    except Fault as e:
                        ctl.issues.append({'code':'FINAL_SNAPSHOT_UNAVAILABLE','message':'Итоговый статус не получен в общем бюджете; свежие числа не подтверждены.','cause':e.code})
                        if result['result']=='ok': result['result']='partial'
                result['api_metrics']=getattr(ctl.api,'metrics',{})
                if ctl.service_mode:
                    pending_service=store.pending()
                    result['service']={'mode':'bounded_single_pass','report_reserve_seconds':ctl.report_reserve,'initial_completion_hashes':sorted(ctl.initial_completions or []),'waits':[{'operation_id':k,**v} for k,v in ctl.service_waits.items()],
                        'pending_count':len(pending_service),'checking':result.get('observed',{}).get('checking'),'next_invocation_needed':bool(pending_service) or result['result']!='ok' or result.get('observed',{}).get('completed',0)>0}
                result['actions']=ctl.actions
                result['phase_timings']=ctl.timings
                result['timing_note']='Фазы могут быть вложены; seconds не следует суммировать.'
                result['issues'].extend(i for i in ctl.issues if i not in result['issues'])
            try:
                result['issues'].extend(store.log_warnings)
                pending=store.pending()
                result['pending_operations']=[{'id':o['id'],'hash':o['hash'],'kind':o['kind'],'stage':o['stage']} for o in pending]
                result['operation_progress']=[]
                for o in pending:
                    row=store.db.execute('SELECT updated FROM operations WHERE id=?',(o['id'],)).fetchone()
                    progress={'id':o['id'],'hash':o['hash'],'kind':o['kind'],'stage':o['stage'],'last_file':o.get('moving_file'),'last_activity_at':row[0] if row else None}
                    if o['kind']=='complete':
                        done=sum(f.get('handoff')=='handed_off' for f in o['files'])
                        progress.update(files_total=len(o['files']),files_handed_off=done,files_remaining=len(o['files'])-done)
                    result['operation_progress'].append(progress)
                if result['pending_operations']: result['next_safe_commands']=['qbctl run --apply --json' if ctl and ctl.service_mode else 'qbctl recover --apply --json']
                elif result['result']=='partial': result['next_safe_commands']=['qbctl status --json','qbctl run --apply --json' if mutating else 'qbctl '+args.command+' --json']
                elif result['result']!='ok': result['next_safe_commands']=['qbctl status --json','qbctl doctor --json']
                elif result.get('observed',{}).get('completed',0)>0: result['next_safe_commands']=['qbctl run --apply --json']
                result['report']=build_report(result)
                result['finished_at']=now()
                if owned_request:
                    store.db.execute('UPDATE requests SET result=? WHERE id=? AND signature=?',(json.dumps(result,ensure_ascii=False),args.request_id,signature)); store.db.commit()
            except (sqlite3.Error,OSError):
                result['result']='error'; result['issues'].append({'code':'STATE_WRITE_FAILED','message':'Не удалось сохранить результат; восстановление по журналу операций.'})
                result['finished_at']=now()
        if store: store.close()
    result.setdefault('finished_at',now())
    result['elapsed_seconds']=round(time.monotonic()-execution_start,3)
    return result


def _watch(args):
    if not args.apply: return {**result_base('watch'),'mode':'plan','message':'Для запуска обслуживания используйте watch --apply.','finished_at':now()}
    try: interval=float(args.interval.removesuffix('s'))
    except ValueError: raise Fault('ARGUMENT_INVALID','interval задаётся секундами, например 15s.','error')
    if not 1<=interval<=3600: raise Fault('ARGUMENT_INVALID','interval: 1..3600 секунд.','error')
    metrics=Metrics(load_config()['paths']['working']); end_result=result_base('watch'); tick=0
    next_resource=next_run=0
    try:
        while True:
            tick+=1; c=load_config(); clock=time.monotonic()
            if clock>=next_resource:
                try:
                    with writer_lock():
                        store=Store()
                        try:
                            store.set_setting('watch',{'pid':os.getpid(),'heartbeat':now(),'resource_enabled':c['resources']['enabled']})
                            sample=metrics.sample(c['resources']); store.set_setting('last_metrics',sample)
                            if c['resources']['enabled']:
                                ctl=Controller(c,API(c['api']),store,Budget(min(args.deadline,max(1,c['resources']['sample_seconds'])))); ctl.api.compatible()
                                try: regulate(ctl,sample)
                                except Fault as e: ctl.issues.append(e.issue())
                                if ctl.issues: print(json.dumps({'event':'resources','issues':ctl.issues},ensure_ascii=False),file=sys.stderr,flush=True)
                        finally: store.close()
                except Fault as e:
                    print(json.dumps({'event':'resource_pass_deferred','issue':e.issue()},ensure_ascii=False),file=sys.stderr,flush=True)
                next_resource=clock+c['resources']['sample_seconds']
            if clock>=next_run:
                run_args=argparse.Namespace(**vars(args)); run_args.command='run'; run_args.request_id=None
                run_args.deadline=min(args.deadline,max(1,c['resources']['sample_seconds'])) if c['resources']['enabled'] else args.deadline
                result=execute(run_args); end_result=result
                stream=sys.stdout if args.events_jsonl else sys.stderr
                print(json.dumps({'event':'pass','tick':tick,'result':result},ensure_ascii=False),file=stream,flush=True)
                next_run=time.monotonic()+interval
            time.sleep(min(1,max(0.1,min(next_resource,next_run)-time.monotonic())))
    except KeyboardInterrupt:
        end_result={**result_base('watch'),'result':'ok','message':'Watch остановлен. Загрузки клиента не остановлены.','finished_at':now()}
    finally:
        metrics.close()
        try:
            with writer_lock():
                store=Store(); store.set_setting('watch',{'pid':None,'stopped_at':now()}); store.close()
        except Fault: pass
    return end_result

def watch(args):
    if not args.apply: return _watch(args)
    with writer_lock('watch.lock'):
        return _watch(args)

def emit(result,json_mode):
    result['report']=build_report(result)
    if json_mode: print(json.dumps(result,ensure_ascii=False))
    else:
        print(f"qbctl: {result['result']}  операция {result['operation_id']}")
        obs=result.get('observed',{})
        if obs: print(f"Записей: {obs['client_count']}; разрешено работать: {obs['allowed_downloads']}; передают: {obs['transferring_downloads']}; кандидатов завершения: {obs['completed']}; проверяются: {obs['checking']}")
        if result.get('report'):
            if result.get('response_replayed'): print('Исторический результат; действия не повторялись.')
            print(report_line(result['report']))
        for o in result.get('operation_progress',[]):
            detail=f"; файлов {o['files_handed_off']}/{o['files_total']}" if 'files_total' in o else ''
            print(f"  pending {o['kind']} {o['id']}: {o['stage']}{detail}")
        diagnostics=result.get('diagnostics',[])
        if result.get('command')=='diagnose':
            for d in diagnostics: print(f"  {d['hash']} [{d['code']}]: {d['message']}")
        elif diagnostics:
            from collections import Counter
            print('Состояния:', ', '.join(f'{code}={n}' for code,n in Counter(d['code'] for d in diagnostics).items()))
        for a in result['actions']: print(f"  {a['action']}: {a.get('hash','policy')} — {a['result']}")
        for i in result['issues']: print(f"  {i['code']}: {i.get('message','Отклонение')} {i.get('path','')}")
        for warning in result.get('warnings',[]):
            print(f"  предупреждение {warning['code']}: {warning.get('message','')} {warning.get('hash','')}")
            for path in warning.get('paths',[]): print('    файл:',path)
        if result.get('plan'): print('План:',result['plan']['id'],'; готовых:',len(result['plan']['operations']),'; добавить:',len(result['plan']['additions']))
        if result.get('control_plan'):
            requested=result['control_plan']['control']['changes']['policy']['download_slots']
            print(f'Общее количество: {requested}; download_slots и target_client_count изменяются вместе.')
            if result.get('mode')=='plan': print('Показан план; изменения требуют --apply.')
        for cmd in result.get('next_safe_commands',[]): print('Далее:',cmd)
        if 'events' in result: print(json.dumps(result['events'],ensure_ascii=False,indent=2))
        if result.get('checks'): print(json.dumps(result['checks'],ensure_ascii=False,indent=2))
        if result.get('torrent'): print(json.dumps({k:v for k,v in result.items() if k in ('torrent','files','trackers','stop_reason','explanation')},ensure_ascii=False,indent=2))
        if result.get('audit'): print(json.dumps(result['audit'],ensure_ascii=False,indent=2))
        if result.get('job'): print(json.dumps(result['job'],ensure_ascii=False,indent=2))
        if result.get('jobs') is not None: print(json.dumps(result['jobs'],ensure_ascii=False,indent=2))
        if result.get('executor') and result['command'] in ('executor','job'): print(json.dumps(result['executor'],ensure_ascii=False,indent=2))

def main():
    command_started=time.monotonic(); destination=None; summary_mode=False
    for stream in (sys.stdout,sys.stderr):
        if hasattr(stream,'reconfigure'): stream.reconfigure(encoding='utf-8',errors='replace')
    json_mode='--json' in sys.argv
    try:
        args=parser().parse_args()
        for k,v in {'json':False,'apply':False,'events_jsonl':False,'enqueue':False,'wait':False,'request_id':None,'deadline':30.0}.items():
            if not hasattr(args,k): setattr(args,k,v)
        json_mode=args.json
        summary_mode=getattr(args,'summary',False)
        if summary_mode and json_mode: raise Fault('ARGUMENT_INVALID','--summary и --json выбирают разные форматы stdout.', 'error')
        if getattr(args,'progress',False) and not args.wait: raise Fault('ARGUMENT_INVALID','--progress используется с --wait.', 'error')
        if getattr(args,'output',None):
            from .output import output_path
            destination=output_path(args.output)
        if destination and args.command=='watch': raise Fault('ARGUMENT_INVALID','Для watch используется --events-jsonl; --output только для конечных команд.', 'error')
        if (args.enqueue or args.wait) and (args.command not in ('run','downloads') or not args.apply):
            raise Fault('ARGUMENT_INVALID','--enqueue/--wait допустимы только для run или downloads set с --apply.','error')
        if not 0<args.deadline<=3600: raise Fault('ARGUMENT_INVALID','deadline: 0..3600 секунд.','error')
        for k in ('max_completions','max_additions'):
            if hasattr(args,k) and not 0<=getattr(args,k)<=1000: raise Fault('ARGUMENT_INVALID','Число действий: 0..1000.','error')
        result=watch(args) if args.command=='watch' else execute(args)
    except Fault as e:
        result=result_base('invalid'); result['result']=e.result; result['issues']=[e.issue()]; result['finished_at']=now()
    except Exception as e:
        result=result_base('failed'); result['result']='error'; result['issues']=[{'code':'LOCAL_ERROR','message':'Необработанная локальная ошибка; изменения не объявлены успешными.','exception_type':type(e).__name__,**sqlite_details(e),'locations':[{'file':Path(f.filename).name,'line':f.lineno,'function':f.name} for f in traceback.extract_tb(e.__traceback__)[-5:]]}]; result['finished_at']=now()
    result['report']=build_report(result)
    result['command_elapsed_seconds']=round(time.monotonic()-command_started,3)
    if destination:
        from .output import save_report
        result['output_file']=str(destination)
        try: save_report(result,destination)
        except (OSError,Fault) as error:
            result.pop('output_file',None)
            result['issues'].append({'code':'REPORT_WRITE_FAILED','message':'Не удалось сохранить отчёт; выполненные действия остаются в журнале.', 'exception_type':type(error).__name__})
            if result['result']=='ok': result['result']='partial'
            result['report']=build_report(result)
    if summary_mode and not json_mode:
        from .output import summary_line
        print(summary_line(result))
    else: emit(result,json_mode)
    if result['result']=='error':
        return 2 if any(i['code'] in ('ARGUMENT_INVALID','CONFIG_INVALID','AUTH_REQUIRED','AUTH_FAILED') for i in result['issues']) else 5
    return {'ok':0,'partial':3,'blocked':4,'unknown':6}.get(result['result'],5)
