"""Shrink only client records; keep incomplete payload and metainfo in place."""
import uuid
from pathlib import Path
from .common import Fault,guarded,norm,sha_file
from .registry import aliases
from .ownership import claims

def recover_release(ctl,o):
    from .engine import complete,stopped
    ctl.api.compatible(); ctl.budget.check(); h=o['hash']; src=guarded(ctl.c['paths']['incoming'],Path(o['source_torrent']).name)
    if norm(src)!=norm(o['source_torrent']) or sha_file(src)!=o['torrent_sha256'] or h not in aliases(ctl.metadata(src)):
        raise Fault('PLAN_STALE','Исходный torrent изменился; запись не удаляется.')
    t=ctl.api.get(h)
    if t:
        if not ctl.valid_path(t) or norm(t['save_path'])!=norm(ctl.c['paths']['working']): raise Fault('CLIENT_PATH_POLICY','Убирать из очереди можно только задачу с данными в m.')
        if complete(t):
            o['resolution']='became_complete'; ctl.stage(o,'finished'); return
        if o['stage']=='prepared':
            ctl.stage(o,'stop_requested')
        if o['stage']=='stop_requested':
            if not stopped(t): ctl.request_stop(o,h)
            t=ctl.wait_state(h,stopped)
            if not t or not stopped(t): raise Fault('STOP_PENDING','Остановка перед уменьшением очереди ещё не подтверждена.','partial',hash=h)
            if complete(t): o['resolution']='became_complete'; ctl.stage(o,'finished'); return
            ctl.stage(o,'stopped')
        if not stopped(t): raise Fault('PLAN_STALE','Задача была запущена вручную.')
        ctl.request_remove(o,h)
    elif o['stage']!='remove_requested': raise Fault('RECOVERY_AMBIGUOUS','Запись исчезла до намерения её удаления.')
    if ctl.api.get(h): raise Fault('REMOVE_PENDING','Клиент принял удаление записи; ожидается отсутствие в снимке.','partial',hash=h,request_accepted=bool(o.get('remove_accepted')))
    if not src.is_file() or sha_file(src)!=o['torrent_sha256']: raise Fault('PLAN_STALE','Исходный torrent не подтверждён после удаления записи.')
    ctl.store.set_stop(h,None); ctl.stage(o,'finished')
    ctl.actions.append({'action':'release','hash':h,'result':'verified','postconditions':{'client_removed':True,'delete_files':False,'source_torrent':str(src),'source_torrent_preserved':True,'data_path':ctl.c['paths']['working'],'payload_not_moved':True}})

def trim_queue(ctl):
    from .engine import complete,stopped
    ctl.budget.check()
    if ctl.global_pending(): raise Fault('RECOVERY_REQUIRED','Сначала восстановить глобальное управление.','partial')
    rows=ctl.api.torrents(); target=ctl.c['policy']['target_client_count']
    if len(rows)<=target: return
    entries=ctl.index(); blocked=ctl.pending_hashes()
    candidates=sorted((t for t in rows if not complete(t) and t['hash'] not in blocked and ctl.valid_path(t) and norm(t['save_path'])==norm(ctl.c['paths']['working'])),key=lambda t:(not stopped(t),t.get('progress',0),-t.get('added_on',0),t['hash']))
    for t in candidates:
        if len(ctl.api.torrents())<=target: break
        ctl.budget.check(); matching=[e for e in entries if e['role']=='incoming' and aliases(e)&aliases(t)]
        if len(matching)!=1: ctl.issues.append({'code':'QUEUE_TRIM_SOURCE_AMBIGUOUS','hash':t['hash'],'message':'Лишняя задача сохранена: нет уникального torrent в t.'}); continue
        e=matching[0]; owned=claims(t['save_path'],ctl.api.files(t['hash']))
        ctl.guard_pending(t['hash'],[x['file'] for x in owned])
        ctl.dispatch({'id':uuid.uuid4().hex,'kind':'release','hash':t['hash'],'stage':'prepared','source_torrent':e['path'],'torrent_sha256':e['sha256'],'payload_paths':[x['file'] for x in owned]})
    actual=len(ctl.api.torrents())
    if actual>target: raise Fault('QUEUE_ABOVE_TARGET','Количество записей пока выше цели; готовые/зависимые задачи сохранены.','partial',actual=actual,target=target)
