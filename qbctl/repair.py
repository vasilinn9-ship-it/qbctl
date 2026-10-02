"""Explicit repair of an oversized single-file v1 torrent, retaining every byte."""
from __future__ import annotations
import hashlib, uuid
from pathlib import Path
from .common import Fault, fingerprint, samefile_identity, guarded, norm, sha_file, copy_prefix
from .ownership import claims

def single_info(data):
    pos=0; encoded_info=None
    def parse(depth=0):
        nonlocal pos,encoded_info
        if depth>64 or pos>=len(data): raise Fault('FORMAT_UNSUPPORTED','Неверный bencode.')
        token=data[pos:pos+1]
        if token==b'i':
            end=data.find(b'e',pos+1)
            if end<0: raise Fault('FORMAT_UNSUPPORTED','Неверное целое bencode.')
            raw=data[pos+1:end]; pos=end+1
            if not raw or len(raw)>20: raise Fault('FORMAT_UNSUPPORTED','Неверное целое bencode.')
            return int(raw)
        if token in (b'l',b'd'):
            pos+=1; result=[] if token==b'l' else {}; previous=None
            while pos<len(data) and data[pos:pos+1]!=b'e':
                if token==b'l': result.append(parse(depth+1)); continue
                key=parse(depth+1)
                if not isinstance(key,bytes) or (previous is not None and key<=previous): raise Fault('FORMAT_UNSUPPORTED','Неупорядоченный словарь bencode.')
                previous=key; start=pos; result[key]=parse(depth+1)
                if depth==0 and key==b'info': encoded_info=data[start:pos]
            if pos>=len(data): raise Fault('FORMAT_UNSUPPORTED','Незавершённый bencode.')
            pos+=1; return result
        end=data.find(b':',pos)
        if end<0 or end-pos>12: raise Fault('FORMAT_UNSUPPORTED','Неверная строка bencode.')
        raw=data[pos:end]
        if not raw.isdigit(): raise Fault('FORMAT_UNSUPPORTED','Неверная длина bencode.')
        length=int(raw); pos=end+1
        if pos+length>len(data): raise Fault('FORMAT_UNSUPPORTED','Незавершённая строка bencode.')
        result=data[pos:pos+length]; pos+=length; return result
    try: value=parse()
    except (ValueError,TypeError,OverflowError): raise Fault('FORMAT_UNSUPPORTED','Неверный bencode.')
    if pos!=len(data) or not isinstance(value,dict) or not encoded_info: raise Fault('FORMAT_UNSUPPORTED','Неверный metainfo.')
    info=value.get(b'info')
    if not isinstance(info,dict) or b'files' in info or b'meta version' in info: raise Fault('FORMAT_UNSUPPORTED','Исправление поддерживает только однофайловый v1 torrent.')
    size=info.get(b'length'); piece=info.get(b'piece length'); hashes=info.get(b'pieces')
    if not isinstance(size,int) or size<=0 or not isinstance(piece,int) or not 0<piece<=64*1024*1024 or not isinstance(hashes,bytes) or len(hashes)!=20*((size+piece-1)//piece):
        raise Fault('FORMAT_UNSUPPORTED','Неверная таблица частей v1.')
    return info,hashlib.sha1(encoded_info).hexdigest()

def verify_prefix(path,info,budget):
    remaining=info[b'length']; piece=info[b'piece length']; hashes=info[b'pieces']; checksum=hashlib.sha256(); ordinal=0
    with Path(path).open('rb') as f:
        while remaining:
            amount=min(piece,remaining); left=amount; h=hashlib.sha1()
            while left:
                budget.check(); data=f.read(min(4*1024*1024,left))
                if not data: raise Fault('FILE_MISMATCH','Файл короче описанного torrent.')
                h.update(data); checksum.update(data); left-=len(data)
            if h.digest()!=hashes[20*ordinal:20*(ordinal+1)]: raise Fault('PIECE_MISMATCH','Часть файла не соответствует torrent; исходник сохранён.',piece=ordinal)
            remaining-=amount; ordinal+=1
    return checksum.hexdigest()

def prepare_prefix(ctl,h,apply=False):
    from .engine import complete,stopped
    ctl.api.compatible(); ctl.budget.check(); rows=ctl.api.torrents(); t=ctl.select(rows,h); h=t['hash']; root=ctl.c['paths']['working']
    if not stopped(t) or not complete(t) or not ctl.valid_path(t) or norm(t['save_path'])!=norm(root): raise Fault('REPAIR_PRECONDITION','Нужна завершённая остановленная задача только в m.')
    files=ctl.api.files(h)
    if len(files)!=1 or files[0].get('priority',0)==0 or files[0].get('progress')!=1: raise Fault('REPAIR_PRECONDITION','Нужен один полностью выбранный файл.')
    ctl.ensure_exclusive(h,claims(root,files),rows)
    history=[ctl.store.decode_op(r[0]) for r in ctl.store.db.execute("SELECT body FROM operations WHERE kind='isolate_shared' AND stage='finished'")]
    if not any(o.get('other_hash')==h and o['old_relative']==files[0]['name'] and o['sizes'][1]==files[0]['size'] for o in history):
        raise Fault('REPAIR_PRECONDITION','Нет квитанции изоляции для меньшего файла.')
    source=guarded(root,files[0]['name']); identity=fingerprint(source)
    if identity['size']<=files[0]['size'] or source.stat().st_nlink!=1: raise Fault('REPAIR_PRECONDITION','Нужен обычный файл с лишним хвостом.')
    entries=[e for e in ctl.index() if e['role']=='incoming' and e['id']==h]
    if len(entries)!=1: raise Fault('SOURCE_AMBIGUOUS','Нужен уникальный torrent в корне t.')
    torrent=Path(entries[0]['path']); info,parsed_hash=single_info(torrent.read_bytes())
    if parsed_hash!=h or info[b'length']!=files[0]['size']: raise Fault('PLAN_STALE','Размер или info-hash не соответствует задаче.')
    checksum=verify_prefix(source,info,ctl.budget)
    if not samefile_identity(source,identity): raise Fault('FILE_MISMATCH','Источник изменён при проверке.')
    import shutil
    if shutil.disk_usage(source.parent).free<files[0]['size']+1024**3: raise Fault('DISK_RESERVATION','Недостаточно места для префикса и резерва.')
    oid=uuid.uuid4().hex; backup=f'_recovery/{oid}/original.bin'; temp=f'_recovery/{oid}/prefix.part'
    op={'id':oid,'kind':'repair_prefix','hash':h,'stage':'prepared','relative':files[0]['name'],'backup_relative':backup,'temp_relative':temp,'identity':identity,'size':files[0]['size'],'prefix_sha256':checksum,'torrent':str(torrent),'torrent_sha256':sha_file(torrent),'payload_paths':[str(source),str(guarded(root,backup)),str(guarded(root,temp).parent)]}
    if apply: ctl.dispatch(op)
    return op

def recover_prefix(ctl,o):
    from .engine import complete,stopped
    ctl.api.compatible(); ctl.budget.check(); root=ctl.c['paths']['working']; h=o['hash']; t=ctl.api.get(h)
    if not t or not stopped(t) or not complete(t) or not ctl.valid_path(t) or norm(t['save_path'])!=norm(root): raise Fault('REPAIR_PRECONDITION','Задача должна оставаться завершённой и остановленной в m.')
    files=ctl.api.files(h)
    if len(files)!=1 or files[0]['name']!=o['relative'] or files[0]['size']!=o['size'] or files[0].get('progress')!=1 or not files[0].get('priority'): raise Fault('PLAN_STALE','Файловая ведомость изменена.')
    ctl.ensure_exclusive(h,claims(root,files),ignore=o['id'])
    source=guarded(root,o['relative']); backup=guarded(root,o['backup_relative']); temp=guarded(root,o['temp_relative'])
    folder=Path('_recovery')/o['id']
    if Path(o['backup_relative'])!=folder/'original.bin' or Path(o['temp_relative']).parent!=folder: raise Fault('PATH_OUTSIDE_ROOT','Неверные пути восстановления.')
    torrent=guarded(ctl.c['paths']['incoming'],Path(o['torrent']).name)
    if norm(torrent)!=norm(o['torrent']) or sha_file(torrent)!=o['torrent_sha256']: raise Fault('PLAN_STALE','Исходный torrent изменился.')
    info,parsed_hash=single_info(torrent.read_bytes())
    if parsed_hash!=h or info[b'length']!=o['size']: raise Fault('PLAN_STALE','Неверный metainfo восстановления.')
    if o['stage']=='prepared':
        if backup.exists() or temp.exists() or not samefile_identity(source,o['identity']): raise Fault('PLAN_STALE','Источник или назначение изменены.')
        temp.parent.mkdir(parents=True,exist_ok=True); ctl.stage(o,'copy_requested')
    if o['stage']=='copy_requested':
        if not samefile_identity(source,o['identity']): raise Fault('FILE_MISMATCH','Источник изменён перед копированием.')
        if temp.exists() and temp.stat().st_size!=o['size']:
            o.setdefault('retained_partial_copies',[]).append({'path':str(temp),'identity':fingerprint(temp)})
            o['temp_relative']=(folder/(uuid.uuid4().hex+'.part')).as_posix(); ctl.store.put_op(o); temp=guarded(root,o['temp_relative'])
        if not temp.exists(): copy_prefix(source,temp,o['size'],ctl.budget)
        if verify_prefix(temp,info,ctl.budget)!=o['prefix_sha256'] or not samefile_identity(source,o['identity']): raise Fault('FILE_MISMATCH','Копия или источник изменены.')
        o['copy_identity']=fingerprint(temp); ctl.stage(o,'backup_requested')
    if o['stage']=='backup_requested':
        if source.exists():
            if backup.exists() or not samefile_identity(source,o['identity']): raise Fault('RECOVERY_AMBIGUOUS','Источник или сохранённая копия неоднозначны.')
            move_source(source,backup)
        if not backup.is_file() or not samefile_identity(backup,o['identity']) or source.exists(): raise Fault('RECOVERY_AMBIGUOUS','Сохранение исходника не подтверждено.')
        ctl.stage(o,'install_requested')
    if o['stage']=='install_requested':
        if not samefile_identity(backup,o['identity']): raise Fault('FILE_MISMATCH','Сохранённый исходник изменён.')
        if temp.exists():
            if source.exists() or not samefile_identity(temp,o['copy_identity']): raise Fault('RECOVERY_AMBIGUOUS','Назначение занято или копия изменена.')
            move_source(temp,source)
        if not source.is_file() or not samefile_identity(source,o['copy_identity']) or temp.exists(): raise Fault('RECOVERY_AMBIGUOUS','Установка префикса не подтверждена.')
        ctl.stage(o,'installed')
    if verify_prefix(source,info,ctl.budget)!=o['prefix_sha256'] or not samefile_identity(source,o['copy_identity']) or not samefile_identity(backup,o['identity']): raise Fault('FILE_MISMATCH','Итог исправления не подтверждён.')
    ctl.stage(o,'finished')
    ctl.actions.append({'action':'repair_prefix','hash':h,'result':'verified','postconditions':{'size':o['size'],'pieces_verified':True,'original_preserved':str(backup),'original_bytes':o['identity']['size'],'completed_data_read':False,'task_stopped':True}})

def move_source(src,dst):
    from . import engine
    engine.move_no_replace(src,dst)
