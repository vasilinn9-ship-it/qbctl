"""Read torrent fingerprints in bulk on Windows, with a strict stat fallback."""
from pathlib import Path
import os, stat, struct, ctypes
from .common import Fault

def decode_buffer(data,root,device):
    result=[]; offset=0
    while True:
        if offset+88>len(data): raise Fault('DIRECTORY_RESPONSE_INVALID','Неверная запись каталога Windows.')
        nxt=struct.unpack_from('<I',data,offset)[0]
        written=struct.unpack_from('<q',data,offset+24)[0]; size=struct.unpack_from('<q',data,offset+40)[0]
        attrs,length=struct.unpack_from('<II',data,offset+56)
        inode=int.from_bytes(data[offset+72:offset+88],'little')
        if length%2 or offset+88+length>len(data) or (nxt and (nxt<88+length or offset+nxt>=len(data))):
            raise Fault('DIRECTORY_RESPONSE_INVALID','Неверная длина записи каталога Windows.')
        try: name=data[offset+88:offset+88+length].decode('utf-16-le')
        except UnicodeError: raise Fault('DIRECTORY_RESPONSE_INVALID','Неверное имя файла Windows.')
        if name not in ('.','..') and name.lower().endswith('.torrent'):
            if not name or any(x in name for x in ('/','\\','\0',':')): raise Fault('PATH_OUTSIDE_ROOT','Неверное имя в ответе каталога.')
            if attrs&(0x10|0x400|0x40): raise Fault('PATH_OUTSIDE_ROOT','Torrent должен быть обычным файлом без reparse point.',path=str(Path(root)/name))
            if size<0 or not inode: raise Fault('DIRECTORY_RESPONSE_UNSUPPORTED','Каталог не предоставляет файловые идентификаторы.')
            result.append((Path(root)/name,{'size':size,'mtime_ns':(written-116444736000000000)*100,'dev':device,'ino':inode}))
        if not nxt: return result
        offset+=nxt

def native_scan(root,budget):
    if os.name!='nt': return None
    k=ctypes.WinDLL('kernel32',use_last_error=True)
    k.CreateFileW.argtypes=[ctypes.c_wchar_p,ctypes.c_uint32,ctypes.c_uint32,ctypes.c_void_p,ctypes.c_uint32,ctypes.c_uint32,ctypes.c_void_p]; k.CreateFileW.restype=ctypes.c_void_p
    k.GetFileInformationByHandleEx.argtypes=[ctypes.c_void_p,ctypes.c_int,ctypes.c_void_p,ctypes.c_uint32]; k.GetFileInformationByHandleEx.restype=ctypes.c_int
    k.CloseHandle.argtypes=[ctypes.c_void_p]; k.CloseHandle.restype=ctypes.c_int
    # LIST_DIRECTORY, SHARE_READ|WRITE|DELETE, OPEN_EXISTING,
    # BACKUP_SEMANTICS|OPEN_REPARSE_POINT. No writes or payload access.
    handle=k.CreateFileW(str(root),1,7,None,3,0x02200000,None)
    if handle==ctypes.c_void_p(-1).value:
        error=ctypes.get_last_error()
        if error in (1,50,87,120): return None
        raise Fault('PATH_UNAVAILABLE','Windows не открыла рабочий каталог.',winerror=error,path=str(root))
    result=[]; first=True
    try:
        device=Path(root).stat().st_dev
        while True:
            budget.check(); buffer=ctypes.create_string_buffer(64*1024)
            if not k.GetFileInformationByHandleEx(handle,20 if first else 19,buffer,len(buffer)):
                error=ctypes.get_last_error()
                if error==18: return result
                if error in (1,50,87,120): return None
                raise Fault('PATH_UNAVAILABLE','Windows не прочитала рабочий каталог.',winerror=error,path=str(root))
            first=False
            try: result.extend(decode_buffer(buffer.raw,root,device))
            except Fault as e:
                if e.code=='DIRECTORY_RESPONSE_UNSUPPORTED': return None
                raise
    finally: k.CloseHandle(handle)

def scan_torrents(root,budget):
    result=native_scan(root,budget)
    if result is None:
        result=[]
        for p in Path(root).glob('*.torrent'):
            budget.check(); s=p.lstat()
            if not stat.S_ISREG(s.st_mode) or getattr(s,'st_file_attributes',0)&0x400:
                raise Fault('PATH_OUTSIDE_ROOT','Torrent должен быть обычным файлом без reparse point.',path=str(p))
            result.append((p,{'size':s.st_size,'mtime_ns':s.st_mtime_ns,'dev':s.st_dev,'ino':s.st_ino}))
    return sorted(result,key=lambda x:(x[0].name.casefold(),x[0].name))
