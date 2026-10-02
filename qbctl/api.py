from __future__ import annotations
import contextlib, copy, http.cookiejar, json, os, urllib.error, urllib.parse, urllib.request, uuid, time, threading
from pathlib import Path
from .common import Fault

class API:
    def __init__(self,config,deadline=None,metrics=None,metrics_lock=None):
        self._file_cache = None
        self._file_cache_lock = threading.Lock()
        self.config=config
        self.metrics={} if metrics is None else metrics
        self._metrics_lock=threading.Lock() if metrics_lock is None else metrics_lock; self.deadline=deadline
        self.base=config['url'].rstrip('/')
        u=urllib.parse.urlsplit(self.base)
        if u.scheme!='http' or u.hostname!='127.0.0.1' or u.username or u.password or u.path or u.query:
            raise Fault('CONFIG_INVALID','Разрешён только локальный HTTP API 127.0.0.1.','error')
        self.timeout=config['timeout']
        if not 0<self.timeout<=30: raise Fault('CONFIG_INVALID','API timeout должен быть 0..30 секунд.','error')
        self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()),NoRedirect())
        try: self.version=self.call('app/version',text=True)
        except Fault as e:
            if e.details.get('http_status')!=403: raise
            password=os.environ.get(config['password_env'])
            if password is None: raise Fault('AUTH_REQUIRED','API требует вход. Передайте секрет через защищённое окружение QBCTL_PASSWORD.','error')
            result=self.call('auth/login',{'username':os.environ.get(config['username_env'],'admin'),'password':password},text=True)
            if result.strip()!='Ok.': raise Fault('AUTH_FAILED','Локальный API не подтвердил вход.','error')
            self.version=self.call('app/version',text=True)
        self.api_version=self.call('app/webapiVersion',text=True)
        try:
            self.api_tuple=tuple(int(x) for x in self.api_version.split('.'))
        except ValueError: raise Fault('API_INCOMPATIBLE','Неизвестная версия API.')
    def compatible(self):
        if not self.version.startswith('v5.') or not (2,13)<=self.api_tuple<(2,17):
            raise Fault('API_INCOMPATIBLE','Поддержаны qBittorrent 5 и API 2.13–2.16.',version=self.version,api_version=self.api_version)
    def call(self,endpoint,fields=None,text=False,raw=None,ctype=None,query=None):
        mutation=(fields is not None or raw is not None) and endpoint not in ('auth/login','torrents/parseMetadata')
        if mutation: self.invalidate_files()
        started=time.monotonic()
        try: return self._call(endpoint,fields,text,raw,ctype,query)
        finally:
            if mutation: self.invalidate_files()
            with self._metrics_lock:
                item=self.metrics.setdefault(endpoint,{'calls':0,'seconds':0})
                item['calls']+=1; item['seconds']=round(item['seconds']+time.monotonic()-started,6)
    def _call(self,endpoint,fields=None,text=False,raw=None,ctype=None,query=None):
        url=self.base+'/api/v2/'+endpoint
        if query: url+='?'+urllib.parse.urlencode(query)
        data=raw if raw is not None else (urllib.parse.urlencode(fields).encode() if fields is not None else None)
        headers={'Referer':self.base,'Origin':self.base}
        if data is not None: headers['Content-Type']=ctype or 'application/x-www-form-urlencoded'
        req=urllib.request.Request(url,data=data,headers=headers,method='POST' if data is not None else 'GET')
        timeout=self.timeout
        if self.deadline is not None:
            remaining=self.deadline-time.monotonic()
            if remaining<=0: raise Fault('BUDGET_EXHAUSTED','Общий бюджет запроса исчерпан.','partial',request_sent=False)
            timeout=min(timeout,max(0.05,remaining))
        try:
            with self.opener.open(req,timeout=timeout) as r:
                body=r.read(32*1024*1024).decode('utf-8')
                if text or not body.strip(): return body
                try: return json.loads(body)
                except ValueError: raise Fault('API_RESPONSE_INVALID','API вернул неверный JSON.',endpoint=endpoint)
        except urllib.error.HTTPError as e:
            status=e.code; e.close()
            uncertain=data is not None and endpoint not in ('auth/login','torrents/parseMetadata') and status>=500
            raise Fault('API_HTTP_ERROR','API не подтвердил запрос.','unknown' if uncertain else ('error' if status>=500 else 'blocked'),http_status=status,endpoint=endpoint)
        except (OSError,urllib.error.URLError,TimeoutError):
            # No raw exception or request body: these can contain credentials.
            raise Fault('MUTATION_UNCERTAIN' if data is not None and endpoint!='torrents/parseMetadata' else 'API_UNAVAILABLE','Не удалось получить подтверждение API.','unknown' if data is not None and endpoint!='torrents/parseMetadata' else 'error',endpoint=endpoint)
    def multipart(self,endpoint,files,fields=None):
        boundary='qbctl'+uuid.uuid4().hex
        chunks=[]
        for k,v in (fields or {}).items():
            chunks.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'.encode())
        for i,data in enumerate(files):
            chunks.append(f'--{boundary}\r\nContent-Disposition: form-data; name="torrents{i}"; filename="input{i}.torrent"\r\nContent-Type: application/x-bittorrent\r\n\r\n'.encode()+data+b'\r\n')
        chunks.append(f'--{boundary}--\r\n'.encode())
        return self.call(endpoint,raw=b''.join(chunks),ctype='multipart/form-data; boundary='+boundary,text=endpoint=='torrents/add')
    def torrents(self): return self.call('torrents/info')
    def get(self,h):
        rows=self.call('torrents/info',query={'hashes':h})
        if len(rows)>1: raise Fault('API_RESPONSE_INVALID','Неоднозначный ID клиента.',hash=h)
        return rows[0] if rows else None
    @contextlib.contextmanager
    def file_phase(self):
        outer=self._file_cache is not None
        if not outer: self._file_cache={}
        try: yield
        finally:
            if not outer: self._file_cache=None

    def invalidate_files(self):
        with self._file_cache_lock:
            if self._file_cache is not None: self._file_cache.clear()

    def files(self,h):
        with self._file_cache_lock:
            if self._file_cache is not None and h in self._file_cache:
                return copy.deepcopy(self._file_cache[h])
        result=self.call('torrents/files',query={'hash':h})
        with self._file_cache_lock:
            if self._file_cache is not None: self._file_cache[h]=copy.deepcopy(result)
        return result
    def prefs(self): return self.call('app/preferences')
    def stop(self,h): self.call('torrents/stop',{'hashes':h},text=True)
    def start(self,h): self.call('torrents/start',{'hashes':h},text=True)
    def remove(self,h): self.call('torrents/delete',{'hashes':h,'deleteFiles':'false'},text=True)
    def metadata(self,data):
        result=self.multipart('torrents/parseMetadata',[data])
        if not isinstance(result,list) or len(result)!=1: raise Fault('API_RESPONSE_INVALID','Неожиданный ответ parseMetadata.')
        x=result[0]
        h=x.get('torrent_id') or x.get('id') or x.get('hash')
        v1=x.get('infohash_v1') or x.get('info_hash_v1') or x.get('infohashV1')
        v2=x.get('infohash_v2') or x.get('info_hash_v2') or x.get('infohashV2')
        from .registry import aliases
        aliases({'id':h,'v1':v1,'v2':v2})
        if not h: raise Fault('API_INCOMPATIBLE','parseMetadata не вернул поддерживаемый ID.',fields=list(x))
        if not isinstance(h,str) or len(h) not in (40,64) or any(c not in '0123456789abcdefABCDEF' for c in h):
            raise Fault('FORMAT_UNSUPPORTED','Неподдерживаемый ID metainfo.')
        known={v.lower() for v in (v1,v2) if v}
        if v2: known.add(v2.lower()[:40])
        if known and h.lower() not in known: raise Fault('FORMAT_UNSUPPORTED','ID metainfo не соответствует v1/v2 хешам.')
        # Do not persist tracker URLs/passkeys from metainfo.
        info=x.get('info',{})
        return {'id':h.lower(),'v1':v1.lower() if v1 else None,'v2':v2.lower() if v2 else None,'files':info.get('files',[])}
    def add(self,data,working):
        self.compatible()
        fields={'savepath':str(working),'autoTMM':'false','stopped':'true','seedMode':'false','category':'','downloadPath':str(working),'contentLayout':'Original'}
        if self.api_tuple<(2,16): fields['skip_checking']='false'
        self.multipart('torrents/add',[data],fields)

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs): return None
