from __future__ import annotations
import ctypes as C, re, time
from ctypes import wintypes as W
from .common import Fault, now

class Memory(C.Structure):
    _fields_=[('length',W.DWORD),('load',W.DWORD),('total_phys',C.c_ulonglong),('avail_phys',C.c_ulonglong),('total_page',C.c_ulonglong),('avail_page',C.c_ulonglong),('total_virtual',C.c_ulonglong),('avail_virtual',C.c_ulonglong),('avail_extended',C.c_ulonglong)]
class Fmt(C.Structure):
    _fields_=[('status',W.DWORD),('value',C.c_double)]
class Item(C.Structure):
    _fields_=[('name',C.c_wchar_p),('fmt',Fmt)]

class Metrics:
    def __init__(self,working):
        self.last_cpu=None; self.query=None; self.counters={}; self.last_collect=None
        try:
            self.pdh=C.WinDLL('pdh')
            self.pdh.PdhOpenQueryW.argtypes=[C.c_wchar_p,C.c_size_t,C.POINTER(C.c_void_p)]
            self.pdh.PdhAddEnglishCounterW.argtypes=[C.c_void_p,C.c_wchar_p,C.c_size_t,C.POINTER(C.c_void_p)]
            self.pdh.PdhCollectQueryData.argtypes=[C.c_void_p]
            self.pdh.PdhGetFormattedCounterValue.argtypes=[C.c_void_p,W.DWORD,C.POINTER(W.DWORD),C.POINTER(Fmt)]
            self.pdh.PdhGetFormattedCounterArrayW.argtypes=[C.c_void_p,W.DWORD,C.POINTER(W.DWORD),C.POINTER(W.DWORD),C.c_void_p]
            self.pdh.PdhCloseQuery.argtypes=[C.c_void_p]
            q=C.c_void_p()
            if self.pdh.PdhOpenQueryW(None,0,C.byref(q))==0:
                self.query=q
                drive=str(working)[:2]
                for name,path in [('disk',rf'\LogicalDisk({drive})\% Disk Time'),('network',r'\Network Interface(*)\Bytes Total/sec'),('gpu',r'\GPU Engine(*)\Utilization Percentage')]:
                    counter=C.c_void_p()
                    if self.pdh.PdhAddEnglishCounterW(q,path,0,C.byref(counter))==0: self.counters[name]=counter
        except (OSError,AttributeError): pass
    def cpu(self):
        k=C.WinDLL('kernel32',use_last_error=True)
        values=[C.c_ulonglong() for _ in range(3)]
        if not k.GetSystemTimes(*(C.byref(x) for x in values)): return None
        current=tuple(x.value for x in values); last=self.last_cpu; self.last_cpu=current
        if last is None: return None
        idle,kernel,user=(current[i]-last[i] for i in range(3)); total=kernel+user
        return max(0,min(100,100*(total-idle)/total)) if total>0 else None
    def array(self,counter):
        size=W.DWORD(); count=W.DWORD()
        self.pdh.PdhGetFormattedCounterArrayW(counter,0x200,C.byref(size),C.byref(count),None)
        if not size.value or size.value>16*1024*1024: return []
        buf=C.create_string_buffer(size.value)
        if self.pdh.PdhGetFormattedCounterArrayW(counter,0x200,C.byref(size),C.byref(count),buf)!=0: return []
        items=C.cast(buf,C.POINTER(Item))
        return [(items[i].name,items[i].fmt.value) for i in range(count.value) if items[i].fmt.status in (0,1)]
    def sample(self,config):
        cpu=self.cpu(); m=Memory(); m.length=C.sizeof(m)
        memory=float(m.load) if C.WinDLL('kernel32').GlobalMemoryStatusEx(C.byref(m)) else None
        disk=network=gpu=None
        if self.query and self.pdh.PdhCollectQueryData(self.query)==0:
            if self.last_collect is not None:
                if 'disk' in self.counters:
                    value=Fmt()
                    if self.pdh.PdhGetFormattedCounterValue(self.counters['disk'],0x200,None,C.byref(value))==0 and value.status in (0,1): disk=max(0,min(100,value.value))
                if 'network' in self.counters:
                    items=self.array(self.counters['network'])
                    if items: network=sum(max(0,v) for _,v in items)
                if 'gpu' in self.counters:
                    items=self.array(self.counters['gpu']); grouped={}
                    for name,value in items:
                        # Aggregate processes for the same physical GPU engine.
                        engine=re.sub(r'^pid_\d+_','',name)
                        grouped[engine]=grouped.get(engine,0)+max(0,value)
                    if grouped: gpu=min(100,max(grouped.values()))
            self.last_collect=time.monotonic()
        baseline=config['network_baseline_bps']
        return {'sample_at':now(),'scope':'system','cpu':cpu,'memory':memory,'disk':disk,'gpu':gpu,
            'network':min(100,network*100/baseline) if network is not None and baseline>0 else None,'network_bps':network,
            'sources':{'cpu':'Windows GetSystemTimes, since previous sample','memory':'GlobalMemoryStatusEx physical memory load','disk':'PDH LogicalDisk % Disk Time, clipped 0..100; not physical-device attribution','gpu':'PDH busiest GPU engine across processes','network':'PDH sum interfaces; virtual interfaces may count traffic twice'},
            'process_metrics':{'cpu':None,'memory':None,'reason':'Process attribution is not implemented'},'continuous':True}
    def close(self):
        if self.query: self.pdh.PdhCloseQuery(self.query); self.query=None

def regulate(controller,metrics):
    c=controller.c['resources']; store=controller.store; desired=controller.c['policy']['download_slots']
    previous=store.setting('effective_slots',desired); effective=min(previous,desired)
    state=store.setting('resource_state',{}); clock=time.time()
    last=state.get('last_sample')
    if last is None or clock<last or clock-last>max(15,c['sample_seconds']*3):
        state['high_since']=None; state['low_since']=None
    state['last_sample']=clock
    unknown=[k for k in c['required_metrics'] if metrics.get(k) is None]
    measured=[v for k,v in metrics.items() if k in ('cpu','memory','disk','gpu','network') and isinstance(v,(int,float))]
    high=any(x>c['threshold'] for x in measured)
    low=bool(measured) and all(x<c['resume_below'] for x in measured) and not unknown
    if unknown and c['unknown_policy']=='pause': effective=0
    elif high:
        state['low_since']=None
        state['high_since']=state.get('high_since') or clock
        if clock-state['high_since']>=c['high_seconds']:
            effective=max(0,effective-c['step']); state['high_since']=clock
    elif low:
        state['high_since']=None
        state['low_since']=state.get('low_since') or clock
        if clock-state['low_since']>=c['low_seconds']:
            effective=min(desired,effective+c['step']); state['low_since']=clock
    else: state['high_since']=None; state['low_since']=None
    store.set_setting('resource_state',state); store.set_setting('effective_slots',effective)
    store.set_setting('last_metrics',metrics)
    if unknown: controller.issues.append({'code':'METRICS_UNAVAILABLE','message':'Часть обязательных метрик недоступна.','metrics':unknown,'policy':c['unknown_policy']})
    if effective<desired: controller.issues.append({'code':'RESOURCE_LIMIT','message':'Ресурсная защита уменьшила эффективную одновременность.','desired':desired,'effective':effective})
    controller.enforce_slots()
