import copy, hashlib, json, tempfile, unittest, uuid
from pathlib import Path
from unittest.mock import patch
from qbctl import common, engine
from qbctl.common import Budget, Fault, Store, digest, guarded, move_no_replace, sha_file
from qbctl.engine import Controller

class FakeAPI:
    version='v5.2.4'; api_version='2.15.1'
    def __init__(self,c): self.c=c; self.rows={}; self.contents={}; self.meta={}; self.calls=[]; self.delete_ignored=False
    def compatible(self): pass
    def torrents(self): return copy.deepcopy(list(self.rows.values()))
    def get(self,h): return copy.deepcopy(self.rows.get(h))
    def files(self,h): return copy.deepcopy(self.contents.get(h,[{'name':h+'.bin','size':100,'priority':1,'progress':self.rows[h].get('progress',0)}]))
    def metadata(self,data): return copy.deepcopy(self.meta[data])
    def stop(self,h): self.calls.append(('stop',h)); self.rows[h]['state']='stoppedUP' if self.rows[h]['progress']==1 else 'stoppedDL'
    def start(self,h): self.calls.append(('start',h)); self.rows[h]['state']='downloading'
    def remove(self,h):
        self.calls.append(('delete',h,False))
        if not self.delete_ignored: self.rows.pop(h,None)
    def prefs(self): return {'save_path':self.c['paths']['working'],'temp_path_enabled':False,'auto_tmm_enabled':False}
    def call(self,endpoint,fields=None,**kw):
        self.calls.append((endpoint,fields))
        if endpoint=='transfer/info': return {}
        return None
    def add(self,data,working):
        self.calls.append(('add',str(working)))
        h=self.meta[data]['id']; self.rows[h]={'hash':h,'state':'stoppedDL','progress':0,'amount_left':100,'save_path':str(working),'auto_tmm':False,'added_on':0,'priority':1}
        self.contents[h]=[{'name':f['path'],'size':f['length'],'priority':1,'progress':0} for f in self.meta[data]['files']]

class ProtocolTests(unittest.TestCase):
    def setUp(self):
        (Path(__file__).parent/'sandbox').mkdir(exist_ok=True)
        self.tmp=tempfile.TemporaryDirectory(prefix='case-',dir=Path(__file__).parent/'sandbox')
        self.root=Path(self.tmp.name)
        self.patches=[patch.object(common,'ROOT',self.root),patch.object(engine,'ROOT',self.root)]
        for p in self.patches: p.start()
        paths={k:str(self.root/v) for k,v in [('incoming','t'),('archive','t/d'),('working','m'),('completed','k')]}
        for p in paths.values(): Path(p).mkdir(parents=True,exist_ok=True)
        self.c={'revision':1,'paths':paths,'api':{},'policy':{'target_client_count':1,'download_slots':1,'upload_slots':40,'down_bps':1,'up_bps':1,'legacy_k':[]},'resources':{'enabled':False}}
        self.api=FakeAPI(self.c); self.store=Store(); self.ctl=Controller(self.c,self.api,self.store,Budget(30))
        self.h='a'*40; self.payload={'Готовая папка/a.bin':b'first data','Готовая папка/sub/b.bin':b'second data'}
        self.data=b'fixture metainfo'; p=Path(paths['incoming'])/'[fixture].torrent'; p.write_bytes(self.data)
        self.source=p
        files=[]
        for name,data in self.payload.items():
            f=Path(paths['working'])/name; f.parent.mkdir(parents=True,exist_ok=True); f.write_bytes(data)
            files.append({'name':name,'size':len(data),'priority':1,'progress':1})
        self.api.contents[self.h]=files
        self.api.meta[self.data]={'id':self.h,'v1':self.h,'v2':None,'files':[{'path':f['name'],'length':f['size']} for f in files]}
        self.api.rows[self.h]={'hash':self.h,'name':'fixture','state':'stalledUP','progress':1,'amount_left':0,'save_path':paths['working'],'auto_tmm':False,'priority':1,'added_on':0}
    def tearDown(self):
        self.store.close()
        for p in reversed(self.patches): p.stop()
        assert self.root.resolve().is_relative_to((Path(__file__).parent/'sandbox').resolve())
        self.tmp.cleanup()
    def plan(self): return self.ctl.make_plan(max_complete=1,max_add=0)
    def assert_finished(self):
        self.assertFalse(self.source.exists()); self.assertEqual((Path(self.c['paths']['archive'])/self.source.name).read_bytes(),self.data)
        self.assertIsNone(self.api.get(self.h)); self.assertFalse(self.store.pending())
        for relative,data in self.payload.items():
            self.assertEqual((Path(self.c['paths']['completed'])/relative).read_bytes(),data)
            self.assertFalse((Path(self.c['paths']['working'])/relative).exists())
        self.assertEqual(self.api.calls[-1],('delete',self.h,False))
    def test_plan_read_only(self):
        plan=self.plan(); self.assertEqual(len(plan['operations']),1); self.assertTrue(self.source.exists()); self.assertFalse(self.api.calls); self.assertFalse(self.store.pending())
    def test_normal_move_and_order(self):
        plan=self.plan(); observed=[]; original=engine.move_no_replace
        def move(s,d):
            observed.append((Path(s).name,Path(d).name))
            self.assertEqual(self.api.rows[self.h]['state'],'stoppedUP')
            if Path(s)!=self.source: self.assertFalse(self.source.exists())
            original(s,d)
        with patch.object(engine,'move_no_replace',move): self.ctl.apply_plan(plan)
        self.assertEqual(observed[0][0],self.source.name); self.assert_finished()
    def test_payload_destination_conflict_no_mutation(self):
        p=Path(self.c['paths']['completed'])/'Готовая папка/a.bin'; p.parent.mkdir(); p.write_bytes(b'foreign')
        plan=self.plan(); self.assertFalse(plan['operations']); self.assertEqual(plan['issues'][0]['code'],'DESTINATION_EXISTS'); self.assertTrue(self.source.exists()); self.assertEqual(p.read_bytes(),b'foreign'); self.assertFalse(self.api.calls)
    def test_archive_conflict_no_mutation(self):
        (Path(self.c['paths']['archive'])/self.source.name).write_bytes(self.data)
        plan=self.plan(); self.assertFalse(plan['operations']); self.assertTrue(self.source.exists()); self.assertFalse(self.api.calls)
    def test_partial_selection_blocks(self):
        self.api.contents[self.h][0]['priority']=0
        plan=self.plan(); self.assertFalse(plan['operations']); self.assertEqual(plan['issues'][0]['code'],'PARTIAL_SELECTION')
    def test_wrong_size_blocks(self):
        next(iter(self.payload)); p=Path(self.c['paths']['working'])/next(iter(self.payload)); p.write_bytes(b'x')
        self.assertFalse(self.plan()['operations']); self.assertTrue(self.source.exists())
    def test_foreign_file_blocks(self):
        (Path(self.c['paths']['working'])/'Готовая папка/foreign.txt').write_text('foreign')
        plan=self.plan(); self.assertFalse(plan['operations']); self.assertEqual(plan['issues'][0]['code'],'FOREIGN_FILES')
    def test_shared_file_blocks(self):
        b='b'*40; self.api.rows[b]={**self.api.rows[self.h],'hash':b,'progress':0,'amount_left':1}; self.api.contents[b]=self.api.contents[self.h][:1]
        self.assertFalse(self.plan()['operations'])
    def test_stale_file_before_apply_blocks(self):
        plan=self.plan(); (Path(self.c['paths']['working'])/next(iter(self.payload))).write_bytes(b'changed')
        with self.assertRaises(Fault): self.ctl.apply_plan(plan)
        self.assertTrue(self.source.exists()); self.assertFalse(self.api.calls)
    def test_crash_after_archive_then_recover(self):
        plan=self.plan(); original=engine.move_no_replace
        def move(s,d):
            original(s,d)
            if Path(s)==self.source: raise RuntimeError('simulated crash after archive')
        with patch.object(engine,'move_no_replace',move),self.assertRaises(RuntimeError): self.ctl.apply_plan(plan)
        self.assertTrue(self.store.pending()); self.assertFalse(self.source.exists())
        self.ctl.recover(); self.assert_finished()
    def test_crash_after_payload_move_then_recover(self):
        plan=self.plan(); original=engine.move_no_replace; calls=0
        def move(s,d):
            nonlocal calls
            original(s,d); calls+=1
            if calls==2: raise RuntimeError('simulated crash after first data move')
        with patch.object(engine,'move_no_replace',move),self.assertRaises(RuntimeError): self.ctl.apply_plan(plan)
        self.ctl.recover(); self.assert_finished()
    def test_crash_after_delete_then_recover(self):
        plan=self.plan(); original=self.api.remove
        def remove(h): original(h); raise RuntimeError('simulated delete response loss')
        with patch.object(self.api,'remove',remove),self.assertRaises(RuntimeError): self.ctl.apply_plan(plan)
        self.ctl.recover(); self.assert_finished()
    def test_delete_http_success_without_effect_is_unknown(self):
        plan=self.plan(); self.api.delete_ignored=True
        with self.assertRaises(Fault) as caught: self.ctl.apply_plan(plan)
        self.assertEqual(caught.exception.code,'REMOVE_UNCONFIRMED'); self.assertTrue(self.store.pending()); self.assertIn(self.h,self.api.rows)
        self.api.delete_ignored=False; self.ctl.recover(); self.assert_finished()
    def test_replay_completed_operation_no_extra_delete(self):
        plan=self.plan(); op=copy.deepcopy(plan['operations'][0]); self.ctl.apply_plan(plan); n=len(self.api.calls); self.ctl.dispatch(op)
        self.assertEqual(len(self.api.calls),n); self.assert_finished()
    def test_archived_hash_not_refilled(self):
        (Path(self.c['paths']['archive'])/'old.torrent').write_bytes(self.data); self.api.rows.clear(); self.c['policy']['target_client_count']=55
        plan=self.ctl.make_plan(0,5); self.assertFalse(plan['additions'])
    def test_slots_pause_keep_records_user_pause_not_resumed(self):
        self.api.rows.clear()
        for i in range(4):
            h=f'{i:040x}'; self.api.rows[h]={'hash':h,'state':'downloading','progress':0,'amount_left':10,'save_path':self.c['paths']['working'],'priority':i,'added_on':i}
        user='f'*40; self.api.rows[user]={'hash':user,'state':'stoppedDL','progress':0,'amount_left':10,'save_path':self.c['paths']['working'],'priority':4,'added_on':4}
        self.c['policy']['download_slots']=2; self.ctl.enforce_slots(); self.assertEqual(len(self.api.rows),5)
        self.assertEqual(sum(not engine.stopped(t) for t in self.api.rows.values()),2)
        self.c['policy']['download_slots']=4; self.ctl.enforce_slots(); self.assertEqual(sum(not engine.stopped(t) for t in self.api.rows.values()),4); self.assertTrue(engine.stopped(self.api.rows[user]))
    def test_windows_rename_no_overwrite(self):
        a=self.root/'a'; b=self.root/'b'; a.write_bytes(b'one'); b.write_bytes(b'two')
        with self.assertRaises(Fault): move_no_replace(a,b)
        self.assertEqual(a.read_bytes(),b'one'); self.assertEqual(b.read_bytes(),b'two')
    def test_path_traversal_blocks(self):
        for name in ('../escape',r'C:\escape','file:ads','CON.txt'):
            with self.subTest(name=name),self.assertRaises(Fault): guarded(self.c['paths']['working'],name)

    def test_delayed_stop_confirmation_is_polled(self):
        self.api.stop(self.h); original=self.api.get; reads=0
        def delayed(h):
            nonlocal reads
            reads+=1; row=original(h)
            if reads<=2: row['state']='stalledUP'
            return row
        with patch.object(self.api,'get',delayed):
            self.assertTrue(engine.stopped(self.ctl.wait_state(self.h,engine.stopped)))
        self.assertEqual(reads,3)
    def test_selected_hash_plans_only_one(self):
        other='b'*40; self.api.rows[other]={**self.api.rows[self.h],'hash':other}
        self.api.contents[other]=self.api.contents[self.h]
        # No shared ownership in this test: remove other's content to isolate selection.
        self.api.contents[other]=[]
        plan=self.ctl.make_plan(5,0,selected_hash=self.h)
        self.assertEqual([o['hash'] for o in plan['operations']],[self.h])
        self.assertTrue(self.source.exists()); self.assertFalse(self.api.calls)
    def test_zero_slots_pauses_without_deleting(self):
        self.api.rows[self.h]['progress']=0; self.api.rows[self.h]['amount_left']=10; self.api.rows[self.h]['state']='downloading'
        self.c['policy']['download_slots']=0; self.ctl.enforce_slots()
        self.assertTrue(engine.stopped(self.api.rows[self.h])); self.assertTrue(self.source.exists()); self.assertEqual(len(self.api.rows),1)

if __name__=='__main__':
    (Path(__file__).parent/'sandbox').mkdir(exist_ok=True)
    unittest.main(verbosity=2)
