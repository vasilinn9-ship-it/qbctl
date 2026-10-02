import copy, json, time, unittest, uuid
from pathlib import Path
from unittest.mock import patch
from test_protocol import ProtocolTests
from qbctl import engine, common, cli
from qbctl.common import Fault, Store, Budget, now, fingerprint
from qbctl.ownership import claims, conflicts, scope

class ReliabilityTests(ProtocolTests):
    def single_pair(self, suffix=''):
        self.api.rows.clear(); self.api.contents.clear(); self.payload={'shared.bin':b'original source bytes'}
        self.file=Path(self.c['paths']['working'])/('shared.bin'+suffix); self.file.write_bytes(self.payload['shared.bin'])
        self.other='b'*40
        for h,size in ((self.h,len(self.payload['shared.bin'])),(self.other,5)):
            self.api.rows[h]={'hash':h,'name':'shared','state':'stoppedDL','progress':0.5,'amount_left':5,'save_path':self.c['paths']['working'],'auto_tmm':False}
            self.api.contents[h]=[{'name':'shared.bin','size':size,'priority':1,'progress':0.5}]
        old_call=self.api.call
        def call(endpoint,fields=None,**kw):
            if endpoint=='torrents/renameFile':
                self.api.calls.append((endpoint,fields)); root=Path(self.c['paths']['working'])
                src=root/(fields['oldPath']+suffix); dst=root/(fields['newPath']+suffix)
                engine.move_no_replace(src,dst); self.api.contents[fields['hash']][0]['name']=fields['newPath']; return None
            return old_call(endpoint,fields,**kw)
        self.api.call=call
        old_prefs=self.api.prefs; self.api.prefs=lambda:{**old_prefs(),'incomplete_files_ext':bool(suffix)}

    def restart(self):
        self.store.close(); self.store=Store(); self.ctl.store=self.store; self.ctl.budget=Budget(30)

    def test_case_and_partial_extension_collision(self):
        a=claims(self.c['paths']['working'],[{'name':'Folder/A.bin'}])
        b=claims(self.c['paths']['working'],[{'name':'folder/a.BIN.!qB'}])
        self.assertTrue(conflicts(a,b))

    def test_file_directory_collision(self):
        self.assertTrue(conflicts(claims(self.c['paths']['working'],[{'name':'a'}]),claims(self.c['paths']['working'],[{'name':'a/sub.bin'}])))

    def test_shared_tree_different_files_collision(self):
        self.assertTrue(conflicts(claims(self.c['paths']['working'],[{'name':'folder/a.bin'}]),claims(self.c['paths']['working'],[{'name':'folder/b.bin'}])))

    def test_different_root_names_no_collision(self):
        self.assertFalse(conflicts(claims(self.c['paths']['working'],[{'name':'a.bin'}]),claims(self.c['paths']['working'],[{'name':'b.bin'}])))

    def test_add_collision_before_api_submission(self):
        other='b'*40; data=b'new torrent'; p=Path(self.c['paths']['incoming'])/'new.torrent'; p.write_bytes(data)
        self.api.meta[data]={**copy.deepcopy(self.api.meta[self.data]),'id':other,'v1':other}
        self.c['policy']['target_client_count']=2
        o={'id':uuid.uuid4().hex,'kind':'add','hash':other,'stage':'prepared','path':str(p),'sha256':common.sha_file(p)}
        with self.assertRaises(Fault) as caught: self.ctl.dispatch(o)
        self.assertEqual(caught.exception.code,'SHARED_FILES'); self.assertFalse(any(x[0]=='add' for x in self.api.calls))

    def test_resume_collision_rejected_before_start(self):
        self.single_pair(); self.c['policy']['download_slots']=2
        o={'id':uuid.uuid4().hex,'kind':'control','hash':self.h,'stage':'prepared','tasks':[{'hash':self.h,'action':'resume'}]}
        with self.assertRaises(Fault) as caught: self.ctl.dispatch(o)
        self.assertEqual(caught.exception.code,'SHARED_FILES'); self.assertFalse(any(x[0]=='start' for x in self.api.calls))

    def test_active_collision_both_stopped_by_scheduler(self):
        self.single_pair(); self.c['policy']['download_slots']=2
        for row in self.api.rows.values(): row['state']='downloading'
        self.ctl.enforce_slots()
        self.assertTrue(all(engine.stopped(t) for t in self.api.rows.values()))
        self.assertEqual(self.store.stop_reason(self.h),'path_collision')

    def test_isolation_success_independent_bytes(self):
        self.single_pair(); original=self.file.read_bytes(); o=self.ctl.isolate_shared(self.h,self.other,True)
        dst=Path(self.c['paths']['working'])/o['new_relative']
        self.assertEqual(self.file.read_bytes(),original[:5]); self.assertEqual(dst.read_bytes(),original)
        self.assertNotEqual(fingerprint(dst)['ino'],fingerprint(self.file)['ino']); self.assertFalse(self.store.pending())

    def test_isolation_partial_extension(self):
        self.single_pair('.!qB'); o=self.ctl.isolate_shared(self.h,self.other,True)
        self.assertEqual(o['actual_suffix'],'.!qB'); self.assertTrue(self.file.exists()); self.assertFalse(self.store.pending())

    def test_isolation_crash_before_copy_resumes_after_restart(self):
        self.single_pair(); original=self.ctl.stage
        def stage(o,s):
            original(o,s)
            if s=='copy_requested': raise RuntimeError('crash before copy')
        with patch.object(self.ctl,'stage',stage),self.assertRaises(RuntimeError): self.ctl.isolate_shared(self.h,self.other,True)
        self.assertFalse(self.file.exists()); self.restart(); self.ctl.recover()
        self.assertFalse(self.store.pending()); self.assertEqual(self.file.read_bytes(),self.payload['shared.bin'][:5])

    def test_isolation_partial_copy_retained_and_retried(self):
        self.single_pair(); original=self.ctl.stage
        def stage(o,s):
            original(o,s)
            if s=='copy_requested':
                (Path(self.c['paths']['working'])/o['temp_relative']).write_bytes(b'partial')
                raise RuntimeError('crash during copy')
        with patch.object(self.ctl,'stage',stage),self.assertRaises(RuntimeError): self.ctl.isolate_shared(self.h,self.other,True)
        self.restart(); self.ctl.recover(); o=self.store.pending()[0]
        self.ctl.resolve_isolation(o['id'],'retry-copy',True)
        saved=list((Path(self.c['paths']['working'])/'_recovery'/o['id']).glob('*.part'))
        self.assertEqual(len(saved),1); self.assertEqual(saved[0].read_bytes(),b'partial')
        self.assertEqual(self.file.read_bytes(),self.payload['shared.bin'][:5]); self.assertFalse(self.store.pending())

    def test_isolation_crash_after_rename_no_api_repeat(self):
        self.single_pair(); original=self.api.call
        def call(endpoint,*a,**kw):
            r=original(endpoint,*a,**kw)
            if endpoint=='torrents/renameFile': raise RuntimeError('response loss')
            return r
        with patch.object(self.api,'call',call),self.assertRaises(RuntimeError): self.ctl.isolate_shared(self.h,self.other,True)
        count=sum(x[0]=='torrents/renameFile' for x in self.api.calls); self.restart(); self.ctl.recover()
        self.assertFalse(self.store.pending()); self.assertEqual(sum(x[0]=='torrents/renameFile' for x in self.api.calls),count)

    def test_isolation_crash_after_install_recovers(self):
        self.single_pair(); original=engine.move_no_replace
        def move(s,d):
            original(s,d)
            if Path(s).name.startswith('.clone-'): raise RuntimeError('crash after install')
        with patch.object(engine,'move_no_replace',move),self.assertRaises(RuntimeError): self.ctl.isolate_shared(self.h,self.other,True)
        self.restart(); self.ctl.recover(); self.assertFalse(self.store.pending()); self.assertTrue(self.file.exists())

    def test_isolation_changed_source_preserved_and_cancelled(self):
        self.single_pair(); o=self.ctl.isolate_shared(self.h,self.other)
        self.file.write_bytes(b'changed')
        with self.assertRaises(Fault): self.ctl.dispatch(o)
        self.ctl.resolve_isolation(o['id'],'cancel-untouched',True)
        self.assertEqual(self.file.read_bytes(),b'changed'); self.assertFalse(self.store.pending())

    def test_isolation_cancellation_after_rename_rejected(self):
        self.single_pair(); original=self.ctl.stage
        def stage(o,s):
            original(o,s)
            if s=='renamed': raise RuntimeError('crash')
        with patch.object(self.ctl,'stage',stage),self.assertRaises(RuntimeError): self.ctl.isolate_shared(self.h,self.other,True)
        with self.assertRaises(Fault): self.ctl.resolve_isolation(self.store.pending()[0]['id'],'cancel-untouched',True)

    def test_pending_local_control_does_not_block_other_completion(self):
        other='b'*40; self.api.rows[other]={**self.api.rows[self.h],'hash':other,'progress':0,'amount_left':10,'state':'stoppedDL'}
        op={'id':uuid.uuid4().hex,'kind':'control','hash':other,'stage':'task_requested','tasks':[{'hash':other,'action':'recheck','sent':True}]}
        self.store.put_op(op); plan=self.ctl.make_plan(1,0)
        self.ctl.apply_plan(plan)
        self.assertIsNone(self.api.get(self.h)); self.assertEqual(self.store.pending()[0]['id'],op['id'])

    def test_pending_global_policy_blocks_scheduler(self):
        self.store.put_op({'id':uuid.uuid4().hex,'kind':'control','hash':'policy','stage':'prepared','preferences':{'max_active_downloads':50},'tasks':[]})
        with self.assertRaises(Fault): self.ctl.enforce_slots()
        self.assertFalse(self.api.calls)

    def test_isolation_locks_both_hashes_and_destination_tree(self):
        self.single_pair(); o=self.ctl.isolate_shared(self.h,self.other); self.store.put_op(o)
        self.assertIn(self.other,self.ctl.pending_hashes())
        with self.assertRaises(Fault): self.ctl.guard_pending('c'*40,[str(Path(self.c['paths']['working'])/Path(o['new_relative']).parent/'foreign.bin')])

    def recheck_op(self):
        return {'id':uuid.uuid4().hex,'kind':'control','hash':self.h,'stage':'prepared','tasks':[{'hash':self.h,'action':'recheck'}]}

    def test_atomic_settings_insert_merge_and_no_write_for_unchanged(self):
        other=Store()
        try:
            default={}
            def first(value): value['first']=1; return value
            self.store.update_setting('merge-test',first,default)
            self.assertEqual(default,{})
            def second(value): value['second']=2; return value
            other.update_setting('merge-test',second,{})
            self.assertEqual(self.store.setting('merge-test'),{'first':1,'second':2})
            before=self.store.db.total_changes
            self.store.update_setting('merge-test',lambda value:value,{})
            self.assertEqual(self.store.db.total_changes,before)
        finally: other.close()

    def test_stale_snapshot_cannot_confirm_new_recheck(self):
        self.store.set_setting('rechecks',{self.h:{'requested_at':'2099-01-01T00:00:00+00:00','phase':'unobserved','start_observed':False}})
        self.api.rows[self.h]['state']='checkingDL'
        self.ctl.observe_rechecks(self.api.torrents(),'2000-01-01T00:00:00+00:00')
        self.assertEqual(self.store.setting('rechecks')[self.h]['phase'],'unobserved')

    def test_isolation_unknown_rename_only_explicit_retry(self):
        self.single_pair(); original=self.api.call
        def call(endpoint,*args,**kwargs):
            if endpoint=='torrents/renameFile': raise RuntimeError('request not delivered')
            return original(endpoint,*args,**kwargs)
        with patch.object(self.api,'call',call),self.assertRaises(RuntimeError): self.ctl.isolate_shared(self.h,self.other,True)
        self.restart(); self.ctl.recover(); o=self.store.pending()[0]
        self.assertEqual(o['stage'],'rename_requested'); self.assertTrue(self.file.exists())
        self.ctl.resolve_isolation(o['id'],'retry-rename',True)
        self.assertFalse(self.store.pending()); self.assertEqual(self.file.read_bytes(),self.payload['shared.bin'][:5])

    def test_isolation_crash_after_partial_retention_recovers(self):
        self.single_pair(); original_stage=self.ctl.stage
        def stage(o,s):
            original_stage(o,s)
            if s=='copy_requested':
                (Path(self.c['paths']['working'])/o['temp_relative']).write_bytes(b'partial')
                raise RuntimeError('partial copy')
        with patch.object(self.ctl,'stage',stage),self.assertRaises(RuntimeError): self.ctl.isolate_shared(self.h,self.other,True)
        o=self.store.pending()[0]; original_move=engine.move_no_replace
        def move(src,dst):
            original_move(src,dst)
            if '_recovery' in Path(dst).parts: raise RuntimeError('crash after retention')
        with patch.object(engine,'move_no_replace',move),self.assertRaises(RuntimeError): self.ctl.resolve_isolation(o['id'],'retry-copy',True)
        self.restart(); self.ctl.recover()
        self.assertFalse(self.store.pending()); self.assertEqual(self.file.read_bytes(),self.payload['shared.bin'][:5])
        self.assertEqual(len(list((Path(self.c['paths']['working'])/'_recovery'/o['id']).glob('*.part'))),1)

    def test_recheck_accepted_without_observation_not_global_pending(self):
        o=self.recheck_op()
        with patch.object(self.ctl,'wait_state',return_value=self.api.get(self.h)): self.ctl.dispatch(o)
        self.assertFalse(self.store.pending()); r=self.store.setting('rechecks')[self.h]
        self.assertTrue(r['request_accepted']); self.assertFalse(r['completion_observed']); self.assertEqual(r['phase'],'unobserved')

    def test_recheck_unknown_response_never_repeated(self):
        o=self.recheck_op(); calls=[]
        def call(endpoint,*a,**kw): calls.append(endpoint); raise Fault('MUTATION_UNCERTAIN','loss','unknown')
        with patch.object(self.api,'call',call),self.assertRaises(Fault): self.ctl.dispatch(o)
        self.ctl.recover(); self.assertEqual(calls,['torrents/recheck']); self.assertTrue(self.store.pending())

    def test_recheck_observed_start_and_finish_persisted(self):
        old_call=self.api.call
        def call(endpoint,*a,**kw):
            if endpoint=='torrents/recheck': self.api.rows[self.h]['state']='checkingDL'
            return old_call(endpoint,*a,**kw)
        with patch.object(self.api,'call',call): self.ctl.dispatch(self.recheck_op())
        self.assertTrue(self.store.setting('rechecks')[self.h]['start_observed'])
        self.api.rows[self.h].update(state='stoppedDL',progress=0.9,amount_left=1)
        self.ctl.snapshot(); r=self.store.setting('rechecks')[self.h]
        self.assertTrue(r['completion_observed']); self.assertFalse(r['fully_downloaded']); self.assertEqual(r['phase'],'finished')
        self.restart(); self.assertEqual(self.store.setting('rechecks')[self.h]['phase'],'finished')

    def test_recheck_completed_root_rejected_without_payload_read(self):
        self.api.rows[self.h]['save_path']=self.c['paths']['completed']; self.c['policy']['legacy_k']=[self.h]
        with self.assertRaises(Fault) as caught: self.ctl.dispatch(self.recheck_op())
        self.assertEqual(caught.exception.code,'COMPLETED_DATA_OUTSIDE_SCOPE'); self.assertFalse(self.api.calls)

    def test_isolation_no_k_reads(self):
        self.single_pair(); original=Path.stat
        def stat(p,*a,**kw):
            if p.is_relative_to(Path(self.c['paths']['completed'])): raise AssertionError('k read')
            return original(p,*a,**kw)
        with patch.object(Path,'stat',stat): self.ctl.isolate_shared(self.h,self.other,True)
        self.assertFalse(self.store.pending())

    def test_confirm_recheck_wrong_report_rejected(self):
        o=self.recheck_op(); o['tasks'][0]['sent']=True; self.store.put_op(o)
        p=self.root/'observation.json'; p.write_text(json.dumps({'schema_version':1,'command':'explain','result':'ok','torrent':{'hash':'b'*40,'state':'checkingDL'}}))
        with self.assertRaises(Fault): self.ctl.confirm_recheck(o['id'],str(p),True)
        self.assertTrue(self.store.pending())

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ReliabilityTests(name) for name in ReliabilityTests.__dict__ if name.startswith('test_'))

if __name__=='__main__': unittest.main(verbosity=2)
