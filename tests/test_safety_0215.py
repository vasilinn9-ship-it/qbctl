import asyncio,contextlib,copy,json,sqlite3,subprocess,sys,threading,time,unittest
from pathlib import Path
from unittest.mock import patch
from qbctl import common,engine,executor
from qbctl.output import Progress
from qbctl.common import Fault,Store
from test_protocol import ProtocolTests

class SafetyTests(ProtocolTests):
    def test_progress_unexpected_errors_become_warnings(self):
        for error in (ValueError('bad value'),KeyError('field'),TypeError('format')):
            p=Progress(True,True)
            with patch.object(p,'_read',side_effect=error): p({'id':'simulation'})
            self.assertEqual(p.warnings[0]['code'],'PROGRESS_UNAVAILABLE')

    def test_progress_does_not_swallow_keyboard_interrupt(self):
        progress=Progress(True,True)
        with patch.object(progress,'_read',side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt): progress({'id':'simulation'})

    def test_runtime_unexpected_diagnostic_error_keeps_finished_result(self):
        @contextlib.contextmanager
        def database(*args,**kwargs): yield self.store
        job={'id':'a'*32,'state':'finished','actions':[],'warnings':[],'initial_pending':[],
             'reason':{'code':'FINISHED'},'last_result':{}}
        with patch.object(executor,'database',database),patch.object(executor,'runtime_status',side_effect=TypeError('diagnostic')):
            result=executor.response(job)
        self.assertEqual(result['result'],'ok')
        self.assertEqual(result['warnings'][0]['code'],'RUNTIME_STATUS_UNAVAILABLE')

    def test_monitor_fault_cannot_override_finished_worker(self):
        job={'id':'a'*32,'state':'running'}
        @contextlib.contextmanager
        def database(*args,**kwargs): yield None
        def worker(identifier): job['state']='finished'; return job
        class BadProgress:
            def __init__(self): self.warnings=[]
            def __call__(self,job): raise ValueError('presentation')
            def note_error(self,error): self.warnings.append(type(error).__name__)
        progress=BadProgress()
        with patch.object(executor,'database',database),patch.object(executor,'read_job',side_effect=lambda store,identifier:job),patch.object(executor,'requested_stop',return_value=False),patch.object(executor,'next_job',return_value=job),patch.object(executor,'work_one',side_effect=worker),patch.object(executor,'update_job'):
            asyncio.run(executor.coordinate('simulation','foreground',only=job['id'],progress=progress))
        self.assertEqual(job['state'],'finished'); self.assertIn('ValueError',progress.warnings)

    def test_broken_warning_callback_does_not_override_worker(self):
        job={'id':'a'*32,'state':'running'}
        @contextlib.contextmanager
        def database(*args,**kwargs): yield None
        def worker(identifier): job['state']='finished'; return job
        class BadProgress:
            def __call__(self,job): raise ValueError('presentation')
            def note_error(self,error): raise RuntimeError('warning callback')
        with patch.object(executor,'database',database),patch.object(executor,'read_job',side_effect=lambda store,identifier:job),patch.object(executor,'requested_stop',return_value=False),patch.object(executor,'next_job',return_value=job),patch.object(executor,'work_one',side_effect=worker),patch.object(executor,'update_job'):
            asyncio.run(executor.coordinate('simulation','foreground',only=job['id'],progress=BadProgress()))
        self.assertEqual(job['state'],'finished')

    def test_completion_stage_does_not_regress(self):
        plan=self.plan(); op=plan['operations'][0]; op['stage']='moving'; self.store.put_op(op)
        self.ctl.stage(op,'stopped'); self.ctl.stage(op,'archived')
        self.assertEqual(op['stage'],'moving')
        self.assertEqual(self.store.pending()[0]['stage'],'moving')

    def test_no_sent_remove_can_explicitly_roll_back(self):
        op=self.plan()['operations'][0]; op.update(stage='remove_requested',remove_sent=False)
        self.store.put_op(op); self.ctl.stage(op,'data_verified')
        self.assertEqual(op['stage'],'data_verified')

    def test_same_stage_persists_new_file_intention(self):
        op=self.plan()['operations'][0]; op['stage']='moving'; self.store.put_op(op)
        op['files'][0]['handoff']='move_requested'; self.ctl.stage(op,'moving')
        self.assertEqual(self.store.pending()[0]['files'][0]['handoff'],'move_requested')

    def test_worker_failure_is_not_converted_to_success(self):
        job={'id':'a'*32,'state':'running'}
        @contextlib.contextmanager
        def database(*args,**kwargs): yield None
        with patch.object(executor,'database',database),patch.object(executor,'read_job',return_value=job),patch.object(executor,'requested_stop',return_value=False),patch.object(executor,'next_job',return_value=job),patch.object(executor,'work_one',side_effect=ValueError('worker failure')),patch.object(executor,'update_job'):
            with self.assertRaisesRegex(ValueError,'worker failure'):
                asyncio.run(executor.coordinate('simulation','foreground',only=job['id']))

    def test_sqlite_write_failure_prevents_file_mutation(self):
        plan=self.plan()
        def reject(code,arg1,arg2,db,trigger):
            return sqlite3.SQLITE_DENY if code in (sqlite3.SQLITE_INSERT,sqlite3.SQLITE_UPDATE,sqlite3.SQLITE_DELETE) else sqlite3.SQLITE_OK
        self.store.db.set_authorizer(reject)
        try:
            with self.assertRaises(sqlite3.DatabaseError): self.ctl.apply_plan(plan)
        finally: self.store.db.set_authorizer(None)
        self.assertTrue(self.source.exists()); self.assertFalse(self.api.calls)
        self.assertTrue(all((Path(self.c['paths']['working'])/name).exists() for name in self.payload))

    def test_second_writer_is_rejected(self):
        errors=[]
        def writer():
            try: s=Store(); s.close()
            except Fault as error: errors.append(error.code)
        thread=threading.Thread(target=writer); thread.start(); thread.join(5)
        self.assertFalse(thread.is_alive()); self.assertEqual(errors,['WRITER_BUSY'])

    def crash_and_recover(self,scenario):
        fixture={'config':self.c,'rows':self.api.rows,'contents':self.api.contents,
                 'metainfo_hex':self.data.hex(),'metadata':self.api.meta[self.data],
                 'source':str(self.source),'plan':self.plan()}
        (self.root/'crash-fixture.json').write_text(json.dumps(fixture,ensure_ascii=False),encoding='utf-8')
        self.store.close()
        result=subprocess.run([sys.executable,'-B',str(Path(__file__).parent/'crash_worker_0215.py'),str(self.root),scenario],capture_output=True,text=True,timeout=30)
        self.assertEqual(result.returncode,71,result.stderr)
        self.store=Store(); self.ctl.store=self.store
        state=json.loads((self.root/'fake-client.json').read_text(encoding='utf-8')); self.api.rows=state['rows']; self.api.contents=state['contents']
        self.api.calls=[tuple(call) for call in state['calls']]
        op=self.store.pending()[0]
        handed={str(Path(f['destination'])) for f in op['files'] if f.get('handoff')=='handed_off'}
        original=Path.stat
        def stat(path,*args,**kwargs):
            if str(path) in handed: raise AssertionError('handed-off k reread')
            return original(path,*args,**kwargs)
        with patch.object(Path,'stat',stat): self.ctl.recover()
        self.assert_finished()
        self.assertEqual(sum(call[0]=='delete' for call in self.api.calls),1)

    def test_process_exit_after_archive(self): self.crash_and_recover('archive')
    def test_process_exit_after_payload_before_receipt(self): self.crash_and_recover('payload')
    def test_process_exit_after_receipt_does_not_read_handed_off(self): self.crash_and_recover('receipt')
    def test_process_exit_after_remove_does_not_delete_twice(self): self.crash_and_recover('remove')
