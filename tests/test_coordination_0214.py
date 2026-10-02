import copy, sqlite3, threading, unittest
from unittest.mock import patch
from qbctl.api import API
from qbctl import cli, executor
from qbctl.common import Store
from test_protocol import ProtocolTests

class CoordinationTests(ProtocolTests):
    def cache_api(self):
        a=API.__new__(API); a._file_cache=None; a._file_cache_lock=threading.Lock()
        a.metrics={}; a._metrics_lock=threading.Lock()
        return a

    def test_status_store_is_readonly_while_writer_held(self):
        with self.read_store():
            pass

    def read_store(self):
        import contextlib
        @contextlib.contextmanager
        def opened():
            s=Store(readonly=True)
            try:
                self.assertEqual(s.db.execute('PRAGMA query_only').fetchone()[0],1)
                self.assertEqual(s.db.execute('SELECT count(*) FROM jobs').fetchone()[0],0)
                with self.assertRaises(sqlite3.OperationalError): s.db.execute('DELETE FROM jobs')
                yield s
            finally: s.close()
        return opened()

    def test_phase_reuses_files_and_returns_independent_values(self):
        a=self.cache_api()
        with patch.object(a,'call',return_value=[{'name':'x'}]) as call:
            with a.file_phase():
                one=a.files(self.h); one[0]['name']='changed'
                self.assertEqual(a.files(self.h)[0]['name'],'x')
                self.assertEqual(call.call_count,1)
            a.files(self.h); self.assertEqual(call.call_count,2)

    def test_mutation_invalidates_before_and_after_request(self):
        a=self.cache_api()
        with patch.object(a,'_call',return_value=[]):
            with a.file_phase():
                a.files(self.h)
                self.assertIn(self.h,a._file_cache)
                a.call('torrents/start',{'hashes':self.h})
                self.assertEqual(a._file_cache,{})

    def test_second_dispatch_has_fresh_cache(self):
        a=self.cache_api(); self.ctl.api=a
        o={'id':'x','kind':'control','hash':self.h,'stage':'prepared'}
        with a.file_phase(),patch.object(a,'call',return_value=[]),patch.object(self.ctl,'headroom'):
            a.files(self.h)
            with patch.object(self.ctl,'control',side_effect=lambda op:self.assertEqual(a._file_cache,{})):
                self.ctl.dispatch_once(o)

    def test_status_recheck_observation_does_not_write(self):
        self.store.set_setting('rechecks',{self.h:{'requested_at':'2000','phase':'checking','start_observed':True}})
        before=self.store.setting('rechecks')
        s=Store(readonly=True); self.ctl.store=s
        try:
            self.ctl.observe_rechecks(self.api.torrents())
            self.assertEqual(s.setting('rechecks'),before)
        finally: s.close(); self.ctl.store=self.store

    def test_accepted_wait_skips_new_plan(self):
        self.ctl.nonblocking=True
        self.ctl.issues=[{'code':'STOP_PENDING'}]
        args=type('Args',(),{'_completion_scope':[]})()
        with patch.object(self.ctl,'recover'),patch.object(self.store,'pending',return_value=[{'id':'x'}]),patch.object(self.ctl,'make_plan') as plan:
            result={}; cli.service_pass(self.ctl,args,result)
            self.assertEqual(result['continuation']['mode'],'recover_only'); plan.assert_not_called()

    def test_hard_scoped_error_does_not_skip_independent_plan(self):
        self.ctl.nonblocking=True; self.ctl.issues=[{'code':'TORRENT_DUPLICATE'}]
        args=type('Args',(),{'_completion_scope':[],'max_completions':0,'max_additions':0})()
        with patch.object(self.ctl,'recover'),patch.object(self.store,'pending',return_value=[{'id':'x'}]),patch.object(self.ctl,'global_pending',return_value=False),patch.object(self.ctl,'clean_duplicates'),patch.object(self.ctl,'dispatch'),patch.object(self.ctl,'enforce_slots'),patch.object(self.ctl,'make_plan',return_value={}) as plan,patch.object(self.ctl,'apply_plan'):
            cli.service_pass(self.ctl,args,{})
            plan.assert_called_once()

    def test_progress_failure_is_warning_not_exception(self):
        from qbctl.output import Progress
        progress=Progress(True,True)
        with patch.object(progress,'_read',side_effect=sqlite3.OperationalError('busy')):
            progress({'id':'x'}); progress({'id':'x'})
        self.assertEqual(len(progress.warnings),1)
        self.assertTrue(progress.enabled)

    def test_job_metrics_sum_all_steps(self):
        job={'id':'a'*32,'state':'running','pause_requested':False,'actions':[],
             'selected_completions':[],'max_additions':10,'policy_revision':1}
        result={'result':'ok','actions':[],'issues':[],'observed':{'client_count':1,'allowed_downloads':1},
                'desired':{'target_client_count':1},'api_metrics':{'torrents/files':{'calls':3,'seconds':0.4}},
                'phase_timings':{},'elapsed_seconds':1}
        def update(identifier,change): change(job,self.store); return job
        with patch.object(executor,'update_job',side_effect=update),patch.object(executor,'operation_counts',return_value=(0,set(),None)):
            executor.record_step(job['id'],result); executor.record_step(job['id'],result)
        self.assertEqual(job['performance']['steps'],2)
        self.assertEqual(job['performance']['api_metrics']['torrents/files']['calls'],6)

    def test_readonly_status_entrypoint_does_not_change_recheck_receipt(self):
        self.store.set_setting('rechecks',{self.h:{'requested_at':'2000','phase':'checking','start_observed':True}})
        args=type('Args',(),{'command':'status','apply':False,'request_id':None,'deadline':10,'torrents':False})()
        before=self.store.setting('rechecks')
        with patch.object(cli,'load_config',return_value=self.c),patch.object(cli,'API',return_value=self.api):
            result=cli.execute_locked(args,False)
        self.assertEqual(result['result'],'ok')
        self.assertEqual(self.store.setting('rechecks'),before)

    def test_runtime_diagnostic_failure_keeps_finished_result(self):
        import contextlib
        @contextlib.contextmanager
        def database(*args,**kwargs): yield self.store
        job={'id':'a'*32,'state':'finished','actions':[],'warnings':[],'initial_pending':[],
             'reason':{'code':'FINISHED'},'last_result':{}}
        with patch.object(executor,'database',database),patch.object(executor,'runtime_status',side_effect=ValueError('bad json')):
            result=executor.response(job)
        self.assertEqual(result['result'],'ok')
        self.assertEqual(result['warnings'][0]['code'],'RUNTIME_STATUS_UNAVAILABLE')

    def test_phase_discards_cache_on_error(self):
        a=self.cache_api()
        with patch.object(a,'call',return_value=[]):
            with self.assertRaises(ValueError):
                with a.file_phase():
                    a.files(self.h); raise ValueError('stop')
        self.assertIsNone(a._file_cache)

    def test_parallel_readonly_store_does_not_need_writer_lock(self):
        observed=[]
        def reader():
            s=Store(readonly=True)
            try: observed.append(s.db.execute('SELECT count(*) FROM jobs').fetchone()[0])
            finally: s.close()
        thread=threading.Thread(target=reader); thread.start(); thread.join(5)
        self.assertFalse(thread.is_alive()); self.assertEqual(observed,[0])

    def test_check_gate_records_polls_and_wait_without_planning(self):
        self.api.metrics={'torrents/info':{'calls':1,'seconds':0.01}}
        job={'id':'a'*32,'state':'waiting','current_step':None,'pause_requested':False,
             'policy_revision':1,'last_result':{'issues':[{'code':'CHECK_PENDING'}]}}
        def update(identifier,change): change(job,self.store); return job
        self.api.rows[self.h]['state']='checkingDL'
        with patch.object(executor,'load_config',return_value=self.c),patch('qbctl.api.API',return_value=self.api),patch.object(executor,'update_job',side_effect=update):
            self.assertTrue(executor.observe_check_wait(job))
            self.api.rows[self.h]['state']='stalledUP'
            self.assertFalse(executor.observe_check_wait(job))
        self.assertEqual(job['performance']['check_poll_count'],2)
        self.assertEqual(job['performance']['api_metrics']['torrents/info']['calls'],2)
        self.assertNotIn('check_wait_since',job)
