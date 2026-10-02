import copy, json, time, unittest, uuid, argparse,threading
from pathlib import Path
from unittest.mock import patch
from test_protocol import ProtocolTests
from qbctl import engine,registry,cli
from qbctl.common import Fault,Budget,Store

class ServiceTests(ProtocolTests):
    def start_op(self):
        self.api.rows[self.h].update(state='stoppedDL',progress=0.2,amount_left=100)
        return {'id':uuid.uuid4().hex,'kind':'control','hash':self.h,'stage':'prepared','tasks':[{'hash':self.h,'action':'resume'}]}

    def test_delayed_start_one_request_no_repeated_preflight(self):
        o=self.start_op(); self.ctl.service_mode=True; old_get=self.api.get; requests=[]; observations=0
        def start(h): requests.append(h)
        def wait(h,predicate,seconds=2):
            nonlocal observations
            observations+=1
            if observations>=3: self.api.rows[h]['state']='downloading'
            return old_get(h)
        with patch.object(self.api,'start',start),patch.object(self.ctl,'wait_state',wait),patch.object(self.api,'files',wraps=self.api.files) as files:
            self.ctl.dispatch(o)
        self.assertEqual(requests,[self.h]); self.assertEqual(files.call_count,1)  # Own hash is excluded from ownership API reads (0.2.10).
        self.assertFalse(self.store.pending()); self.assertGreaterEqual(self.ctl.service_waits[o['id']]['polls'],2)

    def test_unknown_start_not_sent_again_after_restart(self):
        o=self.start_op(); calls=[]
        def start(h): calls.append(h); raise Fault('MUTATION_UNCERTAIN','response lost','unknown')
        with patch.object(self.api,'start',start),self.assertRaises(Fault): self.ctl.dispatch(o)
        self.store.close(); self.store=Store(); self.ctl.store=self.store; self.ctl.service_mode=True
        self.ctl.recover(); self.assertEqual(calls,[self.h]); self.assertTrue(self.store.pending())
        self.assertEqual(self.ctl.issues[-1]['code'],'START_UNCONFIRMED')

    def test_unknown_start_observed_active_finishes_without_resend(self):
        o=self.start_op(); calls=[]
        def start(h): calls.append(h); self.api.rows[h]['state']='downloading'; raise Fault('MUTATION_UNCERTAIN','lost','unknown')
        with patch.object(self.api,'start',start),self.assertRaises(Fault): self.ctl.dispatch(o)
        with patch.object(self.api,'start',start): self.ctl.recover()
        self.assertEqual(calls,[self.h]); self.assertFalse(self.store.pending())

    def test_accepted_start_restart_observes_only(self):
        o=self.start_op(); self.ctl.service_mode=False; requests=[]
        with patch.object(self.api,'start',side_effect=lambda h:requests.append(h)),patch.object(self.ctl,'wait_state',return_value=self.api.get(self.h)),self.assertRaises(Fault): self.ctl.dispatch(o)
        self.store.close(); self.store=Store(); self.ctl.store=self.store; self.ctl.service_mode=True
        self.api.rows[self.h]['state']='downloading'
        with patch.object(self.api,'start',side_effect=lambda h:requests.append(h)): self.ctl.recover()
        self.assertEqual(requests,[self.h]); self.assertFalse(self.store.pending())

    def test_short_budget_does_not_journal_new_start(self):
        o=self.start_op(); self.ctl.service_mode=True; self.ctl.budget=Budget(0.5)
        with self.assertRaises(Fault) as e: self.ctl.dispatch(o)
        self.assertEqual(e.exception.code,'SERVICE_DEFERRED'); self.assertFalse(self.store.pending()); self.assertFalse(self.api.calls)

    def test_same_working_volume_checked_once_per_admission(self):
        self.api.rows.clear()
        for i in range(50): self.api.rows['%040x'%i]={'hash':'%040x'%i,'amount_left':i+1,'save_path':self.c['paths']['working']}
        with patch.object(registry,'guarded',wraps=registry.guarded) as guard:
            result=registry.disk_admission(self.ctl)
        self.assertEqual(guard.call_count,1); self.assertEqual(result['reserved_bytes'],1275)
        self.api.rows[next(iter(self.api.rows))]['amount_left']=1001
        self.assertEqual(registry.disk_admission(self.ctl)['reserved_bytes'],2275)

    def test_run_initial_completion_scope_frozen(self):
        self.ctl.initial_completions=set()
        self.assertFalse(self.ctl.make_plan(5,0)['operations'])

    def test_parallel_ownership_reads_bounded_and_complete(self):
        self.api.rows.clear(); rows=[]
        for i in range(12): rows.append({'hash':'%040x'%i,'save_path':self.c['paths']['working']})
        active=0; maximum=0; lock=threading.Lock()
        def files(h):
            nonlocal active,maximum
            with lock: active+=1; maximum=max(maximum,active)
            time.sleep(0.02)
            with lock: active-=1
            return [{'name':h+'.bin'}]
        with patch.object(self.api,'files',files): result=self.ctl.owned_paths(rows)
        self.assertEqual(len(result),12); self.assertGreater(maximum,1); self.assertLessEqual(maximum,4)

    def test_known_long_start_returns_waiting_without_resend(self):
        o=self.start_op(); requests=[]; self.ctl.service_mode=True; self.ctl.budget=Budget(4)
        with patch.object(self.api,'start',side_effect=lambda h:requests.append(h)),patch.object(self.ctl,'wait_state',return_value=self.api.get(self.h)),self.assertRaises(Fault) as e: self.ctl.dispatch(o)
        self.assertIn(e.exception.code,('SERVICE_WAITING','BUDGET_EXHAUSTED')); self.assertEqual(requests,[self.h]); self.assertTrue(self.store.pending())

    def test_delayed_pause_one_request(self):
        self.api.rows[self.h]['state']='downloading'; o={'id':uuid.uuid4().hex,'kind':'control','hash':self.h,'stage':'prepared','tasks':[{'hash':self.h,'action':'pause','reason':'user'}]}
        self.ctl.service_mode=True; calls=[]; n=0
        def wait(h,predicate,seconds=2):
            nonlocal n
            n+=1
            if n>=2: self.api.rows[h]['state']='stoppedDL'
            return self.api.get(h)
        with patch.object(self.api,'stop',side_effect=lambda h:calls.append(h)),patch.object(self.ctl,'wait_state',wait): self.ctl.dispatch(o)
        self.assertEqual(calls,[self.h]); self.assertFalse(self.store.pending())

    def test_completed_stop_delayed_is_not_sent_again(self):
        op=self.plan()['operations'][0]; self.ctl.service_mode=True; calls=[]; n=0
        def wait(h,predicate,seconds=2):
            nonlocal n
            n+=1
            if n>=2: self.api.rows[h]['state']='stoppedUP'
            return self.api.get(h)
        with patch.object(self.api,'stop',side_effect=lambda h:calls.append(h)),patch.object(self.ctl,'wait_state',wait): self.ctl.dispatch(op)
        self.assertEqual(calls,[self.h]); self.assert_finished()

    def test_start_error_is_not_verified(self):
        o=self.start_op()
        def start(h): self.api.rows[h]['state']='error'
        with patch.object(self.api,'start',start),patch.object(self.ctl,'wait_state',return_value={**self.api.get(self.h),'state':'error'}),self.assertRaises(Fault) as e: self.ctl.dispatch(o)
        self.assertEqual(e.exception.code,'START_FAILED'); self.assertTrue(self.store.pending()); self.assertFalse(any(a['action']=='resume' for a in self.ctl.actions))

    def test_cli_partial_still_has_fresh_observed_and_service(self):
        args=cli.parser().parse_args(['run','--apply','--deadline','2','--json']); args.request_id=None
        with patch.object(cli,'ROOT',self.root),patch.object(cli,'load_config',return_value=self.c),patch.object(cli,'API',return_value=self.api): result=cli.execute(args)
        self.assertEqual(result['result'],'partial'); self.assertIn('observed',result); self.assertIn('service',result)
        self.assertTrue(self.source.exists()); self.assertFalse(any(c[0]=='delete' for c in self.api.calls))

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ServiceTests(name) for name in ServiceTests.__dict__ if name.startswith('test_'))
