import copy, json, sqlite3, unittest
from pathlib import Path
from unittest.mock import patch
from test_protocol import ProtocolTests
from qbctl import cli, common, engine, resources
from qbctl.common import Fault, Store, validate_roots
from qbctl.registry import aliases, disk_admission, finished_aliases

class UpgradeTests(ProtocolTests):
    # Reuse fixture and also run the original protocol against the new implementation.
    def test_handed_off_file_never_stat_on_recovery(self):
        plan=self.plan(); original=self.api.remove
        def remove(h): original(h); raise RuntimeError('response loss')
        with patch.object(self.api,'remove',remove),self.assertRaises(RuntimeError): self.ctl.apply_plan(plan)
        for relative in self.payload:
            (Path(self.c['paths']['completed'])/relative).unlink()
        original_stat=Path.stat
        def stat(p,*a,**kw):
            if p.is_relative_to(Path(self.c['paths']['completed'])): raise AssertionError('k access after handoff')
            return original_stat(p,*a,**kw)
        with patch.object(Path,'stat',stat): self.ctl.recover()
        self.assertFalse(self.store.pending()); self.assertIsNone(self.api.get(self.h))

    def test_finished_replay_does_not_read_k(self):
        plan=self.plan(); self.ctl.apply_plan(plan)
        original_stat=Path.stat
        def stat(p,*a,**kw):
            if p.is_relative_to(Path(self.c['paths']['completed'])): raise AssertionError('k access')
            return original_stat(p,*a,**kw)
        with patch.object(Path,'stat',stat): self.ctl.dispatch(plan['operations'][0]); self.ctl.audit()

    def test_processed_hash_excluded_without_archive(self):
        plan=self.plan(); self.ctl.apply_plan(plan)
        (Path(self.c['paths']['archive'])/self.source.name).unlink(); self.source.write_bytes(self.data)
        self.assertIn(self.h,finished_aliases(self.store))
        p=self.ctl.make_plan(0,10); self.assertFalse(p['additions'])
        self.assertIn('ALREADY_PROCESSED_INPUT',[i['code'] for i in p['issues']])

    def test_duplicate_incoming_and_archive_report(self):
        (Path(self.c['paths']['incoming'])/'copy.torrent').write_bytes(self.data)
        (Path(self.c['paths']['archive'])/'archived.torrent').write_bytes(self.data)
        p=self.plan(); self.assertFalse(p['operations']); self.assertIn('TORRENT_DUPLICATE',[i['code'] for i in p['issues']])
        self.assertEqual(p['registry']['indexed'],3)

    def test_hybrid_alias_blocks_duplicate(self):
        data=b'hybrid'; h2='b'*64
        self.api.meta[data]={'id':h2,'v1':self.h,'v2':h2,'files':self.api.meta[self.data]['files']}
        (Path(self.c['paths']['incoming'])/'hybrid.torrent').write_bytes(data)
        p=self.plan(); self.assertFalse(p['operations']); self.assertIn(self.h,p['registry']['blocked_hashes'])

    def test_historical_archive_unverified(self):
        self.source.replace(Path(self.c['paths']['archive'])/self.source.name); self.api.rows.clear()
        r=self.ctl.audit(); self.assertEqual(r['registry']['historical_unverified'],[self.h]); self.assertFalse(r['completed_data_audited'])

    def test_dedupe_quarantine_and_no_payload_mutation(self):
        other=Path(self.c['paths']['incoming'])/'copy.torrent'; other.write_bytes(self.data)
        op=self.ctl.dedupe(self.h,str(self.source),True)
        self.assertTrue(self.source.exists()); self.assertFalse(other.exists()); self.assertTrue(Path(op['files'][0]['destination']).is_file())
        self.assertFalse(self.api.calls); self.assertTrue((Path(self.c['paths']['working'])/next(iter(self.payload))).is_file())

    def test_dedupe_cannot_keep_root_for_archive(self):
        (Path(self.c['paths']['archive'])/'copy.torrent').write_bytes(self.data)
        with self.assertRaises(Fault): self.ctl.dedupe(self.h,str(self.source),True)
        self.assertTrue(self.source.exists())

    def test_dedupe_crash_after_rename(self):
        other=Path(self.c['paths']['incoming'])/'copy.torrent'; other.write_bytes(self.data)
        original=engine.move_no_replace
        def move(s,d): original(s,d); raise RuntimeError('crash')
        with patch.object(engine,'move_no_replace',move),self.assertRaises(RuntimeError): self.ctl.dedupe(self.h,str(self.source),True)
        self.ctl.recover(); self.assertFalse(self.store.pending()); self.assertTrue(self.source.exists())

    def test_disk_reservation_includes_other_downloads(self):
        self.api.rows[self.h]['amount_left']=100
        class Usage: free=1024**3+149
        with patch('qbctl.registry.shutil.disk_usage',return_value=Usage()):
            r=disk_admission(self.ctl,50)
        self.assertFalse(r['ok']); self.assertEqual(r['reserved_bytes'],100)

    def test_low_disk_stops_preserves_payload(self):
        self.api.rows[self.h].update(state='downloading',progress=0,amount_left=10)
        class Usage: free=1
        with patch('qbctl.registry.shutil.disk_usage',return_value=Usage()): self.ctl.enforce_slots()
        self.assertTrue(engine.stopped(self.api.rows[self.h])); self.assertEqual(self.store.stop_reason(self.h),'disk'); self.assertTrue(self.source.exists())

    def test_invalid_path_is_stopped(self):
        self.api.rows[self.h].update(state='downloading',progress=0,amount_left=10,save_path=str(self.root))
        self.ctl.enforce_slots(); self.assertTrue(engine.stopped(self.api.rows[self.h])); self.assertEqual(self.store.stop_reason(self.h),'path_policy')

    def test_manual_resume_live_gate(self):
        self.api.rows[self.h].update(state='stoppedDL',progress=0,amount_left=10)
        b='b'*40; self.api.rows[b]={**self.api.rows[self.h],'hash':b,'state':'downloading'}
        o={'id':'gate','kind':'control','hash':self.h,'stage':'prepared','tasks':[{'hash':self.h,'action':'resume'}]}
        with self.assertRaises(Fault) as caught: self.ctl.dispatch(o)
        self.assertEqual(caught.exception.code,'SLOTS_EXCEEDED'); self.assertFalse(any(c[0]=='start' for c in self.api.calls))

    def test_policy_newer_than_pending_is_not_overwritten(self):
        o={'id':'old','kind':'control','hash':'policy','stage':'prepared','policy_revision':0,'changes':{'policy':{'download_slots':999}},'tasks':[]}
        with self.assertRaises(Fault): self.ctl.dispatch(o)
        self.assertEqual(self.c['policy']['download_slots'],1)

    def test_secondary_log_failure_does_not_undo_db(self):
        op=self.plan()['operations'][0]
        original=Path.open
        def opening(p,*a,**kw):
            if p.name=='operations.jsonl': raise PermissionError('log unavailable')
            return original(p,*a,**kw)
        with patch.object(Path,'open',opening): self.ctl.dispatch(op)
        self.assertFalse(self.store.pending()); self.assertTrue(self.store.log_warnings)

    def test_migration_backup_and_no_k_access(self):
        op=self.plan()['operations'][0]; self.ctl.dispatch(op)
        self.store.db.execute('PRAGMA user_version=0'); self.store.db.execute('DROP TABLE registry'); self.store.db.commit(); self.store.close()
        original_stat=Path.stat
        def stat(p,*a,**kw):
            if p.is_relative_to(Path(self.c['paths']['completed'])): raise AssertionError('migration read k')
            return original_stat(p,*a,**kw)
        with patch.object(Path,'stat',stat): self.store=Store(migrate=True)
        self.assertEqual(self.store.registry()[0]['state'],'finished'); self.assertTrue(list((self.root/'backups').glob('*.sqlite')))
        self.ctl.store=self.store

    def test_root_overlap_rejected(self):
        paths={**self.c['paths'],'working':str(Path(self.c['paths']['incoming'])/'m')}
        with self.assertRaises(Fault): validate_roots(paths,self.root/'app')

    def test_invalid_alias_rejected(self):
        with self.assertRaises(Fault): aliases({'id':self.h,'v2':'not-a-hash'})

    def test_unknown_add_not_repeated_then_explicit_cancel(self):
        self.api.rows.clear()
        op={'id':'unknown','kind':'add','hash':self.h,'stage':'add_requested','path':str(self.source),'sha256':common.sha_file(self.source)}
        self.store.put_op(op)
        with self.assertRaises(Fault): self.ctl.dispatch(op)
        self.assertFalse(any(c[0]=='add' for c in self.api.calls))
        self.ctl.resolve_add('unknown','cancel',True); self.assertFalse(self.store.pending())

    def test_request_replay_complete_and_conflict_stable(self):
        def arguments(extra):
            a=cli.parser().parse_args(['pause','--hash',self.h,'--apply','--json','--request-id','r1']+extra)
            for k,v in {'deadline':30,'events_jsonl':False}.items():
                if not hasattr(a,k): setattr(a,k,v)
            return a
        args=arguments([])
        with patch.object(cli,'ROOT',self.root),patch.object(cli,'load_config',return_value=self.c),patch.object(cli,'API',return_value=self.api):
            first=cli.execute(args); calls=len(self.api.calls); replay=cli.execute(args)
            self.assertEqual(len(self.api.calls),calls); self.assertTrue(replay['response_replayed']); self.assertEqual(first['next_safe_commands'],replay['next_safe_commands'])
            conflict=copy.deepcopy(args); conflict.hash='b'*40
            self.assertEqual(cli.execute(conflict)['issues'][0]['code'],'REQUEST_ID_CONFLICT')
            again=cli.execute(args); self.assertEqual(again['actions'],first['actions'])

    def test_resource_gap_resets_timer(self):
        self.c['resources'].update(required_metrics=['cpu'],unknown_policy='hold',threshold=50,resume_below=40,high_seconds=20,low_seconds=60,step=1,sample_seconds=5)
        self.store.set_setting('resource_state',{'last_sample':0,'high_since':1})
        with patch.object(resources.time,'time',return_value=1000),patch.object(self.ctl,'enforce_slots'):
            resources.regulate(self.ctl,{'cpu':90})
        self.assertEqual(self.store.setting('effective_slots'),1); self.assertEqual(self.store.setting('resource_state')['high_since'],1000)

    def test_first_handoff_deleted_while_second_pending(self):
        plan=self.plan(); original=engine.move_no_replace; count=0
        def move(s,d):
            nonlocal count
            count+=1
            if count==3: raise RuntimeError('before second payload rename')
            original(s,d)
        with patch.object(engine,'move_no_replace',move),self.assertRaises(RuntimeError): self.ctl.apply_plan(plan)
        op=self.store.pending()[0]; first=Path(op['files'][0]['destination']); first.unlink()
        original_stat=Path.stat
        def stat(p,*a,**kw):
            if p==first: raise AssertionError('confirmed first file accessed')
            return original_stat(p,*a,**kw)
        with patch.object(Path,'stat',stat): self.ctl.recover()
        self.assertFalse(self.store.pending()); self.assertTrue(Path(op['files'][1]['destination']).is_file())

    def test_change_selection_after_stop_blocks_archive(self):
        op=self.plan()['operations'][0]; original=self.api.stop
        def stop(h): original(h); self.api.contents[h][0]['priority']=0
        with patch.object(self.api,'stop',stop),self.assertRaises(Fault) as caught: self.ctl.dispatch(op)
        self.assertEqual(caught.exception.code,'PARTIAL_SELECTION'); self.assertTrue(self.source.exists())

    def test_restart_store_preserves_policy_and_progress(self):
        self.api.rows[self.h].update(state='downloading',progress=0.5,amount_left=10)
        self.c['policy']['download_slots']=0; self.ctl.enforce_slots(); self.store.close(); self.store=Store(); self.ctl.store=self.store
        self.assertEqual(self.store.stop_reason(self.h),'slots'); self.assertEqual(self.api.rows[self.h]['progress'],0.5)
        self.c['policy']['download_slots']=1; self.ctl.enforce_slots(); self.assertFalse(engine.stopped(self.api.rows[self.h]))

    def test_request_interrupted_recovers_only_recorded_receipts(self):
        op=self.plan()['operations'][0]; op['request_id']='interrupted'; self.ctl.dispatch(op)
        args=cli.parser().parse_args(['run','--apply','--json','--request-id','interrupted'])
        for k,v in {'deadline':30,'events_jsonl':False}.items():
            if not hasattr(args,k): setattr(args,k,v)
        signature=common.digest({k:v for k,v in vars(args).items() if k not in ('json','events_jsonl','request_id')})
        self.store.db.execute('INSERT INTO requests VALUES(?,?,NULL)',('interrupted',signature)); self.store.db.commit(); calls=len(self.api.calls)
        with patch.object(cli,'load_config',return_value=self.c),patch.object(cli,'API',return_value=self.api): r=cli.execute(args)
        self.assertEqual(len(self.api.calls),calls); self.assertTrue(r['response_replayed']); self.assertEqual(r['result'],'partial')

    def test_fifty_five_to_thirty_to_zero_and_back(self):
        self.api.rows.clear()
        for i in range(55):
            h=f'{i:040x}'; self.api.rows[h]={'hash':h,'state':'downloading','progress':0.25,'amount_left':10,'save_path':self.c['paths']['working'],'priority':i,'added_on':i}
        for limit in (30,0,55):
            # Each user command gets its own budget. This integration case
            # permits slow FULL commits; deadline behavior is tested separately.
            self.ctl.budget=common.Budget(120)
            self.c['policy']['download_slots']=limit; self.ctl.enforce_slots()
            self.assertEqual(sum(not engine.stopped(t) for t in self.api.rows.values()),limit)
            self.assertEqual(len(self.api.rows),55); self.assertTrue(all(t['progress']==0.25 for t in self.api.rows.values()))

    def test_incomplete_client_missing_source_reported(self):
        self.source.unlink(); self.api.rows[self.h].update(progress=0,state='stalledDL',amount_left=10)
        r=self.ctl.audit(); self.assertIn('CLIENT_SOURCE_MISSING',[i['code'] for i in r['findings']])

    def test_result_matches_schema(self):
        schema=json.loads((Path(__file__).parent.parent/'result.schema.json').read_text(encoding='utf-8'))
        args=cli.parser().parse_args(['pause','--hash',self.h,'--apply','--json'])
        for k,v in {'deadline':30,'events_jsonl':False,'request_id':None}.items():
            if not hasattr(args,k): setattr(args,k,v)
        with patch.object(cli,'load_config',return_value=self.c),patch.object(cli,'API',return_value=self.api): result=cli.execute(args)
        def validate(value,s):
            if 'const' in s: self.assertEqual(value,s['const'])
            if 'enum' in s: self.assertIn(value,s['enum'])
            kind=s.get('type')
            if kind:
                types={'object':dict,'array':list,'string':str,'integer':int,'number':(int,float),'boolean':bool}
                self.assertIsInstance(value,types[kind])
                if kind=='integer': self.assertNotIsInstance(value,bool)
            if isinstance(value,dict):
                for k in s.get('required',[]): self.assertIn(k,value)
                for k,child in s.get('properties',{}).items():
                    if k in value: validate(value[k],child)
            if isinstance(value,list) and 'items' in s:
                for item in value: validate(item,s['items'])
            if 'minimum' in s: self.assertGreaterEqual(value,s['minimum'])
        validate(result,schema); json.dumps(result,ensure_ascii=False)

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(UpgradeTests(name) for name in UpgradeTests.__dict__ if name.startswith('test_'))

if __name__=='__main__': unittest.main(verbosity=2)
