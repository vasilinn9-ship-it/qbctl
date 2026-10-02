import copy, json, unittest
from pathlib import Path
from unittest.mock import patch
from test_protocol import ProtocolTests
from qbctl import common, engine
from qbctl.common import Fault, Store


class EfficiencyTests(ProtocolTests):
    def archive(self, data=None, name='archive.torrent'):
        p=Path(self.c['paths']['archive'])/name
        p.write_bytes(self.data if data is None else data)
        return p

    def test_cleanup_preview_no_mutation(self):
        keeper=self.archive()
        r=self.ctl.clean_duplicates()
        self.assertEqual(r['candidate_count'],1)
        self.assertTrue(self.source.exists()); self.assertTrue(keeper.exists())
        self.assertFalse(self.store.pending()); self.assertFalse(self.api.calls)

    def test_cleanup_different_names_same_hash_and_no_k_reads(self):
        keeper=self.archive(); original=Path.stat
        def stat(p,*a,**kw):
            if p.is_relative_to(Path(self.c['paths']['completed'])): raise AssertionError('read k')
            return original(p,*a,**kw)
        with patch.object(Path,'stat',stat): self.ctl.clean_duplicates(True)
        self.assertFalse(self.source.exists()); self.assertEqual(keeper.read_bytes(),self.data)
        self.assertFalse(self.store.pending()); self.assertFalse(self.api.calls)

    def test_cleanup_same_hash_different_metainfo_bytes(self):
        data=b'changed trackers, unchanged info'
        self.api.meta[data]=copy.deepcopy(self.api.meta[self.data]); keeper=self.archive(data)
        self.ctl.clean_duplicates(True)
        self.assertFalse(self.source.exists()); self.assertEqual(keeper.read_bytes(),data)

    def test_cleanup_same_name_different_hash_preserved(self):
        data=b'different info'; self.api.meta[data]={**self.api.meta[self.data],'id':'b'*40,'v1':'b'*40}
        self.archive(data,self.source.name)
        self.assertEqual(self.ctl.clean_duplicates(True)['candidate_count'],0)
        self.assertTrue(self.source.exists())

    def test_cleanup_archive_changed_blocks(self):
        keeper=self.archive(); o=self.ctl.clean_duplicates()['operations'][0]
        keeper.write_bytes(b'changed')
        with self.assertRaises(Fault): self.ctl.dispatch(o)
        self.assertTrue(self.source.exists())

    def test_cleanup_source_changed_blocks(self):
        self.archive(); o=self.ctl.clean_duplicates()['operations'][0]
        self.source.write_bytes(b'changed')
        with self.assertRaises(Fault): self.ctl.dispatch(o)
        self.assertTrue(self.source.exists())

    def test_cleanup_pending_deferred(self):
        o=self.plan()['operations'][0]; self.store.put_op(o); self.archive()
        r=self.ctl.clean_duplicates(True)
        self.assertEqual(r['candidate_count'],0); self.assertEqual(len(r['skipped']),1)
        self.assertTrue(self.source.exists())

    def test_cleanup_crash_after_unlink_recovers_after_store_restart(self):
        keeper=self.archive(); original=Path.unlink
        def unlink(p,*a,**kw):
            original(p,*a,**kw)
            if p==self.source: raise RuntimeError('power loss simulation')
        with patch.object(Path,'unlink',unlink),self.assertRaises(RuntimeError): self.ctl.clean_duplicates(True)
        self.assertEqual(self.store.pending()[0]['stage'],'delete_requested')
        self.store.close(); self.store=Store(); self.ctl.store=self.store
        self.ctl.recover(); self.assertFalse(self.store.pending()); self.assertTrue(keeper.exists())

    def test_cleanup_absent_before_intent_blocks(self):
        self.archive(); o=self.ctl.clean_duplicates()['operations'][0]; self.source.unlink()
        with self.assertRaises(Fault) as caught: self.ctl.dispatch(o)
        self.assertEqual(caught.exception.code,'RECOVERY_AMBIGUOUS')

    def test_cleanup_recovery_changed_recreated_source_preserved(self):
        self.archive(); o=self.ctl.clean_duplicates()['operations'][0]
        o['stage']='delete_requested'; self.store.put_op(o); self.source.write_bytes(b'new root file')
        self.ctl.recover(); self.assertEqual(self.source.read_bytes(),b'new root file')
        self.assertTrue(self.store.pending())

    def test_deferred_run_preflight_once(self):
        p=self.ctl.make_plan(1,0,defer_manifests=True)
        self.assertEqual(p['completion_scope'],'single_snapshot')
        self.assertNotIn('preflight',self.ctl.timings)
        self.ctl.apply_plan(p)
        self.assertEqual(self.ctl.timings['preflight']['calls'],1); self.assert_finished()

    def test_deferred_stale_source_preserved(self):
        p=self.ctl.make_plan(1,0,defer_manifests=True)
        self.source.write_bytes(b'changed')
        with self.assertRaises((Fault,KeyError)): self.ctl.apply_plan(p)
        self.assertTrue(self.source.exists()); self.assertFalse(self.api.calls)

    def test_memory_index_detects_modified_file(self):
        self.ctl.index(); data=b'new info bytes'
        self.api.meta[data]={**self.api.meta[self.data],'id':'b'*40,'v1':'b'*40}; self.source.write_bytes(data)
        entries=self.ctl.index(); self.assertEqual(entries[0]['id'],'b'*40)

    def test_targeted_receipt_updates_one_of_thousand_rows_atomically(self):
        o=self.plan()['operations'][0]
        o['files']=[{**o['files'][0],'relative':str(i)} for i in range(1000)]
        self.store.put_op(o); trace=[]; self.store.db.set_trace_callback(trace.append)
        o['files'][500]['handoff']='handed_off'; self.store.put_op(o,file_ordinal=500)
        self.store.db.set_trace_callback(None)
        writes=[q for q in trace if q.startswith('INSERT INTO operation_files')]
        self.assertEqual(len(writes),1)
        self.assertEqual(self.store.registry()[0]['handoffs'],1)
        loaded=self.store.pending()[0]; self.assertEqual(len(loaded['files']),1000)
        self.assertEqual(loaded['files'][500]['handoff'],'handed_off')

    def test_targeted_receipt_failed_transaction_rolls_back(self):
        o=self.plan()['operations'][0]; self.store.put_op(o)
        self.store.db.execute("CREATE TRIGGER reject_receipt BEFORE INSERT ON events BEGIN SELECT RAISE(ABORT,'failure'); END"); self.store.db.commit()
        o['files'][0]['handoff']='handed_off'
        import sqlite3
        with self.assertRaises(sqlite3.Error): self.store.put_op(o,file_ordinal=0)
        self.assertEqual(self.store.pending()[0]['files'][0]['handoff'],'prepared')
        self.assertEqual(self.store.registry()[0]['handoffs'],0)

    def test_snapshot_progress_no_k_reads(self):
        o=self.plan()['operations'][0]; self.store.put_op(o)
        row=self.ctl.snapshot()['operation_progress'][0]
        self.assertEqual(row['files_remaining'],2); self.assertIsNotNone(row['last_activity_at'])

    def test_budget_exhaustion_preserves_source_and_progress(self):
        self.api.rows[self.h].update(state='downloading',progress=0.5,amount_left=10)
        self.c['policy']['download_slots']=0; self.ctl.budget=common.Budget(0)
        with self.assertRaises(Fault) as caught: self.ctl.enforce_slots()
        self.assertEqual(caught.exception.code,'BUDGET_EXHAUSTED')
        self.assertEqual(self.api.rows[self.h]['progress'],0.5); self.assertTrue(self.source.exists())

    def test_deferred_conflicting_payload_skipped_without_mutation(self):
        p=self.ctl.make_plan(1,0,defer_manifests=True)
        target=Path(self.c['paths']['completed'])/next(iter(self.payload))
        target.parent.mkdir(parents=True); target.write_bytes(b'foreign')
        self.ctl.apply_plan(p)
        self.assertTrue(self.source.exists()); self.assertEqual(target.read_bytes(),b'foreign')
        self.assertIn('DESTINATION_EXISTS',[i['code'] for i in self.ctl.issues]); self.assertFalse(self.api.calls)

    def test_unchanged_registry_no_rewrites(self):
        self.ctl.audit(); before=self.store.registry(); trace=[]
        self.store.db.set_trace_callback(trace.append); r=self.ctl.audit(); self.store.db.set_trace_callback(None)
        self.assertEqual(r['registry']['changed_rows'],0); self.assertEqual(self.store.registry(),before)
        self.assertFalse(any(q.startswith('INSERT OR REPLACE INTO registry') for q in trace))

    def test_deferred_run_never_processes_new_completions(self):
        p=self.ctl.make_plan(1,0,defer_manifests=True)
        other='b'*40; self.api.rows[other]={**self.api.rows[self.h],'hash':other}; self.api.contents[other]=[]
        # Client membership change invalidates the old snapshot, rather than
        # silently enlarging its scope.
        with self.assertRaises(Fault) as caught: self.ctl.apply_plan(p)
        self.assertEqual(caught.exception.code,'PLAN_STALE'); self.assertFalse(self.api.calls)


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(EfficiencyTests(name) for name in EfficiencyTests.__dict__ if name.startswith('test_'))

if __name__=='__main__': unittest.main(verbosity=2)
