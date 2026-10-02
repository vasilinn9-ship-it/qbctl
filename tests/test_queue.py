import unittest
from pathlib import Path
from unittest.mock import patch
from test_protocol import ProtocolTests
from qbctl.queue import trim_queue
from qbctl.common import Fault

class QueueTests(ProtocolTests):
    def queue_fixture(self):
        self.api.rows.clear(); self.api.contents.clear(); self.source.unlink(); self.inputs=[]; self.downloads=[]
        self.c['policy'].update(target_client_count=2,download_slots=2)
        for i in range(4):
            data=('queued-'+str(i)).encode(); h='%040x'%(i+1); p=Path(self.c['paths']['incoming'])/(str(i)+'.torrent'); p.write_bytes(data)
            self.api.meta[data]={'id':h,'v1':h,'v2':None,'files':[{'path':str(i)+'.bin','length':100}]}
            self.api.add(data,self.c['paths']['working']); self.api.rows[h]['added_on']=i
            f=Path(self.c['paths']['working'])/(str(i)+'.bin.!qB'); f.write_bytes(b'partial-'+str(i).encode())
            self.inputs.append(p); self.downloads.append(f)
        self.api.calls.clear()

    def test_trim_preserves_torrents_and_partial_data(self):
        self.queue_fixture(); trim_queue(self.ctl)
        self.assertEqual(len(self.api.rows),2); self.assertFalse(self.store.pending())
        self.assertTrue(all(p.exists() for p in self.inputs+self.downloads))
        self.assertTrue(all(c[2] is False for c in self.api.calls if c[0]=='delete'))

    def test_trim_active_stops_only_removed_tasks(self):
        self.queue_fixture()
        for t in self.api.rows.values(): t['state']='downloading'
        trim_queue(self.ctl)
        self.assertEqual(sum(c[0]=='stop' for c in self.api.calls),2); self.assertEqual(len(self.api.rows),2)

    def test_trim_prefers_stopped_tasks(self):
        self.queue_fixture(); running=list(self.api.rows)[:2]
        for h in running: self.api.rows[h]['state']='downloading'
        trim_queue(self.ctl); self.assertEqual(set(self.api.rows),set(running)); self.assertFalse(any(c[0]=='stop' for c in self.api.calls))

    def test_trim_unknown_delete_recovers_without_payload_deletion(self):
        self.queue_fixture(); self.api.delete_ignored=True
        with self.assertRaises(Fault): trim_queue(self.ctl)
        self.assertTrue(self.store.pending()); self.api.delete_ignored=False; self.ctl.recover()
        trim_queue(self.ctl); self.assertEqual(len(self.api.rows),2); self.assertTrue(all(p.exists() for p in self.downloads))

    def test_trim_crash_after_delete_recovers(self):
        self.queue_fixture(); original=self.api.remove
        def remove(h): original(h); raise RuntimeError('delete response loss')
        with patch.object(self.api,'remove',remove),self.assertRaises(RuntimeError): trim_queue(self.ctl)
        self.ctl.recover(); trim_queue(self.ctl); self.assertFalse(self.store.pending()); self.assertEqual(len(self.api.rows),2)

    def test_trim_no_unique_source_keeps_record(self):
        self.queue_fixture()
        for p in self.inputs: p.unlink()
        with self.assertRaises(Fault): trim_queue(self.ctl)
        self.assertEqual(len(self.api.rows),4); self.assertFalse(any(c[0]=='delete' for c in self.api.calls))

    def test_run_above_target_all_completed_can_finish(self):
        self.queue_fixture()
        for h,t in self.api.rows.items():
            t.update(state='stalledUP',progress=1,amount_left=0)
            for f in self.api.contents[h]:
                f['progress']=1
                (Path(self.c['paths']['working'])/f['name']).write_bytes(b'x'*f['size'])
        self.ctl.apply_plan(self.ctl.make_plan(4,0))
        self.assertFalse(self.api.rows); self.assertFalse(self.store.pending())

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(QueueTests(name) for name in QueueTests.__dict__ if name.startswith('test_'))
