import copy, hashlib, unittest, uuid
from pathlib import Path
from unittest.mock import patch
from test_reliability import ReliabilityTests
from qbctl import engine,cli
from qbctl.common import Fault,Budget
from qbctl.repair import prepare_prefix,single_info

def encode(value):
    if isinstance(value,int): return b'i'+str(value).encode()+b'e'
    if isinstance(value,bytes): return str(len(value)).encode()+b':'+value
    if isinstance(value,dict): return b'd'+b''.join(encode(k)+encode(value[k]) for k in sorted(value))+b'e'
    if isinstance(value,list): return b'l'+b''.join(encode(v) for v in value)+b'e'
    raise TypeError()

class RepairTests(ReliabilityTests):
    def repair_fixture(self):
        self.single_pair(); self.prefix=self.file.read_bytes()[:5]; self.original=self.file.read_bytes()
        info={b'name':b'shared.bin',b'length':5,b'piece length':3,b'pieces':hashlib.sha1(self.prefix[:3]).digest()+hashlib.sha1(self.prefix[3:]).digest()}
        self.h=hashlib.sha1(encode(info)).hexdigest(); self.data=encode({b'info':info}); self.source.write_bytes(self.data)
        self.api.rows={self.h:{'hash':self.h,'state':'stoppedUP','progress':1,'amount_left':0,'save_path':self.c['paths']['working'],'auto_tmm':False}}
        self.api.contents={self.h:[{'name':'shared.bin','size':5,'progress':1,'priority':1}]}
        self.api.meta[self.data]={'id':self.h,'v1':self.h,'v2':None,'files':[{'path':'shared.bin','length':5}]}
        self.store.put_op({'id':uuid.uuid4().hex,'kind':'isolate_shared','hash':'f'*40,'other_hash':self.h,'stage':'finished','old_relative':'shared.bin','sizes':[len(self.original),5]})
        self.payload={'shared.bin':self.prefix}

    def test_repair_valid_pieces_preserves_all_original_bytes(self):
        self.repair_fixture(); op=prepare_prefix(self.ctl,self.h,True)
        self.assertEqual(self.file.read_bytes(),self.prefix)
        self.assertEqual((Path(self.c['paths']['working'])/op['backup_relative']).read_bytes(),self.original)
        self.assertFalse(self.store.pending()); self.ctl.apply_plan(self.plan()); self.assert_finished()

    def test_repair_invalid_piece_no_mutation(self):
        self.repair_fixture(); self.file.write_bytes(b'wrong'+self.original[5:])
        with self.assertRaises(Fault) as e: prepare_prefix(self.ctl,self.h,True)
        self.assertEqual(e.exception.code,'PIECE_MISMATCH'); self.assertFalse(self.store.pending()); self.assertTrue(self.source.exists())
        self.assertFalse((Path(self.c['paths']['working'])/'_recovery').exists())

    def test_repair_crash_after_backup_recovers(self):
        self.repair_fixture(); original=engine.move_no_replace
        def move(s,d):
            original(s,d)
            if Path(d).name=='original.bin': raise RuntimeError('crash after backup')
        with patch.object(engine,'move_no_replace',move),self.assertRaises(RuntimeError): prepare_prefix(self.ctl,self.h,True)
        self.assertFalse(self.file.exists()); self.restart(); self.ctl.recover()
        self.assertFalse(self.store.pending()); self.assertEqual(self.file.read_bytes(),self.prefix)

    def test_repair_crash_after_install_recovers(self):
        self.repair_fixture(); original=engine.move_no_replace
        def move(s,d):
            original(s,d)
            if Path(d)==self.file: raise RuntimeError('crash after install')
        with patch.object(engine,'move_no_replace',move),self.assertRaises(RuntimeError): prepare_prefix(self.ctl,self.h,True)
        self.restart(); self.ctl.recover(); self.assertFalse(self.store.pending()); self.assertEqual(self.file.read_bytes(),self.prefix)

    def test_repair_crash_before_copy_recovers(self):
        self.repair_fixture(); original=self.ctl.stage
        def stage(o,s):
            original(o,s)
            if s=='copy_requested': raise RuntimeError('before copy')
        with patch.object(self.ctl,'stage',stage),self.assertRaises(RuntimeError): prepare_prefix(self.ctl,self.h,True)
        self.restart(); self.ctl.recover(); self.assertFalse(self.store.pending()); self.assertEqual(self.file.read_bytes(),self.prefix)

    def test_repair_source_started_blocks_recovery(self):
        self.repair_fixture(); op=prepare_prefix(self.ctl,self.h); self.store.put_op(op)
        self.api.rows[self.h]['state']='stalledUP'; self.ctl.recover()
        self.assertTrue(self.store.pending()); self.assertEqual(self.file.read_bytes(),self.original)

    def test_repair_partial_copy_retained_on_restart(self):
        self.repair_fixture()
        def partial(src,dst,size,budget): Path(dst).write_bytes(b'pa'); raise RuntimeError('copy interrupted')
        with patch('qbctl.repair.copy_prefix',partial),self.assertRaises(RuntimeError): prepare_prefix(self.ctl,self.h,True)
        op=self.store.pending()[0]; retained=Path(self.c['paths']['working'])/op['temp_relative']
        self.restart(); self.ctl.recover(); self.assertFalse(self.store.pending())
        self.assertEqual(retained.read_bytes(),b'pa'); self.assertEqual(self.file.read_bytes(),self.prefix)

    def test_repair_requires_isolation_history(self):
        self.repair_fixture(); self.store.db.execute("DELETE FROM operations WHERE kind='isolate_shared'"); self.store.db.commit()
        with self.assertRaises(Fault) as e: prepare_prefix(self.ctl,self.h,True)
        self.assertEqual(e.exception.code,'REPAIR_PRECONDITION'); self.assertEqual(self.file.read_bytes(),self.original)

    def test_repair_source_changed_before_copy_blocked(self):
        self.repair_fixture(); op=prepare_prefix(self.ctl,self.h); self.file.write_bytes(self.original+b'changed')
        with self.assertRaises(Fault): self.ctl.dispatch(op)
        self.assertFalse((Path(self.c['paths']['working'])/op['backup_relative']).exists())

    def test_repair_never_reads_completed_root(self):
        self.repair_fixture(); original=Path.stat
        def stat(p,*a,**kw):
            if p.is_relative_to(Path(self.c['paths']['completed'])): raise AssertionError('k accessed')
            return original(p,*a,**kw)
        with patch.object(Path,'stat',stat): prepare_prefix(self.ctl,self.h,True)

    def test_repair_rejects_v2_or_multifile(self):
        for info in ({b'files':[]},{b'meta version':2}):
            with self.assertRaises(Fault): single_info(encode({b'info':info}))

    def test_default_run_refills_to_target_not_five(self):
        self.assertEqual(cli.parser().parse_args(['run']).max_additions,1000)

    def test_refill_long_check_returns_bounded_partial(self):
        self.api.rows[self.h]['state']='checkingDL'; self.c['policy']['target_client_count']=2
        data=b'new-candidate'; h='b'*40; p=Path(self.c['paths']['incoming'])/'new.torrent'; p.write_bytes(data)
        self.api.meta[data]={'id':h,'v1':h,'v2':None,'files':[{'path':'new.bin','length':10}]}
        self.ctl.budget=Budget(4)
        self.ctl.apply_plan(self.ctl.make_plan(0,1000))
        self.assertTrue(any(i['code']=='CHECK_PENDING' for i in self.ctl.issues)); self.assertFalse(any(c[0]=='add' for c in self.api.calls))

    def test_refill_continues_after_serial_check(self):
        self.api.rows.clear(); self.api.contents.clear(); self.c['policy'].update(target_client_count=2,download_slots=2)
        self.source.unlink()
        for i in range(2):
            data=('candidate-'+str(i)).encode(); h=('%040x'%(i+1)); (Path(self.c['paths']['incoming'])/(str(i)+'.torrent')).write_bytes(data)
            self.api.meta[data]={'id':h,'v1':h,'v2':None,'files':[{'path':str(i)+'.bin','length':10}]}
        original_add=self.api.add; original_rows=self.api.torrents; seen={}
        def add(data,working):
            original_add(data,working); h=self.api.meta[data]['id']; self.api.rows[h]['state']='checkingDL'; seen[h]=0
        def torrents():
            for h in seen:
                if self.api.rows[h]['state']=='checkingDL':
                    seen[h]+=1
                    if seen[h]>=3: self.api.rows[h]['state']='stoppedDL'
            return original_rows()
        with patch.object(self.api,'add',add),patch.object(self.api,'torrents',torrents): self.ctl.apply_plan(self.ctl.make_plan(0,1000))
        self.assertEqual(len(self.api.rows),2); self.assertFalse(self.store.pending())
        self.assertEqual(sum(c[0]=='add' for c in self.api.calls),2)

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(RepairTests(name) for name in RepairTests.__dict__ if name.startswith('test_'))
