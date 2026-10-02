"""Measure isolated journal writes locally; never access real torrent/payload state."""
import json, tempfile, time, uuid
from pathlib import Path
from qbctl import common
from qbctl.common import Store, now

root=Path(__file__).resolve().parent
folder=root/'benchmarks'; folder.mkdir(exist_ok=True)
with tempfile.TemporaryDirectory(prefix='receipts-',dir=folder) as temporary:
    sandbox=Path(temporary)
    assert sandbox.resolve().is_relative_to(folder.resolve())
    previous=common.ROOT; common.ROOT=sandbox; store=None
    try:
        store=Store(); h='a'*40
        op={'id':uuid.uuid4().hex,'hash':h,'kind':'complete','stage':'moving',
            'archive_torrent':str(sandbox/'example.torrent'),'torrent_sha256':'0'*64,
            'files':[{'relative':f'{i}.bin','size':1,'handoff':'move_requested'} for i in range(1000)]}
        start=time.monotonic(); store.put_op(op); initial=time.monotonic()-start
        trace=[]; store.db.set_trace_callback(trace.append); start=time.monotonic()
        for i in range(20):
            op['files'][i].update(handoff='handed_off',handed_off_at=now())
            store.put_op(op,file_ordinal=i)
        elapsed=time.monotonic()-start; store.db.set_trace_callback(None)
        result={'created_at':now(),'manifest_rows':1000,'receipts':20,'file_row_writes':sum(q.startswith('INSERT INTO operation_files') for q in trace),
            'initial_write_seconds':round(initial,4),'receipts_seconds':round(elapsed,4),'seconds_per_receipt':round(elapsed/20,4),
            'handoffs_recorded':store.registry()[0]['handoffs'],'live_client_mutated':False,'payload_accessed':False,
            'scope':'SQLite storage benchmark, not full transfer throughput'}
        (folder/'receipt-results.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
        print(json.dumps(result))
    finally:
        if store: store.close()
        common.ROOT=previous
