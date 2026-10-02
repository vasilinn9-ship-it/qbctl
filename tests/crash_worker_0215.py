import json,os,sys
from pathlib import Path
root=Path(__file__).resolve().parent.parent
sys.path[:0]=[str(root),str(root/'tests')]
from qbctl import common,engine,executor
from qbctl.common import Store,Budget
from test_protocol import FakeAPI
fixture=Path(sys.argv[1]).resolve(); scenario=sys.argv[2]
if not fixture.is_relative_to((root/'tests/sandbox').resolve()): raise RuntimeError('unsafe fixture')
data=json.loads((fixture/'crash-fixture.json').read_text(encoding='utf-8'))
common.ROOT=engine.ROOT=executor.ROOT=fixture
api=FakeAPI(data['config']); api.rows=data['rows']; api.contents=data['contents']; api.meta={bytes.fromhex(data['metainfo_hex']):data['metadata']}
def checkpoint():
    with (fixture/'fake-client.json').open('w',encoding='utf-8') as f:
        json.dump({'rows':api.rows,'contents':api.contents,'calls':api.calls},f); f.flush(); os.fsync(f.fileno())
stop=api.stop
def stop_saved(h): stop(h); checkpoint()
api.stop=stop_saved
remove=api.remove
def remove_saved(h):
    remove(h); checkpoint()
    if scenario=='remove': os._exit(71)
api.remove=remove_saved
store=Store(); controller=engine.Controller(data['config'],api,store,Budget(30))
original=engine.move_no_replace
def move(s,d):
    original(s,d)
    if scenario=='archive' and Path(s)==Path(data['source']): os._exit(71)
    if scenario=='payload' and Path(s)!=Path(data['source']): os._exit(71)
engine.move_no_replace=move
put=store.put_op
def receipt(op,file_ordinal=None):
    put(op,file_ordinal)
    if scenario=='receipt' and file_ordinal is not None: os._exit(71)
store.put_op=receipt
controller.apply_plan(data['plan'])
raise RuntimeError('crash hook not reached')
