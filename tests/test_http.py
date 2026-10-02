"""Real urllib/multipart contract against a local disposable HTTP server."""
import json, threading, unittest, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch
from qbctl.api import API
from qbctl.common import Fault

class HTTPTests(unittest.TestCase):
    def test_expired_deadline_no_network_request(self):
        api=API(self.config); count=len(self.seen); api.deadline=time.monotonic()-1
        with self.assertRaises(Fault) as e: api.start('a'*40)
        self.assertEqual(e.exception.code,'BUDGET_EXHAUSTED'); self.assertFalse(e.exception.details['request_sent']); self.assertEqual(len(self.seen),count)

    def test_api_metrics_shared_without_fields_or_secrets(self):
        api=API(self.config); child=API(self.config,metrics=api.metrics,metrics_lock=api._metrics_lock)
        child.get('a'*40); self.assertEqual(api.metrics['app/version']['calls'],2); self.assertEqual(api.metrics['torrents/info']['calls'],1)
        self.assertNotIn('a'*40,json.dumps(api.metrics)); self.assertTrue(all(item['seconds']>=0 for item in api.metrics.values()))
    def setUp(self):
        self.seen=[]; self.auth=False; self.malformed=False; self.version='2.15.1'; outer=self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def do_GET(self): self.handle_request()
            def do_POST(self): self.handle_request()
            def handle_request(self):
                body=self.rfile.read(int(self.headers.get('Content-Length','0')))
                outer.seen.append((self.path,dict(self.headers),body))
                endpoint=self.path.split('/api/v2/')[-1].split('?')[0]
                if outer.auth and endpoint!='auth/login' and self.headers.get('Cookie')!='SID=fixture':
                    self.send_response(403); self.end_headers(); return
                self.send_response(200)
                if endpoint=='auth/login': self.send_header('Set-Cookie','SID=fixture; Path=/')
                self.end_headers()
                response={'app/version':'v5.2.4','app/webapiVersion':outer.version,'auth/login':'Ok.','torrents/info':'[]','torrents/add':'Ok.','torrents/delete':''}
                if endpoint=='torrents/parseMetadata':
                    data=[{'torrent_id':'a'*40,'infohash_v1':'a'*40,'infohash_v2':'b'*64,'announce':'secret/passkey','info':{'files':[{'path':'a.bin','length':3}]}}]
                    response[endpoint]='broken JSON' if outer.malformed else json.dumps(data)
                self.wfile.write(response.get(endpoint,'{}').encode())
        self.server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True); self.thread.start()
        self.config={'url':f'http://127.0.0.1:{self.server.server_port}','timeout':1,'username_env':'QBCTL_TEST_USER','password_env':'QBCTL_TEST_PASS'}
    def tearDown(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join()
    def test_metadata_multipart_and_no_trackers_stored(self):
        api=API(self.config); m=api.metadata(b'fixture')
        self.assertEqual(m['v2'],'b'*64); self.assertNotIn('announce',m)
        path,headers,body=self.seen[-1]; self.assertTrue(headers['Content-Type'].startswith('multipart/form-data; boundary=')); self.assertIn(b'fixture',body)
    def test_stopped_add_paths_and_delete_false(self):
        api=API(self.config); api.add(b'fixture',r'C:\qbctl-test\working'); body=self.seen[-1][2]
        for token in (b'name="stopped"\r\n\r\ntrue',b'name="autoTMM"\r\n\r\nfalse',b'name="skip_checking"\r\n\r\nfalse',b'V:\\temp\\m'): self.assertIn(token,body)
        api.remove('a'*40); self.assertIn(b'deleteFiles=false',self.seen[-1][2])
    def test_version_adapter_216_omits_removed_flag(self):
        self.version='2.16.0'; api=API(self.config); api.add(b'fixture',r'C:\qbctl-test\working'); self.assertNotIn(b'skip_checking',self.seen[-1][2])
    def test_future_version_mutation_blocked(self):
        self.version='2.17.0'; api=API(self.config)
        with self.assertRaises(Fault): api.add(b'fixture',r'C:\qbctl-test\working')
        self.assertFalse(any(p.endswith('torrents/add') for p,h,b in self.seen))
    def test_auth_cookie_and_no_secret_in_fault(self):
        self.auth=True
        with patch.dict('os.environ',{'QBCTL_TEST_PASS':'private-fixture'}): api=API(self.config)
        self.assertEqual(api.version,'v5.2.4'); self.malformed=True
        with self.assertRaises(Fault) as caught: api.metadata(b'fixture')
        self.assertNotIn('private-fixture',str(caught.exception.issue()))
    def test_auth_missing_secret(self):
        self.auth=True
        with patch.dict('os.environ',{},clear=True),self.assertRaises(Fault) as caught: API(self.config)
        self.assertEqual(caught.exception.code,'AUTH_REQUIRED')

if __name__=='__main__': unittest.main(verbosity=2)

