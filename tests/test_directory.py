import os,struct,unittest
from pathlib import Path
from unittest.mock import patch
from test_protocol import ProtocolTests
from qbctl import directory,engine
from qbctl.common import Budget,Fault,fingerprint

def record(name='file.torrent',attrs=32,inode=123):
    name=name.encode('utf-16-le'); data=bytearray(88+len(name))
    struct.pack_into('<q',data,24,116444736000000012); struct.pack_into('<q',data,40,99)
    struct.pack_into('<II',data,56,attrs,len(name)); data[72:88]=inode.to_bytes(16,'little'); data[88:]=name
    return bytes(data)

class DirectoryTests(ProtocolTests):
    def test_native_fingerprints_match_every_fixture_stat(self):
        root=Path(self.c['paths']['incoming'])
        for i in range(30): (root/('Файл '+str(i)+'.torrent')).write_bytes(str(i).encode())
        if os.name!='nt': self.skipTest('Windows native directory enumeration')
        rows=directory.native_scan(root,Budget(10)); self.assertIsNotNone(rows)
        self.assertEqual(len(rows),31)
        for p,fp in rows: self.assertEqual(fp,fingerprint(p))

    def test_fallback_fingerprints_match_stat(self):
        root=Path(self.c['paths']['incoming'])
        with patch.object(directory,'native_scan',return_value=None): rows=directory.scan_torrents(root,Budget(5))
        self.assertEqual(rows,[(self.source,fingerprint(self.source))])

    def test_decoder_preserves_identity_and_timestamp(self):
        rows=directory.decode_buffer(record(),self.root,999)
        self.assertEqual(rows[0][1],{'size':99,'mtime_ns':1200,'dev':999,'ino':123})

    def test_decoder_rejects_reparse_and_directory_torrents(self):
        for attrs in (0x400,0x10):
            with self.assertRaises(Fault): directory.decode_buffer(record(attrs=attrs),self.root,999)

    def test_decoder_rejects_escape_and_bad_offset(self):
        with self.assertRaises(Fault): directory.decode_buffer(record('../file.torrent'),self.root,999)
        data=bytearray(record()); struct.pack_into('<I',data,0,4)
        with self.assertRaises(Fault): directory.decode_buffer(bytes(data),self.root,999)

    def test_bulk_index_detects_modified_fingerprint_second_scan(self):
        original=directory.scan_torrents; count=0
        def scan(root,budget):
            nonlocal count
            count+=1; rows=original(root,budget)
            if count==4: rows=[(p,{**fp,'mtime_ns':fp['mtime_ns']+1}) for p,fp in rows]
            return rows
        with patch.object(directory,'scan_torrents',scan),self.assertRaises(Fault) as e: self.ctl.index()
        self.assertEqual(e.exception.code,'INDEX_CHANGED')

    def test_bulk_index_never_scans_payload(self):
        original=directory.scan_torrents; roots=[]
        def scan(root,budget): roots.append(root); return original(root,budget)
        with patch.object(directory,'scan_torrents',scan): self.ctl.index()
        self.assertTrue(all(str(p) in (self.c['paths']['incoming'],self.c['paths']['archive']) for p in roots))

    def test_index_loads_sqlite_cache_once_for_all_files(self):
        for i in range(12):
            p=Path(self.c['paths']['incoming'])/(str(i)+'.torrent'); p.write_bytes(self.data)
        self.ctl.index(); self.ctl._index_memory={}; trace=[]; self.store.db.set_trace_callback(trace.append)
        self.ctl.index(); self.store.db.set_trace_callback(None)
        reads=[q for q in trace if q.startswith('SELECT path,fingerprint,metadata FROM cache')]
        self.assertEqual(len(reads),1)
        self.assertFalse(any('FROM cache WHERE' in q for q in trace))

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(DirectoryTests(name) for name in DirectoryTests.__dict__ if name.startswith('test_'))
