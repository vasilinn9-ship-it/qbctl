"""Presentation and atomic report export; never accesses torrent payload."""
from __future__ import annotations
import json, os, sqlite3, sys, tempfile, time
from pathlib import Path
from .common import ROOT, Fault, guarded, now
from .presentation import build_report, report_line

def output_path(value):
    path = Path(os.path.abspath(ROOT / value))
    try: relative = path.relative_to(ROOT / 'reports')
    except ValueError:
        raise Fault('ARGUMENT_INVALID', '--output должен указывать JSON внутри V:\\temp\\cli\\reports.', 'error')
    if path.suffix.lower() != '.json' or not relative.parts:
        raise Fault('ARGUMENT_INVALID', '--output требует имя .json.', 'error')
    return guarded(ROOT, str(path.relative_to(ROOT)))

def save_report(result, path):
    path = output_path(str(path))
    path.parent.mkdir(parents=True, exist_ok=True)
    output_path(str(path))
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent,
                                         prefix='.qbctl-', suffix='.tmp', delete=False) as file:
            temporary = file.name
            json.dump(result, file, ensure_ascii=False, indent=2)
            file.write('\n'); file.flush(); os.fsync(file.fileno())
        output_path(str(path))
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            try: os.unlink(temporary)
            except OSError: pass

def summary_line(result):
    job = result.get('job', {})
    obs = result.get('observed', {})
    parts = [f"qbctl: {result['result']}"]
    if result.get('response_replayed'):
        parts.append(f"Сохранённый результат; действия не повторялись. Снимок клиента: {result.get('snapshot_at', 'недоступен')}. Свежие числа — status.")
    elif job:
        parts.append(f"Результат выполнения задания по квитанциям. Снимок клиента: {result.get('snapshot_at', 'недоступен')}. Свежие числа — status.")
    if job: parts.append(f"задание {job['id']} [{job['state']}]")
    if obs:
        speed = obs.get('transfer', {}).get('dl_info_speed', 0) / 1_000_000
        parts.append(f"записей {obs['client_count']}; незавершённых разрешено {obs['allowed_downloads']}; готовых {obs['completed']}; проверяются {obs['checking']}; скорость {speed:.2f} МБ/с")
    if obs.get('legacy_paths'): parts.append(f"Старых исключений с путём k: {len(obs['legacy_paths'])}; это не новые переносы.")
    performance=job.get('performance',{})
    if performance:
        files=performance.get('api_metrics',{}).get('torrents/files',{})
        parts.append(f"Шагов: {performance.get('steps',0)}; запросов файловых списков: {files.get('calls',0)}.")
    parts.append(report_line(result['report']))
    for issue in result.get('issues', []) + result.get('warnings', []):
        prefix='Предупреждение: ' if issue in result.get('warnings', []) else ''
        parts.append(f"{prefix}{issue['code']}: {issue.get('message', 'Конфликт требует диагностики')}")
        if issue.get('hash'): parts.append('  Хеш: '+issue['hash'])
        for path in issue.get('paths', []): parts.append('  Файл: '+path)
        if issue.get('path'): parts.append('  Файл: '+issue['path'])
        if issue.get('causes'): parts.append('  Причины: '+', '.join(issue['causes']))
        if issue.get('hashes'): parts.append('  Хеши: '+', '.join(issue['hashes']))
    if result.get('issues') or result.get('warnings'):
        for command in result.get('next_safe_commands', []): parts.append('Безопасное действие: '+command)
    if result.get('output_file'): parts.append('Отчёт: ' + result['output_file'])
    parts.append(f"Длительность команды: {result.get('command_elapsed_seconds', 0):.2f} с")
    return '\n'.join(parts)

class Progress:
    def __init__(self, enabled, json_mode):
        self.enabled, self.json_mode = enabled, json_mode
        self.warnings = []
        self.started = time.monotonic(); self.last_at = 0; self.last_signature = None

    def note_error(self, error):
        from .common import sqlite_details
        issue = {'code':'PROGRESS_UNAVAILABLE',
                 'message':'Диагностика прогресса недоступна; результат операций определяется журналом.',
                 'exception_type':type(error).__name__, **sqlite_details(error)}
        if issue not in self.warnings: self.warnings.append(issue)

    def __call__(self, job):
        if not self.enabled: return
        try: self._read(job)
        except Exception as error:
            self.note_error(error)

    def _read(self, job):
        # Reads durable metadata only. No extra API calls or access to k.
        from .executor import database, read_job
        with database() as store:
            job = read_job(store, job['id'])
            operations = []
            prefix = 'job:' + job['id'] + ':'
            for raw, updated in store.db.execute("SELECT body,updated FROM operations WHERE json_extract(body,'$.stage') != 'finished'"):
                operation = json.loads(raw)
                if (operation.get('request_id') or '').startswith(prefix) or operation['id'] in job.get('initial_pending', []):
                    item = {key: operation[key] for key in ('id', 'kind', 'stage', 'hash')}
                    item['last_activity_at'] = updated
                    if operation['kind'] == 'complete':
                        total, handed = store.db.execute("SELECT count(*),coalesce(sum(json_extract(body,'$.handoff')='handed_off'),0) FROM operation_files WHERE operation_id=?", (operation['id'],)).fetchone()
                        item.update(files_total=total, files_handed_off=handed)
                    operations.append(item)
        event = {'event': 'progress', 'at': now(), 'job_id': job['id'], 'state': job['state'],
                 'step': job['step_number'], 'reason': job['reason'], 'operations': operations,
                 'wait_observation':job.get('wait_observation'),
                 'elapsed_seconds': round(time.monotonic() - self.started, 2),
                 'basis': 'durable journal; not a fresh client snapshot'}
        signature = json.dumps({k: event[k] for k in ('job_id','state','step','reason','operations')}, sort_keys=True)
        stamp = time.monotonic()
        if signature == self.last_signature and stamp - self.last_at < 10: return
        self.last_at, self.last_signature = stamp, signature
        try:
            if self.json_mode: line = json.dumps(event, ensure_ascii=False)
            else:
                stages = ', '.join(f"{o['kind']}:{o['stage']}" + (f" файлов {o['files_handed_off']}/{o['files_total']}" if 'files_total' in o else '') for o in operations)
                observed=job.get('wait_observation') or {}
                if observed.get('checking'):
                    stages += '; проверка: ' + ', '.join(f"{t['hash'][:8]} {t['state']}, progress={t.get('progress')}" for t in observed['checking'])
                line = f"[{event['elapsed_seconds']:.1f} с] {job['id']} {job['state']}, шаг {job['step_number']}: {stages or job['reason'].get('message', '')}"
            print(line, file=sys.stderr, flush=True)
        except OSError as error:
            # Console failure never changes the payload worker's outcome.
            self.note_error(error)
            self.enabled = False
