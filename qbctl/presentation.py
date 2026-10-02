"""Read-only explanations and reports derived from API snapshots and receipts."""
from collections import Counter
from .ownership import scope


def diagnosis(t, reason, pending, path_ok, *, done, in_check, is_stopped):
    h = t['hash']
    related = [o for o in pending if h in scope(o)['hashes'] or scope(o)['global']]
    evidence = {k: t.get(k) for k in ('dlspeed', 'num_seeds', 'num_leechs', 'availability')}
    evidence['stop_reason'] = reason
    code, message = 'CLIENT_STATE', 'Состояние клиента требует уточнения.'
    if not path_ok:
        code, message = 'PATH_POLICY', 'Путь или автоматическое управление не соответствуют правилам.'
    elif t['state'] in ('error', 'missingFiles', 'unknown'):
        code, message = 'CLIENT_ERROR', 'Клиент сообщает ошибку или отсутствие файлов; запуск не подтверждён.'
    elif related:
        code, message = 'OPERATION_PENDING', 'Есть незавершённая операция CLI; сначала продолжить её по журналу.'
        evidence['operations'] = [{'id': o['id'], 'kind': o['kind'], 'stage': o['stage']} for o in related]
        if any(o.get('stage', '').endswith('_requested') for o in related):
            message += ' Этап запроса сам по себе не доказывает его приём или завершение.'
        requests = [o for o in related]
        requests.extend(task for o in related for task in o.get('tasks', []) if task.get('hash') == h)
        unknown = any(r.get(sent) and not r.get(accepted) for r in requests for sent, accepted in (
            ('start_sent', 'start_accepted'), ('stop_sent', 'stop_accepted'), ('pause_sent', 'pause_accepted'),
            ('add_stop_sent','add_stop_accepted'),('remove_sent','remove_accepted'),('preferences_sent','preferences_accepted'),('sent', 'accepted')))
        if unknown:
            code, message = 'REQUEST_UNCONFIRMED', 'Приём запроса не записан; восстановление должно уточнить состояние без повторной отправки.'
    elif in_check:
        code, message = 'CHECKING', 'Клиент проверяет имеющиеся данные; дождаться результата.'
    elif done:
        code, message = 'READY', 'Клиент сообщает завершение; требуется проверка и передача обычным run.'
    elif is_stopped:
        pauses = {
            'user': ('USER_PAUSE', 'Остановлено пользователем; автоматика не возобновляет.'),
            'slots': ('DOWNLOAD_LIMIT', 'Остановлено CLI из-за количества разрешённых загрузок.'),
            'resources': ('RESOURCE_LIMIT', 'Остановлено CLI из-за ресурсной политики.'),
            'disk': ('DISK_LIMIT', 'Остановлено CLI из-за недостаточного запаса места.'),
            'path_policy': ('PATH_POLICY', 'Остановлено CLI из-за нарушения правил пути.'),
            'path_collision': ('SHARED_FILES', 'Остановлено CLI из-за пересечения файловых путей разных задач.'),
            'completion': ('COMPLETION_STOP', 'Остановлено для обработки завершения.'),
        }
        code, message = pauses.get(reason, ('EXTERNAL_PAUSE', 'Остановлено без причины, записанной CLI; автоматически не возобновляется.'))
    elif t['state'] == 'queuedDL':
        code, message = 'CLIENT_QUEUE', 'Ожидает в очереди qBittorrent; это состояние клиента.'
    elif 'meta' in t['state'].lower():
        code, message = 'METADATA', 'Ожидает получения метаданных torrent.'
    elif t['state'] == 'moving':
        code, message = 'CLIENT_MOVING', 'Клиент перемещает данные; готовность ещё не подтверждена.'
    elif t.get('dlspeed', 0) > 0:
        code, message = 'DOWNLOADING', 'Получает данные.'
    elif t.get('num_seeds') == 0 and t.get('num_leechs') == 0:
        code, message = 'NO_CONNECTED_PEERS', 'Сейчас нет подключённых пиров. Это не доказывает отсутствие раздающих в сети.'
    else:
        code, message = 'WAITING_DATA', 'Сейчас данные не поступают; по этому снимку точная причина неизвестна.'
    return {'hash': h, 'state': t['state'], 'code': code, 'message': message, 'evidence': evidence,
            'basis': 'API snapshot and CLI journal; no payload inspection'}


def build_report(result):
    # Count verified receipts only. A replay is historical, not a new action.
    verified = [a for a in result.get('actions', []) if a.get('result') == 'verified']
    completed = [a for a in verified if a['action'] == 'complete']
    counts = Counter(a['action'] for a in verified)
    pending = result.get('pending_operations', [])
    return {
        'basis': 'historical_result' if result.get('response_replayed') else ('job_receipts' if result.get('job') else 'verified_actions_in_this_call'),
        'file_count_scope': 'whole completed operations; recovery may include receipts from previous calls',
        'completed_tasks': len(completed),
        'archived_torrents': sum(bool(a.get('postconditions', {}).get('torrent_archived')) for a in completed),
        'confirmed_files': sum(a.get('postconditions', {}).get('files', 0) for a in completed),
        'confirmed_bytes': sum(a.get('postconditions', {}).get('bytes', 0) for a in completed),
        'files_moved': sum(a.get('postconditions', {}).get('files_moved', 0) for a in completed),
        'bytes_moved': sum(a.get('postconditions', {}).get('bytes_moved', 0) for a in completed),
        'files_retained_legacy': sum(a.get('postconditions', {}).get('files_retained_legacy', 0) for a in completed),
        'added': counts['add'], 'started': counts['resume'], 'paused': counts['pause'],
        'dedupe_deleted': sum(a.get('deleted',0) for a in verified if a['action']=='dedupe'),
        'released_without_data_deletion': counts['release'], 'root_duplicates_removed': counts['duplicate_cleanup'],
        'pending_count': len(pending),
        'pending': [{'id': o['id'], 'kind': o['kind'], 'hash': o['hash'], 'stage': o['stage']} for o in pending],
        'issue_codes': sorted({i['code'] for i in result.get('issues', [])}),
        'warning_count': len(result.get('warnings', [])),
        'warning_codes': sorted({i['code'] for i in result.get('warnings', [])}),
        'next_safe_commands': list(result.get('next_safe_commands', [])),
        'observed': {k: result.get('observed', {}).get(k) for k in ('client_count', 'allowed_downloads', 'completed', 'checking')},
    }


def report_line(report):
    return (f"Завершено: {report['completed_tasks']}; архивировано torrent: {report['archived_torrents']}; "
            f"подтверждено файлов: {report['confirmed_files']} ({report['confirmed_bytes']} байт); "
            f"добавлено: {report['added']}; запущено: {report['started']}; остановлено: {report['paused']}; "
            f"освобождено записей: {report['released_without_data_deletion']}; удалено torrent-дублей: {report.get('dedupe_deleted',0)}; pending: {report['pending_count']}")
