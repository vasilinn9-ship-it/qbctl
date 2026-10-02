import unittest

from qbctl.presentation import build_report, report_line


class PresentationTests(unittest.TestCase):
    def test_counts_completed_removals_and_queue_releases_without_data_deletion(self):
        actions = [
            # Legacy completion receipts omitted delete_files; that operation always requests false.
            {'action': 'complete', 'result': 'verified', 'postconditions': {'client_removed': True, 'data_handed_off': True}},
            {'action': 'complete', 'result': 'verified', 'postconditions': {'client_removed': True, 'data_handed_off': True, 'delete_files': False}},
            {'action': 'release', 'result': 'verified', 'postconditions': {'client_removed': True, 'data_handed_off': True, 'delete_files': False}},
            {'action': 'complete', 'result': 'verified', 'postconditions': {'client_removed': False, 'delete_files': False}},
            {'action': 'release', 'result': 'verified', 'postconditions': {'client_removed': True, 'delete_files': True}},
            {'action': 'release', 'result': 'blocked', 'postconditions': {'client_removed': True, 'data_handed_off': True, 'delete_files': False}},
        ]
        result = {'actions': actions, 'issues': [], 'warnings': [], 'pending_operations': []}
        report = build_report(result)
        self.assertEqual(report['completed_tasks'], 3)
        self.assertEqual(report['released_without_data_deletion'], 3)
        self.assertIn('записей удалено без удаления данных: 3', report_line(report))


if __name__ == '__main__':
    unittest.main()
