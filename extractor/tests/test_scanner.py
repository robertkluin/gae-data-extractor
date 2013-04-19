
import unittest

import mock


class TestScanner(unittest.TestCase):
    """Test the class."""

    def test_creation_sets_name(self):
        """Test that creating a simple scanner sets the name correctly."""
        from extractor.scanner import Scanner

        with mock.patch('uuid.uuid4') as uuid_mock:
            uuid_mock.return_value.hex = 'abc123'
            scanner = Scanner(kind='SomeKind', query_filters={})

        task_args = scanner.get_task_args()

        self.assertEqual(task_args['name'], 'SomeKind-abc123-0-0')

    def test_creation_with_base_name(self):
        """Test that specifying a base name works."""
        from extractor.scanner import Scanner

        scanner = Scanner(kind='SomeKind', query_filters={},
                          options={'scanner_info': {'base_name': 'test'}})

        task_args = scanner.get_task_args()

        self.assertEqual(task_args['name'], 'test-0-0')

    def test_creation_with_shard(self):
        """Test that specifying a shard works."""
        from extractor.scanner import Scanner

        with mock.patch('uuid.uuid4') as uuid_mock:
            uuid_mock.return_value.hex = 'xyzew'
            scanner = Scanner(kind='SomeKind', query_filters={},
                              options={'scanner_info': {'shard': 7}})

        task_args = scanner.get_task_args()

        self.assertEqual(task_args['name'], 'SomeKind-xyzew-7-0')

    def test_creation_with_iteration(self):
        """Test that specifying an iteration works."""
        from extractor.scanner import Scanner

        with mock.patch('uuid.uuid4') as uuid_mock:
            uuid_mock.return_value.hex = 'xyzew'
            scanner = Scanner(kind='SomeKind', query_filters={},
                              options={'scanner_info': {'shard': 33,
                                                        'iteration': 89}})

        task_args = scanner.get_task_args()

        self.assertEqual(task_args['name'], 'SomeKind-xyzew-33-89')

    def test_get_next_scanner(self):
        """Test that the next scanner is built correctly."""
        from extractor.scanner import Scanner

        with mock.patch('uuid.uuid4') as uuid_mock:
            uuid_mock.return_value.hex = 'jkfls'
            scanner = Scanner(kind='SomeKind', query_filters={},
                              options={'scanner_info': {'shard': 33,
                                                        'iteration': 89}})

        next_scanner = scanner.get_next_scanner('nextcursor')

        options = next_scanner.get_options()

        expected_options = {
            'scanner_info': {
                'base_name': 'SomeKind-jkfls',
                'shard': 33,
                'iteration': 90},  # This should now be one larger.
            'job': ('extractor.scanner.scan_data', None,
                    {'cursor': 'nextcursor',  # The cursor should now be set.
                     'kind': 'SomeKind',
                     'query_filters': {}})
        }

        self.assertEqual(options, expected_options)

