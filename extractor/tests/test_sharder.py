
import datetime
import unittest


class TestGetQueryDicts(unittest.TestCase):
    """Test that get_query_dicts returns the proper query dicts."""

    def test_simple_case(self):
        """Test that the expected query dicts are returned."""
        from extractor.sharder import get_query_dicts

        query_filters = {
            'something': 123,
            'otherthing': 'abcd'
        }

        today = datetime.datetime.today()
        shard_range = (today - datetime.timedelta(days=3), today)

        queries = get_query_dicts(
            3, 'stamp', 'datetime', shard_range, query_filters)

        self.assertEqual(3, len(queries))

        expected = []
        for day in xrange(3):
            query = query_filters.copy()
            query['stamp >='] = today - datetime.timedelta(days=3 - day)
            query['stamp <'] = today - datetime.timedelta(days=2 - day)
            expected.append(query)

        self.assertEqual(expected, queries)


class TestGenerateShardRanges(unittest.TestCase):
    """Test _generate_shard_ranges to ensure it functions correctly."""

    def test_nondatetime_fails(self):
        """Make sure a non-datetime field raises a NotImplementedError."""
        from extractor.sharder import _generate_shard_ranges

        self.assertRaises(
            NotImplementedError,
            _generate_shard_ranges, 1, 'int', (1, 5))

    def test_larger_start(self):
        """Make sure larger start than stop raises ValueError."""
        from extractor.sharder import _generate_shard_ranges

        today = datetime.datetime.today()

        shard_range = (today + datetime.timedelta(days=1), today)
        self.assertRaises(
            ValueError, _generate_shard_ranges, 1, 'datetime', shard_range)

    def test_equal_start_stop(self):
        """Make sure equal start and stop raises ValueError."""
        from extractor.sharder import _generate_shard_ranges

        today = datetime.datetime.today()

        shard_range = (today, today)

        self.assertRaises(
            ValueError, _generate_shard_ranges, 1, 'datetime', shard_range)

    def test_datetime_one_shard(self):
        """Make sure one shard gets returned with the right start and stop."""
        from extractor.sharder import _generate_shard_ranges

        today = datetime.datetime.today()

        shard_range = (today - datetime.timedelta(days=1), today)
        shards = _generate_shard_ranges(1, 'datetime', shard_range)

        self.assertEqual([shard_range], shards)

    def test_datetime_two_shards(self):
        """Make sure two shards get returned with the right start and stop."""
        from extractor.sharder import _generate_shard_ranges

        today = datetime.datetime.today()

        shard_range = (today - datetime.timedelta(days=2), today)

        shards = _generate_shard_ranges(2, 'datetime', shard_range)

        expected_shards = [
            (today - datetime.timedelta(days=2),
             today - datetime.timedelta(days=1)),
            (today - datetime.timedelta(days=1), today)
        ]

        self.assertEqual(expected_shards, shards)

    def test_many_shards(self):
        """Make sure lots of shards get returned with the right start and stop
        values.
        """
        from extractor.sharder import _generate_shard_ranges

        today = datetime.datetime.today()

        shard_range = (today - datetime.timedelta(days=1), today)

        shards = _generate_shard_ranges(24, 'datetime', shard_range)

        expected_shards = [
            (today - datetime.timedelta(hours=24 - i),
             today - datetime.timedelta(hours=23 - i))
            for i in xrange(24)]

        self.assertEqual(expected_shards, shards)

