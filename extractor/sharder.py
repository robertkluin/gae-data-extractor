"""This module contains the logic used to shard a query according a simple set
of rules, then return an object which may be iterated over to produce the
query shards.

The primary items provided:

    Scanner:
        class to handle the chained mapping over the entities.

"""

import datetime

from google.appengine.api import datastore


def get_query_from_dict(kind, query_filters, cursor=None):
    """Return a datastore query from a query dict."""
    return datastore.Query(kind=kind, filters=query_filters, cursor=cursor)


def get_query_dicts(shards, shard_field, shard_field_type, shard_range,
                    query_filters):
    """Using shard_field, return an iterator that will yield out query objects
    that will scan over the range for a shard.
    """

    shard_value_ranges = _generate_shard_ranges(
        shards, shard_field_type, shard_range)

    queries = []
    for start_value, stop_value in shard_value_ranges:
        query = query_filters.copy()

        query["%s >=" % shard_field] = start_value
        query["%s <" % shard_field] = stop_value

        queries.append(query)

    return queries


def _generate_shard_ranges(shards, shard_field_type, shard_range):
    """Return a list of (start, stop) values, only works with datetime fields
    currently. Additional types will be added as needed.
    """
    ranges = []

    if shard_field_type != 'datetime':
        raise NotImplementedError('Sharding is only supported on datetime.')

    start, stop = shard_range

    if stop <= start:
        raise ValueError('start must be < stop value.')

    difference = stop - start
    range_in_seconds = (difference.days * 24 * 3600) + difference.seconds

    skip_seconds = range_in_seconds / shards

    current_stop = start

    for value in xrange(0, range_in_seconds, skip_seconds):
        current_start = current_stop
        current_stop = current_start + datetime.timedelta(seconds=skip_seconds)

        ranges.append((current_start, current_stop))

    return ranges

