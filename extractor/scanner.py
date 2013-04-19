"""This module contains the basic framework to handle scanning over a range of
entities, defined by a user-provided query, then calling a user-provided
function to get the desired "output" from processing those entities.

The primary items provided:

    Scanner:
        class to handle the chained mapping over the entities.

"""

import copy

import uuid

from furious.async import Async
from furious.context import get_current_async

from extractor.sharder import get_query_from_dict


class Scanner(Async):
    """This is the core work-horse of the extractor library."""

    def __init__(self, kind, query_filters, cursor=None, options=None):
        """Setup the Scanner object."""
        if not options:
            options = {}

        scanner_info = {
            'base_name': "%s-%s" % (kind, uuid.uuid4().hex),
            'shard': 0,
            'iteration': 0
        }

        if 'scanner_info' in options:
            scanner_info.update(options['scanner_info'])

        options['scanner_info'] = scanner_info

        super(Scanner, self).__init__(
            target=scan_data,
            kwargs={
                'kind': kind,
                'query_filters': query_filters,
                'cursor': cursor},
            **options)

        print options

    def _build_task_name(self):
        """Set a task name from options['scanner_info']."""

        info = self._options['scanner_info']

        return "%s-%s-%s" % (
            info['base_name'], info['shard'], info['iteration'])

    def get_task_args(self):
        """Get user-specified task kwargs."""

        args = super(Scanner, self).get_task_args()

        args['name'] = self._build_task_name()

        return args

    def get_next_scanner(self, cursor):
        """Get a scanner to continue this one."""
        options = copy.deepcopy(self._options)

        _, _, kwargs = options.pop('job')

        kwargs['cursor'] = cursor

        options['scanner_info']['iteration'] += 1

        return Scanner(options=options, **kwargs)

