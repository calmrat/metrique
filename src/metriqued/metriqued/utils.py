#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import os
import pql
import re

from metriqueu.utils import dt2ts

OBJECTS_MAX_BYTES = 16777216
EXISTS_SPEC = {'$exists': 1}


def date_pql_string(date):
    if date is None:
        return '_end == None'
    if date == '~':
        return ''

    before = lambda d: '_start <= %f' % dt2ts(d)
    after = lambda d: '(_end >= %f or _end == None)' % dt2ts(d)
    split = date.split('~')
    # replace all occurances of 'T' with ' '
    # this is used for when datetime is passed in
    # like YYYY-MM-DDTHH:MM:SS instead of
    #      YYYY-MM-DD HH:MM:SS as expected
    # and drop all occurances of 'timezone' like substring
    split = [re.sub('\+\d\d:\d\d', '', d.replace('T', ' ')) for d in split]
    if len(split) == 1:
        # 'dt'
        return '%s and %s' % (before(split[0]), after(split[0]))
    elif split[0] == '':
        # '~dt'
        return before(split[1])
    elif split[1] == '':
        # 'dt~'
        return after(split[0])
    else:
        # 'dt~dt'
        return '%s and %s' % (before(split[1]), after(split[0]))


def query_add_date(query, date):
    date_pql = date_pql_string(date)
    if query and date_pql:
        return '%s and %s' % (query, date_pql)
    return query or date_pql


def get_pids(pid_dir, clear_stale=True):
    pid_dir = os.path.expanduser(pid_dir)
    # eg, server.22325.pid, server.23526.pid
    pids = []
    for f in os.listdir(pid_dir):
        pid_re = re.search(r'metriqued.(\d+).pid', f)
        if pid_re:
            pids.append(pid_re.groups()[0])
    if clear_stale:
        pids = clear_stale_pids(pids, pid_dir)
    return map(int, pids)


def clear_stale_pids(pids, pid_dir):
    'check for and remove any pids which have no corresponding process'
    procs = os.listdir('/proc')
    running = [pid for pid in pids if pid in procs]
    for pid in pids:
        if pid not in running:
            pid_file = 'metriqued.%s.pid' % pid
            path = os.path.join(pid_dir, pid_file)
            os.remove(path)
    return running


def parse_pql_query(query):
    if not query:
        return {}
    if not isinstance(query, basestring):
        raise TypeError("query expected as a string")
    pql_parser = pql.SchemaFreeParser()
    spec = pql_parser.parse(query)
    return spec
