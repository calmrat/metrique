#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
from dateutil.parser import parse as dt_parse
from operator import itemgetter
import simplejson as json

from metrique.server.job import job_save


@job_save('log_tail')
def tail(self, spec=None, limit=None, format_=None):
    if not spec:
        spec = {}
    else:
        spec = json.loads(spec)

    if not format_:
        format_ = '%(processName)s:%(message)s'

    # spec 'when' key needs to be converted from string to datetime
    if 'when' in spec:
        spec['when']['$gt'] = dt_parse(spec['when']['$gt'])

    if not limit:
        limit = 20
    else:
        limit = int(limit)
        if limit < 0:
            raise ValueError("limit must be an integer value > 0")

    docs = self.mongodb_config.c_logs.find(spec, limit=limit,
                                           sort=[('when', -1)])

    _result = sorted([doc for doc in docs], key=itemgetter('when'))

    try:
        # get the last log.when so client knows from where to
        # start next...
        last_when = _result[-1]['when']
        meta = last_when
        result = '\n'.join([format_ % doc for doc in _result])
    except KeyError:
        raise KeyError("Invalid log format key (%s)" % format_)
    except ValueError:
        raise ValueError("Invalid log format string (%s)" % format_)
    except IndexError:
        result = None
        meta = None

    return result, meta
