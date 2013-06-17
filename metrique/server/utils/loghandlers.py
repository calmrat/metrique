#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

from datetime import datetime
from pymongo.errors import InvalidDocument
import time

from socket import gethostname
HOSTNAME = gethostname()

from metrique.tools.constants import UTC


class MongoLogHandler(logging.Handler):
    def __init__(self, target, level=None):
        if not level:
            level = logging.NOTSET

        logging.Handler.__init__(self, level)

        self._collection = target

    def emit(self, record):
        # this saves just level and message
        # LogRecord class show what record vars
        # are available by default
        r = record

        now = datetime.now(UTC)
        now_time = time.mktime(now.timetuple())

        dbrecord = {"name": r.name,
                    "when": now,
                    "time": now_time,
                    "level": r.levelname,
                    "message": r.msg,
                    "args": unicode(r.args),
                    "hostname": HOSTNAME,
                    "exc_info": unicode(r.exc_info),
                    "exc_text": unicode(r.exc_text),
                    "lineno": r.lineno,
                    "funcName": r.funcName,
                    "created": r.created,
                    "threadName": r.threadName,
                    "processName": r.processName}

        try:
            self._collection.insert(dbrecord)
        except InvalidDocument:
            dbrecord['message'] = '(__MESSAGE_SLICE__) %s' % dbrecord['message'][:1000]
            self._collection.insert(dbrecord)
