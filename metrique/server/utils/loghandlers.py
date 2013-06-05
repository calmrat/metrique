#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

from datetime import datetime
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
        self.r = r = record

        now = datetime.now(UTC)
        now_time = time.mktime(now.timetuple())

        self.dbrecord = {"name": r.name,
                         "when": now,
                         "time": now_time,
                         "level": r.levelname,
                         "message": str(r.msg.decode('utf-8')),
                         "args": str(r.args),
                         "hostname": HOSTNAME,
                         "exc_info": str(r.exc_info),
                         "exc_text": str(r.exc_text),
                         "lineno": r.lineno,
                         "funcName": r.funcName,
                         "created": r.created,
                         "threadName": r.threadName,
                         "processName": r.processName}

        self._collection.insert(self.dbrecord)
        key_1 = [('when', -1)]
        self._collection.ensure_index(key_1, unique=False)
