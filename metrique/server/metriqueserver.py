#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger()

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from multiprocessing import cpu_count
import os

from metrique.server import baseserver
from metrique.server.defaults import BACKUP_COUNT, MAX_BYTES
from metrique.server.drivers.drivermap import get_cubes
from metrique.server.utils.loghandlers import MongoLogHandler

from metrique.tools.constants import UTC
from metrique.tools.decorators import memo

# FIXME: add as metrique_config property
# NOTE: this means the tornado server will only be able to
# handle this number of requests simultaneously
MAX_WORKERS = cpu_count() * 10


class MetriqueServer(baseserver.BaseServer):
    executor = ThreadPoolExecutor(MAX_WORKERS)

    admin = baseserver.Admin()
    job = baseserver.JobManage()
    query = baseserver.Query()

    def __init__(self, **kwargs):
        super(MetriqueServer, self).__init__(**kwargs)
        logger.debug('Debug: %s' % self.metrique_config.debug)
        logger.debug('Async: %s' % self.metrique_config.async)
        self.admin = baseserver.Admin(**kwargs)
        self.job = baseserver.JobManage(**kwargs)
        self.query = baseserver.Query(**kwargs)

    @property
    def pid(self):
        if os.path.exists(self.metrique_config.pid_file):
            lines = open(self.metrique_config.pid_file).readlines()
            if not lines:
                pid = 0
            else:
                pid = int(lines[0])
        else:
            pid = 0
        return pid

    def _set_pid(self):
        if self.pid:
            raise RuntimeError("(%s) found in %s" % (self.pid, self.metrique_config.pid_file))

        _pid = os.getpid()
        with open(self.metrique_config.pid_file, 'w') as file:
            file.write(str(_pid))
        return _pid

    def _remove_pid(self):
        try:
            os.remove(self.metrique_config.pid_file)
        except IOError:
            pass

    def start(self):
        if self.metrique_config.log_to_mongo:
            hdlr = MongoLogHandler(self.mongodb_config.c_logs)
            logger.addHandler(hdlr)

        if self.metrique_config.log_to_file:
            hdlr = logging.handlers.RotatingFileHandler(self.log_file_path,
                                                        maxBytes=MAX_BYTES,
                                                        backupCount=BACKUP_COUNT)
            hdlr.setFormatter(self.metrique_config.log_formatter)
            logger.addHandler(hdlr)

        #### # FIXME: Need to clear user jobs as well!  ####
        self._set_pid()
        logger.debug("%s - Start" % __name__)

    def stop(self):
        logger.debug("%s - Stop" % __name__)
        self._remove_pid()

    def ping(self):
        logger.debug('got ping @ %s' % datetime.now(UTC))
        return 'pong'

    @property
    @memo
    def cubes(self):
        return get_cubes()
