#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)

from concurrent.futures import ThreadPoolExecutor
import os

from metrique.server.baseserver import BaseServer
from metrique.server.defaults import BACKUP_COUNT, MAX_BYTES


class MetriqueServer(BaseServer):
    def __init__(self, **kwargs):
        super(MetriqueServer, self).__init__(**kwargs)
        logger.debug('Debug: %s' % self.metrique_config.debug)
        logger.debug('Async: %s' % self.metrique_config.async)
        logger.debug(' Auth: %s' % self.metrique_config.auth)
        logger.debug('  SSL: %s' % self.metrique_config.ssl)

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
            raise RuntimeError(
                "(%s) found in %s" % (self.pid, self.metrique_config.pid_file))

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
        if self.metrique_config.log_to_file:
            hdlr = logging.handlers.RotatingFileHandler(
                self.log_file_path, maxBytes=MAX_BYTES,
                backupCount=BACKUP_COUNT)
            hdlr.setFormatter(self.metrique_config.log_formatter)
            logger.addHandler(hdlr)
        self._set_pid()
        k = self.metrique_config.server_thread_count
        self.executor = ThreadPoolExecutor(k)
        logger.debug("Metrique Server - Started")

        # Fail to start if we can't communicate with mongo
        try:
            assert self.mongodb_config.db_metrique_admin.db
        except Exception as e:
            raise SystemExit('%s\nFailed to communicate with MongoDB' % e)

    def stop(self):
        self._remove_pid()
        self.executor.shutdown()
        logger.debug("Metrique Server - Stopped")
