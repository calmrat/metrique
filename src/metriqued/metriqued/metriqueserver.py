#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)

from concurrent.futures import ThreadPoolExecutor
import os

from metriqued.config import metrique, mongodb
from metriqued.config import DEFAULT_METRIQUE_CONF
from metriqued.config import DEFAULT_MONGODB_CONF


class MetriqueServer(object):
    def __init__(self, config_dir=None,
                 metrique_config_file=None,
                 mongodb_config_file=None,
                 async=True, debug=None,
                 **kwargs):
        # FIXME: config's should have a single entry
        # where the file can include the directory
        # else it's assumed the config_dir is '/.'

        if not metrique_config_file:
            metrique_config_file = DEFAULT_METRIQUE_CONF
        if not mongodb_config_file:
            mongodb_config_file = DEFAULT_MONGODB_CONF

        self._config_dir = config_dir

        self._metrique_config_file = metrique_config_file
        self.metrique_config = metrique(metrique_config_file, config_dir)

        if debug is not None:
            self.metrique_config.debug = debug

        if debug is not True:
            self.metrique_config.async = async

        self._mongodb_config_file = metrique_config_file
        self.mongodb_config = mongodb(mongodb_config_file, config_dir)

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
        self._set_pid()
        k = self.metrique_config.server_thread_count
        self.executor = ThreadPoolExecutor(k)
        logger.debug("Metrique Server - Started")

        # Fail to start if we can't communicate with mongo
        try:
            assert self.mongodb_config.db_metrique_admin.db
        except Exception as e:
            host = self.mongodb_config.host
            raise SystemExit(
                '%s\nFailed to communicate with MongoDB (%s)' % (e, host))

    def stop(self):
        self._remove_pid()
        self.executor.shutdown()
        logger.debug("Metrique Server - Stopped")
