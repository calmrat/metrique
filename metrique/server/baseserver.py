#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
from datetime import datetime
from socket import getfqdn

FQDN = getfqdn()

from metrique.server.config import metrique, mongodb
from metrique.server.cubes import list_cubes, list_cube_fields
from metrique.server.defaults import METRIQUE_CONF, MONGODB_CONF


class BaseServer(object):
    def __init__(self, config_dir=None,
                 metrique_config_file=None, mongodb_config_file=None):
        if not metrique_config_file:
            metrique_config_file = METRIQUE_CONF
        if not mongodb_config_file:
            mongodb_config_file = MONGODB_CONF

        self._config_dir = config_dir

        self._metrique_config_file = metrique_config_file
        self.metrique_config = metrique(metrique_config_file, config_dir)

        self._mongodb_config_file = metrique_config_file
        self.mongodb_config = mongodb(mongodb_config_file, config_dir)

    def ping(self):
        logger.debug('got ping @ %s' % datetime.utcnow())
        return 'PONG (%s)' % FQDN

    def list_cubes(self):
        # arg = username... return only cubes with 'r' access
        return list_cubes()

    def list_cube_fields(self, cube,
                         exclude_fields=None, _mtime=False):
        # arg = username... return only cubes with 'r' access
        return list_cube_fields(cube, exclude_fields, _mtime=_mtime)
