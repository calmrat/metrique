#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
import simplejson as json

from metrique.server.drivers.basedriver import BaseDriver


class JSON(BaseDriver):
    """
    Object used for communication with JSON files
    """

    def _loads(self, json_str):
        try:
            # 'strict=False: control characters will be allowed in strings'
            self._reader = json.loads(json_str, strict=False)
        except Exception:
            logger.debug("Failed to json.loads: \n %s" % json_str)
            raise
        return self._reader

    def _loadi(self, json_iter):
        self._reader = self._loads(''.join(json_iter))
        return self._reader

    def _get_values(self, item, key, default):
        try:
            value = item.get(key, default)
        except AttributeError:
            return default
        if value:
            return value
        else:
            return default
