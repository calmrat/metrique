#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
import simplejson as json

from metrique.client.cubes.basecube import BaseCube


class BaseJSON(BaseCube):
    """
    Object used for communication with JSON files
    """

    def loads(self, json_str):
        # 'strict=False: control characters will be allowed in strings'
        return json.loads(json_str, strict=False)

    def loadi(self, json_iter):
        return self.loads(''.join(json_iter))

    def get_value(self, item, key, default):
        try:
            value = item.get(key, default)
        except AttributeError:
            return default
        if value:
            return value
        else:
            return default
