#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import simplejson as json

from metrique.client.cubes.basecube import BaseCube


class BaseJSON(BaseCube):
    """
    Object used for extracting data in JSON format
    """

    def loads(self, json_str, strict=False):
        '''
        Given a string of valid JSON, load it into memory.

        No strict loading (DEFAULT); ignore control characters
        '''
        # 'strict=False: control characters will be allowed in strings'
        return json.loads(json_str, strict=strict)

    def loadi(self, json_iter):
        '''
        Given an iterator object, convert it to simple
        joined string, then try to load the json
        as as a string.
        '''
        return self.loads(''.join(json_iter))
