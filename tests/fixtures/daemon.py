#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from __future__ import unicode_literals

import logging
import os
from time import sleep

logger = logging.getLogger('metrique')

CACHE_DIR = os.environ.get('METRIQUE_CACHE') or '/tmp'


class sleeper(object):
    def __init__(self, stime=10):
        super(sleeper, self).__init__()
        self.stime = stime or 10

    def run(self):
        pid_file = os.path.join(CACHE_DIR, 'sleeper.pid')
        daemonize(pid_file=pid_file)
        sleep(self.stime)


if __name__ == '__main__':
    from metrique.utils import daemonize
    sleeper().run()
