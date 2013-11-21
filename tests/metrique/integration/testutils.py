#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
Utility functions for testing metrique and metriqued (etc) integration
'''

from functools import wraps
from multiprocessing import Process
import os
import sys
import time

from metriqued.tornadohttp import TornadoHTTPServer
from metriqued.utils import get_pids

USER_DIR = os.path.expanduser('~/.metrique')


def runner(func):
    @wraps(func)
    def _runner(*args, **kwargs):
        p = Process(target=start_server)
        p.start()
        time.sleep(1)  # give a moment to startup
        try:
            func()
        finally:
            for pid in get_pids(USER_DIR):
                try:
                    os.kill(pid, 15)
                except:
                    pass
            p.join()
    return _runner


def start_server():
    pid = os.fork()
    if pid == 0:
        TornadoHTTPServer(debug=2).start()
    else:
        sys.exit()
