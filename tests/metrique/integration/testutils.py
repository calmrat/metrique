#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
Utility functions for testing metrique and metriqued (etc) integration
'''

from functools import wraps
from multiprocessing import Process
import os
import time

from metriqued.tornadohttp import MetriqueHTTP
from metriqueu.utils import get_pids

pid_dir = os.path.expanduser('~/.metrique/pids')


def runner(func):
    @wraps(func)
    def _runner(*args, **kwargs):
        for pid in get_pids(pid_dir):
            os.kill(pid, 9)
        p = Process(target=start_server)
        p.start()
        time.sleep(1)  # give a moment to startup
        try:
            func()
        finally:
            for pid in get_pids(pid_dir):
                os.kill(pid, 15)
            p.join()
    return _runner


def start_server():
    pid = os.fork()
    if pid == 0:
        MetriqueHTTP(debug=2).start()
