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
from requests import HTTPError

from metrique import pyclient
from metriqued.tornadohttp import TornadoHTTPServer
from metriqued.utils import get_pids


def runner(func):
    @wraps(func)
    def _runner(*args, **kwargs):
        p = Process(target=start_server)
        p.start()
        time.sleep(1)  # give a moment to startup
        try:
            assert func()
        finally:
            for pid in get_pids('~/.metrique/'):
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


def user_register(username, password):
    try:
        m = pyclient(username=username, password=password)
        m.user_register(username, password)
    except HTTPError:
        pass
