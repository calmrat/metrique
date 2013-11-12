#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from copy import copy
import multiprocessing as mp
import os
import signal
import time

from metriqued.tornadohttp import TornadoHTTPServer
from metriqued.utils import get_pid_from_file
from metriqueu.jsonconf import JSONConf

here = os.path.dirname(os.path.abspath(__file__))

metriqued_config = JSONConf()
mongodb_config = JSONConf()

pid_file = '~/.metrique/server.pid'
cert = os.path.join(here, 'cert.pem')
pkey = os.path.join(here, 'pkey.pem')

kwargs = {'debug': True,
          'mongodb_config': mongodb_config}


def start_server(**kwargs):
    m = TornadoHTTPServer(config_file=metriqued_config, **kwargs)
    m.start(fork=False)


def validate_startup(p):
    try:
        p.start()
        time.sleep(1)  # give a moment to startup
        child_pid = get_pid_from_file(pid_file)
    finally:
        os.kill(child_pid, signal.SIGINT)
        p.join()


def test_default_start():
    _kwargs = copy(kwargs)
    p = mp.Process(target=start_server, kwargs=_kwargs)
    validate_startup(p)


def test_ssl_start():
    _kwargs = copy(kwargs)
    kwargs.update({
        'ssl': True,
        'ssl_certificate': cert,
        'ssl_certificate_key': pkey
    })
    p = mp.Process(target=start_server, kwargs=_kwargs)
    validate_startup(p)
