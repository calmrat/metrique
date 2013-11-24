#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from copy import copy
import os
import time

from metriqued.tornadohttp import TornadoHTTPServer
from metriqued.utils import get_pids
from metriqueu.jsonconf import JSONConf

here = os.path.dirname(os.path.abspath(__file__))

metriqued_config = JSONConf()
mongodb_config = JSONConf()

pid_dir = '~/.metrique/pids'
pid_file = '~/.metrique/server.pid'
cert = os.path.join(here, 'cert.pem')
pkey = os.path.join(here, 'pkey.pem')

kwargs = {'debug': True,
          'mongodb_config': mongodb_config}


def start(**kw):
    m = TornadoHTTPServer(metriqued_config, **kw)
    pid = m.start(fork=True)
    if pid == 0:
        pass
    else:
        time.sleep(1)
        pids = get_pids(pid_dir)
        try:
            assert pid in pids
        finally:
            os.kill(pid, 9)  # insta-kill


def test_default_start():
    start()


def test_ssl_start():
    kw = copy(kwargs)
    kw.update({
        'ssl': True,
        'ssl_certificate': cert,
        'ssl_certificate_key': pkey
    })
    start(**kw)
