#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from copy import copy
import os
import pytest
import time

from metriqued.tornadohttp import TornadoHTTPServer
from metriqued.utils import get_pids
from metriqueu.jsonconf import JSONConf

here = os.path.dirname(os.path.abspath(__file__))

metriqued_config = JSONConf()
mongodb_config = JSONConf()

pid_file = '~/.metrique/server.pid'
cert = os.path.join(here, 'cert.pem')
pkey = os.path.join(here, 'pkey.pem')

kwargs = {'debug': True,
          'mongodb_config': mongodb_config}


def start(**kw):
    m = TornadoHTTPServer(metriqued_config, **kw)
    try:
        pid = m.start(fork=True)
    except SystemExit:
        return True
    else:
        time.sleep(1)
        pids = get_pids('~/.metrique')
        open('/tmp/out', 'w').write(str(pids))
        try:
            assert pid in pids
        finally:
            try:
                os.kill(pid, 9)
            except OSError:
                pass


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
