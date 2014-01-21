#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import os
import time

from metriqued.tornadohttp import MetriqueHTTP
from metriqued.utils import get_pids

here = os.path.dirname(os.path.abspath(__file__))

pid_dir = '~/.metrique/pids'
pid_file = '~/.metrique/server.pid'
cert = os.path.join(here, 'cert.pem')
pkey = os.path.join(here, 'pkey.pem')


def start(**kwargs):
    m = MetriqueHTTP(**kwargs)
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
    kwargs = {
        'ssl': True,
        'ssl_certificate': cert,
        'ssl_certificate_key': pkey
    }
    start(**kwargs)
