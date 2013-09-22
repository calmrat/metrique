#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import os

from metriqued.tornadohttp import TornadoHTTPServer

test_root = os.path.dirname(os.path.abspath(__file__))
fixtures = os.path.join(test_root, 'fixtures')
configs = os.path.join(test_root, 'configs')


def init_new(metrique_config_file=None, mongodb_config_file=None):
    return TornadoHTTPServer(metrique_config_file, mongodb_config_file)


def test_initialize():
    assert init_new()


def test_start_stop():
    server = init_new()
    server.start(fork=True)
    server.stop()
