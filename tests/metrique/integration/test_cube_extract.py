#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
Test that local test cubes can be loaded, data files extracted,
resulting objects pushed to the server, queried and so on.

This test will fail if metriqued is not started on whatever host
the default pyclient config points too! ie, http_api.json
'''

from multiprocessing import Process
import os
import signal
import time
from requests import HTTPError

from metrique import pyclient
from metriqued.tornadohttp import TornadoHTTPServer
from metriqued.utils import get_pid_from_file

cwd = os.path.dirname(os.path.abspath(__file__))
tests_root = '/'.join(cwd.split('/')[0:-1])

paths = [tests_root]
pkgs = ['testcubes']

username = password = 'testuser'

pid_file = '~/.metrique/server.pid'

m = pyclient(username=username, password=password)


def start_server():
    m = TornadoHTTPServer(debug=True)
    m.start(fork=False)


def runner(func):
    p = Process(target=start_server)
    p.start()
    time.sleep(1)  # give a moment to startup
    try:
        assert func()
    finally:
        child_pid = get_pid_from_file(pid_file)
        os.kill(child_pid, signal.SIGINT)
        p.join()


def user_register(username, password):
    try:
        m.user_register(username, password)
    except HTTPError:
        pass


def drop_extract():
    user_register(username, password)
    cubes = ['csvcube_local', 'jsoncube_local']
    for cube in cubes:
        _cube = m.get_cube(cube=cube, pkgs=pkgs, cube_paths=paths, init=True)
        try:
            _cube.cube_drop(force=True)
        except HTTPError:
            pass
        _cube.cube_register()
        result = _cube.extract()
        assert result
        assert _cube.find(fields='~', date='~')
    return True


def test_drop_extract():
    runner(drop_extract)
