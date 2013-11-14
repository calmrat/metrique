#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
Test that local test cubes can be loaded, data files extracted,
resulting objects pushed to the server, queried and so on. In
other words, test the 'cube_api' functionality of metrique client
and metrique server.
'''

from . import testutils
import os
from metrique import pyclient
from metrique.result import Result

cwd = os.path.dirname(os.path.abspath(__file__))
tests_root = '/'.join(cwd.split('/')[0:-1])

paths = [tests_root]
pkgs = ['testcubes']

username = password = 'testuser'

m = pyclient(username=username, password=password)


@testutils.runner
def test_extract():
    testutils.user_register(username, password)
    cubes = ['csvcube_local', 'jsoncube_local']
    for cube in cubes:
        _cube = m.get_cube(cube=cube, pkgs=pkgs, cube_paths=paths, init=True)
        _cube.cube_drop(quiet=True)  # to be sure it doesn't exist already...

        assert _cube.cube_register()

        result = _cube.extract()
        assert result

        # we should get back some results
        df = _cube.find(fields='~', date='~')
        assert df
        # default obj type returned should be metrique.result.Result
        assert isinstance(df, Result)

        # raw should return back a list of dicts
        raw = _cube.find(raw=True)
        assert isinstance(raw, list)
        assert len(raw) > 0
        assert isinstance(raw[0], dict)

        k = len(result)
        assert k == _cube.count(date='~')

        # a second extract of the same data should not result
        # new objects being saved
        result = _cube.extract()
        assert k == _cube.count(date='~')

        # journal should have been created
        journal_path = '~/.metrique/journal/%s__%s' % (username, _cube.name)
        assert os.path.exists(os.path.expanduser(journal_path))

        assert _cube.cube_drop()
    return True
