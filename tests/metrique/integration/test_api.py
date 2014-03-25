#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
Test that local test cubes can be loaded, data files extracted,
resulting objects pushed to the server, queried and so on. In
other words, test the 'cube_api' functionality of metrique client
and metrique server.
'''

import os
from metrique import pyclient
from metrique.result import Result

from pymongo.errors import OperationFailure

cwd = os.path.dirname(os.path.abspath(__file__))
TESTS_ROOT = '/'.join(cwd.split('/')[0:-1])

paths = [TESTS_ROOT]
pkgs = ['testcubes']

config = {
    "auth": False,
    "fsync": False,
    "host": "127.0.0.1",
    "journal": False,
    "tz_aware": True,
    "username": "test_user",
    "password": "test_user"
}

objects = [
    {'_oid': 1, 'test': 'yipee'}
]


def test_api():
    m = pyclient()
    m.user_remove('test_user', clear_db=True)
    assert m.user_register('test_user', 'test_user')
    cubes = ['csvcube_local', 'jsoncube_local']
    for cube in cubes:
        _cube = m.get_cube(cube=cube, pkgs=pkgs, cube_paths=paths, init=True)
        _cube.drop()

        assert _cube.count(date='~') == 0

        # first, just pull objects into memory
        result = _cube.get_objects().objects
        assert result is not None
        assert len(result) > 0

        assert _cube.count(date='~') == 0

        # second, flush to backend db
        _ids = _cube.get_objects(flush=True)
        assert _ids is not None
        assert len(_ids) > 0

        k = len(_ids)

        assert _cube.count(date='~') == k
        assert _cube.count() == k

        # a second flush of the same data should not result in
        # new objects being saved
        result = _cube.get_objects(flush=True)
        assert _cube.count(date='~') == k
        assert _cube.count() == k

        # FIXME: change some of the data and flush again
        # then update k so remaining count checks are consistent
        #k = len(result)

        # we should get back some results
        df = _cube.find(fields='~', date='~')
        assert df is not None
        # default obj type returned should be metrique.result.Result
        assert isinstance(df, Result)
        assert not df.empty

        # raw should return back a list of dicts
        raw = _cube.find(raw=True, fields='~', date='~')
        assert isinstance(raw, list)
        assert len(raw) > 0
        assert isinstance(raw[0], dict)

        # rename cube
        name = _cube.name
        new_name = 'renamed_%s' % name
        assert _cube.rename(new_name=new_name, drop_target=True)

        # can't rename if name already exists...
        try:
            assert _cube.rename(new_name=new_name)
        except OperationFailure:
            pass

        assert _cube.name == new_name
        assert _cube.name in _cube.ls()
        ## count should remain the same in renamed cube
        assert _cube.count(date='~') == k
        # drop the cube
        assert _cube.drop()
        assert _cube.name not in _cube.ls()

    assert _cube.user_remove('test_user', clear_db=True)


def test_user_api():
    m = pyclient(name='test_user', **config)

    m.user_remove('test_user', clear_db=True)
    assert m.user_register('test_user', 'test_user')

    # FIXME: what if user tries registering multiple times?
    #assert m.user_register('test_user', password)

    m.objects = objects
    _ids = m.flush()
    assert _ids is not None
    assert len(_ids) == 1

    assert m.user_remove('test_user', clear_db=True)
