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
TESTS_ROOT = '/'.join(cwd.split('/')[0:-1])
GNUPG_DIR = os.path.join(cwd, 'gnupg')

paths = [TESTS_ROOT]
pkgs = ['testcubes']

username = password = 'admin'

config = dict(username=username,
              password=password,
              debug=2)


@testutils.runner
def test_admin():
    m = pyclient(**config)
    m.user_remove(username, quiet=True)  # to be sure it doesn't exist already
    assert m.user_register(username, password)
    m.user_remove(username, quiet=True)  # to be sure it doesn't exist already


@testutils.runner
def test_api():
    m = pyclient(**config)
    m.user_remove(username, quiet=True)  # to be sure it doesn't exist already
    assert m.user_register(username, password)
    cubes = ['csvcube_local', 'jsoncube_local']
    for cube in cubes:
        _cube = m.get_cube(cube=cube, pkgs=pkgs, cube_paths=paths, init=True)
        _cube.cookiejar_clear()
        _cube.cube_drop(quiet=True)  # to be sure it doesn't exist already...

        assert _cube.cube_register()

        _cube.extract()
        result = _cube.result
        assert result

        # we should get back some results
        df = _cube.find(fields='~', date='~')
        assert df
        # default obj type returned should be metrique.result.Result
        assert isinstance(df, Result)

        # raw should return back a list of dicts
        raw = _cube.find(raw=True, fields='~', date='~')
        assert isinstance(raw, list)
        assert len(raw) > 0
        assert isinstance(raw[0], dict)

        k = len(result)
        assert k == _cube.count(date='~')

        # a second extract of the same data should not result
        # new objects being saved
        result = _cube.extract()
        assert k == _cube.count(date='~')

        # rename cube
        name = _cube.name[:]
        new_name = 'renamed_%s' % name
        assert _cube.cube_rename(new_name=new_name)
        assert _cube.name == new_name
        ## count should remain the same in renamed cube
        assert k == _cube.count(date='~')
        assert _cube.cube_rename(new_name=name)
        # drop the cube
        assert _cube.cube_drop()
        assert _cube.cube_id not in _cube.cube_list_all()

    # with the last cube, do a few more things...
    # re-register
    _cube = m.get_cube(cube=cubes[0], pkgs=pkgs, cube_paths=paths, init=True)
    assert _cube.cube_register()
    name = '%s__%s' % (username, _cube.name)
    assert name in _cube.cube_list_all()
    # drop the cube
    assert _cube.cube_drop()
    assert name not in _cube.cube_list_all()
    # then drop the user
    assert _cube.user_remove()


@testutils.runner
def test_user_api():
    fingerprint = '894EE1CEEA61DC3D7D20327C4200AD1F2F22F46C'

    m = pyclient(name='test_user',
                 gnupg_dir=GNUPG_DIR,
                 gnupg_fingerprint=fingerprint,
                 **config)

    assert m.config.gnupg_fingerprint == fingerprint

    assert m.user_register(username, password)
    # should except if trying to register again
    try:
        m.user_register(username, password)
    except:
        pass

    aboutme = m.aboutme()
    assert aboutme

    try:
        # py2.6 doesn't have OrderedDict and gnupg module
        # depends on it at the moment; pull request to fix
        # it has been made;
        # https://github.com/isislovecruft/python-gnupg/pull/36
        import collections.OrderedDict
    except ImportError:
        pass
    else:
        assert m.config.gnupg_pubkey
        pubkey = m.config.gnupg_pubkey
        gnupg = {'pubkey': pubkey, 'fingerprint': fingerprint}
        result = m.user_update_profile(gnupg=gnupg)
        assert result['previous'] == aboutme
        assert 'gnupg' in result['now']
        assert result['now']['gnupg']['fingerprint'] == fingerprint
        assert result['now']['gnupg']['pubkey'] == pubkey

    assert m.user_remove()
