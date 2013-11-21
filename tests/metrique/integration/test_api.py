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
def test_api():
    m = pyclient(**config)
    assert m.user_register(username, password)
    cubes = ['csvcube_local', 'jsoncube_local']
    for cube in cubes:
        _cube = m.get_cube(cube=cube, pkgs=pkgs, cube_paths=paths, init=True)
        _cube.cookiejar_clear()
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

        # journal should have been created
        journal_path = '~/.metrique/journal/%s__%s' % (username, _cube.name)
        assert os.path.exists(os.path.expanduser(journal_path))

        # rename cube
        name = _cube.name[:]
        new_name = 'renamed_%s' % name
        assert _cube.cube_rename(new_name=new_name)
        assert _cube.name == new_name
        ## count should remain the same in renamed cube
        assert k == _cube.count(date='~')
        assert _cube.cube_rename(new_name=name)

        assert _cube.cube_drop()

    # with the last cube, do a few more things...
    # re-register and then drop the user
    assert _cube.cube_register()
    assert _cube.user_remove()
    name = '%s__%s' % (username, _cube.name)
    assert name not in _cube.cube_list_all()


@testutils.runner
def test_user_api():
    fingerprint = '894EE1CEEA61DC3D7D20327C4200AD1F2F22F46C'

    m = pyclient(gnupg_dir=GNUPG_DIR,
                 gnupg_fingerprint=fingerprint,
                 **config)

    assert m.config.gnupg_fingerprint == fingerprint
    assert m.config.gnupg_pubkey

    assert m.user_register(username, password)
    # should except if trying to register again
    try:
        m.user_register(username, password)
    except:
        pass

    aboutme = m.aboutme()
    assert aboutme

    pubkey = m.config.gnupg_pubkey
    gnupg = {'pubkey': pubkey, 'fingerprint': fingerprint}
    result = m.user_update_profile(gnupg=gnupg)
    assert result['previous'] == aboutme
    assert 'gnupg' in result['now']
    assert result['now']['gnupg']['fingerprint'] == fingerprint
    assert result['now']['gnupg']['pubkey'] == pubkey

    assert m.user_remove()
