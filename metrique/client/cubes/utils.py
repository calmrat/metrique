#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import os
import sys

from metrique.client.config import DEFAULT_SYSTEM_CUBES_PATH


def cube_pkg_mod_cls(cube):
    '''
    Convert 'pkg_mod' -> pkg, mod, Cls

    eg:
        tw_tweet -> tw, tweet, Tweet
        tw_tweet_users -> tw, tweet_users, TweetUsers

    Use for dynamically importing cube classes

    Assumes `Metrique Cube Naming Convention` is used
    '''
    _cube = cube.split('_')
    pkg = _cube[0]
    mod = '_'.join(_cube[1:])
    cls = ''.join([s[0].upper() + s[1:] for s in _cube[1:]])
    return pkg, mod, cls


def set_cube_path(path):
    '''
    Add a given path to sys.path to enable calls to import.

    If no path provided, default to making *metrique.client.cubes*
    get added to the current namespace.
    '''
    if path:
        path = os.path.expanduser(path)
        sys.path.append(path)
    # also append system cubes path for easy/consistent importing
    sys.path.append(DEFAULT_SYSTEM_CUBES_PATH)
    sys.path = sorted(set(sys.path))  # make sure we don't have dups...
    return sys.path


def get_cube(cube, path=None):
    '''
    Wraps __import__ to dynamically locate and load a client cube.

    Paremeters
    ----------
    module : str
        Module name (eg, 'jkns.build')
    cube : str
        Name of the cube Class to be imported from given module (eg, 'Build')

    '''
    set_cube_path(path)
    pkg, mod, cls = cube_pkg_mod_cls(cube)
    _pkg = __import__(pkg, fromlist=[mod])
    _mod = getattr(_pkg, mod)
    _cls = getattr(_mod, cls)
    return _cls
