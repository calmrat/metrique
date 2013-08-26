#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import inspect
import os
import sys


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

# MODULE name of where default built-in cubes live
DEFAULT_MODULE = 'metrique.client.cubes'

# PATH to where default client cubes are expected to live
ipath = inspect.getfile(inspect.currentframe())
cwd = os.path.dirname(os.path.abspath(ipath))
base_path = '/'.join(cwd.split('/')[:-1])
SYSTEM_CUBES_PATH = '/'.join((base_path, 'client/cubes'))


def set_cube_path(path):
    '''
    Add a given path to sys.path to enable calls to import.

    If no path provided, default to making *metrique.client.cubes*
    get added to the current namespace.
    '''
    path = os.path.expanduser(path)
    sys.path.append(path)
    # also append system cubes path for easy/consistent importing
    sys.path.append(SYSTEM_CUBES_PATH)
    sys.path = list(set(sys.path))  # make sure we don't have dups...
    return path


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
