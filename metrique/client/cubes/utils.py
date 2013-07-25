#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import os
import sys

from metrique.tools.defaults import CLIENT_CUBES_PATH, SYSTEM_CUBES_PATH
from metrique.tools import cube_pkg_mod_cls

CLIENT_CUBES_PATH = os.path.expanduser(CLIENT_CUBES_PATH)
SYSTEM_CUBES_PATH = os.path.expanduser(SYSTEM_CUBES_PATH)
DEFAULT_MODULE = 'metrique.client.cubes'


# FIXME: RENAME TO 'set_cubes_path'?
def set_cube_path(path=None):
    '''
    Add a given path to sys.path to enable calls to import.

    If no path provided, default to making *metrique.client.cubes*
    get added to the current namespace.
    '''
    if not path:
        path = CLIENT_CUBES_PATH
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


# FIXME: OBSOLETE? IS THIS USED ANYWHERE?
def check_paths(path=None):
    default_paths = [SYSTEM_CUBES_PATH, CLIENT_CUBES_PATH]
    if not path:
        paths = default_paths
    else:
        paths = []
        if not path.startswith('/'):
            for abs_path in default_paths:
                mod_path = os.path.join(abs_path, path)
                if not os.path.exists(mod_path):
                    continue
                paths.append(mod_path)
    return paths
