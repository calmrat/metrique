#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import os
import sys

from metrique.tools.defaults import CLIENT_CUBES_PATH, SYSTEM_CUBES_PATH
CLIENT_CUBES_PATH = os.path.expanduser(CLIENT_CUBES_PATH)
SYSTEM_CUBES_PATH = os.path.expanduser(SYSTEM_CUBES_PATH)

DEFAULT_MODULE = 'metrique.client.cubes'


def set_cube_path(path=None):
    if not path:
        path = CLIENT_CUBES_PATH
    path = os.path.expanduser(path)
    sys.path.append(path)
    return path


def get_cube(module, cube, path=None):
    '''
    Wraps __import__ to dynamically locate and load a client cube.

    Paremeters
    ----------
    module : str
        Module name (eg, 'jkns.build')
    cube : str
        Name of the cube Class to be imported from given module (eg, 'Build')

    '''
    assert set_cube_path(path)

    try:
        _module = __import__(module, {}, {}, [cube])
    except ImportError as e:
        module = '%s.%s' % (DEFAULT_MODULE, module)
        try:
            _module = __import__(module, {}, {}, [cube])
        except:
            raise ImportError(e)
    return getattr(_module, cube)


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
