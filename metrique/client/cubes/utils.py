#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import os
import sys

from metrique.tools.defaults import CUBES_PATH

DEFAULT_MODULE = 'metrique.client.cubes'


def set_cube_path(path=None):
    if not path:
        path = CUBES_PATH
    path = os.path.expanduser(path)
    sys.path.append(path)
    return path


def get_cube(module, cube, path=None):
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
