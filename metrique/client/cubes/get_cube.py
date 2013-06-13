#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import os
import sys

from metrique.tools.decorators import memo
from metrique.tools.defaults import CUBES_PATH


@memo
def get_cube(module, cube, path=None):
    if not path:
        path = CUBES_PATH
    path = os.path.expanduser(path)
    sys.path.append(path)
    _module = __import__(module, {}, {}, [cube])
    return getattr(_module, cube)
