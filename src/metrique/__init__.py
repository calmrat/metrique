#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
Client package covers client side of metrique,
including tornado access, work with results,
client side configuration and opensource cubes.
'''

from result import Result
from metrique.client.cubes import set_cube_path
from metrique.client.cubes.utils import get_cube
from metrique.client.config import DEFAULT_CLIENT_CUBES_PATH
from metrique.client.http_api import HTTPClient as pyclient

# auto_load default cube paths (eg, ~/.metrique/cubes
# load defaults; can be overridden later
try:
    set_cube_path(DEFAULT_CLIENT_CUBES_PATH)
except:
    pass
