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

# auto_load default cube paths (eg, ~/.metrique/cubes
try:
    set_cube_path()
except:
    pass
