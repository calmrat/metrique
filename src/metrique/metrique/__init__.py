#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

# ATTENTION: this is the main interface for clients!
from metrique.core_api import HTTPClient as pyclient
# grrr ... avoids 'pep8' 'import but not used' error
pyclient

from metrique.utils import set_cube_path

# auto_load default cube paths (eg, ~/.metrique/cubes
# load defaults; can be overridden later
try:
    set_cube_path()
except:
    pass
