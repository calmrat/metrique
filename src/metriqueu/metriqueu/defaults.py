#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from distutils.sysconfig import get_python_lib
import os


CONFIG_DIR = '~/.metrique'

METRIQUE_HTTP_HOST = '127.0.0.1'
METRIQUE_HTTP_PORT = 5420

API_REL_PATH = 'api'
API_SSL = False

METRIQUE_LOGIN_URL = '/login'

SYS_CUBES_BASE_PATH = 'metriquec/'
SYSTEM_CUBES_PATH = os.path.join(
    get_python_lib(), SYS_CUBES_BASE_PATH)

CLIENT_CUBES_BASE_PATH = 'cubes/'
CLIENT_CUBES_PATH = os.path.join(
    CONFIG_DIR, CLIENT_CUBES_BASE_PATH)
