#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from distutils.sysconfig import get_python_lib
import os


DEFAULT_CONFIG_DIR = '~/.metrique'

DEFAULT_METRIQUE_HTTP_HOST = '127.0.0.1'
DEFAULT_METRIQUE_HTTP_PORT = 5420

DEFAULT_API_REL_PATH = 'api'
DEFAULT_API_SSL = False

DEFAULT_METRIQUE_LOGIN_URL = '/login'

DEFAULT_SYS_CUBES_BASE_PATH = 'metriquec/'
DEFAULT_SYSTEM_CUBES_PATH = os.path.join(
    get_python_lib(), DEFAULT_SYS_CUBES_BASE_PATH)

DEFAULT_CLIENT_CUBES_BASE_PATH = 'cubes/'
DEFAULT_CLIENT_CUBES_PATH = os.path.join(
    DEFAULT_CONFIG_DIR, DEFAULT_CLIENT_CUBES_BASE_PATH)
