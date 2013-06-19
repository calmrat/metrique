#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import inspect
import os

CONFIG_DIR = '~/.metrique/'
DEFAULT_CONFIG_FILE = 'http_api'

CLIENT_CUBES_PATH = os.path.join(CONFIG_DIR, 'cubes')

ipath = inspect.getfile(inspect.currentframe())
cwd = os.path.dirname(os.path.abspath(ipath))
base_path = '/'.join(cwd.split('/')[:-1])
SYSTEM_CUBES_PATH = '/'.join((base_path, 'client/cubes'))

METRIQUE_HTTP_HOST = '127.0.0.1'
METRIQUE_HTTP_PORT = 8080

MONGODB_HOST = '127.0.0.1'

JSON_EXT = 'json'
