#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from ._version import __version__
__version__  # touch it to avoid pep8 error 'imported but unused'

# setup default root logger
import logging
log_format = "%(name)s.%(process)s:%(asctime)s:%(message)s"
log_format = logging.Formatter(log_format)
logger = logging.getLogger()
logger.setLevel(logging.WARN)
hdlr = logging.StreamHandler()
hdlr.setFormatter(log_format)
logger.addHandler(hdlr)

import os
# if HOME environment variable is set, use that
# useful when running 'as user' with root (supervisord)
home = os.environ['METRIQUE_HOME'] = os.environ.get(
    'METRIQUE_HOME', os.path.expanduser('~/'))
prefix = os.environ['METRIQUE_PREFIX'] = os.environ.get(
    'METRIQUE_PREFIX', os.path.join(home, '.metrique'))
os.environ['METRIQUE_ETC'] = os.environ.get(
    'METRIQUE_ETC', os.path.join(prefix, 'etc'))
os.environ['METRIQUE_LOGS'] = os.environ.get(
    'METRIQUE_LOGS', os.path.join(prefix, 'logs'))
os.environ['METRIQUE_TMP'] = os.environ.get(
    'METRIQUE_TMP', os.path.join(prefix, 'tmp'))
os.environ['METRIQUE_CACHE'] = os.environ.get(
    'METRIQUE_CACHE', os.path.join(prefix, 'cache'))

# FIXME: good idea?
#import locale
#locale.setlocale(locale.LC_ALL, '')

# ATTENTION: this is the main interface for clients!
from metrique.mongodb_api import MongoDBClient as pyclient
pyclient
