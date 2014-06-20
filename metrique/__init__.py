#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from __future__ import unicode_literals, absolute_import

from ._version import __version__, version_info
# touch it to avoid pep8 error 'imported but unused'
__version__, version_info

# setup default root logger
import logging
log_format = "%(levelname)s.%(name)s.%(process)s:%(asctime)s:%(message)s"
logging.basicConfig(format=log_format)
logger = logging.getLogger()
logger.setLevel(logging.WARN)
logger = logging.getLogger('metrique')
logger.setLevel(logging.WARN)

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
os.environ['METRIQUE_STATIC'] = os.environ.get(
    'METRIQUE_STATIC', os.path.join(prefix, 'static'))
# if this isn't set in env already, we can only guess...
# first guess is top-level directory holding the metrique module
_mod_file = os.path.abspath(__file__)
_cwd = os.path.abspath('.')
_src = _cwd.replace(_mod_file, '')
_src_git = os.path.join(_src, '.git')
if not (os.path.exists(_src) and os.path.exists(_src_git)):
    _src = os.path.abspath('.')
    # last guess is just 'current working directory'
os.environ['METRIQUE_SRC'] = os.environ.get(
    'METRIQUE_SRC', _src)


# Don't locale.setlocale(); ... "Setting system default encoding is a
# bad idea because some modules and libraries you use can
# rely on the fact it is ascii. Don't do it.
# ... http://stackoverflow.com/questions/492483/

# Force all writes to stdout to be done with utf8
# This causes output coruption (in Ipython shell)
# import sys
# import codecs
# sys.stdout = codecs.getwriter('utf8')(sys.stdout)

# ATTENTION: this is the main interface for clients!
from metrique.metrique import Metrique as pyclient
from metrique.core_api import MetriqueContainer, MetriqueObject
# avoid lint 'defined by not used' error
pyclient, MetriqueContainer, MetriqueObject
