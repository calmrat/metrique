#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

# setup default root logger
import logging
log_format = "%(name)s.%(process)s:%(asctime)s:%(message)s"
log_format = logging.Formatter(log_format)
logger = logging.getLogger()
logger.setLevel(logging.WARN)
hdlr = logging.StreamHandler()
hdlr.setFormatter(log_format)
logger.addHandler(hdlr)

# FIXME: good idea?
#import locale
#locale.setlocale(locale.LC_ALL, '')

# ATTENTION: this is the main interface for clients!
from metrique.core_api import pyclient
pyclient  # touch it to avoid pep8 error 'imported but unused'
