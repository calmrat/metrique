#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

from metrique.tools.jsonconfig import JSONConfig


def get_config(config_file, config_dir=None, **kwargs):
    return JSONConfig(config_file, config_dir, **kwargs)
