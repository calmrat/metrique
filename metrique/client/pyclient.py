#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging
logging.basicConfig()
logger = logging.getLogger()

from metrique.client.http import client as http_client


class pyclient(http_client):
    def __init__(self, config_file=None, config_dir=None, host=None,
                 port=None, ssl=None, username=None, password=None):
        super(pyclient, self).__init__(config_dir, config_file)

        if host is not None:
            self.config.api_host = host
        if port is not None:
            self.config.api_port = port
        if username is not None:
            self.config.api_username = username
        if password is not None:
            self.config.api_username = password
