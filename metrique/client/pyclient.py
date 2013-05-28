#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

import logging
logging.basicConfig()
logger = logging.getLogger()

from metrique.client.http import client as http_client


class pyclient(http_client):
    def __init__(self, config_file=None, config_dir=None, metrique_http_host=None,
                 metrique_http_port=None):
        super(pyclient, self).__init__(config_dir, config_file)

        if metrique_http_host:
            self.config.metrique_http_host = metrique_http_host
        if metrique_http_port:
            self.config.metrique_http_port = metrique_http_port
