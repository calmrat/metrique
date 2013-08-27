#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

try:
    from client.http_api import HTTPClient as pyclient
except ImportError:
    pyclient = None
