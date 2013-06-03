#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>
# Contributor: "Juraj Niznan" <jniznan@redhat.com>

from datetime import timedelta


def get_timezone_converter(hours_delta):
    def timezone_converter(self, dt):
        try:
            return dt + timedelta(hours=hours_delta)
        except Exception:
            return None
    return timezone_converter
