#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
from datetime import datetime

from metriqued.utils import dt2ts
from metriqued.cubes import get_cube


def register(owner, cube):
    spec = {'_id': '__meta__'}
    update = {
        '$set': {
            '__created__': dt2ts(datetime.utcnow()),
            '__owner__': owner,
        },
        '$set': {
            '__read__': [owner],
        },
        '$set': {
            '__write__': [owner],
        },
        '$set': {
            '__admin__': [owner],
        }
    }
    _cube = get_cube(owner, cube, admin=True, create=True)
    return _cube.update(spec, update, upsert=True)
