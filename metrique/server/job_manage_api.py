#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
from bson.objectid import ObjectId

from metrique.server.job import job_save


@job_save('job_status')
def status(self, job_key):
    _id = ObjectId(job_key)
    spec = {'_id': _id}
    return self.mongodb_config.c_job_activity.find_one(spec)
