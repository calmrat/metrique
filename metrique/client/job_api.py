#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

''' "Metrique Job" related funtions '''

import logging
logger = logging.getLogger(__name__)

CMD = 'job'


def status(self, job_key):
    '''
    Fetch job status for a given metrique job
    identified by the job_key argument

    :param Integer job_key: id of the job
    '''
    return self._get(CMD, 'status', job_key)
