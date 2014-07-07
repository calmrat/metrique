#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique.cubes.osinfo.rpm
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This module contains the generic metrique cube used
for extracting installed RPM details on a RPM based system.

.. note:: Target system expected to be RPM based!
'''

from __future__ import unicode_literals, absolute_import

import logging
logger = logging.getLogger('metrique')

import getpass
try:
    import paramiko
    HAS_PARAMIKO = True
except ImportError:
    logger.warn('paramiko not found (ssh: disabled)')
    HAS_PARAMIKO = False
import shlex
import socket
import subprocess

from metrique import pyclient
from metrique.utils import utcnow

FIELDS = ["name", "version", "release", "arch", "nvra", "license",
          "os", "packager", "platform", "sourcepackage", "sourcerpm",
          "summary"]


class Rpm(pyclient):
    """
    Class used for extracting data related to RPM's installed on
    a given system.

    :param fields: rpm -q fields to query
    :param ssh_host: hostname for running query on a remote host
    :param ssh_user: username for running query on a remote host
    :param ssh_pass: password for running query on a remote host
    """
    name = 'osinfo_rpm'

    def __init__(self, fields=FIELDS, ssh_host=None,
                 ssh_user=None, ssh_pass=None, **kwargs):
        self._fields = fields
        self.ssh_host = ssh_host
        self.ssh_user = ssh_user or getpass.getuser()
        self.ssh_pass = ssh_pass
        super(Rpm, self).__init__(**kwargs)

    def _ssh_cmd(self, fmt):
        if not HAS_PARAMIKO:
            raise ImportError("`pip install paramiko` required!")
        cmd = "rpm -qa --queryformat '%s'" % fmt
        logger.debug('[%s] Running: %s' % (self.ssh_host, cmd))
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.connect(
            self.ssh_host, username=self.ssh_user, password=self.ssh_pass)
        stdin, stdout, stderr = ssh.exec_command(cmd)
        output = stdout.readlines()
        return output

    def _local_cmd(self, fmt):
        cmd = "rpm -qa --queryformat '%s\n'" % fmt
        logger.debug('[LOCAL] Running: %s' % cmd)
        cmd = shlex.split(cmd)
        return subprocess.check_output(cmd)

    def get_objects(self, **kwargs):
        '''
        Run `rpm -q` command on a {local, remote} system to get back
        details of installed RPMs.

        Default rpm details extracted are as follows:
            * name
            * version
            * release
            * arch
            * nvra
            * license
            * os
            * packager
            * platform
            * sourcepackage
            * sourcerpm
            * summary
        '''
        fmt = ':::'.join('%%{%s}' % f for f in self._fields)
        if self.ssh_host:
            output = self._ssh_cmd(fmt)
        else:
            output = self._local_cmd(fmt)
        if isinstance(output, basestring):
            output = unicode(output, 'utf-8')
            output = output.strip().split('\n')
        lines = [l.strip().split(':::') for l in output]
        now = utcnow()
        host = self.ssh_host or socket.gethostname()
        for line in lines:
            obj = {'host': host, '_start': now}
            for i, item in enumerate(line):
                if item == '(none)':
                    item = None
                obj[self._fields[i]] = item
            obj['_oid'] = '%s__%s' % (host, obj['nvra'])
            self.objects.add(obj)
        return super(Rpm, self).get_objects(**kwargs)
