#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import csv
import cStringIO
import logging
logger = logging.getLogger(__name__)

from metrique.server.drivers.basedriver import BaseDriver


class CSV(BaseDriver):
    """
    Object used for communication with CSV files
    """

    def _loadi(self, csv_iter):
        return self._loads('\n'.join([s.strip() for s in csv_iter]))

    def _loads(self, csv_str):
        csvfile = cStringIO.StringIO(csv_str)
        sample = csvfile.read(1024)
        csvfile.seek(0)
        dialect = csv.Sniffer().sniff(sample)
        reader = csv.reader(csvfile, dialect)
        self._has_header = csv.Sniffer().has_header(sample)
        self._reader = [row for row in reader]

        if self._has_header:
            self._header = self._reader.pop(0)
            # header check, if we have one defined
            exp_header = self.get_field_property('header', default=[])
            if self._header != exp_header:
                raise ValueError("Header mismatch!\n Got: %s\n Expected: %s" % (self._header, exp_header))
        return self._reader
