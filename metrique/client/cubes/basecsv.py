#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
import csv
import cStringIO
import re
from urllib2 import urlopen

from metrique.client.cubes.basecube import BaseCube


class BaseCSV(BaseCube):
    """
    Object used for communication with CSV files
    """

    def loaduri(self, uri):
        if re.match('https?://', uri):
            content = urlopen(uri).readlines()
        else:
            uri = re.sub('^file://', '', uri)
            content = open(uri).readlines()

        return self.loadi(content)

    def loadi(self, csv_iter):
        return self.loads('\n'.join([s.strip() for s in csv_iter]))

    def header_fields_dialect(self, csv_str):
        csvfile = cStringIO.StringIO(csv_str)
        sample = csvfile.read(1024)
        csvfile.seek(0)
        if not csv.Sniffer().has_header(sample):
            raise ValueError("CSV requires header as field map")
        dialect = csv.Sniffer().sniff(sample)

        reader = csv.reader(csvfile, dialect)
        rows = list(reader)
        fields = rows.pop(0)
        # header check, if we have one defined
        exp_fields = self.get_property('header')
        if exp_fields and fields != exp_fields:
            raise ValueError("Header mismatch!\n Got: %s\n Expected: %s" % (
                fields, exp_fields))
        return rows, fields, dialect

    def loads(self, csv_str):
        rows, fields, dialect = self.header_fields_dialect(csv_str)
        objects = []
        for row in rows:
            obj = {}
            for i, field in enumerate(fields):
                obj[field] = row[i]
            objects.append(obj)
        return objects

    def set_column(self, objects, key, value):
        if key == '_id':
            try:
                [o.update({key: o[value]}) for o in objects]
            except KeyError:
                raise KeyError(
                    "Invalid key object (%s). Available: %s" % (
                        value, o.keys()))
        else:
            [o.update({key: value}) for o in objects]
        return objects
