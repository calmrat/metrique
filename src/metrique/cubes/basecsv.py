#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import csv
import cStringIO
import os
import re
from urllib2 import urlopen

from metrique.client.cubes.basecube import BaseCube


class BaseCSV(BaseCube):
    """
    Object used for extracting data from CSV files

    It's possible to load from a http(s):// or file://.

    The field names are defined by the column headers,
    therefore column heders are required in the csv.
    """

    def loaduri(self, uri):
        ''' Load csv from a given uri.

            Supports: http(s) or from file
        '''
        if re.match('https?://', uri):
            content = urlopen(uri).readlines()
        else:
            uri = re.sub('^file://', '', uri)
            uri = os.path.expanduser(uri)
            content = open(uri).readlines()
        return self.loadi(content)

    def loads(self, csv_str):
        '''
        Given a string of newline spaced csv, try
        to sniff out the header to aquire the field
        names, and also guess the *csv dialect*.
        '''
        rows, fields, dialect = self.header_fields_dialect(csv_str)
        objects = []
        for row in rows:
            obj = {}
            for i, field in enumerate(fields):
                obj[field] = row[i]
            objects.append(obj)
        return objects

    def loadi(self, csv_iter):
        '''
        Given an iterator, strip and join all results
        into a newline separated string to load
        the csv as a string and return it.
        '''
        return self.loads('\n'.join([s.strip() for s in csv_iter]))

    # FIXME: REFACTOR to split out header_fields and dialect
    # into two separate methods?
    def header_fields_dialect(self, csv_str):
        '''
        Given a newline separated string of csv, load it
        as a file like object.

        Then, try to sniff out the header to get the field
        names to be used in the objects extracted. If
        there is no header, raise `ValueError`, since
        otherwise we would have no way to know how to
        name the fields.

        While we have the csv file handy, sniff it to
        figure out the dialect of the csv. Dialect refers
        to properties of the csv itself, like which
        type of quotes are used, what separator character,
        etc. A common dialect is `Excel`.
        '''
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

    def set_column(self, objects, key, value, **kwargs):
        '''
        Save an additional column/field to all objects in memory
        '''
        if type(value) is type or hasattr(value, '__call__'):
            # we have class or function; use the resulting object after
            # init/exec
            [o.update({key: str(value(**kwargs))}) for o in objects]
        elif key == '_oid':
            try:
                [o.update({key: o[value]}) for o in objects]
            except KeyError:
                raise KeyError(
                    "Invalid key object (%s). Available: %s" % (
                        value, o.keys()))
        else:
            [o.update({key: value}) for o in objects]
        return objects
