#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
Basic CSV based cube for extracting data from CSV
'''

from metriquec.basecsv import BaseCSV


class Rows(BaseCSV):
    """
    Object used for communication with generic CSV objects
    """
    name = 'csv_rows'

    #defaults = {
        #'header': ['column1', 'column2', ...]
    #}
    # field check can be made by defining 'header' property
    # in defaults above. A check will be made to compare the
    # expected header to actual

    def extract(self, uri, _oid, _start=None, type_map=None, **kwargs):
        '''
        '''
        self.logger.debug("Loading CSV: %s" % uri)
        objects = self.loaduri(uri)
        # save the uri for reference too
        objects = self.set_column(objects, 'uri', uri)
        objects = self.set_column(objects, '_oid', _oid)
        objects = self.set_column(objects, '_start', _start)
        objects = self.normalize_types(objects, type_map)
        objects = self.normalize_nones(objects)
        return self.cube_save(objects)


if __name__ == '__main__':
    from metriquec.argparsers import cube_cli
    cube_cli(Rows)
