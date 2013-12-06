#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import os

from metrique.utils import get_cube
jsondata_objs = get_cube('jsondata_objs')

tmp = os.path.expanduser('~/.metrique/tmp/')

cwd = os.path.dirname(os.path.abspath(__file__))
here = '/'.join(cwd.split('/')[0:-2])
test_file_path = 'meps.json'
JSON_FILE = os.path.join(here, test_file_path)


class Local(jsondata_objs):
    name = 'tests_jsondata'

    def get_objects(self, uri=JSON_FILE, **kwargs):
        content = self.load(uri)
        # the content needs to be re-grouped
        objs = []
        for k, v in content.items():
            v.update({'_oid': k})
            objs.append(v)
        self.objects = objs
        return objs


if __name__ == '__main__':
    from metriquec.argparsers import cube_cli
    cube_cli(Local)
