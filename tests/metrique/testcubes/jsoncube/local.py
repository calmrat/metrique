#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import os

from metrique import pyclient

tmp = os.path.expanduser('~/.metrique/tmp/')

cwd = os.path.dirname(os.path.abspath(__file__))
here = '/'.join(cwd.split('/')[0:-2])
test_file_path = 'meps.json'
JSON_FILE = os.path.join(here, test_file_path)


class Local(pyclient):
    name = 'tests_jsondata'

    def get_objects(self, uri=JSON_FILE, **kwargs):
        content = self.load(uri, filetype='json', raw=True)
        print content
        # the content needs to be re-grouped
        for k, obj in content.items():
            obj.update({'_oid': k})
            self.objects.add(obj)
        return super(Local, self).get_objects(**kwargs)


if __name__ == '__main__':
    from metriquec.argparsers import cube_cli
    cube_cli(Local)
