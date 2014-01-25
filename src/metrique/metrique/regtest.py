#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Juraj Niznan" <jniznan@redhat.com>

'''
This module regression test api related functionality
'''

import simplejson as json
import os


# FIXME: @jniznan: add docstrings

def fetch_objects(client, oids):
    return client.find('_oid in %s' % oids, fields='~', date='~',
                       merge_versions=False, raw=True)


class RegTest(object):
    def __init__(self, client, filename):
        self._client = client
        self._filename = filename

    def _test_objects(self, objs):
        '''
        Note: Run regtests only on objects after the initial activity import.
        '''
        logger = self._client.logger
        oids = list(set([obj['_oid'] for obj in objs]))
        if not oids:
            return
        mobjs = fetch_objects(self._client, oids)
        # build dictionary of the test objects by oid
        d_objs = dict([(oid, []) for oid in oids])
        for obj in objs:
            d_objs[obj['_oid']].append(obj)
            d_objs[obj['_oid']].sort(key=lambda o: o['_start'])
        # build dictionary of the metrique objects by oid
        d_mobjs = dict([(oid, []) for oid in oids])
        for obj in mobjs:
            d_mobjs[obj['_oid']].append(obj)
            d_mobjs[obj['_oid']].sort(key=lambda o: o['_start'])
        # now run the test:
        # won't compare _id
        failed = False
        for oid in oids:
            vers = d_objs[oid]
            mvers = d_mobjs[oid]
            if len(vers) != len(mvers):
                logger.warn('[%s]: Different number of versions '
                            '(%s test, %s metrique)' % (oid, len(vers),
                                                        len(mvers)))
                failed = True
            for obj, mobj in zip(vers, mvers):
                diff_fields = set(obj) ^ set(mobj)
                if diff_fields:
                    logger.warn('[%s]: Different field names: %s' %
                                (oid, list(diff_fields)))
                    failed = True
                for field in obj:
                    if field != '_id':
                        if obj[field] != mobj[field]:
                            logger.warn('[%s] @ %s: **%s**: test *%s*; '
                                        'metrique *%s*' % (oid, obj['_start'],
                                                           field, obj[field],
                                                           mobj[field]))
                            failed = True
        if failed:
            logger.warn('Regression test FAILED.')
        else:
            logger.warn('Regression test PASSED.')
        return not failed

    def _load_objects(self):
        _file = os.path.expanduser('~/.metrique/regtests/%s' % self._filename)
        try:
            with open(_file) as f:
                objs = json.load(f)
            return objs
        except IOError as e:
            self._client.logger.warn(e)

    def test(self):
        objs = self._load_objects()
        if objs is not None:
            return self._test_objects(objs)


def regtest(self, name):
    '''
    Runs a regtest with the given name.
    '''
    rt = RegTest(self, name)
    return rt.test()


def regtest_create(self, name, oids):
    '''
    Creates a regression test with the given name if the name is not already
    in use. All versions of the objects with the given oids will be included
    in the test.
    '''
    folder = os.path.expanduser('~/.metrique/regtests/')
    if not os.path.exists(folder):
        os.mkdir(folder)
    _file = os.path.expanduser('~/.metrique/regtests/%s' % name)
    if os.path.exists(_file):
        self.logger.warn('A regtest with the given name already exists. '
                         'It must be removed before the same name can be '
                         'used again.')
        return
    with open(_file, 'w') as f:
        json.dump(fetch_objects(self, oids), f)


def regtest_list(self):
    '''
    Lists available regtest names.
    '''
    folder = os.path.expanduser('~/.metrique/regtests/')
    return os.listdir(folder)


def regtest_remove(self, name):
    '''
    Removes a regtest with the specified name
    '''
    _file = os.path.expanduser('~/.metrique/regtests/%s' % name)
    os.remove(_file)
