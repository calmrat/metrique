#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import re

DEFAULT_BASE_MODULE = 'metrique.server.drivers'


class BaseDriverMap(object):
    '''
    '''
    prefix = None

    def __init__(self, base_module=None, **kwargs):
        '''
        '''
        if not base_module:
            base_module = DEFAULT_BASE_MODULE
        self._base_module = base_module
        self._kwargs = kwargs
        self.enabled = True  # default: set enabled to true

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)

    def __contains__(self, value):
        return value in self._dict

    def __getitem__(self, name):
        return self._get_driver(name)

    def __str__(self):
        return str(self._dict)

    @property
    def drivers(self):
        # FIXME: memoize
        __dict = {}
        for key in self._dict:
            __dict[key] = self[key]
        return __dict

    def _get_driver(self, name, force=False):
        stripped_name = re.sub('%s_' % self.prefix, '', name)
        try:
            mod_str = self._dict[stripped_name]
        except KeyError:
            raise KeyError("Invalid Driver: %s_%s" % (self.prefix, stripped_name))
        inst = self._mapper(mod_str, name)
        # FIXME: only return back enabled drivers
        # unless force is in affect...
        return inst

    def _mapper(self, rel_module, name):
        '''
        Expects the class name to be the same as the module name!
        '''
        name = '_'.join((self.prefix, name))
        cls_name = rel_module.split('.')[-1]
        mod_full = '.'.join((self._base_module, rel_module))
        mod = __import__(mod_full, globals(), locals(), [True], -1)
        cls = getattr(mod, cls_name)
        instance = cls(name=name, **self._kwargs)
        return instance
