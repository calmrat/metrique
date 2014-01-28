#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

import codecs
from collections import MutableMapping
import os
import re

try:
    import simplejson as json
except ImportError:
    import json


class JSONConf(MutableMapping):
    '''
    Config object using json as its underlying data store

    Provides helper-methods for setting and saving
    options and config object properties

    When subclassing, make sure the __init__() executes in the following
    order::

        def __init__(self, config_file=None, **kwargs):
            # define top-level default config values
            config = {
                ...
            {

            # apply default config values on top of empty .config dict
            self.config.update(config)

            # update the config with the args from the config_file
            super(Config, self).__init__(config_file=config_file)

            # anything passed in explicitly gets precedence
            self.config.update(kwargs)

    '''
    _config = None
    config_file = None
    defaults = None
    default_config = None
    default_config_dir = None

    def __init__(self, config_file=None, defaults=None, **kwargs):
        if config_file is None and self.default_config:
            self.config_file = self.default_config
        else:
            self.config_file = config_file

        if self.defaults is None:
            self.defaults = {}
        if defaults:
            self.defaults.update(defaults)

        if self.config_file:
            if isinstance(self.config_file, JSONConf):
                self.config.update(self.config_file)
                self.config_file = self.config_file.config_file
            else:
                self.load_config()

        # apply kwargs passed in to config, overriding any preloaded defaults
        # or values set in config_file
        self.config.update(kwargs)

    @property
    def config(self):
        '''main store for top-level configuration key:value pairs'''
        if self._config is None:
            self._config = {}
        return self._config

    def __delitem__(self, key):
        del self.config[key]

    def __getattr__(self, name):
        if name in self.config or name in self.defaults:
            return self[name]
        else:
            raise AttributeError("invalid attribute: %s" % name)

    def __getitem__(self, key):
        if key in self.config:
            return self.config[key]
        elif key in self.defaults:
            return self.defaults[key]
        else:
            raise KeyError("Field %s is not set in config." % key)

    def __iter__(self):
        return iter(self.config)

    def __len__(self):
        return len(self.config)

    def __repr__(self):
        return repr(self.config)

    def __setattr__(self, name, value):
        if 'defaults' in self.__dict__ and name in self.defaults:
            # you can only set those values specified IN defaults
            self[name] = value
        else:
            super(JSONConf, self).__setattr__(name, value)

    def __setitem__(self, key, value):
        self.config[key] = value

    def __str__(self):
        return str(self.config)

    def _default(self, option, default=None, required=False):
        ''' Helper-Method for setting config argument, with default '''
        try:
            self.config[option]
        except KeyError:
            if default is None and required:
                raise ValueError(
                    "%s attribute is not set (required)" % option)
            else:
                self.config[option] = default
        return self.config[option]

    def load_config(self):
        ''' load json config file from disk '''
        # We don't want to throw exceptions if the default config file does not
        # exist.
        silent = self.config_file == self.default_config
        config_file = self.config_file
        if not isinstance(config_file, basestring):
            raise TypeError(
                "Unknown config_file type; got: %s" % type(config_file))
        if not re.search(r'\.json$', config_file, re.I):
            config_file = '.'.join((config_file, 'json'))

        config_file = os.path.expanduser(config_file)
        if not os.path.exists(config_file):
            # if default_config_dir is set and the config_file is
            # relative, attempt to find the conf relative to the path
            if self.default_config_dir and not os.path.isabs(config_file):
                path = os.path.expanduser(self.default_config_dir)
                _config_file = os.path.join(path, config_file)
            if not (os.path.exists(_config_file) or silent):
                raise IOError('Config file %s does not exist.' % config_file)
            else:
                config_file = _config_file
        try:
            with codecs.open(config_file, 'r', 'utf-8') as f:
                config = json.load(f)
        except Exception as e:
            raise TypeError(
                "Failed to load json file [%s] %s" % (config_file, e))
        self.config.update(config)
        self.config_file = config_file

    def dumps(self):
        '''dump the config as json string'''
        try:
            return json.dumps(self.config, indent=2)
        except TypeError:
            return unicode(self.config)

    def save(self, force=True, config_file=None):
        ''' save config json string dump to disk '''
        config_file = config_file or self.config_file
        if not os.path.exists(config_file):
            if force:
                #FIXME config dir
                config_dir = os.path.dirname(config_file)
                os.makedirs(config_dir)
            else:
                raise IOError("Path does not exist: %s" % config_file)
        with codecs.open(config_file, 'w', 'utf-8') as f:
            f.write(self.dumps())

    def setdefault(self, key, value):
        '''set "secondary" default value for key'''
        self.defaults[key] = value

    def values(self):
        '''return config (dict) values'''
        return self.config.values()
