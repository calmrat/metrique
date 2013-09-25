#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
import os
import re
import cStringIO

try:
    import simplejson as json
except ImportError:
    import json

JSON_EXT = 'json'


class JSONConf(object):
    '''
        Config object using json as its underlying data store

        Provides helper-methods for setting and saving
        options and config object properties
    '''
    def __init__(self, config_file, default=None, autosave=False,
                 force=False, ignore_comments=True):
        if not config_file:
            raise ValueError("No config file defined")
        elif isinstance(config_file, JSONConf):
            config_file = config_file.config_file
        elif isinstance(config_file, basestring):
            if not re.search('%s$' % JSON_EXT, config_file, re.I):
                config_file = '.'.join((config_file, JSON_EXT))
            else:
                config_file = config_file
        else:
            raise TypeError(
                "Unknown config_file type; got: %s" % type(config_file))

        config_file = os.path.expanduser(config_file)
        config_dir = os.path.dirname(config_file)

        self.config = {}
        self.config_file = config_file
        self.config_dir = config_dir
        self.autosave = autosave
        self.force = force
        self.ignore_comments = ignore_comments

        self._set_defaults(default)
        self._prepare()
        self._load()

    defaults = {}

    def _set_defaults(self, default):
        if default and isinstance(default, dict):
            self.defaults.update(default)
        if default and isinstance(default, JSONConf):
            self.defaults.update(default.config)

    def __getitem__(self, key):
        if key in self.config:
            return self.config[key]
        elif key in self.defaults:
            return self.defaults[key]
        else:
            raise KeyError("Field %s is not set in config." % key)

    def __setitem__(self, key, value):
        self.config[key] = value
        if self.autosave:
            self.save()

    def __delitem__(self, key):
        del self.config[key]

    def __iter__(self):
        return iter(self.config)

    def __len__(self):
        return len(self.config)

    def __repr__(self):
        return repr(self.config)

    def __str__(self):
        return str(self.config)

    def __getattr__(self, name):
        if name in self.config or name in self.defaults:
            return self[name]

    def __setattr__(self, name, value):
        if 'config' in self.__dict__:
            if name in self.config or name in self.defaults:
                self[name] = value
                return
        super(JSONConf, self).__setattr__(name, value)

    def setdefault(self, key, value):
        self.defaults[key] = value

    def values(self):
        return self.config.values()

    def _prepare(self):
        if os.path.exists(self.config_file):
            pass
        elif self.force:
            if not os.path.exists(self.config_dir):
                logger.debug('mkdirs (%s)' % self.config_dir)
                os.makedirs(self.config_dir)
            # init an empty json dict at path
            write_empty_json_dict(self.config_file)
        else:
            raise IOError("Path does not exist: %s" % self.config_file)

    def _to_stringio(self, fpath=None):
        if not fpath:
            fpath = self.config_file
        fpath = os.path.expanduser(fpath)
        config_io = cStringIO.StringIO()
        # ignore comments
        # ignore everything starting from # to eol
        with open(fpath) as f:
            for l in f:
                if self._prepare:
                    l = l.split('#', 1)[0].strip()
                config_io.write(l)
        return config_io

    def _load(self):
        ''' load config data from disk '''
        config_io = self._to_stringio()
        config_json = config_io.getvalue()
        try:
            config = json.loads(config_json)
        except Exception:
            raise TypeError("Failed to load json file: %s" % self.config_file)
        self.config.update(config)
        config_io.close()

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

    def save(self):
        ''' save config data to disk '''
        with open(self.config_file, 'w') as config_file:
            config_string = json.dumps(self.config, indent=2)
            config_file.write(config_string)

    def setup_basic(self, option, prompter):
        '''
            Helper-Method for getting user input with prompt text
            and saving the result
        '''
        x_opt = self.config.get(option)
        print '\n(Press ENTER to use current: %s)' % x_opt
        n_opt = prompter()
        if n_opt:
            self.config[option] = n_opt
            logger.debug('Updated Config: \n%s' % self.config)
        return n_opt

    @staticmethod
    def yes_no_prompt(question, default='yes'):
        ''' Helper-Function for getting Y/N response from user '''
        # FIXME: make this as regex...
        valid_yes = ["Y", "y", "Yes", "yes", "YES", "ye", "Ye", "YE"]
        valid_no = ["N", "n", "No", "no", "NO"]
        if default == 'yes':
            valid_yes.append('')
        else:
            valid_no.append('')
        valid = valid_yes + valid_no
        prompt = '[Y/n]' if (default == 'yes') else '[y/N]'
        ans = raw_input('%s %s ' % (question, prompt))
        while ans not in valid:
            print 'Invalid selection.'
            ans = raw_input("%s " % prompt)
        return ans in valid_yes


def write_empty_json_dict(fname):
    fname = os.path.expanduser(fname)
    logger.debug('Write empty config (%s)' % fname)
    with open(fname, 'a') as f:
        f.write('{}')


def test_write_empty_json_dict():
    from random import random
    random_filename = 'test_empty_dict.%s' % random()
    path = os.path.join('/tmp/', random_filename)
    write_empty_json_dict(path)
    with open(path) as f:
        empty_dct = json.load(f)
        assert empty_dct == {}
