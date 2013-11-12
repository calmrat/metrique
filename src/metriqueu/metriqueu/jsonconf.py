#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
import os
import re
from collections import MutableMapping

try:
    import simplejson as json
except ImportError:
    import json


class JSONConf(MutableMapping):
    '''
        Config object using json as its underlying data store

        Provides helper-methods for setting and saving
        options and config object properties
    '''
    def __init__(self, config_file=None, defaults=None, autosave=False):
        self.config_file = config_file
        if self.defaults is None:
            self.defaults = {}
        if defaults:
            self.defaults.update(defaults)
        if self.config is None:
            self.config = {}
        if config_file:
            if isinstance(self.config_file, JSONConf):
                self.config.update(self.config_file)
                self.config_file = self.config_file.config_file
            else:
                self.load_config()
        self.autosave = autosave

    config = None
    config_file = None
    defaults = None
    default_config = None

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
        if 'config' in self.__dict__:
            if 'defaults' in self.__dict__ and name in self.defaults:
                # you can only set those values specified IN defaults
                self[name] = value
                return
        super(JSONConf, self).__setattr__(name, value)

    def __setitem__(self, key, value):
        self.config[key] = value
        if self.autosave:
            self.save()

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
        ''' load config data from disk '''
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
            if config_file[0] != '/':
                # try to look in the default config folder:
                old_conf = config_file
                config_file = '~/.metrique/%s' % config_file
                config_file = os.path.expanduser(config_file)
                if not os.path.exists(config_file):
                    if not silent:
                        raise IOError('Config files %s and %s do not exist.' %
                                      (config_file, old_conf))
                    return
            else:
                if not silent:
                    raise IOError('Config file %s does not exist.' %
                                  config_file)
                return
        try:
            with open(config_file) as f:
                config = json.load(f)
        except Exception:
            raise TypeError("Failed to load json file: %s" % config_file)
        self.config.update(config)
        self.config_file = config_file

    def save(self, force=True, config_file=None):
        ''' save config data to disk '''
        config_file = config_file or self.config_file
        if not os.path.exists(config_file):
            if force:
                #FIXME config dir
                config_dir = os.path.dirname(config_file)
                logger.debug('mkdirs (%s)' % config_dir)
                os.makedirs(config_dir)
            else:
                raise IOError("Path does not exist: %s" % config_file)
        with open(config_file, 'w') as f:
            f.write(json.dumps(self.config, indent=2))

    def setdefault(self, key, value):
        self.defaults[key] = value

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

    def values(self):
        return self.config.values()

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
