#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author:   Jan Grec <jgrec@redhat.com>

import logging
logger = logging.getLogger(__name__)
from copy import copy
import os
import re
import simplejson as json

from metrique.tools.defaults import JSON_EXT, CONFIG_DIR


class JSONConfig(object):
    '''
    '''
    def __init__(self, config_file, config_dir=None, default=None,
                 autosave=False):
        self._name = config_file
        if not re.search('%s$' % JSON_EXT, config_file, re.I):
            config_file = '.'.join((config_file, JSON_EXT))
        self._config_file = config_file
        self._autosave = autosave

        if not config_dir:
            config_dir = os.path.expanduser(CONFIG_DIR)
        self._dir_path = config_dir

        if default and isinstance(default, dict):
            self._config = default
        else:
            self._config = {}

        self._changed = False
        self._set_path()
        self._load()

    def _set_path(self):
        _dir_path = os.path.expanduser(self._dir_path)
        _config_path = os.path.join(_dir_path, self._config_file)
        if not os.path.exists(_config_path):
            if not os.path.exists(_dir_path):
                if _dir_path == os.path.expanduser(CONFIG_DIR):
                    os.makedirs(_dir_path)
                else:
                    raise IOError(
                        "Directory doesn't exist (%s)" % _dir_path)
        self.path = _config_path

    def _load(self):
        try:
            with open(self.path, 'r') as config_file:
                self._config = json.load(config_file)
        except IOError as e:
            logger.debug('(%s): Creating empty config' % e)
            self.save()

    def save(self):
        with open(self.path, 'w') as config_file:
            config_string = json.dumps(self._config, indent=2)
            config_file.write(config_string)

    def __getitem__(self, key):
        return copy(self._config[key])

    def setdefault(self, key, value):
        self._config.setdefault(key, value)

    def __setitem__(self, key, value):
        self._config[key] = value
        if self._autosave:
            self.save()

    def __delitem__(self, key):
        del self._config[key]

    def __repr__(self):
        return repr(self._config)

    def __str__(self):
        return str(self._config)

    def setup_basic(self, option, prompter):
        x_opt = self._config.get(option)
        print '\n(Press ENTER to use current: %s)' % x_opt
        n_opt = prompter()
        if n_opt:
            self._config[option] = n_opt
            logger.debug('Updated Config: \n%s' % self._config)
        return n_opt

    def _property_default(self, option, default):
        try:
            self._properties[option]
        except KeyError:
            self._properties[option] = default
        return self._properties[option]

    def _default(self, option, default=None, required=False):
        try:
            self._config[option]
        except KeyError:
            if default is None and required:
                raise ValueError(
                    "%s attribute is not set (required)" % option)
            else:
                self._config[option] = default
        return self._config[option]

    @staticmethod
    def _yes_no_prompt(question, default='yes'):
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

    def _json_bool(self, value):
        if value in [0, 1]:
            return value
        elif value is True:
            return 1
        elif value is False:
            return 0
        else:
            raise TypeError('expected 0/1 or True/False')
