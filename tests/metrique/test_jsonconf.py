#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# author: "Chris Ward" <cward@redhat.com>

import json
import os

from metrique.jsonconf import JSONConf

here = os.path.dirname(os.path.abspath(__file__))

defaults = {
    "async": False
}


class TestConf(JSONConf):
    def __init__(self, *args, **kwargs):
        self.defaults = defaults
        super(TestConf, self).__init__(*args, **kwargs)

    @property
    def anaconda(self):
        if 'anaconda' in self:
            return self['anaconda']
        else:
            return 1

    @anaconda.setter
    def anaconda(self, val):
        self['anaconda'] = str(val)


def test_from_class_no_defaults():
    config = TestConf()
    assert config.anaconda == 1
    # doesn't make sense, but we're testing that @*.setter works properly
    config.anaconda = 1
    assert config.anaconda == "1"

    # check assignment works
    assert 'wow' not in config
    config.wow = 1
    assert config.wow == 1

    # checkout that bad getattr return AttributeError as expected
    try:
        assert config.lala
    except AttributeError:
        pass


def test_from_class_extra_defaults():
    config = TestConf(defaults=defaults)
    assert config.defaults == defaults
    assert config == {}


def test_from_file():
    _conf = 'test_conf.json'
    path = os.path.join(here, _conf)
    config = TestConf(path)
    with open(path) as f:
        saved = json.load(f)
    # check that the config contains only what was loaded
    assert saved == config.config

    config.async = False
    assert config['async'] is False
    assert config.async is False

    config.noway = True
    assert 'noway' not in config.config
    config['noway'] = True
    assert 'noway' in config.config
    assert config.noway is True
    config.noway = False
    assert config.noway is False


def test_bad_file():
    _conf = 'bad_conf.json'
    path = os.path.join(here, _conf)
    try:
        TestConf(path)
    except TypeError:
        pass
