from metriqueu.jsonconf import JSONConf
import json


class TestConf(JSONConf):
    def __init__(self, config_file):
        self.defaults = {"lala": "la",
                         "async": False}
        super(TestConf, self).__init__(config_file)

    @property
    def anaconda(self):
        return self['anaconda']

    @anaconda.setter
    def anaconda(self, val):
        self['anaconda'] = str(val)


def test_jsonconf():
    config = JSONConf("test_conf")
    with open("test_conf.json", "r") as f:
        saved = json.load(f)
    assert saved == config.config
    assert config.defaults == {}


def test_testconf():
    config = TestConf("test_conf")
    with open("test_conf.json", "r") as f:
        saved = json.load(f)
    assert saved == config.config
    assert config.lala == "la"
    assert config.async == saved['async']
    assert config.defaults['async'] is not config.async
    config.async = False
    assert config.config['async'] is False

    config.noway = True
    assert 'noway' not in config.config
    config['noway'] = True
    assert 'noway' in config.config
    assert config.noway is True
    config.noway = False
    assert config.noway is False

    # test if properties are handled correctly:
    config.anaconda = 1
    assert config.anaconda == "1"
