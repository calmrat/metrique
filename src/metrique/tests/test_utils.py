#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from copy import copy
from datetime import datetime
import os
import pytz
import simplejson as json
import sys


def test_csv2list():
    ' args: csv, delimiter=",") '
    ' always expect output of a list of sorted strings '
    from metrique.utils import csv2list

    d = ','

    l = ['1', '2', '3']
    t = ('1', '2', '3')
    s = set(['1', '2', '3'])
    _s = '1,2,      3'
    _s_semi = '   1; \t  2;   3    '

    assert csv2list(l, d) == l
    assert csv2list(t, d) == l
    assert csv2list(s, d) == l

    assert csv2list(_s, d) == l

    d = ';'
    assert csv2list(_s_semi, d) == l

    assert csv2list(None, d) == []

    try:
        csv2list(True, d)
    except TypeError:
        pass


def test_cube_pkg_mod_cls():
    ' args: cube '
    from metrique.utils import cube_pkg_mod_cls

    pkg, mod, _cls = 'eg', 'csvfile', 'Csvfile'
    cube = 'eg_csvfile'

    assert cube_pkg_mod_cls(cube) == (pkg, mod, _cls)


def test_get_cube():
    ' args: cube, path '
    from metrique.utils import get_cube

    cwd = os.path.dirname(os.path.abspath(__file__))
    cube = 'eg_csvfile'
    path = os.path.join(cwd, 'cubes')
    get_cube(cube=cube, path=path)


# FIXME: THIS IS REALLY SLOW... reenable by adding test_ prefix
def get_timezone_converter():
    ' args: from_timezone '
    ' convert is always TO utc '
    from metrique.utils import get_timezone_converter

    # note: caching timezones always takes a few seconds
    good = 'US/Eastern'
    good_tz = pytz.timezone(good)

    now_utc = datetime.now(pytz.utc)

    now_est = copy(now_utc)
    now_est = now_est.astimezone(good_tz)
    now_est = now_est.replace(tzinfo=None)

    c = get_timezone_converter(good)
    assert c(None, now_est) == now_utc


def test_json_encode():
    ' args: obj '
    from metrique.utils import json_encode

    now = datetime.utcnow()

    dct = {"a": now}

    _dct = json.loads(json.dumps(dct, default=json_encode))
    assert isinstance(_dct["a"], float)


def test_set_cube_path():
    ' args: path=None '
    from metrique.utils import SYSTEM_CUBES_PATH, CLIENT_CUBES_PATH
    from metrique.utils import set_cube_path

    cwd = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(cwd, 'cubes')

    _old_sys_path = copy(sys.path)
    sys.path = []

    set_cube_path(path)

    assert SYSTEM_CUBES_PATH in sys.path
    assert CLIENT_CUBES_PATH in sys.path
    assert path in sys.path

    sys.path = _old_sys_path
