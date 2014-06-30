#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>


def test_init():
    from metrique.result import Result
    from metrique.utils import utcnow

    try:
        Result()
    except RuntimeError:
        pass
    else:
        assert False, "Can't init with null data"

    try:
        Result({})
    except RuntimeError:
        pass
    else:
        assert False, "Can't init with empty data"

    try:
        data = [{'a': 1, 'b': 2}]
        Result(data)
    except RuntimeError:
        pass
    else:
        assert False, "_start and _end must be defined..."

    data = [{'_start': utcnow(), '_end': None, 'a': 1, 'b': 2}]
    Result(data)
