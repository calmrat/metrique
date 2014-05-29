#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>


def test_init():
    from metrique.result import Result
    from metrique.utils import utcnow

    # can't init with empty/null data
    try:
        Result()
    except RuntimeError:
        pass

    try:
        Result({})
    except RuntimeError:
        pass

    # _start and _end must be defined...
    try:
        data = [{'a': 1, 'b': 2}]
        Result(data)
    except RuntimeError:
        pass

    data = [{'_start': utcnow(), '_end': None, 'a': 1, 'b': 2}]
    Result(data)
