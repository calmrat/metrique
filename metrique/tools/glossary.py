#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>


def join_and(*args):
    return ' and '.join(args)


def join_or(*args):
    return ' or '.join(args)


def o_in(field_token):
    field, token = field_token
    if type(token) is list:
        return '%s in %s' % field_token
    else:
        return '%s in [%s]' % field_token


def o_has(field_token):
    if isinstance(field_token, str):
        return '%s == "%s"' % field_token
    else:
        return '%s == %s' % field_token


def o_matches(field_token):
    return '%s == regex("%s")' % field_token
