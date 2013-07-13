#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

from decorator import decorator


def _memo(func, *args, **kw):
    if kw:  # frozenset is used to ensure hashability
        key = args, frozenset(kw.iteritems())
    else:
        # normalize to str to ensure hashability
        # (ie, 'list' type is unhashable)
        # FIXME: THIS IS A HACK! but otherwise,
        # arguments that get a list, for example,
        # won't be able to be memoized...
        key = str(args)
    cache = func.cache  # attributed added by memoize
    if key in cache:
        return cache[key]
    else:
        cache[key] = result = func(*args, **kw)
        return result


def memo(f):
    ''' memoize function output '''
    f.cache = {}
    return decorator(_memo, f)
