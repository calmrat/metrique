#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
argparsers.py contains a CLI for metrique client cubes.

To use the cli, cubes must import the cube_cli function
and initiatlize it with the cube class. The initiated cube
(using args parsed) plus argparser Namespace object with
all available arguments will be returned, as such::

    # ... ^^^ cube class definition above ^^^ ...
    if __name__ == '__main__':
        from metrique.argparsers import cube_cli
        obj, args = cube_cli(Bug)
        obj.extract(force=args.force)

'''

import argparse
import simplejson as json

from metrique.utils import get_cube


class _ArgParser(argparse.Action):
    '''
    json.loads args value strings
    '''
    def __call__(self, parser, namespace, values, option_string=None):
        # decode json...
        args = [json.loads(a) for a in values]
        setattr(namespace, 'extract_args', args)


class _KwargParser(argparse.Action):
    '''
    split kwargs key:value strings into a dict
    '''
    def __call__(self, parser, namespace, values, option_string=None):
        # decode json...
        kwargs = {}
        for e in values:
            k, s, v = e.partition(':')
            if not v:
                raise SystemExit(
                    "kwargs should be separated with ':' (eg, key:value)")
            # key remains a string; json convert v
            try:
                kwargs[k] = json.loads(v)
            except Exception:
                # assume we're working with a string...
                # this is akward, since we're expecting
                # objects in json form; but it'd be annoying
                # to quote every actual string...
                kwargs[k] = v
        setattr(namespace, 'extract_kwargs', kwargs)


_cube_args = argparse.ArgumentParser(prog='Cube CLI')
_cube_args.add_argument('-d', '--debug', type=int, default=2)
_cube_args.add_argument('-a', '--async', action='store_true')
_cube_args.add_argument('-H', '--api-host', action='store_true')
_cube_args.add_argument('-P', '--api-port', action='store_true')
_cube_args.add_argument('-u', '--api-username', action='store_true')
_cube_args.add_argument('-p', '--api-password', action='store_true')
_cube_args.add_argument('-c', '--cube-config-file', type=str)

_sub_args = _cube_args.add_subparsers(description='Cube Extract CLI',
                                      dest='extract')
_extr_args = _sub_args.add_parser('extract', help='Extract help')
_extr_args.add_argument('-g', '--extract_args', type=str,
                        action=_ArgParser, nargs='+', default=[])
_extr_args.add_argument('-k', '--extract_kwargs', type=str,
                        action=_KwargParser, nargs='+', default={})


def cube_cli(cube_cls=None):
    '''
    :param class cube_cls:
        The cube class to initiatlize

    Available options::

        --debug: 0/False (OFF), 1/True (INFO), 2 (DEBUG)
        --async: Turn on/off async/parallel/threaded processing
        --force: set to pass this option to extract()
        --cube-config-file: api config file name
        --cube-config-dir: config dir path
        --cube-init-kwargs-config-file: load additional __init__ kwargs
    '''
    if not cube_cls:
        cube_cls = get_cube(cube_cls)
    args = _cube_args.parse_args()
    kwargs = {}
    kwargs['debug'] = args.debug
    kwargs['async'] = args.async
    kwargs['config_file'] = args.cube_config_file
    kwargs['api_host'] = args.api_host
    kwargs['api_port'] = args.api_port
    kwargs['api_username'] = args.api_username
    kwargs['api_password'] = args.api_password

    cube = cube_cls(**kwargs)

    ext_args = args.extract_args
    ext_kwargs = args.extract_kwargs
    result = None
    if args.extract:
        result = cube.extract(*ext_args, **ext_kwargs)

    dct = {'cube': cube,
           'args': args,
           'result': result}
    return dct
