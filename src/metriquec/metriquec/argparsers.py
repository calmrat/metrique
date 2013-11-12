#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

'''
argparsers.py contains a CLI for metrique client cubes.

To use the cli, cubes must import the cube_cli function
    and initiatlize it with the cube class::

    # ... ^^^ cube class definition above ^^^ ...
    if __name__ == '__main__':
        from metriquec.argparsers import cube_cli
        cube_cli(Bug)

'''
import argparse
import simplejson as json

from metrique.utils import get_cube
from metriqueu.jsonconf import JSONConf


def extract(args, cube):
    ext_args = args.extract_args
    ext_kwargs = args.extract_kwargs
    if args.force:
        ext_kwargs.update({'force': args.force})
    if args.extract_config_file:
        config = JSONConf(config_file=args.extract_config_file)
        cube.config.update(config)
    return cube.extract(*ext_args, **ext_kwargs)


def register(args, cube):
    _cube = args.cube or cube.name
    if not _cube:
        raise ValueError("cube name required")
    return cube.cube_register(_cube)


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
_cube_args.add_argument('-d', '--debug', type=int, default=None)
_cube_args.add_argument('-L', '--no-login', action='store_true')
_cube_args.add_argument('-H', '--api-host', type=str)
_cube_args.add_argument('-P', '--api-port', type=str)
_cube_args.add_argument('-u', '--api-username', type=str)
_cube_args.add_argument('-p', '--api-password', type=str)
_cube_args.add_argument('-C', '--cube-config-file', type=str)
_cube_args.add_argument('-c', '--cube', type=str)
_cube_args.add_argument('-o', '--owner', type=str)

_sub = _cube_args.add_subparsers(description='Cube Commands CLI')
_ext_args = _sub.add_parser('extract', help='Extract help')
_ext_args.add_argument('-xC', '--extract-config-file', type=str)
_ext_args.add_argument('-g', '--extract_args', type=str,
                       action=_ArgParser, nargs='+', default=[])
_ext_args.add_argument('-k', '--extract_kwargs', type=str,
                       action=_KwargParser, nargs='+', default={})
_ext_args.add_argument('-f', '--force', action='store_true')
_ext_args.set_defaults(func=extract)

_reg_args = _sub.add_parser('register', help='Extract help')
_reg_args.set_defaults(func=register)


def cube_cli(cube_cls=None):
    '''
    :param class cube_cls:
        The cube class to initiatlize

    Available options::

        --debug: 0/False (OFF), 1/True (INFO), 2 (DEBUG)
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
    kwargs['config_file'] = args.cube_config_file
    kwargs['host'] = args.api_host
    kwargs['port'] = args.api_port
    kwargs['username'] = args.api_username
    kwargs['password'] = args.api_password

    cube = cube_cls(**kwargs)

    if not args.no_login:
        cube.login(cube.config.username,
                   cube.config.password)

    return args.func(args, cube)
