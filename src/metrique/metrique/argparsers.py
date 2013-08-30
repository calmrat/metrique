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

from metrique.config import Config


class _CubeInitConfigAction(argparse.Action):
    '''
    Initialize an empty dictionary for config if
    no config file can be loaded
    '''
    def __call__(self, parser, namespace, values, option_string=None):
        c = Config(values).config
        if not c:
            c = {}
        setattr(namespace, 'cube_init_kwargs_config_file', c)


_cube_args = argparse.ArgumentParser(description='Cube CLI')
_cube_args.add_argument('-d', '--debug', type=int, default=2)
_cube_args.add_argument('-a', '--async', action='store_true')
_cube_args.add_argument('-f', '--force', action='store_true')
_cube_args.add_argument('-c', '--cube-config-file', type=str)
_cube_args.add_argument('-cd', '--cube-config-dir', type=str)
_cube_args.add_argument(
    '-x', '--cube-init-kwargs-config-file', action=_CubeInitConfigAction,
    default={})


def cube_cli(cube_cls):
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

    args = _cube_args.parse_args()
    kwargs = {}
    kwargs.update(args.cube_init_kwargs_config_file)
    if args.debug:
        kwargs.update({'debug': args.debug})
    obj = cube_cls(config_file=args.cube_config_file, **kwargs)
    return obj, args
