#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import argparse

from metrique.client.config import Config


class CubeInitConfigAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        c = Config(values).config
        if not c:
            c = {}
        setattr(namespace, 'cube_init_kwargs_config_file', c)


cube_cli = argparse.ArgumentParser(description='Cube CLI')
cube_cli.add_argument('-d', '--debug', type=int, default=2)
cube_cli.add_argument('-a', '--async', action='store_true')
cube_cli.add_argument('-f', '--force', action='store_true')
cube_cli.add_argument('-c', '--cube-config-file', type=str)
cube_cli.add_argument(
    '-x', '--cube-init-kwargs-config-file', action=CubeInitConfigAction,
    default={})
