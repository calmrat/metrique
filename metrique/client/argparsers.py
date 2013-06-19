#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import argparse

from metrique.client.config import Config


class CubeConfigAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):

        for k, v in Config(values).config.items():
            setattr(namespace, k, v)

cube_cli = argparse.ArgumentParser(description='Cube CLI')
cube_cli.add_argument('-d', '--debug', type=int, default=1)
cube_cli.add_argument('-a', '--async', action='store_true')
cube_cli.add_argument('-c', '--cube-config-file', action=CubeConfigAction)
