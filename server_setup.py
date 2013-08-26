#!/usr/bin/python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import metrique.server as mclient

from distutils.core import setup
from setup import default_setup


default_setup.update(dict(
    description=mclient.__desc__,
    name=mclient.__pkg__,
    packages=mclient.__pkgs__,
    provides=mclient.__provides__,
    requires=mclient.__requires__,
    scripts=mclient.__scripts__,
    version=mclient.__version__,
))

setup(**default_setup)
