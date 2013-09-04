#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
This package contains client cube object definitions.

Modules with `base*` prefix indicate they provide only
base level functionality for the given cube type. It
is expected that clients will subclass these basecubes
to create specific cubes that extract their data from
the sources the basecubes are designed to facilitate.

'''
