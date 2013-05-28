#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from metrique.server.drivers.git import GIT
from metrique.server.drivers.jenkins import Jenkins

from metrique.tools.constants import RE_DRIVER_CUBE
from metrique.tools.decorators import memo

DRIVERS = (Jenkins,
           GIT)


class DriverMap(object):
    def __init__(self):
        _drivers = {}
        for d in DRIVERS:
            _drivers[d.prefix] = d
        self.drivers = _drivers

    @memo
    def __getitem__(self, cube):
        _prefix, _cube = RE_DRIVER_CUBE.match(cube).groups()
        dmap = self.drivers[_prefix]()
        driver = dmap[_cube]
        return driver

drivermap = DriverMap()


def get_cube(cube):
    return drivermap[cube]


def get_cubes():
    _cubes = []
    drivermap = DriverMap()
    for driver_name, driver_cls in drivermap.drivers.items():
        driver = driver_cls()
        if not driver.enabled:
            # skip the whole driver if not enabled
            continue
        for cube_name, cube in driver.drivers.items():
            if not cube.enabled:
                # skip the driver.cube if it's not enabled
                continue
            _dc = '%s_%s' % (driver_name, cube_name)
            _cubes.append(_dc)
    return sorted(_cubes)


def get_fields(cube, fields):
    c = get_cube(cube)
    if not fields:
        return []
    elif type(fields) is list:
        return sorted([s.strip() for s in fields])
    elif fields == '__all__':
        return sorted(c.fields.keys())
    elif isinstance(fields, basestring):
        fields = [s.strip() for s in fields.split(',')]

        for field in fields:
            if not field:
                continue
            if field not in c.fields.keys():
                raise ValueError("Invalid field: '%s'" % field)
        return sorted(set(fields))
    else:
        raise ValueError("Unable to parse fields. Got (%s)" % fields)
