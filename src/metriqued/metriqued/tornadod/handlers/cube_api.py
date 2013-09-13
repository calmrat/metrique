#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)
from tornado.web import authenticated

from metriqued.tornadod.handlers.core_api import MetriqueHdlr
from metriqued import cube_api
from metriqued.utils import list_cubes, list_cube_fields


class SaveObjectsHdlr(MetriqueHdlr):
    '''
    RequestHandler for saving a given object to a metrique server cube
    '''
    @authenticated
    def post(self, owner, cube):
        self._requires_owner_write(owner, cube)
        objects = self.get_argument('objects')
        mtime = self.get_argument('mtime')
        result = cube_api.save_objects(owner=owner, cube=cube,
                                       objects=objects, mtime=mtime)
        self.write(result)


class RemoveObjectsHdlr(MetriqueHdlr):
    '''
    RequestHandler for saving a given object to a metrique server cube
    '''
    @authenticated
    def delete(self, owner, cube):
        self._requires_owner_admin(owner, cube)
        ids = self.get_argument('ids')
        backup = self.get_argument('backup')
        result = cube_api.remove_objects(owner=owner, cube=cube,
                                         ids=ids, backup=backup)
        self.write(result)


class IndexHdlr(MetriqueHdlr):
    '''
    RequestHandler for ensuring mongodb indexes
    in timeline collection for a given cube
    '''
    @authenticated
    def get(self, owner, cube):
        self.write(cube_api.index(owner=owner, cube=cube))

    @authenticated
    def post(self, owner, cube):
        self._requires_owner_admin(owner, cube)
        ensure = self.get_argument('ensure')
        self.write(cube_api.index(owner=owner, cube=cube, ensure=ensure))

    @authenticated
    def delete(self, owner, cube):
        self._requires_owner_admin(owner, cube)
        drop = self.get_argument('drop')
        self.write(cube_api.index(owner=owner, cube=cube, drop=drop))


class ActivityImportHdlr(MetriqueHdlr):
    '''
    RequestHandler for building pre-calculated
    object timelines given a 'activity history'
    data source that can be used to recreate
    objects in time
    '''
    @authenticated
    def post(self, owner, cube):
        self._requires_owner_write(owner, cube)
        ids = self.get_argument('ids')
        result = cube_api.activity_import(owner=owner, cube=cube, ids=ids)
        self.write(result)


class StatsHdlr(MetriqueHdlr):
    '''
    RequestHandler for managing cube role properties

    action can be push, pop
    role can be __read__, __write__, __admin__
    '''
    @authenticated
    def get(self, owner, cube):
        self._requires_owner_read(owner, cube)
        result = cube_api.stats(owner=owner, cube=cube)
        self.write(result)


class UpdateRoleHdlr(MetriqueHdlr):
    '''
    RequestHandler for managing cube role properties

    action can be push, pop
    role can be __read__, __write__, __admin__
    '''
    @authenticated
    def post(self, owner, cube):
        self._requires_owner_admin(owner, cube)
        username = self.get_argument('username')
        action = self.get_argument('action', 'push')
        role = self.get_argument('role', '__read__')
        result = cube_api.update_role(owner=owner, cube=cube,
                                      username=username,
                                      action=action, role=role)
        self.write(result)


class RegisterHdlr(MetriqueHdlr):
    '''
    RequestHandler for registering new users to metrique
    '''
    def post(self, owner, cube):
        self._requires_owner_admin(owner, cube)
        if self._cube_exists(owner, cube):
            self._raise(409, "this cube already exists")
        self.write(cube_api.register(owner=owner, cube=cube))


class DropHdlr(MetriqueHdlr):
    ''' RequestsHandler for droping given cube from timeline '''
    @authenticated
    def delete(self, owner, cube):
        self._requires_owner_admin(owner, cube)
        self.write(cube_api.drop_cube(owner=owner, cube=cube))


class ListHdlr(MetriqueHdlr):
    '''
    RequestHandler for querying about available cubes and cube.fields
    '''
    @authenticated
    def get(self, owner=None, cube=None):
        self._requires_owner_read(owner, cube)
        _mtime = self.get_argument('_mtime')
        exclude_fields = self.get_argument('exclude_fields')
        if not owner:
            result = list_cubes()
        elif cube is None:
            # return a list of cubes
            result = list_cubes(owner=owner)
        else:
            # return a list of fields in a cube
            # arg = username... return only cubes with 'r' access
            result = list_cube_fields(owner, cube,
                                      # SAMPLE_SIZE
                                      exclude_fields, _mtime=_mtime)
        self.write(result)
