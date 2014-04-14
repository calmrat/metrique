#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward" <cward@redhat.com>

'''
metrique.mongodb_api
~~~~~~~~~~~~~~~~~~~~

MongoDB client API for persisting and querying of
data cubes backed by MongoDB.

'''
from __future__ import unicode_literals

import logging
logger = logging.getLogger(__name__)

from datetime import datetime
from getpass import getuser
import os
import re

try:
    from sqlalchemy import create_engine, MetaData
    from sqlalchemy import Column, Integer, Unicode, DateTime
    from sqlalchemy import Float, BigInteger, Boolean, UnicodeText
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.ext.declarative import declarative_base
    HAS_SQLALCHEMY = True
except ImportError:
    logger.warn('sqlalchemy not installed!')
    HAS_SQLALCHEMY = False

try:
    import psycopg2
    psycopg2  # avoid pep8 'imported, not used' lint error
    import sqlalchemy.dialects.postgresql as pg
    from sqlalchemy.dialects.postgresql import ARRAY
    HAS_PSYCOPG2 = True
except ImportError:
    logger.warn('psycopg2 not installed!')
    HAS_PSYCOPG2 = False

from metrique.core_api import BaseClient
from metrique.utils import batch_gen, utcnow

ETC_DIR = os.environ.get('METRIQUE_ETC')

TYPE_MAP = {
    None: UnicodeText,
    int: Integer,
    float: Float,
    long: BigInteger,
    str: UnicodeText,
    unicode: UnicodeText,
    bool: Boolean,
    datetime: DateTime
}


class SQLAlchemyClient(BaseClient):
    sqlalchemy_config_key = 'sqlalchemy'

    def __init__(self, sqlalchemy_engine=None, sqlalchemy_schema=None,
                 sqlalchemy_owner=None, sqlalchemy_batch_size=None,
                 sqlalchemy_debug=None, sqlalchemy_config_key=None,
                 sqlalchemy_drop_target=None,
                 *args, **kwargs):
        if not HAS_SQLALCHEMY:
            raise NotImplementedError('`pip install sqlalchemy` required')

        super(SQLAlchemyClient, self).__init__(*args, **kwargs)
        cache = self.config['metrique'].get('cache_dir')
        owner = sqlalchemy_owner or getuser()
        default_db_path = os.path.join(cache, '%s.db' % owner)
        default_engine = 'sqlite:///%s' % default_db_path
        options = dict(
            engine=sqlalchemy_engine,
            schema=sqlalchemy_schema,
            owner=sqlalchemy_owner,
            batch_size=sqlalchemy_batch_size,
            debug=sqlalchemy_debug,
            config_key=sqlalchemy_config_key,
            drop_target=sqlalchemy_drop_target,
        )
        defaults = dict(
            engine=default_engine,
            schema={},
            owner=owner,
            batch_size=1000,
            debug=logging.INFO,
            config_key=self.sqlalchemy_config_key,
            drop_target=False,
        )
        self.configure(sqlalchemy_config_key, options, defaults)
        self.debug_setup()

        self._initiate_table()

    def debug_setup(self, *args, **kwargs):
        super(SQLAlchemyClient, self).debug_setup(*args, **kwargs)
        self._debug_setup_sqlalchemy_logging()

    def _debug_setup_sqlalchemy_logging(self):
        logger = logging.getLogger('sqlalchemy')
        level = self.config[self.sqlalchemy_config_key].get('debug')
        super(SQLAlchemyClient, self).debug_setup(logger=logger, level=level)

######################### DB API ##################################
    def _initiate_table(self):
        engine = self.get_engine()
        meta = self.get_meta()
        cube = self.name
        conf_key = self.sqlalchemy_config_key
        drop_target = self.config[conf_key].get('drop_target')

        if drop_target:
            for table in reversed(meta.sorted_tables):
                if table.name == cube:
                    # FIXME: table is still in meta, so a second
                    # call to this same func with drop_target=True
                    # will fail with 'table already exists...'
                    logger.debug("Dropping table...")
                    table.drop(engine)
                    break

        logger.debug("Creating Table...")
        table = self._schema2table()
        setattr(self, cube, table)
        meta.create_all(engine)

    def _cube_factory(self, name, schema, cached=False):
        Base = self.get_base(cached=cached)
        if not schema:
            raise ValueError('schema definition can not be null')

        defaults = {
            '__tablename__': name,
            '__table_args__': {'useexisting': True},
            'id': Column('id', Integer, primary_key=True),
            '_id': Column(Unicode(40), nullable=False,
                          index=True, unique=True),
            '_hash': Column(Unicode(40), nullable=False,
                            index=True, unique=False),
            '_oid': Column(Integer, nullable=False,
                           index=True, unique=False),
            '_start': Column(DateTime, default=utcnow(), nullable=False,
                             index=True, unique=False),
            '_end': Column(DateTime, default=None, nullable=True,
                           index=True, unique=False),
        }

        for k, v in schema.iteritems():
            __type = v.get('type')
            _type = TYPE_MAP.get(__type)
            if v.get('container', False):
                # FIXME: alternative association table implementation?
                # FIXME: requires postgresql+psycopg2
                schema[k] = Column(ARRAY(_type))
            else:
                schema[k] = Column(_type)
        defaults.update(schema)
        _cube = type(str(name), (Base,), defaults)
        return _cube

    def _check_compatible(self, uri, driver, msg=None):
        msg = '%s required!' % driver
        if not HAS_PSYCOPG2:
            raise NotImplementedError(
                '`pip install psycopg2` required')
        if uri[0:10] != 'postgresql':
            raise RuntimeError(msg)
        return True

    def _schema2table(self, schema=None, name=None):
        config = self.config[self.sqlalchemy_config_key]
        schema = schema or config.get('schema')

        name = name or self.name
        if not name:
            raise RuntimeError("table name not defined!")
        logger.debug("Creating Table from Schema: %s" % name)

        if isinstance(schema, dict):
            table = self._cube_factory(name, schema)
        else:
            raise TypeError("unsupported schema type: %s" % type(schema))
        return table

    def get_table(self, name):
        return self.get_tables().get(name)

    def get_tables(self):
        return self.get_meta().tables

    def get_meta(self):
        return self.get_base().metadata

    def get_base(self, cached=True):
        if cached and not hasattr(self, '_Base'):
            self._metadata = MetaData()
            self._Base = declarative_base(metadata=self._metadata)
        return self._Base

    def get_engine(self, engine=None, connect=False, cached=True, **kwargs):
        if not cached or not hasattr(self, '_sql_engine'):
            _engine = self.config[self.sqlalchemy_config_key].get('engine')
            engine = engine or _engine
            if re.search('sqlite', engine):
                uri, kwargs = self._sqla_sqlite(engine)
            elif re.search('teiid', engine):
                uri, kwargs = self._sqla_postgresql(engine)
            elif re.search('postgresql', engine):
                uri, kwargs = self._sqla_postgresql(engine)
            else:
                raise NotImplementedError("Unsupported engine: %s" % engine)
            self._sql_engine = create_engine(uri, echo=False, **kwargs)
            # SEE: http://docs.sqlalchemy.org/en/rel_0_9/orm/session.html
            # ... #unitofwork-contextual
            # scoped sessions
            metadata = self.get_meta()
            metadata.bind = self._sql_engine
            self._sessionmaker = sessionmaker(bind=self._sql_engine)
            if connect:
                self._sql_engine.connect()
        return self._sql_engine

    def get_session(self, **kwargs):
        return self._sessionmaker(**kwargs)

    @property
    def proxy(self):
        engine = self.config[self.sqlalchemy_config_key].get('engine')
        return self.get_engine(engine=engine, connect=True, cached=True)

    def _sqla_sqlite(self, uri):
        kwargs = {}
        return uri, kwargs

    def _sqla_postgresql(self, uri, version=None, iso_level="AUTOCOMMIT"):
        '''
        expected uri form:
        postgresql+psycopg2://%s:%s@%s:%s/%s' % (
            username, password, host, port, vdb)
        '''
        self._check_compatible(uri, 'postgresql+psycopg2')
        iso_level = iso_level or "AUTOCOMMIT"
        version = version or (8, 2)
        if re.search('teiid', uri):
            uri = re.sub('\+?teiid', '', uri)
            # version normally comes "'Teiid 8.5.0.Final'", which sqlalchemy
            # failed to parse
            r_none = lambda *i: None
            pg.base.PGDialect.description_encoding = str('utf8')
            pg.base.PGDialect._check_unicode_returns = lambda *i: True
            pg.base.PGDialect._get_server_version_info = lambda *i: version
            pg.base.PGDialect.get_isolation_level = lambda *i: iso_level
            pg.base.PGDialect._get_default_schema_name = r_none
            pg.psycopg2.PGDialect_psycopg2.set_isolation_level = r_none
        kwargs = dict(isolation_level=iso_level)
        return uri, kwargs

# ######################## Cube API ################################
    def ls(self, startswith=None):
        '''
        List all cubes available to the calling client.

        :param startswith: string to use in a simple "startswith" query filter
        :returns list: sorted list of cube names
        '''
        engine = self.get_engine()
        cubes = engine.table_names()
        startswith = unicode(startswith or '')
        cubes = [name for name in cubes if name.startswith(startswith)]
        logger.info(
            '[%s] Listing cubes starting with "%s")' % (engine, startswith))
        return sorted(cubes)

    def drop(self, cube=None, engine=None):
        _cube = self.get_table(cube, engine)
        return _cube.drop()

# ####################### ETL API ##################################
    def flush(self, schema=None, autosnap=True, batch_size=None,
              engine=None, cube=None):
        config = self.config[self.sqlalchemy_config_key]
        batch_size = batch_size or config.get('batch_size')
        _cube = self.get_table(cube, engine)
        _ids = []
        for batch in batch_gen(self.objects.values(), batch_size):
            _ = self._flush(_cube=_cube, objects=batch, autosnap=autosnap,
                            schema=schema)
            _ids.extend(_)
        return sorted(_ids)

    def _flush(self, _cube, objects, autosnap=True, schema=None):
        pass
