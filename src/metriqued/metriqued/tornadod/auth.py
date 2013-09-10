#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import logging
logger = logging.getLogger(__name__)

try:
    import kerberos
except ImportError:
    kerberos = None

from passlib.hash import sha256_crypt

#from metriqued.defaults import VALID_ROLES as VR
from metriqued.cubes import get_auth_keys

auth_keys = get_auth_keys()


def find_user_passhash(username):
    spec = {'_id': username}
    docs = auth_keys.find_one(spec, {'passhash': 1})
    if docs:
        return docs.get('passhash')
    else:
        return None


def is_admin(admin_user, admin_password,
             username, password):
    '''
    admin pass is stored in metrique server config
    admin user gets 'rw' to all cubes
    '''
    if username == admin_user:
        if password == admin_password:
            logger.debug('AUTH ADMIN: True')
            return True
        else:
            return False
    else:
        return None


def basic(username, password):
    passhash = find_user_passhash(username)
    if passhash:
        if sha256_crypt.verify(password, passhash):
            logger.debug('AUTH BASIC: True')
            return True
    else:
        return None
#
#
#def auth_kerb(handler, username, password):
#    if not password:
#        return -1
#    krb_realm = handler.metrique_config.krb_realm
#    if not (kerberos and krb_realm):
#        # if kerberos isn't available or krb_realm
#        # is not set, skip
#        return 0
#
#    try:
#        ret = kerberos.checkPassword(username,
#                                     password, '',
#                                     krb_realm)
#        return ret
#    except kerberos.BasicAuthError as e:
#        logger.debug('KRB ERROR: %s' % e)
#        return -1
#
#
#def _get_resource_acl(handler, resource, lookup):
#    ''' Check if user is listed to access to a given resource '''
#    resource = [resource, '__all__']
#    _lookup = [{lookup:  {'$exists': True}},
#               {'__all__': {'$exists': True}}]
#    spec = {'_id': {'$in': resource},
#            '$or': _lookup}
#    logger.debug("Cube Check: spec (%s)" % spec)
#    return handler.mongodb_config.c_auth_keys.find_one(spec)
#
#
#def _check_acl(permissions, user):
#    try:
#        return VR.index(user['permissions']) >= VR.index(permissions)
#    except (TypeError, KeyError):
#        # permissions is not defined; assume they're unpermitted
#        return -1
#
#
#def _check_perms(doc, username, cube, permissions):
#    if doc and '__all__' in doc:
#        user = doc['__all__']
#        is_ok = _check_acl(permissions, user)
#        no_auth = True
#    elif doc:
#        user = doc[username]
#        is_ok = _check_acl(permissions, user)
#        no_auth = False
#    elif cube:
#        user = {}
#        is_ok = -1
#        no_auth = False
#    return user, is_ok, no_auth
#
#
#def authenticate(handler, username, password, permissions):
#    ''' Helper-Function for determining whether a given
#        user:password:permissions combination provides
#        client with enough privleges to execute
#        the requested command against the given cube
#    '''
#    cube = handler.get_argument('cube')
#
#    is_admin = _auth_admin(handler, username, password)
#    if is_admin in [True, -1]:
#        # ... or if user is admin with correct admin pass
#        logger.debug('AUTH OK: admin')
#        return is_admin
#
#    doc = _get_resource_acl(handler, cube, username)
#    user, is_ok, no_auth = _check_perms(doc, username, cube, permissions)
#
#    if not is_ok:
#        return is_ok
#    elif no_auth:
#        return True
#    elif _auth_kerb(handler, username, password) is True:
#        # or if user is kerberous auth'd
#        logger.debug(
#            'AUTH OK: krb (%s:%s)' % (username, cube))
#        return True
#    elif _auth_basic(handler, password, user) is True:
#        # or if the user is authed by metrique (built-in; auth_keys)
#        logger.debug(
#            'AUTH OK: basic (%s:%s)' % (username, cube))
#        return True
#    else:
#        return -1
#
#
