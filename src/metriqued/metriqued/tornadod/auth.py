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
    spec = {'_oid': username}
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
        return False


def krb_basic(username, password, krb_realm):
    if not password:
        return False
    elif not kerberos:
        logger.error('IMPORT ERROR: kerberos module DOES NOT EXIST!')
        # if kerberos isn't available, no go
        return False
    else:
        try:
            return kerberos.checkPassword(username, password,
                                          '', krb_realm)
        except kerberos.BasicAuthError as e:
            logger.error('KRB ERROR: %s' % e)
            return False
