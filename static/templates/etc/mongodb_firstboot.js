db = db.getSiblingDB('admin')
db.addUser({'user': 'admin', 'pwd': '%s', 'roles': ['dbAdminAnyDatabase',
        'userAdminAnyDatabase', 'clusterAdmin', 'readWriteAnyDatabase']});
