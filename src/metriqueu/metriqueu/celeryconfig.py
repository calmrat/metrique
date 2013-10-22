'''
celeryconfig.py contains the main configuration object for
metrique celery applications, which includes built-in
defaults.

Pure defaults assume local, insecure 'test', 'development'
or 'personal' environment. The defaults are not meant for
production use.

To customize local client configuration, add/update
`~/.metrique/celery_config.json` (default).

Paths are UNIX compatible only.
'''

CELERY_ACCEPT_CONTENT = ['json']
CELERY_ACKS_LATE = False
CELERY_ANNOTATIONS = []
CELERY_DEFAULT_RATE_LIMIT = None
CELERY_ENABLE_UTC = True
CELERY_IGNORE_RESULT = False
CELERY_IMPORTS = []
CELERY_INCLUDE = []
CELERY_MAX_CACHED_RESULTS = 5000
CELERY_MESSAGE_COMPRESSION = "gzip"  # alt: 'bzip2', None
CELERY_RESULT_SERIALIZER = 'json'
CELERY_STORE_ERRORS_EVEN_IF_IGNORED = True
CELERY_TASK_RESULT_EXPIRES = None

CELERYD_CONCURRENCY = None
CELERYD_FORCE_EXECV = True
CELERYD_TASK_TIME_LIMIT = None
CELERYD_TASK_SOFT_TIME_LIMIT = None
CELERYD_TIMER_PRECISION = 1

CELERYBEAT_SCHEDULE = None
CELERYBEAT_SCHEDULE_FILENAME = None
CELERYBEAT_MAX_LOOP_INTERVAL = 300  # 5 minutes

BROKER_URL = 'mongodb://127.0.0.1:27017'
BROKER_USE_SSL = False
BROKER_POOL_LIMIT = 10
BROKER_CONNECTION_TIMEOUT = 4
BROKER_CONNECTION_RETRY = True
BROKER_CONNECTION_MAX_RETRIES = 100

CELERY_RESULT_BACKEND = 'mongodb'
CELERY_MONGODB_BACKEND_SETTINGS = {
    'host': 'localhost',
    'port': 27017,
    'database': 'celery',
    'user': None,
    'password': None,
    'taskmeta_collection': 'taskmeta',
    'max_pool_size': 10
}
