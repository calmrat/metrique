#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

import re
from pytz import timezone


UTC = timezone("UTC")

# console escapes for colored output
WHITE = '\033[97m'
BLUE = '\033[96m'
YELLOW = '\033[93m'
GREEN = '\033[92m'
RED = '\033[91m'
ENDC = '\033[0m'

# either date or datetime
RE_DATE_DATETIME = re.compile(
    '(\d\d\d\d)-(\d\d)-(\d\d)((T| )?(\d\d):(\d\d):(\d\d)Z?)?')

RE_PROP = re.compile('^_')
