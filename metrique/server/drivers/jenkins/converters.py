#!/usr/bin/env python
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
# Author: "Chris Ward <cward@redhat.com>

from datetime import datetime
from dateutil.parser import parse as dt_parse
import re


def id_when(id):
    if isinstance(id, datetime):
        return id
    else:
        when = re.sub('[_ T](\d\d)[:-](\d\d)[:-](\d\d)$', 'T\g<1>:\g<2>:\g<3>', id)
        return dt_parse(when)
