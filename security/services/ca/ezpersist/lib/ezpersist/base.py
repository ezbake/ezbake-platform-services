#   Copyright (C) 2013-2014 Computer Sciences Corporation
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

# -*- coding: utf-8 -*-
"""
Created on Mon Apr 14 12:46:43 2014

@author: jhastings
"""

KEY_SEP = ":"
ESCAPE_SEP = "_:_"

def escape_key_part(part):
    return str(part).replace(KEY_SEP, ESCAPE_SEP)


def unescape_key_part(part):
    return str(part).replace(ESCAPE_SEP, KEY_SEP)


class Persist(object):

    @classmethod
    def key(cls, row, colf=None, colq=None):
        return "{1}{0}{2}{0}{3}".format(KEY_SEP, escape_key_part(row),
            escape_key_part(colf), escape_key_part(colq))

    @classmethod
    def key_parts(cls, key):
        a, b, c = (unescape_key_part(i) for i in key.split(KEY_SEP))
        return (a, b, c)


class MemoryPersist(Persist):
    DEFAULT_TABLE = "default"

    def __init__(self, tableNames=[DEFAULT_TABLE]):
        # table names should be a list, make it one if it is not
        if type(tableNames) is not list:
            tableNames = [tableNames]
        # add the 'default' table if it doesn't exist
        if MemoryPersist.DEFAULT_TABLE not in tableNames:
            tableNames.append(MemoryPersist.DEFAULT_TABLE)
        # create the dict where we will store stuff
        self.store = dict()
        for table in tableNames:
            self.store[table] = dict()

    def all(self, table=None):
        if table is None:
            table = self.store.keys()[0]
        os = []
        lkey = None
        lgrp = None
        for key in sorted(self.store[table].keys()):
            a, b, c = self.key_parts(key)
            if lkey is None or a != lkey:
                if lgrp is not None:
                    os.append(lgrp)
                lkey = a
                lgrp = {}
            lgrp[key] = self.store[table][key]
        # add last group, it gets missed in the last loop
        if lgrp is not None:
            os.append(lgrp)
        return os

    def write(self, row, colf=None, colq=None, table=None, value=None):
        if table is None:
            table = self.store.keys()[0]
        if table not in self.store.keys():
            self.store[table] = {}

        self.store[table][self.key(row, colf, colq)] = value

    def read(self, row, colf=None, colq=None, table=None):
        if table is None:
            table = self.store.keys()[0]
        return self.store[table][self.key(row, colf, colq)]

    def delete(self, row, table=None):
        if table is None:
            table = self.store.keys()[0]
        for key in self.store[table].keys():
            if key.startswith(row):
                del(self.store[table][key])