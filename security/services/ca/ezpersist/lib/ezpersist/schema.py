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
Created on Mon May 19 11:39:21 2014

@author: jhastings
"""
import ezpersist.base

class BaseSchema(object):

    @classmethod
    def row_for(cls, row, colf="", colq="", table=None, value=None):
        return dict(row=row, colf=colf, colq=colq, table=table, value=value)

    @classmethod
    def key_for(cls, row):
        return ezpersist.base.Persist.key(row.get('row', None),
                                          row.get('colf', ""),
                                          row.get('colq', ""))

    @classmethod
    def get_rows(cls, rows, store):
        vals = {}
        for row in rows:
            vals[cls.key_for(row)] = store.read(
                row.get('row', None),
                row.get('colf', ""),
                row.get('colq', ""),
                row.get('table', None))
        return vals

    @classmethod
    def put_rows(cls, rows, store):
        for row in rows:
            store.write(
                row.get('row', None),
                row.get('colf', ""),
                row.get('colq', ""),
                row.get('table', None),
                row.get('value', None))

    def __init__(self, *args, **kwargs):
        """
        Initialize the BaseSchema object with key word arguments.

        Arguments:
            store - the type of persistance store to use
        """
        for k,v in kwargs.iteritems():
            setattr(self, k, v)

        if not kwargs.has_key("store"):
            self.store = ezpersist.base.MemoryPersist()


    def rows(self, write=False):
        return []

    def save(self):
        self.put_rows(self.rows(write=True), self.store)

    def get(self):
        self.get_rows(self.rows(), self.store)

    def destroy(self):
        rows = set()
        for row in self.rows():
            rows.add(row.get('row', None))
        for row in rows:
            self.store.delete(row)
