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
Created on Mon Apr 14 12:49:07 2014

@author: jhastings
"""

import nose.tools as nt
from ezpersist.base import MemoryPersist

class TestEzPersist(object):

    def test_key_underscore(self):
        store = MemoryPersist()
        okey = ("TEST", "test_test", "test")
        key = store.key(okey)
        parts = store.key_parts(key)

    def test_key_parsing(self):
        store = MemoryPersist()
        mykey = ('1', '2', '3')

        key = store.key(*mykey)
        keyparts = store.key_parts(key)

        nt.assert_equal(mykey, keyparts)

    def test_write_read(self):
        store = MemoryPersist()
        store.write('test', value=1)
        nt.assert_equal(1, store.read('test'))

    def test_write_read_colf(self):
        store = MemoryPersist()
        store.write('test', value=1)
        store.write('test', 'colf', value=2)
        nt.assert_equal(1, store.read('test'))
        nt.assert_equal(2, store.read('test', 'colf'))

    def test_write_read_colq(self):
        store = MemoryPersist()
        store.write('test', value=1)
        store.write('test', 'colf', value=2)
        store.write('test', 'colf', 'colq', value=3)
        nt.assert_equal(1, store.read('test'))
        nt.assert_equal(2, store.read('test', 'colf'))
        nt.assert_equal(3, store.read('test', 'colf', 'colq'))

    def test_write_read_delete(self):
        store = MemoryPersist()
        store.write('test', value=1)
        nt.assert_equal(1, store.read('test'))
        store.delete('test')
        nt.assert_raises(KeyError, store.read, ('test',))

    def test_table_name(self):
        store = MemoryPersist("table1")
        store.write("test", "colf", "colq", "table1", 12345)
        nt.assert_equal(12345, store.read("test", "colf", "colq", "table1"))

    def test_table_delete_doesnt_affect_other(self):
        store = MemoryPersist(["table1", "table2"])
        store.write("test", "colf", "colq", "table1", 12345)
        store.write("test", "colf", "colq", "table2", 12345)
        store.delete("test", "table1")
        nt.assert_equal(12345, store.read("test", "colf", "colq", "table2"))
        nt.assert_raises(KeyError, store.read, ('test', 'table1'))

    def test_all(self):
        store = MemoryPersist(["default", "test2table"])
        store.write('test', value=1)
        store.write('test', 'colf', value=2)
        store.write('test', 'colf', 'colq', value=3)
        store.write('test2', table="test2table", value=1)
        store.write('test2', 'colf2', table="test2table", value=2)
        store.write('test2', 'colf2', 'colq2', table="test2table", value=3)
        # TODO: some assertions
