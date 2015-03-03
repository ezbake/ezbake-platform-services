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
Created on Mon Apr 14 09:59:19 2014

@author: jhastings
"""
import nose.tools as nt

import ezpersist.accu

TABLE_NAME = 'ezpersisttest'

@nt.nottest
class TestPersist(object):
    def setUp(self):
        ap = self.ap = ezpersist.accu.AccumuloPersist(TABLE_NAME, 'root',
                                                         'password', '192.168.22.2',
                                                         42424, True)
        ap.delete_table()
        ap.create_table()

    def test_check_table(self):
        nt.assert_equal(True, self.ap.check_table())

    def test_read_no_entry(self):
        nt.assert_raises(KeyError, self.ap.read, 'ezbakeca', 'cert', 'd')

    def test_write_row_and_get(self):
        self.ap.write('ezbakeca', 'cert', 'd', value="ca cert")
        nt.assert_equal("ca cert", self.ap.read('ezbakeca', 'cert', 'd'))

    def test_write_row_no_colq(self):
        self.ap.write('ezbakeca', 'cert', value="ca cert")
        nt.assert_equal("ca cert", self.ap.read('ezbakeca', 'cert'))

    def test_write_row_no_colf_colq(self):
        self.ap.write('ezbakeca', value="ca cert")
        nt.assert_equal("ca cert", self.ap.read('ezbakeca'))

    def test_write_row_mult_col_and_get(self):
        self.ap.write('ezbakeca', 'cert', 'd', value="ca cert")
        self.ap.write('ezbakeca', 'cert', 'e', value="cb cert")
        nt.assert_equal("ca cert", self.ap.read('ezbakeca', 'cert', 'd'))

    def test_write_row_mult_nocolq_and_get(self):
        self.ap.write('ezbakeca', 'cert', value="ca cert")
        self.ap.write('ezbakeca', 'cert', 'e', value="cb cert")
        nt.assert_equal("ca cert", self.ap.read('ezbakeca', 'cert'))
        nt.assert_equal("cb cert", self.ap.read('ezbakeca', 'cert', 'e'))

    def test_delete_one_row(self):
        self.ap.write('ezbakeca', 'cert', value="ca cert")
        self.ap.delete('ezbakeca')
        nt.assert_raises(KeyError, self.ap.read, 'ezbakeca', 'cert')

    def test_delete_mult_row_nocolq(self):
        self.ap.write('ezbakeca', 'cert', value="ca cert")
        self.ap.write('ezbakeca', 'cert', 'e', value="cb cert")
        self.ap.delete('ezbakeca')
        nt.assert_raises(KeyError, self.ap.read, 'ezbakeca', 'cert')
        nt.assert_raises(KeyError, self.ap.read, 'ezbakeca', 'cert', 'e')

@nt.nottest
class TestAccuMultiTable(object):
    DATA_TABLE = "test_data"
    LOOKUP_TABLE = "test_lookup_table"

    def setUp(self):
        self.ap = ezpersist.accu.AccumuloPersist(
            [TestAccuMultiTable.DATA_TABLE, TestAccuMultiTable.LOOKUP_TABLE],
            "root", "password", "192.168.22.2", 42424, True)

    def test_write_simple(self):
        self.ap.write("row", table=TestAccuMultiTable.DATA_TABLE, value="12345")
        nt.assert_equal("12345",
                    self.ap.read("row", table=TestAccuMultiTable.DATA_TABLE))

    def test_write_multi(self):
        self.ap.write("row", table=TestAccuMultiTable.DATA_TABLE, value="12345")
        self.ap.write("row", table=TestAccuMultiTable.LOOKUP_TABLE, value="56789")
        nt.assert_equal("12345",
                    self.ap.read("row", table=TestAccuMultiTable.DATA_TABLE))
        nt.assert_equal("56789",
                    self.ap.read("row", table=TestAccuMultiTable.LOOKUP_TABLE))

    def test_lookup_usecase(self):
        rowid = "123123129292929"
        data = "JDJDJDJJDJDJJDEWRWRWRW"
        ref = "_ref_data"

        self.ap.write(rowid, table=TestAccuMultiTable.DATA_TABLE, value=data)
        self.ap.write(ref, table=TestAccuMultiTable.LOOKUP_TABLE, value=rowid)

        lookup = self.ap.read(ref, table=TestAccuMultiTable.LOOKUP_TABLE)

        value = self.ap.read(lookup, table=TestAccuMultiTable.DATA_TABLE)

        nt.assert_equal(data, value)
