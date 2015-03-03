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
Created on Mon Apr 14 11:59:46 2014

@author: jhastings
"""
import time
from multiprocessing import Process

import nose.tools as nt
import ezpz
from  thriftutils.connectionpool import ThriftConnectionPool

PORT = 31893

class TestPool(object):

    def setUp(self):
        self.serverProcess = Process(target=ezpz.start_ezpz, args=(PORT,))
        self.serverProcess.start()
        time.sleep(1)

    def tearDown(self):
        if self.serverProcess.is_alive():
            self.serverProcess.terminate()

    def test_ezpz_no_pool(self):
        client = ezpz.ezpz_client(port=PORT)
        try:
            nt.assert_equal('pz', client.ez())
        finally:
            try:
                client._iprot.trans.close()
            except:
                pass
            try:
                client._oprot.trans.close()
            except:
                pass

    def test_simple_pool_of_one(self):
        pool = ThriftConnectionPool("localhost", PORT, ezpz.t.EzPz.Client, size=1)
        conn = pool.get_connection()
        try:
            nt.assert_equal('pz', conn.ez())
        finally:
            pool.return_connection(conn)

    def test_semaphore_behavior(self):
        pool = ThriftConnectionPool("localhost", PORT, ezpz.t.EzPz.Client, 1)
        conn = pool.get_connection()
        nt.assert_equal(0, pool._semaphore._Semaphore__value)
        try:
            nt.assert_equal('pz', conn.ez())
        finally:
            pool.return_connection(conn)
            nt.assert_equal(1, pool._semaphore._Semaphore__value)

    def test_queuing_behavior(self):
        pool = ThriftConnectionPool("localhost", PORT, ezpz.t.EzPz.Client, 1)
        conn = pool.get_connection()
        nt.assert_equal(0, pool._connection_queue.qsize())
        try:
            nt.assert_equal('pz', conn.ez())
        finally:
            pool.return_connection(conn)
            nt.assert_equal(1, pool._connection_queue.qsize())