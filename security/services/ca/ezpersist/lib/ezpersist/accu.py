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
Created on Mon Apr 14 09:45:41 2014

@author: jhastings
"""
import inspect
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from accumulo import AccumuloProxy
from accumulo.ttypes import *

from ezbake.thrift.utils.connectionpool import ThriftConnectionPool, PoolingThriftClient
from ezpersist.base import Persist

class ProxyClient(PoolingThriftClient):
    LOGIN_METHOD_NAME = 'login'

    def __init__(self, host, port, user, password,
                 pool_size=ThriftConnectionPool.DEFAULT_SIZE,
                 retries=3, ttransport=TTransport.TFramedTransport,
                 tprotocol=TBinaryProtocol.TBinaryProtocol):
        super(ProxyClient, self).__init__(host, port, AccumuloProxy.Client,
            pool_size, retries, ttransport, tprotocol)
        self.user = user
        self.password = password
        self.login_token = None

    def __login(self, conn):
        """Log in to Accumulo and cache the login token"""
        if not self.login_token:
            self.login_token = getattr(conn, ProxyClient.LOGIN_METHOD_NAME)(
                self.user, {'password':self.password})
        return self.login_token

    def _process_thrift_args(self, client, method, args):
        """
        Process login and prepend to arguments if required for thrift method
        being called.
        """
        if ProxyClient.LOGIN_METHOD_NAME in inspect.getargspec(
            getattr(client, method)).args:
            args = (self.__login(client),)+args
        return args

class AccumuloRowIterator(object):
    def __init__(self, client, scanner):
        self.client = client
        self.scanner = scanner

    def __iter__(self):
        return self

    def next(self):
        try:
            entry = self.client.nextEntry(self.scanner)
            if not entry.hasNext:
                raise StopIteration
            return entry.keyValue
        except NoMoreEntriesException:
            raise StopIteration

class AccumuloPersist(Persist):
    def __init__(self, table, proxy_user, proxy_password, proxy_host="localhost", proxy_port=42424, create_tables=False):
        if type(table) is not list:
            table = [table]
        self.table_names = table
        self.client = ProxyClient(proxy_host, proxy_port, proxy_user, proxy_password)

        for table in self.table_names:
            if not self.check_table(table):
                if create_tables:
                    self.create_table(table)
                else:
                    raise TableNotFoundException("table {} ".format(table) +
                        "does not exist. create it before running this "+
                        "application")

    def _simple_scan_opts(self, row, colf, colq):
        r = Range(Key(row), True, Key("{0}\0".format(row)), True)
        if colf is not None and colq is not None:
            s = [ScanColumn(colf, colq)]
        else:
            s = None
        return ScanOptions(range=r, columns=s)

    def create_table(self, table_name=None):
        if table_name is None:
            table_name = self.table_names[0]
        self.client.createTable(table_name, True, TimeType.MILLIS)

    def delete_table(self, table_name=None):
        if table_name is None:
            table_name = self.table_names[0]
        try:
            self.client.deleteTable(table_name)
        except TableNotFoundException as e:
            print "not deleting table, doesn't exist", e

    def check_table(self, table_name=None):
        if table_name is None:
            table_name = self.table_names[0]
        return self.client.tableExists(table_name)

    def write(self, row, colf="", colq="", table=None, value=None):
        if table is None:
            table = self.table_names[0]
        arow = {row: [ColumnUpdate(colf, colq, value=value)]}
        self.client.updateAndFlush(table, arow)

    def read(self, row, colf="", colq="", table=None):
        if table is None:
            table = self.table_names[0]
        try:
            so = self._simple_scan_opts(row, colf, colq)
            scanner = self.client.createScanner(table, so)
            entry  = self.client.nextEntry(scanner)
            value = entry.keyValue.value
        except NoMoreEntriesException as nme:
            raise KeyError(nme)
        return value

    def delete(self, row, table=None):
        if table is None:
            table = self.table_names[0]
        try:
            so = self._simple_scan_opts(row, None, None)
            scanner = self.client.createScanner(table, so)
            rows = []
            for r in AccumuloRowIterator(self.client, scanner):
                rows.append(ColumnUpdate(
                    colFamily=r.key.colFamily,
                    colQualifier=r.key.colQualifier,
                    deleteCell=True
                    ))
            self.client.updateAndFlush(table, {row: rows})
        except Exception as e:
            print "exception deleting: {0} => {1}".format(row, e)

    def all(self, table=None):
        if table is None:
            table = self.table_names[0]
        try:
            # Scan for all the rows in the table
            scanner = self.client.createScanner(table, ScanOptions(None, None, None, None, None))
            rows = {}
            for row in AccumuloRowIterator(self.client, scanner):
                key = self.key(row.key.row, row.key.colFamily, row.key.colQualifier)
                rows[key] = row.value

            # Sort the rows (by key) and collect them so you have a list
            # of dicts grouped by row ID
            os = []
            lkey = None
            lgrp = None
            for key in sorted(rows):
                a, b, c = self.key_parts(key)
                if lkey is None or a != lkey:
                    if lgrp is not None:
                        os.append(lgrp)
                    lkey = a
                    lgrp = {}
                lgrp[key] = rows[key]
            # add last group, it gets missed in the last loop
            if lgrp is not None:
                os.append(lgrp)
            return os
        except Exception as e:
            print e