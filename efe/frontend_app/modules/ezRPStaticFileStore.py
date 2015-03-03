#   Copyright (C) 2013-2015 Computer Sciences Corporation
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

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
import array
import math
import logging
import logging.handlers
from pyaccumulo import Accumulo, Mutation, Range


class EzRPStaticStore(object):

    '''
    Class to save and retrieve static content from Accumulo.
    cf = "static"                   For all rows
    cq = "hash"                     Stores the hash_value of Static File
    cq = "nofchunks"                Stores the number of Chunks needed to store Static File
    cq = "chunk_000" .. "chunk_nnn" Stores the Chunks of Static File
    '''
    def __init__(self, host="localhost", port=42424, user='root', password='secret', chunk_size=int(5*1048576), logger=None):
        self.__host = host
        self.__port = port
        self.__user = user
        self.__password = password
        self.__table = 'ezfrontend'
        self.__cf = 'static'
        self.__connection = None

        if logger is not None:
            self.__log = logger
        else:
            self.__log = logging.getLogger(self.__module__ + '.' + self.__class__.__name__)
            self.__log.addHandler(logging.NullHandler())

        self.__chunk_size =int(chunk_size)
        self._connect(self.__host, self.__port, self.__user, self.__password)

    def _connect(self, host, port, user, password):
        try:
            self.__connection = Accumulo(host, port, user, password)
            self.__log.debug('Connected to StaticFile Store')
        except Exception as e:
            self.__log.exception('Error while connecting to StaticFile Store: %s' % str(e))
            raise Exception('Error while connecting to StaticFile Store: %s' % str(e))

    def _ensureTableExists(self):
        '''
        Make sure that the table exists before any other operation.
        Reconnect to Accumulo if the Connection is reset.
        '''
        if not self.__connection.table_exists(self.__table):
            self.__log.info('table "{table}" does not exist in StaticFile Store. Creating the table'.format(table=self.__table))
            self.__connection.create_table(self.__table)
            if not self.__connection.table_exists(self.__table):
                self.__log.error('Unable to ensure StaticFile Store table "{table} exists'.format(format(table=self.__table)))
                raise Exception('StaticFile Store:  Unable to ensure table "{table}" exists'.format(table=self.__table))

    def _ensureNoDuplicates(self, usrFacingUrlPrefix):
        '''
         Ensure a single copy of file for a given usrFacingUrlPrefix
        '''
        if self._getHash(usrFacingUrlPrefix) is not None:
            self.deleteFile(usrFacingUrlPrefix)

    def _putNofChunks(self, usrFacingUrlPrefix, length):
        '''
        Put the number of chunks the static contents is stored
        '''

        chunks = int(math.ceil(length / float(self.__chunk_size)))
        writer = self.__connection.create_batch_writer(self.__table)
        m = Mutation(usrFacingUrlPrefix)
        m.put(cf=self.__cf, cq="nofchunks", val=str(chunks))
        writer.add_mutation(m)
        writer.close()

    def _getNofChunks(self, usrFacingUrlPrefix):
        '''
        Get the number of chunks the static contents is stored
        '''
        scan_range = Range(srow=usrFacingUrlPrefix, scf=self.__cf, scq="nofchunks",
                           erow=usrFacingUrlPrefix, ecf=self.__cf, ecq="nofchunks")
        for entry in self.__connection.scan(self.__table, scanrange=scan_range):
            return int(entry.val)
        return 0

    def _getChunks(self, data):
        '''
        Break the blob into CHUNK_SIZE.
        less than maxFrameSize in Accumulo proxy.properties
        '''
        data_length = len(data)
        for i in range(0, data_length + 1, self.__chunk_size):
            yield data[i:i + self.__chunk_size]

    def _putHash(self, usrFacingUrlPrefix, hash_str):
        '''
        Puts the Hash for usrFacingUrlPrefix
        '''
        writer = self.__connection.create_batch_writer(self.__table)
        m = Mutation(usrFacingUrlPrefix)
        m.put(cf=self.__cf, cq="hash", val=hash_str)
        writer.add_mutation(m)
        writer.close()

    def _getHash(self, usrFacingUrlPrefix):
        scan_range = Range(srow=usrFacingUrlPrefix, scf=self.__cf, scq="hash",
                           erow=usrFacingUrlPrefix, ecf=self.__cf, ecq="hash")
        for entry in self.__connection.scan(self.__table, scanrange=scan_range):
            return str(entry.val)
        else:
            return None

    def reConnection(self):
        self._connect(self.__host, self.__port, self.__user, self.__password)

    def putFile(self, usrFacingUrlPrefix, hash_str, data):
        self._ensureTableExists()
        self._ensureNoDuplicates(usrFacingUrlPrefix)
        self._putHash(usrFacingUrlPrefix, hash_str)
        data_length = len(data)
        self._putNofChunks(usrFacingUrlPrefix, data_length)
        writer = self.__connection.create_batch_writer(self.__table)
        for i, chunk in enumerate(self._getChunks(data)):
            m = Mutation(usrFacingUrlPrefix)
            m.put(cf=self.__cf, cq="chunk_{number:010d}".format(number=i), val=chunk)
            writer.add_mutation(m)
        self.__log.debug('added static file for "{url}" with hash "{hash}" of length "{length}"'.format(url=usrFacingUrlPrefix, hash=hash_str, length=data_length))
        writer.close()

    def getFile(self, usrFacingUrlPrefix):
        '''
        Assembles all the chunks for this row
        '''
        self._ensureTableExists()
        data = array.array('c') # Create a byte array
        chunks = self._getNofChunks(usrFacingUrlPrefix)
        chunks_read = 0
        for i in range(chunks):
             cq = 'chunk_{number:010d}'.format(number=i)
             for entry in self.__connection.scan(self.__table, None, cols=[[self.__cf, cq]]):
                 if entry.row == usrFacingUrlPrefix and entry.cq.startswith("chunk_"):
                     chunks_read += 1
                     data.extend(entry.val)

        # This code gets following error while retrieving over 96MB.  Data stops at first chunk_000
        # # java.lang.OutOfMemoryError: Java heap space
        # -XX:OnOutOfMemoryError="kill -9 %p"
        #   Executing /bin/sh -c "kill -9 32597"...
        # [1]+  Exit 137  sudo -u accumulo /opt/accumulo/current/bin/accumulo proxy -p /opt/accumulo/current/conf/proxy.properties

        # startChunk = "chunk_{number:010d}".format(number=0)
        # endChunk = "chunk_{number:010d}".format(number=chunks)
        # scan_range = Range(srow=usrFacingUrlPrefix, scf=self.__cf, scq=startChunk,
        #                    erow=usrFacingUrlPrefix, ecf=self.__cf, ecq=endChunk)
        # for entry in self.__connection.scan(self.__table, scanrange=scan_range):
        #     #self.__log.info("getFile: row = {0} cq= {1}".format(entry.row, entry.cq))
        #     if entry.cq.startswith("chunk_"):
        #         self.__log.info("getFile: row = {0} cq= {1}".format(entry.row, entry.cq))
        #         chunks_read += 1
        #         data.extend(entry.val)
        self.__log.debug('retrieved static file for {url}'.format(url=usrFacingUrlPrefix))
        if chunks_read != chunks:
            self.__log.error("did not read all the chunks from StaticFile Store")
        return data.tostring() if data.buffer_info()[1] > 0 else None

    def deleteFile(self, usrFacingUrlPrefix):
        self._ensureTableExists()
        writer = self.__connection.create_batch_writer(self.__table)
        chunks = self._getNofChunks(usrFacingUrlPrefix)
        m = Mutation(usrFacingUrlPrefix)
        m.put(cf=self.__cf, cq="hash", is_delete=True)
        m.put(cf=self.__cf, cq="nofchunks", is_delete=True)
        for i in range(chunks):
            cq = 'chunk_{number:010d}'.format(number=i)
            m.put(cf=self.__cf, cq=cq, is_delete=True)
        writer.add_mutation(m)
        self.__log.debug('removed static file for {url}'.format(url=usrFacingUrlPrefix))
        writer.close()

    def getAttributes(self):
        '''
        Returns the urlprefix and the hash of all the entries in table as tuple
        '''
        self._ensureTableExists()
        for entry in self.__connection.scan(self.__table, None, cols=[[self.__cf, "hash"]]):
            yield (entry.row, str(entry.val))
        else:
            yield (None, None)
