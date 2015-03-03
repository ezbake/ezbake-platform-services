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

from pyaccumulo import Accumulo, Mutation, Range
import base64
import OpenSSL
import logging
import logging.handlers
from Crypto.Hash import SHA256
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5


class EzRPCertStoreException(RuntimeError):
    def __init__(self, *args):
        super(EzRPCertStoreException, self).__init__(args)



class EzRPCertStore(object):
    """
    Wrapper class to underlying database store which hold server certs for reverse proxy
    """

    def __init__(self, host='localhost', port=42424, user='nobody', password='password', table='ezfrontend', privateKey=None, logger=None):
        self.__table = table
        self.__signer = None
        self.__dbConnection = None
        self.__cf = "pfx"
        self.__cq = "enc"

        if logger is not None:
            self.__logger = logger
        else:
            self.__logger = logging.getLogger(self.__module__ + '.' + self.__class__.__name__)
            self.__logger.addHandler(logging.NullHandler())
        
        if privateKey is not None:
            self.__updateSigner(privateKey)

        self.__connectToAccumulo(host, port, user, password)


    def __connectToAccumulo(self, host, port, user, password):
        try:
            self.__logger.info('connecting to CertStore ...')
            self.__dbConnection = Accumulo(host, port, user, password)
            self.__logger.info('Successfully connected to CertStore')
        except Exception as ex:
            self.__logger.exception('Error in connecting to CertStore: %s' % str(ex))
            raise EzRPCertStoreException('Error in connecting to CertStore: %s' % str(ex))


    def __updateSigner(self, privateKey):
        with open(privateKey) as file:
            self.__signer = PKCS1_v1_5.new(RSA.importKey(file.read()))
            self.__logger.debug('Updated signer for CertStore')


    def __ensureTable(self):
        if not self.__dbConnection.table_exists(self.__table):
            self.__logger.info('DB table %s doesn\'t exist in the Store. Creating ...' % self.__table)
            self.__dbConnection.create_table(self.__table)
            if not self.__dbConnection.table_exists(self.__table):
                self.__logger.error('Unable to ensure DB table exists in the Store.')
                raise EzRPCertStoreException('CertStore: Unable to ensure DB table exists in the Store.')


    def _generatePassword(self, serverName):
        password = 'RUNNING##horse' #salt
        
        if self.__signer is None:
            password = base64.b64encode(password + serverName)
        else:
            digest = SHA256.new(password + serverName)
            signature = self.__signer.sign(digest)
            password = base64.b64encode(signature)

        return password


    def _generatePkcs12(self, serverName, certContents, keyContents, password=None):
        key = OpenSSL.crypto.load_privatekey(OpenSSL.crypto.FILETYPE_PEM, keyContents)
        cert = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, certContents)
        
        pfx = OpenSSL.crypto.PKCS12()
        pfx.set_certificate(cert)
        pfx.set_privatekey(key)
        
        return pfx.export(passphrase=password)


    def _retrieveCertAndKey(self, pfx, serverName, password=None):
        p12 = OpenSSL.crypto.load_pkcs12(pfx, password)
        keycontents = OpenSSL.crypto.dump_privatekey(OpenSSL.crypto.FILETYPE_PEM, p12.get_privatekey())
        certContents = OpenSSL.crypto.dump_certificate( OpenSSL.crypto.FILETYPE_PEM, p12.get_certificate())
        return certContents, keycontents


    def put(self, serverName, certContents, keyContents):
        self.__ensureTable()

        writer = self.__dbConnection.create_batch_writer(self.__table)        
        value = self._generatePkcs12(serverName, certContents, keyContents, self._generatePassword(serverName))
    
        mutation = Mutation(serverName)
        mutation.put(cf=self.__cf, cq=self.__cq, val=value)
        writer.add_mutation(mutation)
        writer.close()
        self.__logger.debug('added cert/key contents for %s to store' % serverName)


    def get(self, serverName):
        self.__ensureTable()
        
        for entry in self.__dbConnection.scan(self.__table, cols=[[self.__cf, self.__cq]]):
            if entry.row == serverName:
                self.__logger.debug('retrieved cert/key for %s from store' % serverName)
                return self._retrieveCertAndKey(entry.val, serverName, self._generatePassword(serverName))
        return None, None


    def remove(self, serverName):
        self.__ensureTable()
        writer = self.__dbConnection.create_batch_writer(self.__table)
        mutation = Mutation(serverName)
        mutation.put(cf=self.__cf, cq=self.__cq, is_delete=True)
        writer.add_mutation(mutation)
        writer.close()
        self.__logger.debug('removed cert/key for %s from store' % serverName)


    def exists(self, serverName):
        self.__ensureTable()
        
        #use a sigle row range to narrow our scan
        range = Range(srow=serverName, scf=self.__cf, scq=self.__cq,
                      erow=serverName, ecf=self.__cf, ecq=self.__cq)
                      
        for entry in self.__dbConnection.scan(self.__table, scanrange=range):
            if entry.row == serverName:
                self.__logger.debug('cert/key for %s exists in store' % serverName)
                return True
        self.__logger.debug('cert/key for %s DOES NOT exist in store' % serverName)
        return False

