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

# Copyright (c) 2007-2012 by Evernote Corporation, All rights reserved.
#
# Use of the source code and binary libraries included in this package
# is permitted under the following terms:
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
# OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
# IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
# NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
# THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#
import os
import socket
import ssl

from thrift.transport import TSocket
from thrift.transport.TTransport import TTransportException


def validateAsEzInfrastructure(server_name):
    if server_name.startswith('_Ez_'):
        return True
    return False


class TSSLSocket(TSocket.TSocket):
    """
    SSL implementation of client-side TSocket

    This class creates outbound sockets wrapped using the
    python standard ssl module for encrypted connections.

    The protocol used is set using the class variable
    SSL_VERSION, which must be one of ssl.PROTOCOL_* and
    defaults to  ssl.PROTOCOL_TLSv1 for greatest security.
    """
    SSL_VERSION = ssl.PROTOCOL_SSLv23

    def __init__(self, host='localhost', port=9090, validate=True,
                 ca_certs=None, certfile=None, keyfile=None, unix_socket=None):
        """
        @param validate: Set to False to disable SSL certificate validation
        entirely.
        @type validate: bool
        @param ca_certs: Filename to the Certificate Authority pem file,
        possibly a file downloaded from: http://curl.haxx.se/ca/cacert.pem
        This is passed to the ssl_wrap function as the 'ca_certs' parameter.
        @type ca_certs: str

        Raises an IOError exception if validate is True and the ca_certs file
        is None, not present or unreadable.
        """
        self.validate = validate
        self.is_valid = False
        self.peercert = None
        if not validate:
            self.cert_reqs = ssl.CERT_NONE
        else:
            self.cert_reqs = ssl.CERT_REQUIRED
        self.ca_certs = ca_certs
        self.certfile = certfile
        self.keyfile = keyfile
        if validate:
            if ca_certs is None or not os.access(ca_certs, os.R_OK):
                raise IOError('Certificate Authority ca_certs file "%s" is not'
                              ' readable, cannot validate SSL certificates.' %
                              (ca_certs))
        TSocket.TSocket.__init__(self, host, port, unix_socket)

    def open(self):
        try:
            res0 = self._resolveAddr()
            for res in res0:
                sock_family, sock_type = res[0:2]
                ip_port = res[4]
                plain_sock = socket.socket(sock_family, sock_type)
                self.handle = ssl.wrap_socket(
                    plain_sock, ssl_version=self.SSL_VERSION,
                    do_handshake_on_connect=True, ca_certs=self.ca_certs,
                    certfile=self.certfile, keyfile=self.keyfile,
                    cert_reqs=self.cert_reqs, ciphers="HIGH:!ADH")
                self.handle.settimeout(self._timeout)
                try:
                    self.handle.connect(ip_port)
                except socket.error, e:
                    if res is not res0[-1]:
                        continue
                    else:
                        raise e
                break
        except socket.error, e:
            if self._unix_socket:
                message = 'Could not connect to secure socket %s' % \
                    self._unix_socket
            else:
                print e
                message = 'Could not connect to %s:%d' % (self.host, self.port)
            raise TTransportException(type=TTransportException.NOT_OPEN,
                                      message=message)
        if self.validate:
            self._validate_cert()

    def _validate_cert(self):
        """
        internal method to validate the peer's SSL certificate, and to check
        the commonName of the certificate to ensure it matches the hostname we
        used to make this connection.  Does not support subjectAltName records
        in certificates.

        raises TTransportException if the certificate fails validation."""
        cert = self.handle.getpeercert()
        self.peercert = cert
        if 'subject' not in cert:
            raise TTransportException(
                type=TTransportException.NOT_OPEN,
                message='No SSL certificate found from %s:%s' %
                (self.host, self.port))
        fields = cert['subject']
        for field in fields:
            # ensure structure we get back is what we expect
            if not isinstance(field, tuple):
                continue
            cert_pair = field[0]
            if len(cert_pair) < 2:
                continue
            cert_key, cert_value = cert_pair[0:2]
            if cert_key != 'commonName':
                continue
            certhost = cert_value
            if certhost == self.host:
                # success, cert commonName matches desired hostname
                self.is_valid = True
                return
            else:
                raise TTransportException(
                    type=TTransportException.UNKNOWN,
                    message='Host name we connected to "%s" doesn\'t match '
                    'certificate provided commonName "%s"' %
                    (self.host, certhost))
        raise TTransportException(
            type=TTransportException.UNKNOWN,
            message='Could not validate SSL certificate from host "%s". '
            ' Cert=%s' % (self.host, cert))


class TSSLServerSocket(TSocket.TServerSocket):
    """
    SSL implementation of TServerSocket

    This uses the ssl module's wrap_socket() method to provide SSL
    negotiated encryption.
    """
    SSL_VERSION = ssl.PROTOCOL_SSLv23

    def __init__(self, port=None, certfile=None, ca_certs=None, keyfile=None,
                 unix_socket=None, host=None):
        """Initialize a TSSLServerSocket

        @param certfile: The filename of the server certificate file, defaults
        to cert.pem
        @type certfile: str
        @param port: The port to listen on for inbound connections.
        @type port: int
        """
        if ca_certs is None or not os.access(ca_certs, os.R_OK):
             raise IOError('Certificate Authority ca_certs file "%s" is not'
                           ' readable, cannot validate SSL certificates.' %
                               (ca_certs))
        if certfile is None or not os.access(certfile, os.R_OK):
            raise IOError('Certificate Authority certfile file "%s" is not'
                           ' readable, cannot validate SSL certificates.' %
                               (certfile))
        if keyfile is None or not os.access(keyfile, os.R_OK):
             raise IOError('Certificate Authority keyfile file "%s" is not'
                           ' readable, cannot validate SSL certificates.' %
                               (keyfile))
        self.certfile = certfile
        self.ca_certs = ca_certs
        self.keyfile = keyfile
        TSocket.TServerSocket.__init__(self, host, port, unix_socket)

    def accept(self):
        plain_client, addr = self.handle.accept()
        try:
            client = ssl.wrap_socket(
                plain_client, certfile=self.certfile, server_side=True, keyfile=self.keyfile,
                ssl_version=self.SSL_VERSION,cert_reqs=ssl.CERT_REQUIRED, ca_certs=self.ca_certs, ciphers="HIGH:!ADH")
        except (ssl.SSLError) as ssl_exc:
            # failed handshake/ssl wrap, close socket to client
            print "SSLError: ", ssl_exc
            if plain_client is not None:
                plain_client.close()
            # raise ssl_exc
            # We can't raise the exception, because it kills most TServer
            # derived serve() methods.
            # Instead, return None, and let the TServer instance deal with it
            # in other exception handling.  (but TSimpleServer dies anyway)
            return None
        result = TSocket.TSocket()
        result.setHandle(client)
        self._validate(client)
        return result

    def _validate(self,client):
        """
        internal method to validate the peer's SSL certificate, and to check
        the commonName of the certificate to ensure it matches  the CNs
        reserved for EzBake Infrastructure.  Does not support subjectAltName records
        in certificates.

        raises TTransportException if the certificate fails validation."""
        cert = client.getpeercert()
        client.peercert = cert
        if 'subject' not in cert:
            raise TTransportException(
                type=TTransportException.NOT_OPEN,
                message='No SSL certificate found from %s:%s' %
                (self.host, self.port))
        fields = cert['subject']
        for field in fields:
            # ensure structure we get back is what we expect
            if not isinstance(field, tuple):
                continue
            cert_pair = field[0]
            if len(cert_pair) < 2:
                continue
            cert_key, cert_value = cert_pair[0:2]
            if cert_key != 'commonName':
                continue
            certhost = cert_value
            #print "CERTHOST:"+str(certhost)
            if validateAsEzInfrastructure(certhost):
                # success, cert commonName matches desired hostname
                client.is_valid = True
                return
            else:
                raise TTransportException(
                    type=TTransportException.UNKNOWN,
                    message='Host name we connected to "%s" doesn\'t match '
                    'certificate provided commonName "%s"' %
                    (self.host, certhost))

        raise TTransportException(
            type=TTransportException.UNKNOWN,
            message='Could not validate SSL certificate from host "%s". '
            ' Cert=%s' % (self.host, cert))