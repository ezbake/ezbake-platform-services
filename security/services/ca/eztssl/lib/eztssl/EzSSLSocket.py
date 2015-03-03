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
Created on Tue Nov 12 08:25:15 2013

@author: jhastings
"""
from thrift.transport.TTransport import TTransportException
import TSSLSocket as TSSL
import re

from ezconfiguration.helpers import SecurityConfiguration

class TSSLSocket(TSSL.TSSLSocket):
    def __init__(self, ezConfig=None, host='localhost', port=9090, validate=True,
                 unix_socket=None, verify_pattern=None, ca_certs=None,
                 cert=None, key=None):
        if ezConfig is not None:
            sc = SecurityConfiguration(ezConfig)
            ca_certs = sc.getTrustedSslCerts()
            cert = sc.getSslCertificate()
            key = sc.getPrivateKey()

        TSSL.TSSLSocket.__init__(self, host=host, port=port, validate=validate,
                                 certfile=cert,
                                 ca_certs=ca_certs,
                                 keyfile=key,
                                 unix_socket=unix_socket)
        self.verify_pattern = verify_pattern
        if verify_pattern is not None:
            self.verify_pattern = re.compile(verify_pattern)

    def _validate_cert(self):
        """
        internal method to validate the peer's SSL certificate, and to check
        that the  commonName is set

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
            certCN = cert_value
            if certCN is not None:
                if self.verify_pattern is not None:
                    if self.verify_pattern.match(certCN):
                        self.is_valid = True
                        return
                else:
                    # success
                    self.is_valid = True
                    return
            else:
                raise TTransportException(
                    type=TTransportException.UNKNOWN,
                    message='Application name we connected to "%s" doesn\'t '
                    'match application name provided commonName "%s"' %
                    (self.targetAppName, certCN))

        raise TTransportException(
            type=TTransportException.UNKNOWN,
            message='Could not validate SSL certificate from host "%s". '
            'Cert=%s' % (self.host, cert))


class TSSLServerSocket(TSSL.TSSLServerSocket):
    def __init__(self, ezConfig=None, host=None, port=None, unix_socket=None,
                 verify_pattern=None, ca_certs=None, cert=None, key=None):
        if ezConfig is not None:
            sc = SecurityConfiguration(ezConfig)
            ca_certs = sc.caCerts()
            cert = sc.certs()
            key = sc.privateKey()

        TSSL.TSSLServerSocket.__init__(self, port=port, host=host,
                                       certfile=cert,
                                       ca_certs=ca_certs,
                                       keyfile=key,
                                       unix_socket=unix_socket)
        self.verify_pattern = verify_pattern
        if verify_pattern is not None:
            self.verify_pattern = re.compile(verify_pattern)

    def _validate(self, client):
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
            certCN = cert_value
            if certCN is not None:
                if self.verify_pattern is not None:
                    if self.verify_pattern.match(certCN):
                        self.is_valid = True
                        return
                else:
                    # success
                    self.is_valid = True
                    return
            else:
                raise TTransportException(
                    type=TTransportException.UNKNOWN,
                    message='Host name we connected to "%s" doesn\'t match '
                    'certificate provided commonName "%s"' %
                    (self.host, certCN))

        raise TTransportException(
            type=TTransportException.UNKNOWN,
            message='Could not validate SSL certificate from host "%s". '
            ' Cert=%s' % (self.host, cert))
