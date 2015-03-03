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
Created on Mon Mar 24 08:15:52 2014

@author: jhastings
"""
import errno
import nose.tools as nt
from kazoo.testing import KazooTestHarness

import time
import shutil
import os
import os.path as osp
from multiprocessing import Process
import OpenSSL.crypto as crypto

from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from ezconfiguration.EzConfiguration import EzConfiguration
from ezconfiguration.constants.EzBakePropertyConstants import EzBakePropertyConstants
from eztssl import EzSSLSocket

from ezpersist.file import FilePersist
from ezbakeca import ca, caservice, cert
import ezca
from ezbakeBaseTypes import ttypes as ezbakeBaseTypes
from ezbakeBaseAuthorizations import ttypes as bvAuths


JAVA_CSR="""-----BEGIN CERTIFICATE REQUEST-----
MIICXTCCAUUCAQAwGDEWMBQGA1UEAxMNR2FyeSBEcm9jZWxsYTCCASIwDQYJKoZI
hvcNAQEBBQADggEPADCCAQoCggEBAJybIH/k+sBbZaDsJFbOFXcd08GaqlFAWaic
T8oqbwRrUsPMkEL47B7Sl4gU2l0txmhJPptQx9sV8pcvtbAIon8TZN+v/yIeS1P3
nXelEzMlEZM6p/FvT2rHKkCHTwaWC3eDxezPb29DdfQRmxgujYaNBJqlFdnVsY+M
3ditjvjOaVf0gEQzN1PProrMiyKEiIR+ZYWZae1+FaJ43sSBvFki45NfevpNt93n
xx6GNKkQldDJO78gkPoTHkplQq4DWLu6FDjRQ41MqIaXYhVU1reLqToF9GUWUBgt
EgWP1eCllfmx7BlF7xc6cWWbVsRy66iMtzOlOnO0WQ7bd8t1IncCAwEAAaAAMA0G
CSqGSIb3DQEBCwUAA4IBAQBEv2JH53+amlCDmnYXFwQ4j00VbEPPsmpOFuDeOv9n
M+YGo0+ByWbVWOsAc1cupU7+2hUerz7uq2DDjFfV84jUQP/jHBK+p9jVTUw+Y+up
139iWfTKz+UoykC3rKIhXKWMo10eTEQNFHsCU3BgM2vX50cnHr7LzBL6Y942694G
mBH9qukDZcg+xefMcLFxQwRrXmKCQF/8oRkTZWw4FLhcn7LEdAFO2pitivJfp2q1
P6A192WeSpPFKDVL+QcQo6xkYbJP98blsZEmG/hE0lmqvAahe7rQbxCQ4satk7hg
ui+dcY/eTH3SlKs5OrAbV4UNufc7zlFeR7shllaEUZZN
-----END CERTIFICATE REQUEST-----"""


class TestEzCAService(KazooTestHarness):

    def setUp(self):
        self.setup_zookeeper()
        zoo_host = ":".join(str(x) for x in self.client.hosts[0])

        ezConfig = EzConfiguration().getProperties()
        ezConfig[EzBakePropertyConstants.ZOOKEEPER_CONNECTION_STRING] = zoo_host
        ezConfig[caservice.EzCAHandler.CLIENT_CERTS] = "client"
        ezConfig[caservice.EzCAHandler.CLIENT_CERT_O] = "tmpcerts"

        # make direcotry for client certs
        try:
            os.mkdir('tmpcerts')
            os.mkdir('tmpcerts/server')
            os.mkdir('tmpcerts/client')
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise e

        #caservice.setup_logging(True, ezConfig)
        # Start the server
        self.serverProcess = Process(target=caservice.ca_server,
                                     args=(ezConfig,),
                                     kwargs={'port': 5049, 'host': 'localhost',
                                     'verify_pattern': r"client",
                                     "ssldir": "tmpcerts/server"})
        self.serverProcess.start()
        # Starting the server takes a while
        time.sleep(5)

        # Write out the client certs
        ca.EzbakeCA.setup(FilePersist(caservice.EzCAHandler.TABLE_NAME))
        cert.Cert.setup(FilePersist(caservice.EzCAHandler.TABLE_NAME))
        try:
            ca_certs = ca.EzbakeCA.get_named("ezbakeca")
        except KeyError:
            ca_certs = ca.EzbakeCA(name="ezbakeca")
            ca_certs.save()

        client_certs = cert.Cert.get_named("client")
        with open(os.path.join("tmpcerts/client", "ezbakeca.crt"), 'w') as f:
            f.write(ca_certs.cert_string())
        with open(os.path.join("tmpcerts/client", "application.crt"), 'w') as f:
            f.write(client_certs.cert_string())
        with open(os.path.join("tmpcerts/client", "application.priv"), 'w') as f:
            f.write(client_certs.pkey_string())

    def tearDown(self):
        if self.serverProcess.is_alive():
            self.serverProcess.terminate()
        try:
            shutil.rmtree('_EZ_CA_')
            shutil.rmtree('tmpcerts')
            pass
        except:
            pass
        self.teardown_zookeeper()

    def get_token(self):
        token = ezbakeBaseTypes.EzSecurityToken()
        token.validity = ezbakeBaseTypes.ValidityCaveats()
        token.validity.signature = ""
        token.validity.notAfter = 12345
        token.validity.issuedTo = "Test"
        token.authorizations = bvAuths.Authorizations()
        token.authorizations.formalAuthorizations = ["U"]
        token.authorizationLevel = "A"
        token.tokenPrincipal = ezbakeBaseTypes.EzSecurityPrincipal()
        token.tokenPrincipal.validity = ezbakeBaseTypes.ValidityCaveats()
        token.tokenPrincipal.id = "1234"
        token.tokenPrincipal.principal = "Test"
        token.tokenPrincipal.name = "Test"
        token.citizenship = "USA"
        token.organization = "CSC"
        token.validate()
        return token

    def get_client(self, port):
        ezConfig = EzConfiguration().getProperties()
        ezConfig[EzBakePropertyConstants.EZBAKE_CERTIFICATES_DIRECTORY] = "tmpcerts/client"

        host = 'localhost'

        transport = EzSSLSocket.TSSLSocket(ezConfig, host=host, port=port,
                                           validate=True,
                                           verify_pattern=r"Ez.*")
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        transport.open()
        return ezca.EzCA.Client(protocol)

    def test_csr_signing(self):
        pkey = ca.private_key()
        req = ca.csr(pkey, CN="App", O="Ezbake", OU="Ezbake Apps", C="US")
        cert = self.get_client(5049).csr(self.get_token(), ca.pem_csr(req))
        cert = crypto.load_certificate(crypto.FILETYPE_PEM, cert)
        nt.assert_is_instance(cert, crypto.X509)
        nt.assert_equal(req.get_subject(), cert.get_subject())

    def test_java_csr_signing(self):
        cert = self.get_client(5049).csr(self.get_token(), JAVA_CSR)
        nt.assert_is_instance(
            crypto.load_certificate(crypto.FILETYPE_PEM, cert),
            crypto.X509)


class TestClientCertGeneration(object):
    def setUp(self):
        try:
            os.mkdir("certout")
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise e

    def tearDown(self):
        try:
            shutil.rmtree("certout")
        except Exception:
            pass

    def test_cert_gen(self):
        ca = caservice.EzCAHandler("ezbakeca")
        certs = ca._server_certs()
        nt.assert_is_instance(certs, dict)
        nt.assert_equal(3, len(certs))
        nt.assert_is_instance(certs.values()[0], str)
        nt.assert_is_instance(certs.values()[1], str)
        nt.assert_is_instance(certs.values()[2], str)

    def test_tar_out(self):
        ca = caservice.EzCAHandler("ezbakeca")
        c = cert.Cert("TestTar")
        c.cert = ca.ca.sign_csr(c.csr())

        caservice.tar_certs(ca.ca.cert_string(), c, "certout")
