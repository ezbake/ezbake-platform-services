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
Created on Mon Mar 24 08:12:03 2014

@author: jhastings
"""

import nose.tools as nt
import multiprocessing.pool
import os
import shutil

from Crypto.PublicKey import RSA
from OpenSSL import crypto, SSL
from ezpersist.base import MemoryPersist
from ezpersist.file import FilePersist
import ezbakeca.ca as ezbakeca

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

class TestEzbakecaModule(object):
    def __init__(self):
        self.pkey = ezbakeca.private_key()
        req = ezbakeca.csr(self.pkey,
                           CN="EzCA", O="Ezbake", OU="Ezbake Core", C="US")
        self.ca = ezbakeca.create_ca_certificate(req, self.pkey)

        self.appKey = ezbakeca.private_key()
        req = ezbakeca.csr(self.appKey, CN="APP")
        self.appCert = ezbakeca.create_certificate(req, (self.ca, self.pkey), 2)

    def test_private_key(self):
        pkey = ezbakeca.private_key()

        nt.assert_is_instance(pkey, RSA._RSAobj)

    def test_java_csr(self):
        csr = crypto.load_certificate_request(crypto.FILETYPE_PEM, JAVA_CSR)
        cert = ezbakeca.create_certificate(csr, (self.ca, self.pkey), 4)
        nt.assert_is_instance(cert, crypto.X509)

    def test_csr(self):
        pkey = ezbakeca.private_key()
        csr = ezbakeca.csr(pkey, CN="Test")

        nt.assert_is_instance(csr, crypto.X509Req)
        nt.assert_equal(csr.get_subject().CN, "Test")
        nt.assert_true(csr.verify(ezbakeca.openssl_key(pkey)))

    def test_create_ca(self):
        pkey = ezbakeca.private_key()
        req = ezbakeca.csr(pkey,
                           CN="EzCA", O="Ezbake", OU="Ezbake Core", C="US")
        ca = ezbakeca.create_ca_certificate(req, pkey)

        nt.assert_is_instance(ca, crypto.X509)
        nt.assert_equal(ca.get_subject(), req.get_subject())
        nt.assert_equal(ca.get_issuer(), req.get_subject())
        nt.assert_equal(ca.get_serial_number(), 1)

    def test_create_cert(self):
        pkey = ezbakeca.private_key()
        req = ezbakeca.csr(pkey,
                           CN="App", O="Ezbake", OU="Ezbake Apps", C="US")
        cert = ezbakeca.create_certificate(req, (self.ca, self.pkey), 2)

        nt.assert_is_instance(cert, crypto.X509)
        nt.assert_equal(cert.get_subject(), req.get_subject())
        nt.assert_equal(cert.get_issuer(), self.ca.get_subject())
        nt.assert_equal(cert.get_serial_number(), 2)
        nt.assert_equal(cert.get_version(), 2)

    def test_trusted_cert(self):
        ctx = SSL.Context(SSL.TLSv1_METHOD)
        ctx.get_cert_store().add_cert(self.ca)
        ctx.use_privatekey(ezbakeca.openssl_key(self.appKey))
        ctx.use_certificate(self.appCert)

        nt.assert_equal(ctx.check_privatekey(), None)

    def _verify_callback(self, conn, x509, i1, i2, i3):
        print i1
        print i2
        return True


class TestEzbakeCA(object):
    def setUp(self):
        ezbakeca.EzbakeCA.setup(MemoryPersist())

    def test_get_named(self):
        ca = ezbakeca.EzbakeCA("named")
        ca.save()
        ca2 = ezbakeca.EzbakeCA.get_named("named")
        nt.assert_equal(ca.name, ca2.name)
        nt.assert_equal(ca.pkey_string(), ca2.pkey_string())
        nt.assert_equal(ca.cert_string(), ca2.cert_string())
        nt.assert_equals(ca.serial, ca2.serial)

    def test_get_named_not_exist(self):
        nt.assert_raises(KeyError, ezbakeca.EzbakeCA.get_named, "doesnotexistca")

    def test_sign_csr(self):
        ca = ezbakeca.EzbakeCA()

        pkey = ezbakeca.private_key()
        req = ezbakeca.csr(pkey,
                           CN="App", O="Ezbake", OU="Ezbake Apps", C="US")
        cert = ca.sign_csr(req)

        nt.assert_is_instance(cert, crypto.X509)
        nt.assert_equal(cert.get_subject(), req.get_subject())
        nt.assert_equal(cert.get_issuer(), ca.ca_cert.get_subject())
        nt.assert_equal(cert.get_serial_number(), 2)
        nt.assert_equal(cert.get_version(), 2)

    def test_sign_serial_increment(self):
        ezbakeca.EzbakeCA.setup(store=MemoryPersist())
        ca = ezbakeca.EzbakeCA()

        # first cert
        pkey = ezbakeca.private_key()
        req = ezbakeca.csr(pkey,
                           CN="App", O="Ezbake", OU="Ezbake Apps", C="US")
        cert = ca.sign_csr(req)
        nt.assert_equal(cert.get_subject(), req.get_subject())
        nt.assert_equal(cert.get_serial_number(), 2)

        # second cert
        pkey = ezbakeca.private_key()
        req = ezbakeca.csr(pkey,
                           CN="App2", O="Ezbake", OU="Ezbake Apps", C="US")
        cert = ca.sign_csr(req)
        nt.assert_equal(cert.get_subject(), req.get_subject())
        nt.assert_equal(cert.get_serial_number(), 3)


def issue_n_certs(ca, r):
    if isinstance(ca, basestring):
        ca = ezbakeca.EzbakeCA(ca)
    serials = []
    for i in r:
        cert = ca.sign_csr(ezbakeca.csr(ezbakeca.private_key(),
                    CN="App{0}".format(i),
                    O="Ezbake",
                    OU="Ezbake Apps",
                    C="US"))
        serials.append(cert.get_serial_number())
    return serials

class TestPersistence(object):
    def setUp(self):
        ezbakeca.EzbakeCA.setup(MemoryPersist())

    def test_create_ca(self):
        # Create new CA
        baseCa = ezbakeca.EzbakeCA("TestCA")
        # Sign 2 certs
        baseCa.sign_csr(ezbakeca.csr(ezbakeca.private_key(),
                           CN="App", O="Ezbake", OU="Ezbake Apps", C="US"))
        baseCa.sign_csr(ezbakeca.csr(ezbakeca.private_key(),
                           CN="App", O="Ezbake", OU="Ezbake Apps", C="US"))
        baseCa.save()

        # Load the CA from persistence
        ca = ezbakeca.EzbakeCA.get_named("TestCA")

        # Next serial should be 3
        nt.assert_equal(3, ca.serial)

    def test_multithread(self):
        ca = ezbakeca.EzbakeCA("threadingCA")
        pool = multiprocessing.pool.ThreadPool(processes=5)

        threads = []
        for i in range(5):
            threads.append(pool.apply_async(issue_n_certs, (ca, range(5))))
        vals = []
        for t in threads:
            vals.extend(t.get())
        nt.assert_equal(sorted(vals), sorted(list(set(vals))))

    # Ignoring this test. I don't think it's a valid test case because it
    # creates a bunch of threads that step on each other's feet because they
    # all create a new private key, etc. This is how the EzbakeCA is expected
    # to work, so the test is bad. leaving it for future reference
    @nt.nottest
    def test_multi_own_ca(self):
        pool = multiprocessing.pool.ThreadPool(processes=5)
        threads = []
        for i in range(5):
            threads.append(pool.apply_async(issue_n_certs, ("ownca", range(5))))
        vals = []
        for t in threads:
            vals.extend(t.get())
        nt.assert_equal(sorted(vals), sorted(list(set(vals))))

class TestFilePersistence(object):
    def setUp(self):
        if not os.path.isdir("test/tmp"):
            os.mkdir("test/tmp")
        fs = FilePersist("test/tmp")
        ezbakeca.EzbakeCA.setup(fs)

    def tearDown(self):
        try:
            pass
            shutil.rmtree("test/tmp")
        except OSError:
            pass

    def test_create_ca(self):
        # Create new CA
        baseCa = ezbakeca.EzbakeCA("TestCA")
        # Sign 2 certs
        baseCa.sign_csr(ezbakeca.csr(ezbakeca.private_key(),
                           CN="App", O="Ezbake", OU="Ezbake Apps", C="US"))
        baseCa.sign_csr(ezbakeca.csr(ezbakeca.private_key(),
                           CN="App", O="Ezbake", OU="Ezbake Apps", C="US"))
        baseCa.save()

        # Load the CA from persistence
        ca = ezbakeca.EzbakeCA.get_named("TestCA")

        # Next serial should be 3
        nt.assert_equal(3, ca.serial)

    def test_multithread(self):
        ca = ezbakeca.EzbakeCA("threadingCA")
        ca.save()
        pool = multiprocessing.pool.ThreadPool(processes=5)

        threads = []
        for i in range(5):
            threads.append(pool.apply_async(issue_n_certs, (ca, range(5))))
        vals = []
        for t in threads:
            vals.extend(t.get())
        ca.save() # save since the threads might still be writing the serial file
        nt.assert_equal(sorted(vals), sorted(list(set(vals))))