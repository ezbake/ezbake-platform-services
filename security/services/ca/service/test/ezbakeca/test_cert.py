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

import nose.tools as nt
import OpenSSL.crypto
from ezpersist.base import MemoryPersist
import ezbakeca.ca
from ezbakeca.cert import Cert

class TestCert(object):
    def setUp(self):
        Cert.setup(MemoryPersist())

    def cert_args(self):
        name = "TestInit"
        owner = "TestOwner"
        admins = ["admin1", "admin2"]
        level = "med"
        visibilities = ["a", "b", "c"]
        status="active"
        pk=ezbakeca.ca.private_key()
        x509 = ezbakeca.ca.create_ca_certificate(
            ezbakeca.ca.csr(pk, CN="TestInit"), pk)
        return [name, owner, admins, level, visibilities, status, pk, x509]

    def test_init(self):
        name, owner, admins, level, visibilities, status, pk, x509 = self.cert_args()

        cert = Cert(name, owner, admins, level, visibilities, status, pk, x509)
        nt.assert_equal(name, cert.name)
        nt.assert_equal(owner, cert.owner)
        nt.assert_equal(admins, cert.admins)
        nt.assert_equal(level, cert.level)
        nt.assert_equal(visibilities, cert.visibility)
        nt.assert_equal(status, cert.status)
        nt.assert_equal(pk, cert.private_key)
        nt.assert_equal(x509, cert.cert)

    def test_save_get_simple(self):
        name = "TestCert"
        Cert("TestCert").save()
        c = Cert.get_named(name)
        nt.assert_equal(name, c.name)

    def test_save_get_full(self):
        name, owner, admins, level, visibilities, status, pk, x509 = self.cert_args()
        cert = Cert(name, owner, admins, level, visibilities, status, pk, x509)
        nt.assert_equal(name, cert.name)
        nt.assert_equal(owner, cert.owner)
        nt.assert_equal(admins, cert.admins)
        nt.assert_equal(level, cert.level)
        nt.assert_equal(visibilities, cert.visibility)
        nt.assert_equal(status, cert.status)
        nt.assert_equal(pk, cert.private_key)
        nt.assert_equal(x509, cert.cert)
        cert.save()

        getter = Cert.get_named(name)
        nt.assert_equal(name, getter.name)
        nt.assert_equal(owner, getter.owner)
        nt.assert_equal(",".join(admins), getter.admins)
        nt.assert_equal(level, getter.level)
        nt.assert_equal(visibilities, getter.visibility)
        nt.assert_equal(status, getter.status)
        nt.assert_equal(ezbakeca.ca.pem_key(pk), getter.pkey_string())
        nt.assert_equal(ezbakeca.ca.pem_cert(x509), getter.cert_string())

    def test_generates_pk(self):
        cert = Cert("Test")
        nt.assert_is_not_none(cert.private_key)

    def test_csr(self):
        cert = Cert("Test2")
        nt.assert_is_instance(cert.csr(), OpenSSL.crypto.X509Req)