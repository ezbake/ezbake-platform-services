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
Created on Mon Mar 24 08:00:29 2014

@author: jhastings
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Mar 18 07:57:08 2014

@author: jhastings
"""
import datetime
import threading
import OpenSSL.crypto
from Crypto.PublicKey import RSA
import ezpersist.base
from ezpersist.schema import BaseSchema
import ezbakeca.cert

import logging
logger = logging.getLogger(__name__)

CA_VALIDITY_YEARS = 30
CERTIFICATE_VALIDITY_YEARS = 30
CERT_DIGEST = "sha1"


PKEY_COLF = "private_key"
def pkey_row(id, value):
    return BaseSchema.row_for(id, colf=PKEY_COLF, table=EzbakeCA.CA_TABLE, value=value)


CERT_COLF = "certificate"
def cert_row(id, value):
    return BaseSchema.row_for(id, colf=CERT_COLF, table=EzbakeCA.CA_TABLE, value=value)


SERIAL_COLF = "serial"
def serial_row(id, value):
    return BaseSchema.row_for(id, colf=SERIAL_COLF, table=EzbakeCA.CA_TABLE, value=value)


class EzbakeCA(BaseSchema):
    CA_TABLE = "ezbake_ca"
    store = ezpersist.base.MemoryPersist()
    serial_lock = threading.Semaphore()

    @classmethod
    def setup(cls, store):
        cls.store = store

    @classmethod
    def get_named(cls, name):
        rows = cls.row_template(name, True, True, True)
        cells = BaseSchema.get_rows(rows, cls.store)
        for k, v in cells.items():
            if not v or v == "":
                continue
            colf = ezpersist.base.Persist.key_parts(k)[1]
            if colf == PKEY_COLF:
                pk = load_privatekey(cells[k])
            elif colf == CERT_COLF:
                cert = load_cert(cells[k])
            elif colf == SERIAL_COLF:
                serial = long(cells[k])
        ca = EzbakeCA(name, pk=pk, certificate=cert, serial=serial)
        return ca

    @classmethod
    def row_template(cls, name, private_key=None, certificate=None, serial=None):
        rows = []
        if private_key:
            rows.append(pkey_row(name, private_key))
        if certificate:
            rows.append(cert_row(name, certificate))
            # write the cert to the registrations table
            rows.append(ezbakeca.cert.cert_row(name, certificate))
        if serial:
            rows.append(serial_row(name, serial))
        return rows

    def __init__(self, name="Ezbake Root", environment=None,
                 pk=None, certificate=None, serial=None):
        super(EzbakeCA, self).__init__(store=self.store)
        self.name = name
        if pk is not None:
            self.private_key = pk
            self.ca_cert = certificate
            self.serial = serial
        else:
            subject = {}
            subject["CN"] = name
            if environment:
                subject["OU"] = environment
            self.private_key = private_key()
            logger.info("Generating new CA certificate using subject {}"
                .format(subject))
            self.ca_cert = create_ca_certificate(
                csr(self.private_key, **subject), self.private_key)
            self.serial = 1
            # delete anything from disk (specifically the serial number)
            try:
                self.destroy()
            except KeyError:
                pass

    def save(self):
        self.serial_lock.acquire()
        super(EzbakeCA, self).save()
        self.serial_lock.release()

    def pkey_string(self):
        string = ""
        if self.private_key is not None:
            string = pem_key(self.private_key)
        return string

    def cert_string(self):
        string = ""
        if self.ca_cert is not None:
            string = pem_cert(self.ca_cert)
        return string

    def sign_csr(self, req):
        serial = self._get_serial()
        logger.info("Signing CSR: CA Certificate:{}, Certificate subject: {}, Serial number: {}".format(
            self.ca_cert.get_subject(), req.get_subject(), serial))
        return create_certificate(req, (self.ca_cert, self.private_key), serial)

    def rows(self, write=False):
        if write:
            r = self.row_template(self.name, self.pkey_string(),
                                  self.cert_string(), self.serial)
        else:
            r = self.row_template(self.name)
        return r

    def _get_serial(self):
        self.serial_lock.acquire()
        logger.debug("Incrementing serial number")
        serial = self.serial = self._read_serial_row() + 1
        # want to save the update in a thread (so this doesn't block), but need
        # synchronization so the server doesn't stop without saving
        #    threading.Thread(target=self._save_serial_update).start()
        self._save_serial_update()
        return serial

    def _save_serial_update(self):
        try:
            self.put_rows(self.row_template(self.name, serial=self.serial), self.store)
            #self.store.write(self.name, "serial", "", None, self.serial)
        finally:
            self.serial_lock.release()

    def _read_serial_row(self):
        serial = None
        try:
            srow = self.get_rows(
                self.row_template(self.name, serial=True), self.store)
            if len(srow) > 1:
                raise KeyError("Too many rows returned")
            key, serial = srow.popitem() # there should be only one
            #serial = self.store.read(self.name, "serial", "", None)
            serial = long(serial)
        except KeyError:
            serial = self.serial
        return serial



def private_key(bits=2048, key_type=OpenSSL.crypto.TYPE_RSA):
    """Just generate a pyopenssl PKey object"""
    #key = OpenSSL.crypto.PKey()
    #key.generate_key(key_type, bits)
    key = RSA.generate(bits)
    return key


def load_privatekey(pem, key_type=OpenSSL.crypto.FILETYPE_PEM):
    return RSA.importKey(pem)
    #return OpenSSL.crypto.load_privatekey(key_type, pem)


def load_cert(cert, cert_type=OpenSSL.crypto.FILETYPE_PEM):
    return OpenSSL.crypto.load_certificate(cert_type, cert)

def load_csr(csr, cert_type=OpenSSL.crypto.FILETYPE_PEM):
     return OpenSSL.crypto.load_certificate_request(cert_type, csr)

def pem_key(pkey, password=None, cipher="des3"):
    """Take a PKey object and dump it to a buffer"""
    pem=""
    if password:
        #pem = OpenSSL.crypto.dump_privatekey(OpenSSL.crypto.FILETYPE_PEM, pkey, cipher, password)
        pem = pkey.exportKey(passphrase=password)
    else:
        #pem = OpenSSL.crypto.dump_privatekey(OpenSSL.crypto.FILETYPE_PEM, pkey)
        pem = pkey.exportKey()
    return pem

def pem_pubkey(pkey):
    key = pkey.publickey()
    return key.exportKey()

def pem_cert(cert):
    """Take an X509 Certificate and dump it to a buffer"""
    return OpenSSL.crypto.dump_certificate(OpenSSL.crypto.FILETYPE_PEM, cert)


def pem_csr(cert):
    return OpenSSL.crypto.dump_certificate_request(OpenSSL.crypto.FILETYPE_PEM, cert)

def openssl_key(key):
    return OpenSSL.crypto.load_privatekey(OpenSSL.crypto.FILETYPE_PEM, pem_key(key))

def csr(pkey, **name):
    """
    Create a csr give a public key and parts of the subject

    Arguments:
        pkey   - The private key used to sign the csr
        **name - The subject name to use for the csr, argument names:
                CN - common name
                OU - organizational unit
                O  - organization
                C  - two digit country code
    """
    # convert the pkey
    pkey = openssl_key(pkey)
    req = OpenSSL.crypto.X509Req()
    subject = req.get_subject()

    for (key, value) in name.items():
        setattr(subject, key, value)

    req.set_pubkey(pkey)
    req.sign(pkey, CERT_DIGEST)

    return req


def _create_certificate(req, issuerCert, serial, years_valid, digest="sha1"):
    """
    Create an X509 certificate ca given a private key and subject
    """
    cert = OpenSSL.crypto.X509()
    now = datetime.datetime.utcnow()

    cert.set_version(2) # 0x02 for X509 version 3
    cert.set_serial_number(serial)
    cert.set_issuer(issuerCert.get_subject())
    cert.set_subject(req.get_subject())
    cert.set_pubkey(req.get_pubkey())

    # Set up CA validity timeframe
    cert.gmtime_adj_notBefore(0)
    cert.set_notAfter(encode_time(now.replace(year=now.year+years_valid)))

    return cert


def create_ca_certificate(req, key):
    cert = _create_certificate(req, req, 1, CA_VALIDITY_YEARS)
    # Add the CA Extensions
    cert.add_extensions([
        OpenSSL.crypto.X509Extension("basicConstraints", True, "CA:TRUE, pathlen:0"),
        OpenSSL.crypto.X509Extension("keyUsage", True, "keyCertSign,cRLSign"),
        OpenSSL.crypto.X509Extension("subjectKeyIdentifier", False, "hash", subject=cert)
    ])
    cert.sign(openssl_key(key), CERT_DIGEST)
    return cert


def create_certificate(req, (issuerCert, issuerKey), serial):
    cert = _create_certificate(req, issuerCert, serial,
                               CERTIFICATE_VALIDITY_YEARS)
    cert.sign(openssl_key(issuerKey), CERT_DIGEST)
    return cert


def encode_time(time):
    """Encode a datetime object with the ASN1 GENERALIZEDTIME format"""
    return time.strftime('%Y%m%d%H%M%SZ')

if __name__ == '__main__':
    cpkey= private_key()
    creq = csr(cpkey, CN="EzCA", O="Ezbake", OU="Ezbake Core", C="US")
    ca = create_ca_certificate(creq, cpkey)

    akey = private_key()
    areq = csr(akey, CN="EzApp", O="Ezbake", OU="Ezbake Apps", C="US")
    app = create_certificate(areq, (ca, cpkey), 2)

    print(OpenSSL.crypto.dump_certificate(OpenSSL.crypto.FILETYPE_TEXT, ca))
    print(OpenSSL.crypto.dump_certificate(OpenSSL.crypto.FILETYPE_TEXT, app))
