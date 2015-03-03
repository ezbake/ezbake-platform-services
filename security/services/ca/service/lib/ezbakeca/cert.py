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
Created on Fri May 23 09:46:26 2014

@author: jhastings
"""
import OpenSSL.crypto
import ezpersist.base
from ezpersist.schema import BaseSchema

import ezbakeca.ca

import logging
logger = logging.getLogger(__name__)


OWNER_COLF = "owner"
def owner_row(name, value):
    return BaseSchema.row_for(name, colf=OWNER_COLF, table=Cert.REG_TABLE, value=value)

ADMIN_COLF = "admins"
def admins_row(name, value):
    return BaseSchema.row_for(name, colf=ADMIN_COLF, table=Cert.REG_TABLE, value=value)

NAME_COLF = "name"
def name_row(name):
    return BaseSchema.row_for(name, colf=NAME_COLF, table=Cert.REG_TABLE, value=name)

LEVEL_COLF = "level"
def level_row(name, value="low"):
    return BaseSchema.row_for(name, colf=LEVEL_COLF, table=Cert.REG_TABLE, value=value)

VIS_COLF = "visibilities"
def visibility_row(name, value=""):
    return BaseSchema.row_for(name, colf=VIS_COLF, table=Cert.REG_TABLE, value=value)

STATUS_COLF = "status"
def status_row(name, value):
    return BaseSchema.row_for(name, colf=STATUS_COLF, table=Cert.REG_TABLE, value=value)

PKEY_COLF = "private_key"
def pkey_row(name=None, value=None):
    return BaseSchema.row_for(name, colf=PKEY_COLF, table=Cert.REG_TABLE, value=value)

PUBKEY_COLF = "public_key"
def pubkey_row(name=None, value=None):
    return BaseSchema.row_for(name, colf=PUBKEY_COLF, table=Cert.REG_TABLE, value=value)

CERT_COLF = "x509_cert"
def cert_row(name=None, value=None):
    return BaseSchema.row_for(name, colf=CERT_COLF, table=Cert.REG_TABLE, value=value)

def lookup_status_row(name, status):
    row = BaseSchema.row_for(status, colf=name, table=Cert.LOOKUP_TABLE, value=name)
    return row



class Cert(BaseSchema):
    REG_TABLE = 'ezsecurity_reg'
    LOOKUP_TABLE = 'ezsecurity_lookup'

    store = ezpersist.base.MemoryPersist()

    @classmethod
    def setup(cls, store):
        cls.store = store

    @classmethod
    def get_named(cls, name):
        cert = None
        try:
            rows = Cert.row_template(name)
            cells = BaseSchema.get_rows(rows, Cert.store)
            args = {'name': name}
            for k, v in cells.items():
                if not v or v == "":
                    continue
                colf = ezpersist.base.Persist.key_parts(k)[1]
                if colf == OWNER_COLF:
                    args['owner'] = v
                elif colf == STATUS_COLF:
                    args['status'] = v
                elif colf == ADMIN_COLF:
                    args['admins'] = v
                elif colf == LEVEL_COLF:
                    args['level'] = v
                elif colf == VIS_COLF:
                    args['visibility'] = v
                elif colf == PKEY_COLF:
                    args['pk'] = ezbakeca.ca.load_privatekey(v)
                elif colf == CERT_COLF:
                    args['cert'] = ezbakeca.ca.load_cert(v)
            cert = Cert(**args)
        except (KeyError, ValueError):
            # didn't exist, create new
            cert = Cert(name)
            cert.save()
        return cert


    @classmethod
    def row_template(cls, name, owner="", admins="", level="", visibility="",
             status=None, pkey="", cert="", pubkey=""):
        rows = []
        rows.append(owner_row(name, owner))
        rows.append(admins_row(name, admins))
        rows.append(name_row(name))
        rows.append(level_row(name, level))
        rows.append(visibility_row(name, visibility))
        rows.append(status_row(name, status))
        rows.append(pkey_row(name, pkey))
        rows.append(pubkey_row(name, pubkey))
        rows.append(cert_row(name, cert))
        if status:
            rows.append(lookup_status_row(name, status))
        return rows


    def __init__(self, name="Ezbake Root", owner="", admins=[], level="", visibility="", status="pending", pk=None, cert=None):
        super(Cert, self).__init__(store=Cert.store)
        if not pk:
            logger.info("Generating new private key for %s", name)
            pk = ezbakeca.ca.private_key()

        self.name = name
        self.owner = owner
        self.admins = admins
        self.level = level
        self.visibility = visibility
        self.status = status
        self.private_key = pk
        self.cert = cert

    def csr(self):
        return ezbakeca.ca.csr(self.private_key, CN=self.name)

    def pkey_string(self, encryptionKey=None):
        string = ""
        if self.private_key is not None:
            password = None
            if encryptionKey:
                password = OpenSSL.crypto.sign(self.encryptionKey, self.name, "sha1")
            string = ezbakeca.ca.pem_key(self.private_key, password=password)
        return string

    def cert_string(self):
        string = ""
        if self.cert is not None:
            string = ezbakeca.ca.pem_cert(self.cert)
        return string

    def pubkey_string(self):
        string = ""
        if self.private_key is not None:
            string = ezbakeca.ca.pem_pubkey(self.private_key)
        return string

    def rows(self, write=False):
        if write:
            r = Cert.row_template(self.name, self.owner, ",".join(self.admins),
                                  self.level, self.visibility, self.status,
                                  self.pkey_string(), self.cert_string(), self.pubkey_string())
        else:
            r = Cert.row_template(self.name)
        return r