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
Created on Mon May 19 15:39:40 2014

@author: jhastings
"""
import os
import os.path
import errno
import shutil
import ezpersist.base

class FilePersist(ezpersist.base.Persist):
    def __init__(self, directory, subdirs=""):
        self.directory = directory
        try:
            os.mkdir(directory, 0o700)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise e

        # table names should be a list, make it one if it is not
        if type(subdirs) is not list:
            subdirs = [subdirs]
        self.subdirs = subdirs
        for sd in subdirs:
            if sd is not "":
                try:
                    os.mkdir(os.path.join(directory, sd), 0700)
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise e

    def write(self, row, colf="", colq="", table=None, value=None):
        if table is None:
            table = self.subdirs[0]

        ld = os.path.join(self.directory, table, row, colf, colq)
        try:
            os.makedirs(ld)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise e
        #print "write: {} = {}".format(ld, str(value))
        of = os.path.join(ld, 'data')
        with os.fdopen(os.open(of, os.O_WRONLY|os.O_CREAT, 0o600), 'w') as ofs:
#        with open(os.path.join(ld, 'data'), 'w') as of:
            ofs.write(str(value))

    def read(self, row, colf="", colq="", table=None):
        if table is None:
            table = self.subdirs[0]
        ld = os.path.join(self.directory, table, row, colf, colq, 'data')
        value = None
        try:
            with open(ld, 'r') as f:
                value = f.read()
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise KeyError(e)
            raise e
        return value

    def delete(self, row, table=None):
        if table is None:
            table = self.subdirs[0]
        ld = os.path.join(self.directory, table, row)
        try:
            shutil.rmtree(ld)
        except OSError as e:
            if e.errno == errno.ENOENT:
                # file doesn't exist, that's what we want anyway
                pass

    def all(self, table=None):
        if table is None:
            table = self.subdirs[0]
        vals = []
        os.chdir(os.path.join(self.directory, table))
        key = None
        for r,ds,fs in os.walk(os.getcwd()):
            row = os.path.basename(r)
            if key is None and row != table:
                key = self.key(row)
            elif key is not None:
                x = key.split(ezpersist.base.KEY_SEP)
                x = [i for i in x if i != str(None)]
                x.append(row)

                key = self.key(*x)
            rgrp = {}
            for f in fs:
                with open(os.path.join(r,f)) as st:
                    rgrp[key] = st.read()
            if rgrp:
                vals.append(rgrp)
        os.chdir('../..')
        return vals