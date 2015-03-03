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

import os
import shutil
import hashlib
import uuid
import ezRPConfig as gConfig

hash_md5 = lambda data: hashlib.md5(data).hexdigest()
remove_file_if_exists = lambda tfile: os.path.isfile(tfile) and os.remove(tfile)

def rootDirs(path):
    dirs = os.walk(path).next()[1]
    return list() if len(dirs) == 0 else dirs

# def hashMD5File(tfile):
#     with open(tfile, 'rb') as f:
#         data = f.read()
#         return str(hash_md5(data))
#     return None

def deletePath(path):
    shutil.rmtree(path)

def ensurePathExists(path):
    if not os.path.exists(path):
        os.makedirs(path)
        
def getTempfile():
    path = os.path.join(gConfig.workingDirectory,'tmp')
    ensurePathExists(path)
    return os.path.join(path, str(uuid.uuid4()))

def ifPathExists(path):
    return os.path.exists(path)

def copyPath(src, des):
    '''
    Copy src directory into des directory
    Note: The destination directory must not already exist
    '''
    shutil.copytree(src, des)

