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
import tarfile
import collections
import ezRPUtils as utils

# Named tuple
Archive = collections.namedtuple('Archive', 'path_to_extract archive_file')

is_archive = lambda file: tarfile.is_tarfile(file)

class NotArchive(Exception):

    '''
    Raised when the file to be extracted is not a
    tar archive file: gzip or bzip2

    '''
    pass


def is_data_archive(data):
    '''
    Check if the data blob is in archive format
    '''
    status = False
    try:
        tfile = utils.getTempfile()
        f = open(tfile,'wb')
        f.write(data)
        f.close()       
        status = is_archive(tfile)
    except IOError:
        "Should not happen"
        status = False
    finally:
        utils.remove_file_if_exists(tfile)
        return status

def openArchive(filelist):
    '''
    Generator; give an list of path and archive_file as tuple,
    will yield path and the opened archive file. If the file
    is not an archive raises NotArchive exception
    '''
    try:
        for path, name in filelist:
            if is_archive(name):
                yield Archive(path, tarfile.open(name,'r:*'))
            else:
                raise NotArchive("Not an Archive %s" % name)
    except IOError as e:
        raise IOError("File not found %s" % str(e))


def extractArchive(archivelist):
    '''
    Extract the archive file into the path supplied in the Archive tuple
    '''
    try:
        for path_to_extract, archive_file in archivelist:
            archive_file.extractall(path_to_extract)
            archive_file.close()
    except Exception as e:
        raise NotArchive("%s" % str(e))


if __name__ == '__main__':
    filelist = [
        Archive(path_to_extract="./temp", archive_file="test.tar.gz"), Archive(path_to_extract="./a", archive_file="test.txt")]
    files_to_extract = openArchive(filelist)
    try:
        extractArchive(files_to_extract)
    except Exception as e:
        print "Exception While Extracting: {}".format(str(e))
