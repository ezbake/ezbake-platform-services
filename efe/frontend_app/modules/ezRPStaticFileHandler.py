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
import subprocess
import random
from ezRPStaticFileStore import EzRPStaticStore
import ezRPConfig as gConfig

from ezbake.reverseproxy.thriftapi.ttypes import (ContentServiceType, StaticContentException)
from ezbake.configuration.helpers import AccumuloConfiguration

import ezRPUtils as utils
import ezRPArchive as archive


class StaticFileHandler(object):

    '''
    Class to manage Static File serving using Static File Store
    '''

    def __init__(self, logger):
        self._logger = logger
        self._staticPathInLocation = "ezbappstatic"
        self._scHash = dict()     # stores {urlPrefix: hash} of static contents directory
        self._sfsHash = dict()    # stores {urlPrefix: hash} of static contents in Static File Store
        self._curDir = None
        self._buildDir = None
        accConfig = AccumuloConfiguration(gConfig.ezproperties)
        self._sFileStore = EzRPStaticStore(host=accConfig.getProxyHost(),
                                         port=accConfig.getProxyPort(),
                                         user=accConfig.getUsername(),
                                         password=accConfig.getPassword(),
                                         chunk_size=int(gConfig.static_content_chunk_size),
                                         logger=self._logger)


    def _removeFromStaticFileStore(self, urlPrefix):
        hash_value =  self._sfsHash.get(urlPrefix)
        if hash_value:
            self._logger.info('Removing Static File fromStaticFileStore for {0}  with hash {1}'.format(urlPrefix, hash_value))
            self._sFileStore.deleteFile(urlPrefix)
        else:
            raise Exception('Content not found')

    def _copyFromStaticFileStore(self, urlPrefix, contentServiceType):
        '''
        Gets the static file from Static File Store if available and put it in
        static contents directory
        '''
        status = False
        hash_value =  self._sfsHash.get(urlPrefix)
        if hash_value:
            bdata = self._sFileStore.getFile(urlPrefix)
            if bdata is not None and archive.is_data_archive(bdata):
                self._logger.debug('Getting Static File from Static File Store for {0} with hash {1}'.format(urlPrefix, hash_value))
                status = self._extractStaticFiles(bdata, hash_value, urlPrefix, contentServiceType)
            else:
                # try one more time
                self._sFileStore.reConnection()
                bdata = self._sFileStore.getFile(urlPrefix)
                if bdata is not None and archive.is_data_archive(bdata):
                    self._logger.debug('Getting Static File from Static File Store for {0} with hash {1} after retrying'.format(urlPrefix, hash_value))
                    status = self._extractStaticFiles(bdata, hash_value, urlPrefix, contentServiceType)
                else:
                    # Delete Static File Store Content; files may be corrupted while uploading to Static File Store
                    self._sFileStore.deleteFile(urlPrefix)
        return status

    def _extractStaticFiles(self, bdata, hash_value, urlPrefix, contentServiceType):
        '''
        Extract static files into ezappStaticdir
        '''
        try:
            tfile = utils.getTempfile()
            with open(tfile, "w+b") as f:
                f.write(bdata)
            url_hash = str(urlPrefix + "_" + hash_value)
            if contentServiceType == ContentServiceType.HYBRID:
                filelist = [archive.Archive(path_to_extract=os.path.join(self._buildDir, url_hash, urlPrefix, self._staticPathInLocation), archive_file=tfile)]
            else:
                filelist = [archive.Archive(path_to_extract=os.path.join(self._buildDir, url_hash, urlPrefix), archive_file=tfile)]
            filesToExtract = archive.openArchive(filelist)
            self._logger.debug('Extracting archive_file "{0}" into path "{1}"'.format(filelist[0].archive_file, filelist[0].path_to_extract))
            archive.extractArchive(filesToExtract)
            utils.remove_file_if_exists(tfile)
            return True
        except Exception as e:
            utils.remove_file_if_exists(tfile)
            self._logger.exception('Exception while Extracting: urlPrefix {0}}'.format(urlPrefix))
            return False

    def _getCurrentStaticDirEntry(self):
        '''
        Generator gets the urlPrefix and hash of all the static contents the
        Nginx is currently serving

        The directory structure is in this format:
        /opt/ezfrontend/ezbappstatic/ <current_static_path> /   /* FOR HYBRID */
                                                            |-- urlPrefix_hash/app1/ezbappstatic/-- Static Contents
                                                            |   /* FOR STATIC_ONLY */
                                                            |-- urlPrefix_hash/app1/...static Contents

        '''
        dirlist = utils.rootDirs(self._curDir) # Get all the directories as seen from current_static_path
        for url_hash in dirlist:
            urlPrefix, hash_value = url_hash.split('_')
            self._logger.debug("_getCurrentStaticDirEntry urlPrefix={0} hash={1}".format(urlPrefix, hash_value))
            yield (urlPrefix, hash_value)
        else:
            yield (None, None)

    def _createBuildDir(self):
        '''
        Sets a build path based on current static path
        '''
        self._curDir = os.readlink(gConfig.static_contents)
        self._buildDir = random.choice([x for x in gConfig.static_contents_dirs if x != self._curDir])
        utils.ensurePathExists(self._buildDir)
        self._logger.info("createBuildDir current dir {0} builddir {1}".format(self._curDir, self._buildDir))

    def _copyFromLocalDir(self, urlPrefix):
        '''
        Copy the static contents of urlPrefix of _previous_static_path into
        _current_static_path
        '''
        try:
            hash_value = self._scHash.get(urlPrefix)
            if not hash_value:
                raise ValueError('Could not get hash for urlPrefix "{0}"'.format(urlPrefix))
            url_hash = str(urlPrefix + "_" + hash_value)
            previous_path = os.path.join(self._curDir, url_hash)
            current_path = os.path.join(self._buildDir, url_hash)
            utils.copyPath(str(previous_path), str(current_path))
            return True
        except Exception as e:
            self._logger.error('Error while copying static contents from "{0}" to "{1}" : {2}'.format(previous_path, current_path, str(e)))
            return False

    def staticLocationForNginx(self, urlPrefix):
        '''
        Returns the path of static content to urlPrefix
        Used while generating Nginx Configuration

        '''
        hash_str = self._sfsHash.get(urlPrefix)
        if hash_str:
            return os.path.join(self._buildDir, str(urlPrefix + "_" + hash_str))
        else:
            self._logger.error('Error while getting the root Path of StaticContent for Nginx; urlPrefix "{0}": The hash content is None'.format(urlPrefix))
            return None

    def _putInStaticFileStore(self, urlPrefix, data):
        hash_str = self._sfsHash.get(urlPrefix)
        hash_data = utils.hash_md5(data)
        if hash_str == hash_data:
          raise RuntimeError('Static content already exists for {0}'.format(urlPrefix))
        self._sFileStore.putFile(urlPrefix, hash_data, data)
        self._sfsHash[urlPrefix]=hash_data

    def _addInStaticFileStore(self, urlPrefix, data):
        '''
        If file is in archive format, and not already in Static File Store
        Update Static File Store and trigger ZookeeperWatch Node.
        '''
        static_content_max_size = int(gConfig.static_content_max_size)
        if 0 < len(data) > static_content_max_size:
            raise ValueError('Data for {0} is empty or exceeds the limit {1}MB'.format(urlPrefix, static_content_max_size/1048576))
        if not archive.is_data_archive(data):
            raise StaticContentException(message='Not an Archive', urls=[urlPrefix])
        self._putInStaticFileStore(urlPrefix, data)

    def addStaticContent(self, contents):
        '''
        Saves contents received through Thrift in Static File Store
        '''
        try:
           for staticContent in contents:
               self._logger.info('addStaticContent urlPrefix "{0}"'.format(staticContent.userFacingUrlPrefix))
               self._addInStaticFileStore(staticContent.userFacingUrlPrefix, staticContent.content)
           return True
        except Exception as e:
            self._logger.error('Error while adding static content for urlPrefix "{0}"; Reason: {1}'.format(staticContent.userFacingUrlPrefix, str(e)))
            raise StaticContentException(message=str(e), urls=[staticContent.userFacingUrlPrefix])

    def removeStaticContent(self, contents):
        '''
        Deletes static contents of urlPrefix from Static File Store
        '''
        try:
            for staticContent in contents:
                self._logger.info('removeStaticContent urlPrefix "{0}"'.format(staticContent.userFacingUrlPrefix))
                self._removeFromStaticFileStore(staticContent.userFacingUrlPrefix)
                del self._sfsHash[staticContent.userFacingUrlPrefix]
            return True
        except Exception as e:
           emsg = 'Error while removing static content for urlPrefix "{0}"; Reason: {1}'.format(staticContent.userFacingUrlPrefix, str(e))
           self._logger.error('{0}'.format(emsg))
           raise StaticContentException(message=emsg, url=[staticContent.userFacingUrlPrefix])

    def isStaticContentPresent(self, urlPrefix):
        '''
        Gets the hash value for urlPrefix from Static File Store
        '''
        if self._sfsHash.get(urlPrefix):
           return True
        else:
           return False

    def updateStaticDirLink(self):
        '''
        Deletes current static path being served by Nginx.
        This is called after generating nginx configuration file and just before reconfiguration.
        '''
        self._logger.info('deleted StaticContents Dir "{0}" created link for Dir "{1}"'.format(self._curDir, self._buildDir))
        if utils.ifPathExists(self._curDir):
            utils.deletePath(self._curDir)
        subprocess.call(['ln', '-sTf', self._buildDir, gConfig.static_contents])


    def updateStaticContentsDict(self):
        '''
        Update the:
        Dictionary which holds the current static contents in Static File Store
        and the
        Dictionary which holds the current static contents in ezappStaticdir
        A call to this signals that Nginx is configuring, switch the static path for configuring
        '''
        self._createBuildDir()
        if not self._sfsHash:
             # Update dict while startup
            self._sfsHash = {urlPrefix: hash_value for urlPrefix, hash_value in self._sFileStore.getAttributes()}
        self._scHash = {urlPrefix: hash_value for urlPrefix, hash_value in self._getCurrentStaticDirEntry()}

    def updateStaticDir(self, urlPrefix, contentServiceType):
        '''
        This will be called form ezRPConfigNginx when static content is needed for urlPrefix
        '''
        try:
            self._logger.info('Updating Nginx Static dir for urlPrefix "{0}"'.format(urlPrefix))

            # static contents hash of local and Static File Store
            scHash = self._scHash.get(urlPrefix)
            sfsHash = self._sfsHash.get(urlPrefix)

            # **** Cases ****
            # No content available for this urlPrefix in Static File Store
            # Then don't update the contents
            if not sfsHash:
                self._logger.debug('No Static File Store content for urlPrefix "{0}" found, static contents not updated'.format(urlPrefix))
                return False

            # Both Static File Store and Local Static contents are available
            if sfsHash != scHash:
                self._logger.debug('Found latest content for urlPrefix "{0}" in Static File Store. Updating'.format(urlPrefix))
                return self._copyFromStaticFileStore(urlPrefix, contentServiceType)
            else:
                # Just copy it from working to build directory
                self._logger.debug('Copying the static contents of urlPrefix "{0}" from the local ezbappstatic dir'.format(urlPrefix))
                return self._copyFromLocalDir(urlPrefix)
        except Exception as e:
          '''
          Any Error, we should not update the link in the configuration
          '''
          self._logger.error('Error occured while updating static dir: {0}'.format(str(e)))
          return False

