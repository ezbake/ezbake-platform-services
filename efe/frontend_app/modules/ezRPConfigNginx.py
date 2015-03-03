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

import os
import stat
import socket
import signal
import subprocess
import shutil
import hashlib
import OpenSSL
import ezRPKazoo
import ezRPNginx
import ezRPRegistration
import ezRPConfig as gConfig

from random import choice
from operator import attrgetter

from kazoo.exceptions import NoNodeError

from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from ezbake.reverseproxy.thriftapi.ttypes import (ContentServiceType, 
    AuthorizationOperation, UpstreamServerRegistration)
from ezRPStaticFileHandler import StaticFileHandler


"""
Module to create Nginx configuration file
"""

class Configurer(object):
    class Sanity(object):
        def __init__(self,UpstreamNumber,UpstreamPath):
            self.UpstreamNumber = UpstreamNumber
            self.UpstreamPath = UpstreamPath

    class Location(object):
        def __init__(self, location, proxy_pass, server_name, upstream_path, upstream_host, upstream_port,
                     upstream_timeout, upstream_timeout_tries, upstream_upload_file_size, sticky,
                     disable_chunked_encoding, authOperations, validate_upstream_connection, static_content_type):

            if not location.startswith('/'):
                location = '/' + location
            if not location.endswith('/'):
                location = location + '/'
            while location.startswith('//'):
                location = location[1:]
            while location.endswith('//'):
                location = location[0:-1]
            self.location = location

            if not upstream_path.startswith('/'):
                upstream_path = '/' + upstream_path
            if not upstream_path.endswith('/'):
                upstream_path = upstream_path + '/'
            while upstream_path.startswith('//'):
                upstream_path = upstream_path[1:]
            while upstream_path.endswith('//'):
                upstream_path = upstream_path[0:-1]

            self.proxy_pass = 'https://' + proxy_pass + upstream_path
            self.upstream_host = upstream_host
            self.upstream_context_root = upstream_path
            self.upstream_port = upstream_port
            self.upstream_timeout = upstream_timeout
            self.upstream_timeout_tries = upstream_timeout_tries
            self.upstream_upload_file_size = upstream_upload_file_size
            self.sticky = sticky
            self.disable_chunked_encoding = disable_chunked_encoding
            self.authOperations = authOperations
            self.validate_upstream_connection = validate_upstream_connection
            self.static_content_type = static_content_type

        def __repr__(self):
            rtn = 'Location<'
            rtn += 'location=%s,' % self.location
            rtn += 'proxy_pass=%s,' % self.proxy_pass
            rtn += 'upstream_host=%s,' % self.upstream_host
            rtn += 'upstream_context_root=%s, ' % self.upstream_context_root
            rtn += 'upstream_port=%s, ' % str(self.upstream_port)
            rtn += 'upstream_timeout=%s, ' % str(self.upstream_timeout)
            rtn += 'upstream_timeout_tries=%s, ' % str(self.upstream_timeout_tries)
            rtn += 'upstream_upload_file_size=%s, ' % str(self.upstream_upload_file_size)
            rtn += 'sticky=%s, ' % str(self.sticky)
            rtn += 'static_content_type=%s, ' % str(self.static_content_type)
            rtn += 'disable_chunked_encoding=%s, ' % str(self.disable_chunked_encoding)
            rtn += 'authOperations=%s, ' % str(self.authOperations)
            rtn += 'validate_upstream_connection=%s, ' % str(self.validate_upstream_connection)
            rtn += '>'
            return rtn

    def __init__(self, kz, ac, logger, staticFileHandler):
        self.kz = kz
        self.ac = ac
        self._sanityCheck = {} #UserFacingUrlPrefix: (upstream#,upstreamPath)
        self._upstreams = {}
        self._servers = {}
        self._serversWithSpecializedCerts = set()
        self._redirects = {}
        self._logger = logger
        self._newSslDir = None
        self._sfh = staticFileHandler
        self._staticPathInLocation ="ezbappstatic"

    def _deserializeUpstreamServerRegistration(self, serialized):
        transport = TTransport.TMemoryBuffer(serialized)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        registration = UpstreamServerRegistration()
        registration.read(protocol)
        self._logger.info("zookeeper contains the registration: %s" % (registration))
        return registration

    def _addUpstreamAndServer(self,upstream_number,registration):
        upstream_group_name = 'server'+str(upstream_number)
        upstream_name = registration.UpstreamHostAndPort
        upstream_timeout = registration.timeout
        upstream_timeout_tries = registration.timeoutTries
        upstream_upload_file_size = registration.uploadFileSize
        static_content_type = registration.contentServiceType
        server_name = registration.UserFacingUrlPrefix.split('/',1)[0]
        loc = self.Location(registration.UserFacingUrlPrefix.split('/',1)[1], upstream_group_name, server_name,
                            registration.UpstreamPath, upstream_name.split(':',1)[0], upstream_name.split(':',1)[1],
                            upstream_timeout, upstream_timeout_tries, upstream_upload_file_size, registration.sticky,
                            registration.disableChunkedTransferEncoding, registration.authOperations,
                            registration.validateUpstreamConnection, static_content_type)
        name_to_resolve, port_to_use = upstream_name.split(':',1)
        try:
            #try to resolve the upstream name
            socket.gethostbyname(name_to_resolve)

            if upstream_group_name not in self._upstreams:
                self._upstreams[upstream_group_name] = {'location':loc.location,
                                                        'upstreams':[],
                                                        'sticky':bool(loc.sticky or False),
                                                        'timeout':int(loc.upstream_timeout or 0),
                                                        'timeout_tries':int(loc.upstream_timeout_tries or 0)
                                                       }
            self._upstreams[upstream_group_name]['upstreams'].append(upstream_name)

        except Exception as e:
            self._logger.error("Exception (%s) resolving upstream %s. Dropping that upstream path [%s:%s]. Location %s will not be configured unless it has other (valid) upstreams" % (str(e), name_to_resolve, loc.upstream_host, loc.upstream_port, loc.location))
            #remove from local upstream cache
            if upstream_group_name in self._upstreams:
                del self._upstreams[upstream_group_name]
            #remove from zookeeper
            try:
                self.kz.delete(ezRPKazoo.KZCONFLOC + '/' + ezRPRegistration.getNodeName(registration))
                self.kz.set(ezRPKazoo.KZWATCHLOC, str(gConfig.current_milli_time()))
            except NoNodeError:
                #node didn't exist before
                pass
            except Exception as e:
                self._logger.error('Exception in removing unresolved registration: %s' % str(e))
                raise

        self._logger.info('Configuring Location %s' % str(loc))

        if server_name not in self._servers:
            self._servers[server_name] = {}
        self._servers[server_name][loc.location] = loc

        self._serversWithSpecializedCerts.discard(server_name)
        if self.kz.exists(ezRPKazoo.KZSSLCONFLOC + '/' + server_name):
            if os.path.isfile(os.path.join(self._newSslDir, server_name + '.crt')) and \
               os.path.isfile(os.path.join(self._newSslDir, server_name + '.key')):
                self._serversWithSpecializedCerts.add(server_name)
            else:
                self._logger.error('Certs for configured %s server are not present in %s. Registration will use defaults' % (server_name, gConfig.ssl_server_certs))

        if server_name not in self._redirects:
            self._redirects[server_name] = {}
        if loc.location not in self._redirects[server_name]:
            self._redirects[server_name][loc.location] = []
        self._redirects[server_name][loc.location].append((loc.upstream_host,loc.upstream_port))


    def _addRpEntry(self,registration):
        if registration.UserFacingUrlPrefix not in self._sanityCheck:
            current = self.Sanity(len(self._sanityCheck), registration.UpstreamPath)
            self._sanityCheck[registration.UserFacingUrlPrefix] = current
            self._addUpstreamAndServer(current.UpstreamNumber,registration)
        else:
            current = self._sanityCheck[registration.UserFacingUrlPrefix]
            if current.UpstreamPath == registration.UpstreamPath:
                self._addUpstreamAndServer(current.UpstreamNumber,registration)
            else:
                log("Error registering %s. It's UpstreamPath does not match %s" % (str(registration),self.UpstreamPath))


    def addProxyLocation(self, location_details, server):
        '''
        Adds Nginx location block for Proxy server
        '''
        head=['\tlocation {location} {{\n'.format(location=location_details.location)]

        proxy_header_variables = ['eznginx_ops {auth_op};\n'.format(auth_op=' '.join([AuthorizationOperation._VALUES_TO_NAMES[x] for x in location_details.authOperations])),
                                  'proxy_http_version 1.1;\n',
                                  'proxy_set_header X-Original-Request $request_uri;\n',
                                  'proxy_set_header X-Original-Uri $uri;\n',
                                  'proxy_set_header X-Upstream-Context-Root {0:s};\n'.format(location_details.upstream_context_root),
                                  # We are using the nginx map directive
                                  # in nginx.conf file to send connection header field to proxy server if
                                  # there is a presence of the upgrade field in the client. No need of a flag
                                  # for WebSocket connection
                                  'proxy_set_header Upgrade $http_upgrade;\n',
                                  'proxy_set_header Connection $connection_upgrade;\n',
                                  'proxy_set_header X-client-cert-s-dn $ssl_client_s_dn;\n',
                                  # 'proxy_set_header X-NginX-Proxy true;\n',#is identifying as Nginx necessary?
                                  # Host should be change to be the original host once we no longer use OpenShift 
                                  'proxy_set_header Host xxx;\n',
                                  #'{0:s};\n'.format(location_details.upstream_host), 
                                  'proxy_set_header X-Original-Host $http_host;\n',
                                  'proxy_pass {0:s};\n'.format(location_details.proxy_pass),
                                  'proxy_redirect default;\n'
                                  ]

        if location_details.location == '/ezfrontend/':
            proxy_header_variables.append('access_log {log_dir}/http_access.log admin;\n'.format(log_dir=gConfig.logDirectory))

        upload_file_size = int(location_details.upstream_upload_file_size or 0)
        if upload_file_size:
            proxy_header_variables.append('client_max_body_size {file_size}M;\n'.format(file_size=upload_file_size))

        if location_details.disable_chunked_encoding:
            proxy_header_variables.append('chunked_transfer_encoding off;\n')

        if gConfig.useProxyProcol:
            proxy_header_variables.append('proxy_set_header X-Real-IP $proxy_protocol_addr;\n')
            proxy_header_variables.append('proxy_set_header X-Forwarded-For "$proxy_protocol_addr, $proxy_add_x_forwarded_for";\n')
        else:
            proxy_header_variables.append('proxy_set_header X-Real-IP $remote_addr;\n')
            proxy_header_variables.append('proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;\n')

        if AuthorizationOperation.USER_INFO in location_details.authOperations:
            proxy_header_variables.append('proxy_set_header ezb_verified_user_info $ezb_verified_user_info;\n')
            proxy_header_variables.append('proxy_set_header ezb_verified_signature $ezb_verified_signature;\n')
        if AuthorizationOperation.USER_JSON in location_details.authOperations:
            proxy_header_variables.append('proxy_set_header ezb_user_info_json $ezb_user_info_json;\n')
            proxy_header_variables.append('proxy_set_header ezb_user_info_json_signature $ezb_user_info_json_signature;\n')

        # TODO: configure different certs for different locations
        ssl_certificate_variables = ['proxy_ssl_certificate {0:s};\n'.format(gConfig.ez_certfile),
                                     'proxy_ssl_certificate_key {0:s};\n'.format(gConfig.ez_keyfile),
                                     'proxy_ssl_client_certificate {0:s};\n'.format(gConfig.ez_cafile)]

        if location_details.validate_upstream_connection:
            proxy_header_variables.extend(ssl_certificate_variables)

        for redir in self._redirects[server][location_details.location]:
            proxy_header_variables.append('proxy_redirect https://{0:s}{1:s} {2:s};\n'.format(redir[0], location_details.upstream_context_root, location_details.location))
            proxy_header_variables.append('proxy_redirect https://{0:s}:{1:s}{2:s} {3:s};\n'.format(redir[0], redir[1], location_details.upstream_context_root, location_details.location))
            proxy_header_variables.append('proxy_redirect http://{0:s}{1:s} {2:s};\n'.format(redir[0], location_details.upstream_context_root, location_details.location))
            proxy_header_variables.append('proxy_redirect http://{0:s}:{1:s}{2:s} {3:s};\n'.format(redir[0], redir[1], location_details.upstream_context_root, location_details.location))

        proxy_header =['{1}{0}'.format('\t\t'.join(sorted(proxy_header_variables)),'\t\t')]
        tail=['\t}\n\n']
        return ''.join(head + proxy_header + tail)

    def addRootLocation(self, userFacingUrlPrefix, contentService):
        '''
        Add the Nginx location block for static content:
        #For Hybrid
            location /app1/ezbappstatic/ {
		    root /opt/ezfrontend/sc_a/app1_d1d3c1912692369be5a8d90598d961bb;
	    }
        #For Static Only
           location /app1/ {
		    root /opt/ezfrontend/sc_a/app1_d1d3c1912692369be5a8d90598d961bb;
	    }
        '''
        rloc_array = []

        if self._sfh.updateStaticDir(userFacingUrlPrefix, contentService):
            if contentService == ContentServiceType.HYBRID:
                rloc_array.append('\tlocation /{0}/{1}/ {{\n'.format(userFacingUrlPrefix , self._staticPathInLocation))
            else:
                rloc_array.append('\tlocation /{0}/ {{\n'.format(userFacingUrlPrefix))

            ssd = self._sfh.staticLocationForNginx(userFacingUrlPrefix)
            rloc_array.append("\n")
            rloc_array.append('\t\troot {0};\n'.format(ssd))
            rloc_array.append("\n")
            rloc_array.append("\t}\n\n")
        return ''.join(rloc_array)

    def addServerBlock(self, server):
        '''
        Add  Nginx Server Block
        '''
        sblock_array = []
        https_listen_directives = ['{external_hostname}:{https_port}'.format(external_hostname=gConfig.external_hostname, https_port=gConfig.https_port)]

        if gConfig.defaultServerName is not None and server == gConfig.defaultServerName:
            https_listen_directives.append('default_server')

        https_listen_directives.append('ssl')

        if gConfig.useProxyProcol:
            https_listen_directives.append('proxy_protocol')

        sblock_array.append('server {{\n\tlisten {listen_directives};'.format(listen_directives=' '.join(https_listen_directives)))
        sblock_array.append('\n\tserver_name {server};'.format(server=server))

        if gConfig.useProxyProcol:
            sblock_array.append('\n\treal_ip_header proxy_protocol;')

        sblock_array.append('\n')
        if server in self._serversWithSpecializedCerts:
            sblock_array.append('\n\tssl_certificate {path}.crt;'.format(path=os.path.join(gConfig.ssl_server_certs, server)))
            sblock_array.append('\n\tssl_certificate_key {path}.key;'.format(path=os.path.join(gConfig.ssl_server_certs, server)))

        return ''.join(sblock_array)

    def addUpstreamServerBlocks(self):
        '''
        Configure Nginx upstream server blocks
        '''
        usb_array=[]
        for k,v in self._upstreams.iteritems():
            usb_array.append('upstream %s {\n' % (k))
            if v['sticky']:
                usb_array.append('\tsticky name=ezb_upstream_{sha224} path={location} secure httponly;\n'.format(sha224=hashlib.sha224(v['location']).hexdigest(), location=v['location']))
            for server in v['upstreams']:
                usb_array.append('\tserver {server} '.format(server=server))
                if int(v['timeout_tries']) >= 1:
                    usb_array.append(' max_fails={timeout_tries}'.format(timeout_tries=v['timeout_tries']))
                # Check if upstream timeout is within limits 10.. 120 inclusive.
                if  10 <= int(v['timeout']) <= 120:
                    usb_array.append(' fail_timeout={timeout}s'.format(timeout=v['timeout']))
                usb_array.append(';\n')
            usb_array.append('}\n')

        return ''.join(usb_array)

    def _generateConfigFileContents(self):
        text_array = []
        #configure common http variables for all servers and upstreams
        http_variables=['ssl_certificate {ssl_certificate};'.format(ssl_certificate=gConfig.ssl_certfile),
                        'ssl_certificate_key {ssl_key};'.format(ssl_key=gConfig.ssl_keyfile),
                        'ssl_session_timeout 5m;',
                        'ssl_protocols  TLSv1 TLSv1.1 TLSv1.2;',
                        'ssl_ciphers  HIGH:!aNULL:!MD5;',
                        'ssl_prefer_server_ciphers on;',
                        'ssl_verify_client on;',
                        'ssl_client_certificate {ssl_cafile};'.format(ssl_cafile=gConfig.ssl_cafile),
                        'ssl_verify_depth {max_ca_depth};'.format(max_ca_depth=gConfig.max_ca_depth)]
        text_array.append('\n'.join(http_variables))
        if os.path.isfile(gConfig.ssl_crl_file):
            text_array.append("\n\tssl_crl %s;" % (gConfig.ssl_crl_file))

        text_array.append("\n\n")

        #configure upstream server blocks
        text_array.append(self.addUpstreamServerBlocks())

        text_array.append("\n")

        #configure http server block
        http_listen_directives = ['{external_hostname}:{http_port}'.format(external_hostname=gConfig.external_hostname,http_port=gConfig.http_port)]
        if gConfig.useProxyProcol:
            http_listen_directives.append('proxy_protocol')
        text_array.append("server {\n\tlisten %s;\n\treturn 301 https://$host$request_uri;\n}\n\n" % (' '.join(http_listen_directives)))

        #configure https server blocks
        for server, locations in sorted(self._servers.iteritems(), reverse=True, key=lambda tuple: sorted(tuple[1].values(), reverse=True, key=attrgetter('location'))[0].location):
            #get server & location from self._servers sorted based on the values stored in location field of the first Location object (sorted) for a server
            #this allows us to list the most descriptive server location first
            text_array.append(self.addServerBlock(server))

            #add the favicon block
            text_array.append('\n\tlocation =/favicon.ico { alias %s; }\n' % gConfig.favicon_file)

            text_array.append("\n\n")
            for unused,location_details in locations.iteritems():
                text_array.append(self.addProxyLocation(location_details, server))

            text_array.append("}\n\n")

        return ''.join(text_array)

    def _generateConfigFile(self):
        text = self._generateConfigFileContents()
        filename = os.path.join(gConfig.confdDirectory,'servers.conf')
        with open(filename,'w') as config_file:
            config_file.write(text)

    def _reconfigureNginx(self):
        self._logger.info("SIGNALING NGINX TO RECONFIGURE")
        pids = []
        try:
            pids.append(ezRPNginx.get_nginx_master_pid())
        except IOError as e:
            self._logger.error('unable to get nginx master pid from nginx.pid file: %s' % str(e))
            pids.extend(ezRPNginx.get_orphaned_nginx_master_pids())
            self._logger.warn('reconfiguring orphaned nginx masters: %s' % str(pids))

        for pid in pids:
            os.kill(pid, signal.SIGHUP)

    def _getSslFiles(self):
        curssldir = os.readlink(gConfig.ssl_server_certs)
        self._newSslDir = choice([x for x in gConfig.ssl_server_certs_dirs if x != curssldir])
        shutil.rmtree(self._newSslDir)
        os.mkdir(self._newSslDir, 0700)
        for serverName in self.kz.get_children(ezRPKazoo.KZSSLCONFLOC):
            try:
                try:
                    certContents, keyContents = self.ac.get(serverName)
                except OpenSSL.crypto.Error as ex:
                    self._logger.error("SSL Exception in getting cert contents: %s" % ex)
                    raise
                if certContents is None or keyContents is None:
                    self._logger.warn("Read empty certificate or key contents for %s" % serverName)
                    continue
                with open(os.path.join(self._newSslDir, serverName + '.crt'), 'w') as file:
                    file.write(certContents)
                    os.chmod(file.name, stat.S_IRUSR)
                with open(os.path.join(self._newSslDir, serverName + '.key'), 'w') as file:
                    file.write(keyContents)
                    os.chmod(file.name, stat.S_IRUSR)
            except Exception as e:
                self._logger.error("Exception in creating SSL certs for %s: %s" % (serverName, str(e)))
                
    def _updateStaticFiles(self):
        try:
            self._sfh.updateStaticContentsDict()
        except Exception as e:
            self._logger.error("Exception while updating static contents hash {0}".format(str(e)))
            
    def configure(self):
        last_watch = self.kz.get(ezRPKazoo.KZWATCHLOC)[0]
        self._logger.info("Configuring Nginx with WATCH triggered with %s" % last_watch)

        # get ssl files before generating nginx conf files
        self._getSslFiles()
        
        self._updateStaticFiles()

        # get list of entries
        rpEntries = self.kz.get_children(ezRPKazoo.KZCONFLOC)
        for rpEntry in rpEntries:
            serializedRegistration = self.kz.get(ezRPKazoo.KZCONFLOC+'/'+rpEntry)
            registration = self._deserializeUpstreamServerRegistration(serializedRegistration[0])
            self._addRpEntry(registration)
        self._generateConfigFile()

        #update ssl certs directory link before reconfiguring nginx
        subprocess.call(['ln', '-sTf', self._newSslDir, gConfig.ssl_server_certs])

        #update static content directory link
        self._sfh.updateStaticDirLink()

        self._reconfigureNginx()

    def getAllRegistrations(self):
        rpEntries = self.kz.get_children(ezRPKazoo.KZCONFLOC)
        rtn = []
        for rpEntry in rpEntries:
            serializedRegistration = self.kz.get(ezRPKazoo.KZCONFLOC+'/'+rpEntry)
            registration = self._deserializeUpstreamServerRegistration(serializedRegistration[0])
            rtn.append(registration)
        return rtn

    def getAllRegistrationsWihPrefix(self, path):
        registrations = []
        for node in [x for x in self.kz.get_children(ezRPKazoo.KZCONFLOC) if x.startswith(path)]:
            serializedRegistration = self.kz.get(ezRPKazoo.KZCONFLOC + '/' + node)
            registration.append(self._deserializeUpstreamServerRegistration(serializedRegistration[0]))
        return registrations

    def getAllRegistrationsForApp(self, appName):
        return [reg for reg in self.getAllRegistrations() if reg.AppName == appName]


