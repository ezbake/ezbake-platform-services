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

'''
Module to share configuration information across modules.
This global object is used through out to store and retreive configuration.
This is to avoid passing gConfig as variables throughout.
All the configurations needed are added in ezReverseProxy.
'''
# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4

import sys
import os
import time
from gevent.queue import JoinableQueue

from ezbake.reverseproxy.thriftapi.ttypes import AuthorizationOperation


'''
We want addGreenlets() and kill() to access global members without an instance,
perhaps the simplest idea is to just make them simple functions outside the class,
not class methods. I tried @staticmethod decorator.

#class EzRPConfig(object):
'''

appName = 'EzBakeFrontend'

watches = {}
containerDir = os.path.abspath(os.path.join(os.path.abspath(__file__),os.pardir,os.pardir,os.pardir,os.pardir))
configurationChangeQueue = JoinableQueue()
run = True
clientService = None
zkMonitor = None
cfgGreenlet = None
wGreenlet = None

current_milli_time = lambda: int(round(time.time() * 1000))

if getattr(sys, 'frozen', False):
    containerDir = os.path.abspath(os.path.join(os.path.dirname(sys.executable),os.pardir,os.pardir))

templateDir = os.path.join(containerDir,'app','templates')
nginx = os.path.join(containerDir,'app','nginx')
eznginxlibpath = os.path.join(containerDir,'libs')
workingDirectory = os.path.join(containerDir,'wd')
logDirectory = os.path.join(containerDir,'logs')
eznginxmoduleLogProp = os.path.join(logDirectory,'log4j.properties')

configDirectory = os.path.join(workingDirectory,'conf')
mainConfig = os.path.join(configDirectory,'nginx.conf')
confdDirectory = os.path.join(configDirectory,'conf.d')
manualDirectory = os.path.join(containerDir,'manual')

ezconfig_dir = os.path.join(containerDir, 'config')
htmlRootDir = os.path.join(containerDir, 'static_content')
favicon_file = os.path.join(htmlRootDir, 'ezbstatic', 'images', 'favicon.ico')

# external facing ssl files for nginx
ssl_cadir = os.path.join(ezconfig_dir,'ssl/user_ca_files')
ssl_keyfile = os.path.join(ezconfig_dir,'ssl/server/server.key')
ssl_certfile = os.path.join(ezconfig_dir,'ssl/server/server.crt')
ssl_server_certs = os.path.join(workingDirectory, 'ssl')
ssl_server_certs_dirs = [os.path.join(workingDirectory, 'ssl_a'), os.path.join(workingDirectory, 'ssl_b')]
ssl_cafile = os.path.join(containerDir,'wd','CAchain.pem')

# internal ssl files for thrift service w/in EzBake
ezEtc = os.path.join(containerDir,'etc')
ezcertdir = os.path.join(containerDir,'etc/ezbake/pki/cert/config/ssl')
ez_keyfile = os.path.join(ezcertdir,'application.priv')
ez_cafile = os.path.join(ezcertdir,'ezbakeca.crt')
ez_certfile = os.path.join(ezcertdir,'application.crt')

# Static content directory to serve per site static content
static_contents = os.path.join(containerDir,'ezbappstatic')
static_contents_dirs = [os.path.join(containerDir, 'sc_a'), os.path.join(containerDir, 'sc_b')]

mainConfigTemplate = os.path.join(templateDir,'nginx.conf')
mimeTemplate = os.path.join(templateDir,'mime.types')
mimeConfig = os.path.join(configDirectory,'mime.types')
nginxPidFile = os.path.join(workingDirectory,'nginx_%d.pid' % os.getpid())
shutdownFile = os.path.join(workingDirectory,'delete_this_file_to_shutdown_efe')

ezproxyciphers = "HIGH:!DSS:!aNULL@STRENGTH"
defaultEznginxOps = AuthorizationOperation.USER_INFO

def addGreenlets(thriftService, kzMonitor, cfgChange, shutdown):
    global clientService
    global zkMonitor
    global cfgGreenlet
    global wGreenlet
    clientService = thriftService
    zkMonitor = kzMonitor
    cfgGreenlet = cfgChange
    wGreenlet = shutdown 

def kill():
    if clientService:
        clientService.kill()
    if zkMonitor:
        zkMonitor.kill()
    if cfgGreenlet:
        cfgGreenlet.kill()

