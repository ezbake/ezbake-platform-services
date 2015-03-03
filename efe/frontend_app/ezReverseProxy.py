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

from gevent import monkey
monkey.patch_all()
import sys
import os
import gevent
import logging

from modules import ezRPNginx
from modules import ezRPParser
from modules import ezRPService
from modules import ezRPConfig as gConfig
from modules import ezRPStaticFileHandler
from ezbake.configuration.EzConfiguration import EzConfiguration
from ezbake.configuration.helpers import ZookeeperConfiguration, SystemConfiguration
from ezbake.configuration.loaders.PropertiesConfigurationLoader import PropertiesConfigurationLoader
from ezbake.configuration.loaders.DirectoryConfigurationLoader import DirectoryConfigurationLoader
from ezbake.configuration.security.CryptoImplementations import SharedSecretTextCryptoImplementation

from ezbake.discovery import ServiceDiscoveryClient


logger = logging.getLogger('efe_control')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def log(arg):
    print(arg)

def getEzSecurityServers():
    rtn = []
    for endpoint in ServiceDiscoveryClient(gConfig.zk).get_common_endpoints('EzbakeSecurityService'):
        name,port = endpoint.split(':',1)
        rtn.append((name,port))
    return rtn

def getEzProperties():
    #load default configurations
    config = EzConfiguration()
    logger.info("loaded default ezbake configuration properties")

    #load configuration overrides
    overrideLoader = DirectoryConfigurationLoader(gConfig.ezconfig_dir)
    config = EzConfiguration(PropertiesConfigurationLoader(config.getProperties()), overrideLoader)
    logger.info("loaded property overrides")

    #load cryptoImpl
    cryptoImpl = SystemConfiguration(config.getProperties()).getTextCryptoImplementer()
    if not isinstance(cryptoImpl, SharedSecretTextCryptoImplementation):
        logger.warn("Couldn't get a SharedSecretTextCryptoImplementation. Is the EZB shared secret set properly?")

    return config.getProperties(cryptoImpl)


if __name__ == '__main__':
    parser = ezRPParser.setupParser()
    args = parser.parse_args()

    # we're going to run everything from within this packaged application
    # so we need to find our own path
    print os.getpid()

    import logging.handlers
    wfh = logging.handlers.WatchedFileHandler(os.path.join(gConfig.logDirectory,'efe_control.log'))
    wfh.setLevel(logging.INFO)
    wfh.setFormatter(formatter)
    logger.addHandler(wfh)

    gConfig.ezproperties = getEzProperties()
    if args.external_hostname is not None:
        gConfig.external_hostname = args.external_hostname
    else:
        gConfig.external_hostname = gConfig.ezproperties['external_hostname']
    if args.internal_hostname is not None:
        gConfig.internal_hostname = args.internal_hostname
    else:
        gConfig.internal_hostname = gConfig.ezproperties['internal_hostname']
    if args.zookeepers is not None:
        gConfig.zk = args.zookeepers
    else:
        gConfig.zk = ZookeeperConfiguration(gConfig.ezproperties).getZookeeperConnectionString()
    if args.port is not None:
        gConfig.thriftPort = args.port
    else:
        gConfig.thriftPort = gConfig.ezproperties['efe.port']

    gConfig.nginx_worker_username = gConfig.ezproperties['efe.nginx_worker_username']
    gConfig.https_port = gConfig.ezproperties['efe.https_port']
    gConfig.http_port = gConfig.ezproperties['efe.http_port']
    gConfig.max_ca_depth = gConfig.ezproperties['efe.max_ca_depth']
    gConfig.static_content_max_size = int(gConfig.ezproperties.get('efe.static_content_max_size', 100)) * 1048576
    gConfig.static_content_chunk_size = int(gConfig.ezproperties.get('efe.static_content_chunk_size', 5)) * 1048576

    gConfig.ssl_crl_file = gConfig.ezproperties.get('efe.crl_file', '')
    gConfig.useProxyProcol = gConfig.ezproperties.get('efe.use.proxyprotocol', False);
    gConfig.defaultServerName = gConfig.ezproperties.get('efe.default.server.name');

    # Drop the parsed argument dictionary into the global config object
    gConfig.args = args

    runAttributes = []
    if gConfig.useProxyProcol:
        runAttributes.append('ProxyProtocol')

    logger.info('starting frontend service : %s' % ' '.join(runAttributes))

    # shut down all instances of nginx on this box and clean the 
    # config and log directories for this instance
    ezRPNginx.nginx_cleanup(logger)

    # do the basic setup
    ezRPNginx.nginx_basesetup(logger)
    sfh = ezRPStaticFileHandler.StaticFileHandler(logger)
    ezs = ezRPService.EzReverseProxyService(logger, sfh)
    ezs.run() #block and serve frontend services, until interrupted

    #clean up and stop nginx processes
    ezRPNginx.nginx_cleanup(logger)



