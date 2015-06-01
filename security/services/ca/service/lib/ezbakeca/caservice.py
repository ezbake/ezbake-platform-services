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
Created on Mon Mar 24 08:03:20 2014

@author: jhastings
"""
import os
import errno
import socket
import atexit
import shutil
import tarfile
from StringIO import StringIO
import Crypto.Random

from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from eztssl.EzSSLSocket import TSSLServerSocket

from ezconfiguration.EzConfiguration import EzConfiguration
from ezconfiguration.loaders.DirectoryConfigurationLoader import DirectoryConfigurationLoader
from ezconfiguration.helpers import ZookeeperConfiguration
from ezconfiguration.helpers import ApplicationConfiguration
from ezconfiguration.helpers import SystemConfiguration

import ezdiscovery
from kazoo.handlers.threading import TimeoutError

from ezpersist.base import MemoryPersist
from ezpersist.file import FilePersist

import ezbakeca.ca
from ezbakeca.cert import Cert
from ezbakeca.ca import EzbakeCA
from ezca import EzCA
import ezca.ttypes
import ezca.constants
import ezmetrics.ttypes

import logging
import logging.handlers
logger = logging.getLogger(__name__)

integer_123 = 1

class EzCAHandler:
    TABLE_NAME = "_EZ_CA_"
    SERVER_CERT_NAME = "EzCAService"
    PERSIST_MODE = "ezca.persist.mode"
    CLIENT_CERTS = "ezca.autogen.clients"
    CLIENT_CERT_O = "ezca.autogen.clients.out"
    CLIENT_CERT_O_DEF = "gen"

    def __init__(self, ca_name, ezconfig=EzConfiguration().getProperties()):
        mode = ezconfig.get(EzCAHandler.PERSIST_MODE, "file")
        if mode == "file":
            store = FilePersist(EzCAHandler.TABLE_NAME)
        elif mode == "accumulo":
            raise NotImplementedError("accumulo persistance not supported by EzCA yet")
        else:
            store = MemoryPersist()
        EzbakeCA.setup(store=store)
        Cert.setup(store=store)

        self.store = store
        try:
            logger.info("Reading CA certificate {}".format(ca_name))
            self.ca = EzbakeCA.get_named(ca_name)
        except KeyError:
            self.ca = EzbakeCA(name=ca_name)
        self.ca.save()

    def _server_certs(self):
        """returns a dict of {ca_certs, certs, key} and their values"""
        ca_certs = ezbakeca.ca.pem_cert(self.ca.ca_cert)
        cert = Cert.get_named(self.SERVER_CERT_NAME)
        if not cert.cert:
            cert.cert = self.ca.sign_csr(cert.csr())
            cert.save()
        key = ezbakeca.ca.pem_key(cert.private_key)
        cert = ezbakeca.ca.pem_cert(cert.cert)
        return {'ca_certs': ca_certs, 'cert': cert, 'key': key}

    def ping(self):
        return True

    def getMetricRegistryThrift(self):
        return ezmetrics.ttypes.MetricRegistryThrift()

    def csr(self, token, csr):
        csr = ezbakeca.ca.load_csr(csr)
        # but since this is a Protect component, and we are only allowing
        # access to this service by trusted cert CNs, I can accept that.
        # still, we must log what's happening.
        logger.info("CSR signing request for Subject: {}. Token security ID: {}, "
                    "target security ID: {}, userInfo: {}".format(csr.get_subject(),
                                                                  token.validity.issuedTo,
                                                                  token.validity.issuedFor,
                                                                  token.tokenPrincipal.principal))

        # sign the csr and get the cert
        cert = self.ca.sign_csr(csr)

        # return the cert as pem
        return ezbakeca.ca.pem_cert(cert)


def ca_server(ezconfig, service_name=None, ca_name="ezbakeca", zoo_host=None,
              host=None, port=None, verify_pattern=None, ssldir=None):
    Crypto.Random.atfork()
    # make sure zookeeper is available for registering with service discovery
    if zoo_host is None:
        zooConf = ZookeeperConfiguration(ezconfig)
        zoo_host = zooConf.getZookeeperConnectionString()
        if not zoo_host:
            raise RuntimeError("Zookeeper connection string must be specified "
                "in EzConfiguration")

    # make sure the ssl certificate directory is available
    if not ssldir:
        ac = ApplicationConfiguration(ezconfig)
        ssldir = ac.getCertificatesDir()
        if not ssldir:
            raise RuntimeError("Certificates Directory \"{0}\" must be set in"
                " EzConfiguration!".format(
                ApplicationConfiguration.CERTIFICATES_DIRECTORY_KEY))

    # get a free port to bind to (and figure out our hostname)
    if not port:
        port = get_port(range(31005,34999))
    if not host:
        host = socket.gethostname()

    # register with ezdiscovery
    try:
        if service_name is None:
            service_name = ezca.constants.SERVICE_NAME
        ezdiscovery.connect(zoo_host)
        logger.info('Registering with service discovery')
        ezdiscovery.register_common_endpoint(service_name=service_name, host=host, port=port)
    except TimeoutError as e:
        logger.error("Fatal timeout connecting to zookeeper. Unable to "
                     "register with service discovery.")
        raise e
    finally:
        ezdiscovery.disconnect()

    # create the thrift handler
    handler = EzCAHandler(ca_name, ezconfig)

    # generate/get the server SSL certs and write them to disk
    certs = handler._server_certs()
    cert_files = []
    for k, cert in certs.items():
        of = os.path.join(ssldir, k)
        cert_files.append(of)
        with os.fdopen(os.open(of, os.O_WRONLY | os.O_CREAT, 0o600), 'w') as ofs:
            ofs.write(str(cert))

    # generate certs for configured clients (read from ezconfig)
    clients = ezconfig.get(EzCAHandler.CLIENT_CERTS)
    if clients:
        gen_client_certs(handler.ca, clients.split(','),
                         ezconfig.get(EzCAHandler.CLIENT_CERT_O))

    # start the thrift server
    processor = EzCA.Processor(handler)
    transport = TSSLServerSocket(host=host, port=port,
                                 verify_pattern=verify_pattern,
                                 ca_certs=cert_files[0],
                                 cert=cert_files[1],
                                 key=cert_files[2])
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)
    logger.info('Starting ezca service on {}:{}'.format(host,port))
    server.serve()


def ezpersist_instance(mode):
    if mode == "file":
        store = FilePersist(EzCAHandler.TABLE_NAME)
    elif mode == "accumulo":
        raise NotImplementedError("accumulo persistance not supported by EzCA yet")
    else:
        store = MemoryPersist()
    return store


def get_port(rang=None):
    """Find an open port, optionally in a range"""
    port = None
    while port is None:
        p = 0
        if rang:
            p = rang.pop(0)
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("",p))
            s.listen(1)
            port = s.getsockname()[1]
            s.close()
            break
        except:
            port = None
            continue
    return port


def gen_client_certs(ca, clients, directory=None, force=False):
    try:
        for client in filter(None, clients):
            cert = Cert.get_named(client)
            if not cert.cert:
                # client doesn't have a cert yet. generate it
                logger.info("Generating client certificate for {}"
                    .format(client))
                cert.cert = ca.sign_csr(cert.csr())
            elif force:
                logger.info("Force regenerating client certificate for {}"
                    .format(client))
                cert.cert = ca.sign_csr(cert.csr())
            else:
                # check the client's cert was issued by us, if not, regen
                casubject = ca.ca_cert.get_issuer()
                issuer = cert.cert.get_issuer()
                if casubject != issuer:
                    logger.info("Client certification for {} was issued " \
                        "by another certificate authority. Re-issuing " \
                        "the cert".format(client))
                    cert.cert = ca.sign_csr(cert.csr())
            cert.save()

            if directory:
                # Now write the archive to disk for easy retrieval
                tar_certs(ca.cert_string(), cert, directory)
    except TypeError as e:
        logger.warn("not generating client certificates, {}".format(e))


def setup_logging(verbose, config):
    sys_config = SystemConfiguration(config)

    log = logging.getLogger(__package__)
    log.setLevel(logging.INFO)
    hostname = socket.gethostname()
    formatter = logging.Formatter(hostname+' %(asctime)s [%(threadName)s] %(levelname)-5s %(name)s - %(message)s')

    if verbose or sys_config.shouldLogToStdOut():
        sh = logging.StreamHandler()
        sh.setLevel(logging.INFO)
        sh.setFormatter(formatter)
        log.addHandler(sh)

    # Create the log directory
    log_dir = os.path.join(sys_config.getLogDirectory(),'ezca')
    try:
        os.mkdir(log_dir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            logger.error("Unable to create the log directory {}".format(
                log_dir))
            raise e
    wfh = logging.handlers.WatchedFileHandler(os.path.join(log_dir, 'ezca.log'))
    wfh.setLevel(logging.INFO)
    wfh.setFormatter(formatter)
    log.addHandler(wfh)


def tar_certs(ca_cert, cert, directory):
    """Create a tarfile in the given directory from the given ca and certs"""
    if not directory or os.path.isfile(directory):
        logger.warn("Cert output directory {} None or a file".format(directory))
        return
    if not os.path.exists(directory):
        try:
            os.mkdir(directory)
        except OSError as e:
            if e.errno != errno.EEXIST:
                logger.warn("Unable to create cert output directory")
                return

    cdir = os.path.join(directory, "{}.{}".format(cert.name, "tar.gz"))
    with os.fdopen(os.open(cdir, os.O_WRONLY|os.O_CREAT, 0o600), 'w') as ofs:
        with tarfile.open(fileobj=ofs, mode="w:gz") as tar:
            ca_info = tarfile.TarInfo("ezbakeca.crt")
            ca_info.size = len(ca_cert)
            tar.addfile(tarinfo=ca_info, fileobj=StringIO(ca_cert))

            app_cert = cert.cert_string()
            cert_info = tarfile.TarInfo("application.crt")
            cert_info.size = len(app_cert)
            tar.addfile(tarinfo=cert_info, fileobj=StringIO(app_cert))

            pk = cert.pkey_string()
            pk_info = tarfile.TarInfo("application.priv")
            pk_info.size = len(pk)
            tar.addfile(tarinfo=pk_info, fileobj=StringIO(pk))


def create_dirs(ssldir):
    logger.info("Creating the ssl direcotry on disk (required by openssl)")
    try:
        os.mkdir(ssldir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise e
    logger.info("Creating the output directory for client certs")


def delete_ssldir(ssldir):
    """Try to delete the ssl certs from the filesystem"""
    logger.info("Deleting ssl certs from disk")
    try:
        shutil.rmtree(ssldir)
    except Exception as e:
        logger.warn("Unable to remove the ssldir from disk {}".format(e))


def load_configuration(dir=None):
    loaders = [DirectoryConfigurationLoader()]
    if dir:
        loaders.append(DirectoryConfigurationLoader(dir))
    return EzConfiguration(*loaders).getProperties()


def main(config):
    # Load the EzConfiguration
    if config.has_key('ezconfig'):
        ezConfig = config['ezconfig']
    else:
        ezConfig = load_configuration("config")

    # Configure logging
    setup_logging(config['verbose'], ezConfig)

    if config.has_key('clients'):
        ezConfig[EzCAHandler.CLIENT_CERTS] = config['clients']

    # Setting up the SSL and output Directory
    ssldir = config['ssl_dir']
    create_dirs(ssldir)
    # register shutdown hook
    atexit.register(delete_ssldir, ssldir)

    server = ca_server(ezConfig,
                       config['service_name'],
                       host=config['host'],
                       ca_name=config['ca_name'],
                       ssldir=ssldir,
                       verify_pattern=config['verify_pattern'])
    server.start()


def init(config):
    ezConfig = load_configuration("config")
    setup_logging(config.verbose, ezConfig)

    clients = config.clients.split(',')

    # initialize the daos
    store = ezpersist_instance("file")
    EzbakeCA.setup(store=store)
    Cert.setup(store=store)

    if config.force:
        store.delete(config.name)

    try:
        # Try to get it first, to see if it already exists
        ca = EzbakeCA.get_named(config.name)
        logger.info("CA %s not regenerated because it already exists", config.name)
    except KeyError:
        # Create the CA
        ca = EzbakeCA(name=config.name, environment=config.env)
        ca.save()

    gen_client_certs(ca, clients, directory=config.outdir, force=config.force)
