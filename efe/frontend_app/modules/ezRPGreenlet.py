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
import sys
import gevent
import signal

from ezbake.discovery import ServiceDiscoveryClient
from ezbake.configuration.helpers import AccumuloConfiguration

from ezbake.reverseproxy.thriftapi.constants import SERVICE_NAME as EzFrontendServiceName
from ezbake.frontend.thriftapi import EzFrontendService

from ezbake.thrift.transport.EzSSLSocket import TSSLServerSocket
from ezbake.thrift.server.TGeventServer import TGeventServer

from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol



import gevent_inotifyx

import kazoo.client
import kazoo.handlers.gevent
from kazoo.protocol.states import EventType

from functools import partial
import ezRPKazoo
import ezRPKazookeeper
import ezRPConfigNginx
import ezRPNginx
from ezRPCertStore import EzRPCertStore, EzRPCertStoreException
import ezRPConfig as gConfig


'''
Method to flock all the greenlets
'''

class EzReverseProxyGreenlet(object):
    def __init__(self, logger, staticFileHandler):
     self._logger = logger
     self._sfh = staticFileHandler

    def enqueueConfigurationChanges(self):
        # value doesn't matter, as the queue consumer
        # pulls data out of zk
        gConfig.configurationChangeQueue.put(1)

    def clientServiceGreenlet(self):
      ezd = ServiceDiscoveryClient(gConfig.zk)
      ezd.register_endpoint(gConfig.appName, EzFrontendServiceName, gConfig.internal_hostname, gConfig.thriftPort)
      ezd.set_security_id_for_application(gConfig.appName, '_Ez_EFE')

      while gConfig.run:
        try:
          handler = ezRPKazookeeper.EzReverseProxyHandler(self._logger, self._sfh)
          processor = EzFrontendService.Processor(handler)
          transport = TSSLServerSocket(host=gConfig.internal_hostname,
                                       port=gConfig.thriftPort,
                                       verify_pattern=gConfig.ez_frontend_access,
                                       ca_certs=gConfig.ez_cafile,
                                       cert=gConfig.ez_certfile,
                                       key=gConfig.ez_keyfile)
          tfactory = TTransport.TBufferedTransportFactory()
          pfactory = TBinaryProtocol.TBinaryProtocolFactory()
          #server = TServer.TSimpleServer(processor,transport,tfactory,pfactory)
          server = TGeventServer(self._logger, processor, transport, tfactory, pfactory)
          gevent.sleep()
          server.serve()
        except Exception as e:
          self._logger.exception("Error in Thrift server: %s" % e)
      self._logger.info("exiting clientServiceGreenlet")


    def kzWatchHandler(self, data, stat, event):
      self._logger.info('kzWatchHandler callback for %s' % ezRPKazoo.KZWATCHLOC)
      if event is None:
        self._logger.info("None event returned from zookeeper WATCH node. Reconnection or first time watching node.")
        self.enqueueConfigurationChanges()
      elif event.type == EventType.CHANGED:
        self._logger.info("detected changed in zookeeper WATCH node")
        self.enqueueConfigurationChanges()
      elif event.type is not None:
        self._logger.warn("%s event returned from zookeeper WATCH node" % str(event.type))
        #self.enqueueConfigurationChanges()
      return True

   

    def kzMonitorGreenlet(self):
      kz = kazoo.client.KazooClient(gConfig.zk, logger=self._logger, handler=kazoo.handlers.gevent.SequentialGeventHandler())
      kz.start()

      kz.ensure_path(ezRPKazoo.KZCONFLOC)
      kz.ensure_path(ezRPKazoo.KZWATCHLOC)
      kz.ensure_path(ezRPKazoo.KZSSLCONFLOC)

      kz.DataWatch(ezRPKazoo.KZWATCHLOC, func=self.kzWatchHandler)

      while True:
        gevent.sleep(1)
      self._logger.info("zookeeper monitor greenlet exiting")

    def configurationChangeQueueGreenlet(self):
        kz = None
        ac = None
        task = None
        while gConfig.run:
            try:
                if kz is None:
                    kz = kazoo.client.KazooClient(gConfig.zk, logger=self._logger, handler=kazoo.handlers.gevent.SequentialGeventHandler())
                    kz.start()
                if ac is None:
                    accConfig = AccumuloConfiguration(gConfig.ezproperties)
                    ac = EzRPCertStore(host=accConfig.getProxyHost(),
                                       port=accConfig.getProxyPort(),
                                       user=accConfig.getUsername(),
                                       password=accConfig.getPassword(),
                                       privateKey=gConfig.ez_keyfile,
                                       logger=self._logger)
                if task is None:
                    task = gConfig.configurationChangeQueue.get()
                configurer = ezRPConfigNginx.Configurer(kz, ac, self._logger, self._sfh)
                self._logger.info("processing entry in configuration work queue")
                configurer.configure()
                task = None
                gConfig.configurationChangeQueue.task_done()
            except EzRPCertStoreException as ex:
                self._logger.exception('CertStore exception while running configurationChangeQueueGreenlet: %s' % str(ex))
                gConfig.run = False
            except Exception as e:
                self._logger.exception('Exception processing configuration change: %s' % str(e))
                kz = None
                ac = None
        self._logger.info("configurationChangeQueueGreenlet() exiting")

    def watchGreenlet(self,fd):
        gevent_inotifyx.get_events(fd)
        self._logger.warn("shutting down due to change in %s" % (gConfig.shutdownFile))
        gConfig.kill()


