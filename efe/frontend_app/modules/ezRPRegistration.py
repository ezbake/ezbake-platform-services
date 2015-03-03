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

from zbase62 import zbase62
from ezbake.reverseproxy.thriftapi.ttypes import RegistrationInvalidException

"""
Module to extract information from thrift server registration structure
"""

def getNodeName(registration):
    # The node name encodes the UserFacingUrlPrefix, 
    # the UpstreamPath, and the UpstreamHostAandPort in base62
    # and separates them with a _ so that the information
    # can be encoded in a file name in zookeeper
    filename = \
           zbase62.b2a(registration.UserFacingUrlPrefix)+ "_" +\
           zbase62.b2a(registration.UpstreamPath) + "_" +\
           zbase62.b2a(registration.UpstreamHostAndPort)
    return filename

def get_ngx_server_name(registration):
  server_name = registration.UserFacingUrlPrefix.split('/')[0]
  # TODO validate server name if possible - likely not practical
  return server_name 

def get_ngx_location(registration):
  split = registration.UserFacingUrlPrefix.split('/',1)
  if len(split) == 1:
    return '/'
  else:
    # TODO: validate location if possible - likely not practical
    return '/'+split[1]

def getUpstreamHostAndPort(registration):
  # the upstream host and port should be a string of the format host:port
  hap = registration.UpstreamHostAndPort.split(':')
  if len(hap) != 2:
      raise RegistrationInvalidException("registration.UpstreamHostAndPort: %s is not of the form hostname:port" % (registration.UpstreamHostAndPort))
  
  # TODO: can we validate the host name format in some way? - likely not practical

  # validate that the port number is an integer < 65536
  try:
      port = int(hap[1])
      assert port > 0
      assert port < 65536
  except:
      raise RegistrationInvalidException("registration.UpstreamHostAndPort: %s port does not seem to be an integer in the range [1,65535]" % (registration.UpstreamHostAndPort))
  return hap[0],port

def getUpstreamPath(registration):
  # TODO validate the path if possible - likely not practical
  return registration.UpstreamPath
