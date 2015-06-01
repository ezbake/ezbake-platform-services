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

import argparse
from multiprocessing import cpu_count

def setupParser():
    '''
    builds the parser for parsing all the configuraiton options to ezReverseProxy
    '''
    parser = argparse.ArgumentParser(description='ezReverseProxy. Typically, only 1 instance should be running on a machine with --ngx-workers set properly')
    parser.add_argument('--internal-hostname', '-ihn', help='The internal IP on which the thrift service should listen')
    parser.add_argument('--external-hostname', '-ehn', help='The external IP on which nginx should listen for client browser requsts')
    parser.add_argument('--port', '-p', type=argtport, help='the port on which the ezReverseProxy will listen for new services to reverse proxy')
    parser.add_argument('--zookeepers', '-zk', help='The zookeeper connection string')
    parser.add_argument('--ngx-workers', '-nw', type=int, default=cpu_count(), help='The number of nginx workers. Typically the number of cores (incl. ht) on the machine. By default, this ends up being set properly to multiprocessing.cpu_count() *** that number includes HT cores on Linux, but not on OS X. ***')
    return parser

def argtport(portnum):
    '''
    intended as a type for argparse to validate that port numbers are integers
    and in the valid port number range [1,65535]
    '''
    try:
        rtn = int(portnum)
        if rtn < 0 or rtn > 65535:
            raise argparse.ArgumentTypeError("port number must be in the range [1,65545], not: %s" % portnum)
        return rtn
    except ValueError as e:
        raise argparse.ArgumentTypeError("port numbers must be valid integers, not: %s" % portnum)

def argtcslist(input):
    '''
    intended as a type for argparse to retrieve comma separated lists from the argument value
    '''
    try:
        rtn = input.split(',')
    except:
        raise argparse.ArgumentTypeError("%s could not be parsed as a comma separated list" % input)

