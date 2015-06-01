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
Created on Mon Mar 24 08:05:48 2014

@author: jhastings
"""
import sys
import traceback
import argparse
import ezbakeca.caservice

def parse_arguments():
    parser = argparse.ArgumentParser(description="EzCa service")
    parser.add_argument('-v', '--verbose', action="store_true", default=False, help="print logs to standard out")

    subparsers = parser.add_subparsers(dest="command",help='CA command')
    # parser for the init command
    init_parser = subparsers.add_parser('init', help='init CA service', description="EzCA service initialization")
    init_parser.add_argument('-n', '--ca-name', dest='name', metavar="CA", default="ezbakeca", help="name assigned as CN of the CA")
    init_parser.add_argument('-e', '--environment', dest='env', metavar="ENV", default="ezbake", help="the environment this CA is valid for, assigned to cert as OUs")
    init_parser.add_argument('-c', '--clients', dest='clients', metavar="CLIENTS", default="_Ez_Security,_Ez_EFE,_Ez_Registration,_Ez_Deployer", help="comma separated list of clients to generate certificates for")
    init_parser.add_argument('-o', '--tar-output', dest='outdir', metavar="OUT", help="directory in which to export client certs as tar")
    init_parser.add_argument('-f', '--force', action="store_true", help="force regeneration of CA certificates")

    svc_parser = subparsers.add_parser('server', help='run CA service', description="EzCA service thrift server")
    svc_parser.add_argument('--host', metavar="HOST", type=str, default="localhost", help="host name to bind the service to")
    svc_parser.add_argument('-n', '--ca-name', dest='ca_name', metavar="CA", default="ezbakeca", help="name of the CA to load")
    svc_parser.add_argument('-d', '--ssl-dir', metavar="SSLDIR", default="/opt/ezca/pki", help="directory to load ssl certs from")
    svc_parser.add_argument('-c', '--clients', metavar="CLIENTS", default="_Ez_Security,_Ez_EFE,_Ez_Registration,_Ez_Deployer", help="comma separated list of clients to generate certificates for")
    svc_parser.add_argument('-p', '--verify-pattern', metavar="PATTERN", default="_Ez_Registration", help="regex used to verify peer certificate")
    svc_parser.add_argument('-s', '--service-name', metavar="SERVICE_NAME", help="service name used when registering with service discovery")

    return parser.parse_args()

if __name__ == '__main__':
    args = parse_arguments()
    if args.command == 'init':
        ezbakeca.caservice.init(args)
    elif args.command == 'server':
        try:
            ezbakeca.caservice.main(vars(args))
        except Exception as e:
            print "EzCA server caught an unrecoverable exception and will now exit: {0}".format(e)
            traceback.print_exc(file=sys.stdout)
            sys.exit(1)
