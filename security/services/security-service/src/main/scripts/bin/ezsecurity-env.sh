#!/bin/sh

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

EZ_USER=ezsecurity
SERVICE_NAME=EzBakeSecurityService
SERVICE_ID=_Ez_Security
SSL_DIR="${EZ_PREFIX}/etc/ezbake/pki"

THRIFT_RUNNER_JAR="/opt/ezbake/thriftrunner/bin/thriftrunner.jar"
THRIFT_RUNNER_OPTS="-j ${EZ_PREFIX}/lib/ezbake-security-service.jar \
            -s ${SERVICE_NAME} -p ${PORT} \
            -P ${EZ_PREFIX}/config"

STETHOSCOPE_CLIENT="/opt/ezbake/ezbake-discovery-stethoscope-client/ezbake-discovery-stethoscope-client.jar"
STETHOSCOPE="java -jar $STETHOSCOPE_CLIENT \
-D ezbake.security.ssl.dir=${SSL_DIR} \
-D service.name=${SERVICE_NAME} \
-D application.name=${APP_NAME} ${THRIFT_TRANSPORT_OPT} \
--private-service-hostname $(hostname) \
--private-service-port ${PORT} \
--public-service-hostname $(hostname) \
--public-service-port ${PORT} \
--checkin-interval "5" &> /tmp/stethoscope.log"

EZBAKE_LOG_DIR=$(grep -s 'ezbake.log.directory' /etc/sysconfig/ezbake/*.properties | cut -d'=' -f2-)
EZBAKE_LOG_DIR=${EZBAKE_LOG_DIR:-"/tmp"}

export EZBAKE_ENCRYPTION_SECRET=$(runuser ezsecurity -l sh -c 'echo ${EZBAKE_ENCRYPTION_SECRET}')
