#!/bin/bash
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


CWD=$(pwd)
GO_TO_CUR_DIR="cd ${CWD}"
GO_TO_NGINX="cd ${CWD}/nginx"
NGINX_REQUIRED_OPTIONS="--prefix=/opt/nginx --with-http_ssl_module --with-http_realip_module --with-http_stub_status_module"
CONFIGURE_MODULES=""

CONFIGURE_MODULES="${CONFIGURE_MODULES} --add-module=../modules/ezbake-nginx-auth"
#CONFIGURE_MODULES="${CONFIGURE_MODULES} --add-module=../modules/backtrace"
CONFIGURE_MODULES="${CONFIGURE_MODULES} --add-module=../modules/nginx-goodies-nginx-sticky-module-ng-bd312d586752"

CONFIGURE_CMD="./auto/configure ${NGINX_REQUIRED_OPTIONS} ${CONFIGURE_MODULES}"

echo ""
echo ${GO_TO_NGINX}
${GO_TO_NGINX}
echo ""

echo ""
echo "--- Configuring NGINX with ${CONFIGURE_CMD}"
echo ""
${CONFIGURE_CMD}

echo ""
echo ${GO_TO_CUR_DIR}
${GO_TO_CUR_DIR}
echo "" 
