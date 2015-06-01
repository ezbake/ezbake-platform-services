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

. common.sh

REPO_URL=$(git config --get remote.origin.url)
REPO_PATH=${REPO_URL#*:}
REPO_PATH=${REPO_PATH%.*}
REPO_DOMAIN=${REPO_URL#*@}
REPO_DOMAIN=${REPO_DOMAIN%:*}

echo "-- building eznginx module library"
echo_and_execute_cmd "cd $NAR_LIB_EZNIGNX_MODULE"
echo_and_execute_cmd "mvn clean install -Dproject.scm.connection=scm:git:$REPO_URL -Dproject.scm.url=https://$REPO_DOMAIN/$REPO_PATH"
echo_and_execute_cmd "sudo rpm -Uvh --nodeps target/rpm/**/RPMS/**/*.rpm"
echo_and_execute_cmd "cp target/rpm/**/RPMS/**/*.rpm $CWD/."
echo_and_execute_cmd "cd $CWD"

