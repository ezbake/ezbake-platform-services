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

#configure nginx build
if [ "$1" != "--quick-mode" ]; then
    echo "-- configuring nginx with module dependencies"
    echo_and_execute_cmd "cd ${EZNGINX_BUILD_PATH}"
    echo_and_execute_cmd "./configure.sh"
    echo_and_execute_cmd "cd ${CWD}"
fi


##build eznginx libraries
echo_and_execute_cmd "./build_ezbake_nginx_module_rpm.sh"

##build eznginx app
echo_and_execute_cmd "./build_ezbake_nginx_app.sh --quick-mode"

#build RPMs
echo_and_execute_cmd "./build_ezbake_frontend_rpm.sh"

