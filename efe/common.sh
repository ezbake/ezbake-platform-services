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


fail() {
    echo $1
    exit 1
}

function echo_and_execute_cmd() {
    local cmd=$1
    echo ${cmd}
    eval ${cmd} || fail "Error in running: ${cmd}"
}

#   
CWD=$(pwd)
NAR_LIB_EZNIGNX_MODULE="${CWD}/ezbake-nginx-module"
EZNGINX_BUILD_PATH="${CWD}/ezbake-nginx"

