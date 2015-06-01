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

NGINX_DIR=${CWD}/nginx
LIBS_DIR=${CWD}/libs

GO_TO_CUR_DIR="cd ${CWD}"
GO_TO_NGINX="cd ${NGINX_DIR}"

MAKE_CMD="make"
NGINX_MAIN_MAKEFILE="${NGINX_DIR}/Makefile"
NGINX_OBJ_MAKEFILE="${NGINX_DIR}/obj/Makefile"
FORCE_CONFIGURE="-f"

function isNginxConfigured() {
  retval=1
  if [[ ! -f ${NGINX_MAIN_MAKEFILE} ]] && [[ ! -f ${NGINX_OBJ_MAKEFILE} ]]; then
    retval=0
  fi
  return "$retval"
}

set -e

if isNginxConfigured || [ "$1" == ${FORCE_CONFIGURE} ]; then
  echo ""
  . configure.sh
  echo ""
fi

echo ${GO_TO_NGINX}
${GO_TO_NGINX}
echo ""
echo "--- building NGINX with ${MAKE_CMD}"
echo ""
${MAKE_CMD}
echo ""
echo ${GO_TO_CUR_DIR}
${GO_TO_CUR_DIR}
echo "" 

