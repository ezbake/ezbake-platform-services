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

REL_PREFIX=""

CONTAINER=container
APP_CONTAINER=$CONTAINER/fesrv
APP_PATH=$CWD/frontend_app
APP_DESTINATION=/opt/ezfrontend

SYSUSER=ezfrontend
SYSGRP=$SYSUSER
RPM_VENDOR=EzBake.IO
RPM_NAME=ezbake-frontend-service
VERSION=2.1
RELEASE="${REL_PREFIX}$(date +"%Y%m%d%H%M%S").git.$(git rev-parse --short HEAD)"

#copy nginx binary
echo_and_execute_cmd "cp -f $CWD/ezbake-nginx/nginx/objs/nginx $APP_CONTAINER/app/nginx"
echo_and_execute_cmd "mkdir -p $APP_CONTAINER/logs"

#run generate binaries with pyinstaller
PYINST_DIR=`mktemp -d $CWD/pyinstaller_XXX`
echo_and_execute_cmd "mkdir -p $PYINST_DIR"
echo_and_execute_cmd "sudo env PATH=$PATH pip install --pre -r $APP_PATH/pyRequirements.pip"
echo_and_execute_cmd "pyinstaller --distpath=$APP_CONTAINER/app --workpath=$PYINST_DIR -y --specpath=$PYINST_DIR --paths=$APP_PATH $APP_PATH/ezReverseProxy.py"
echo_and_execute_cmd "rm -rf $PYINST_DIR"

##prepare staging area for RPM
echo_and_execute_cmd "sudo chmod -R go-rwx $APP_CONTAINER/app"

##create RPM
DEPENDENCIES="-d 'ezbake-nginx-module = 2.1' -d 'ezbake-frontend-static-content = 2.1' -d boost -d log4cxx"
CONFIG_FILES="--config-files $APP_DESTINATION/config/eznginx.properties"
echo_and_execute_cmd "sudo fpm -s dir -t rpm --rpm-use-file-permissions --rpm-user=$SYSUSER --rpm-group=$SYSGRP --directories=/opt/ezfrontend --vendor=$RPM_VENDOR -n $RPM_NAME -v $VERSION --iteration=$RELEASE $DEPENDENCIES $CONFIG_FILES $APP_CONTAINER/=$APP_DESTINATION $CONTAINER/init.d/=/etc/init.d $CONTAINER/logrotate.d/=/etc/logrotate.d"

