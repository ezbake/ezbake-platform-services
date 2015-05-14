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
if [ "$1" != "--release" ]; then
  REL_PREFIX="SNAPSHOT"
fi

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
echo_and_execute_cmd "pyinstaller --distpath=$APP_CONTAINER/app --workpath=$PYINST_DIR -y --specpath=$PYINST_DIR --paths=$APP_PATH --log-level=INFO $APP_PATH/ezReverseProxy.py"

# Help pyinstaller find its eggs. This requires all eggs to be
# deposited in the eggs/ subdirectory alongside this script. This
# appears to be necessary because pyinstaller will only store one
# location for a module, and it will use this location to attempt to
# load all child modules. After finding ezbake in the
# ezbake-configuration-constants egg, for example, it then looks for
# ezbake.thrift in the same module and then fails.
#
# Note that this is a problem with pyinstaller, not Python: Python can
# handle multiple eggs that share a namespace module.
#
# Also note that this hack is unfortunately reliant on the syntax of
# the .spec file. In particular, it will probably not work with a
# single-file distribution.
#
# After looking at the pyinstaller code, it seems pretty clear that
# the real answer is to move away from it and find another way to ship
# these packages. --Josh

pushd `dirname $0`; SRC_DIR=`pwd`; popd
eggs=
for egg in $SRC_DIR/eggs/*.egg; do
    eggs="('eggs/$(basename $egg)', '$egg', 'ZIPFILE'),$eggs"
done

sed -i "/= COLLECT/i\\
a.zipfiles += [$eggs]" $PYINST_DIR/ezReverseProxy.spec

echo_and_execute_cmd "pyinstaller --distpath=$APP_CONTAINER/app --workpath=$PYINST_DIR -y --specpath=$PYINST_DIR --paths=$APP_PATH --log-level=INFO $PYINST_DIR/ezReverseProxy.spec"

echo_and_execute_cmd "rm -rf $PYINST_DIR"

##prepare staging area for RPM
echo_and_execute_cmd "sudo chmod -R go-rwx $APP_CONTAINER/app"

##create RPM
DEPENDENCIES=(
    -d 'ezbake-nginx-module > 2.0'
    -d 'ezbake-frontend-static-content > 2.0'
    -d boost
    -d log4cxx
)

CONFIG_FILES=(
    --config-files "$APP_DESTINATION/config/eznginx.properties"
)

echo_and_execute_cmd "sudo fpm -s dir -t rpm --rpm-use-file-permissions --rpm-user=$SYSUSER --rpm-group=$SYSGRP --directories=/opt/ezfrontend --vendor=$RPM_VENDOR -n $RPM_NAME -v $VERSION --iteration=$RELEASE ${DEPENDENCIES[@]} ${CONFIG_FILES[@]} $APP_CONTAINER/=$APP_DESTINATION $CONTAINER/init.d/=/etc/init.d $CONTAINER/logrotate.d/=/etc/logrotate.d"
