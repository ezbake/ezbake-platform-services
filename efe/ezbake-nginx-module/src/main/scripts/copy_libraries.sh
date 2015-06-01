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

BASE_PATH=$1
DEST_PATH=$2

dependencyLibs="$BASE_PATH/target/nar/*amd64-Linux-gpp-shared/lib/amd64-Linux-gpp/shared/lib*"
dependencyLibFiles=`find $dependencyLibs -name "*.so*"`
mkdir -p $DEST_PATH
for f in $dependencyLibFiles; do
 cp -f $f $DEST_PATH
done
chmod +x $DEST_PATH/*

#create no-version links to libraries that contain version info in name
pushd $DEST_PATH > /dev/null
for f in ${dependencyLibFiles}; do
    libfile=${f##*/}
    liblink=`echo ${libfile} | sed 's/^\(.*\)-[0-9\.]\+.*$/\1.so/g'`
    chmod +x ${libfile}
    if [ ${libfile} != ${liblink} ]; then
        ln -sf ${libfile} ${liblink}
    fi
done
popd > /dev/null

