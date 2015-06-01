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

ZOOKEEPER_VERSION="3.4.6"

if [[ -d "/opt/zookeeper/zookeeper-${ZOOKEEPER_VERSION}" ]]; then
    echo "zookeeper already installed"
    exit 0
fi

ZOOKEEPER=$(curl -ks http://www.apache.org/dyn/closer.cgi/zookeeper/ 2>&1 | grep -oE 'href="([^"#]+zookeeper[^"#]+)"' | head -n 1 | cut -d'"' -f2)

echo "downloading/installing zookeeper"
(cd /tmp && curl -kO "${ZOOKEEPER}/zookeeper-${ZOOKEEPER_VERSION}/zookeeper-${ZOOKEEPER_VERSION}.tar.gz" && mkdir -p /opt/zookeeper && tar -xzf "zookeeper-${ZOOKEEPER_VERSION}.tar.gz" -C /opt/zookeeper)
rm "/opt/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/bin/"*.cmd

echo "updating zookeeper current symlinks"
ln -sf "/opt/zookeeper/zookeeper-${ZOOKEEPER_VERSION}" /opt/zookeeper/current

echo "setting the zoo configuration"
mkdir -p /etc/zookeeper
cat << EOF > /etc/zookeeper/zoo.cfg
maxClientCnxns=100
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/var/lib/zookeeper
clientPort=2181
EOF

cat << EOF > /etc/zookeeper/zookeeper-env.sh
export ZOO_LOG_DIR=/var/log/zookeeper
export ZOOCFGDIR=/etc/zookeeper
EOF

echo "adding the zookeeper scripts to the path"
cat << EOF > /usr/bin/zkServer
#!/bin/bash
export ZOOCFGDIR=/etc/zookeeper
/opt/zookeeper/current/bin/zkServer.sh $@
EOF

cat << EOF > /usr/bin/zkCli
#!/bin/bash
export ZOOCFGDIR=/etc/zookeeper
/opt/zookeeper/current/bin/zkCli.sh $@
EOF
chmod +x /usr/bin/zkServer
chmod +x /usr/bin/zkCli
