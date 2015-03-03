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


ACCUMULO_VERSION="1.6.0"
ROOT_PASSWORD="password"

if [[ -d "/opt/accumulo/accumulo-${ACCUMULO_VERSION}" ]]; then
    echo "accumulo already installed"
    exit 0
fi

ACCUMULO=$(curl -ks https://www.apache.org/dyn/closer.cgi/accumulo/${ACCUMULO_VERSION}/accumulo-${ACCUMULO_VERSION}-bin.tar.gz 2>&1 | grep -oE 'href="([^"#]+-bin.tar.gz)"' | head -n 1 | cut -d'"' -f2)
(cd /tmp && curl -kO "${ACCUMULO}" && mkdir -p /opt/accumulo && tar -xzf "accumulo-${ACCUMULO_VERSION}-bin.tar.gz" -C /opt/accumulo)
ln -sf "/opt/accumulo/accumulo-${ACCUMULO_VERSION}" /opt/accumulo/current

echo "creating accumulo users"
groupadd accumulo
useradd -g accumulo -d /var/lib/accumulo accumulo
mkdir -p /var/lib/accumulo
chown -R accumulo:accumulo /var/lib/accumulo

echo "setting up ssh"
rm -f /var/lib/accumulo/.ssh/id_rsa*
sudo -Eu accumulo ssh-keygen -q -t rsa -P '' -f /var/lib/accumulo/.ssh/id_rsa
sudo -Eu accumulo cat /var/lib/accumulo/.ssh/id_rsa.pub >> /var/lib/accumulo/.ssh/authorized_keys
sudo -Eu accumulo ssh-keyscan -H localhost > /var/lib/accumulo/.ssh/known_hosts
sudo -Eu accumulo ssh-keyscan -H default > /var/lib/accumulo/.ssh/known_hosts

cat << 'EOF' > /etc/profile.d/accumulo.sh
export ACCUMULO_HOME=/opt/accumulo/current
export PATH="${PATH}:${ACCUMULO_HOME}/bin"
EOF

cp /opt/accumulo/current/conf/templates/accumulo-env.sh /opt/accumulo/current/conf/
cp /opt/accumulo/current/conf/templates/accumulo-site.xml /opt/accumulo/current/conf/
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

cp /opt/accumulo/current/conf/templates/log4j.properties /opt/accumulo/current/conf/

sed -i '0,/[^#]/ a export ACCUMULO_HOME=/opt/accumulo/current\nexport tServerHigh_tServerLow="-Xmx128m -Xms128m"\nexport masterHigh_masterLow="-Xmx128m -Xms128m"\nexport monitorHigh_monitorLow="-Xmx64m -Xms64m"\nexport gcHigh_gcLow="-Xmx64m -Xms64m"\nexport otherHigh_otherLow="-Xmx128m -Xms64m"' /opt/accumulo/current/conf/accumulo-env.sh
sed -i 's,/path/to/hadoop,/usr/lib/hadoop,' /opt/accumulo/current/conf/accumulo-env.sh
sed -i 's,/path/to/java,/etc/alternatives/jre,' /opt/accumulo/current/conf/accumulo-env.sh
sed -i 's,/path/to/zookeeper,/usr/lib/zookeeper,' /opt/accumulo/current/conf/accumulo-env.sh

sed -i '/instance.zookeeper.host/,+1 s/localhost:2181/default:2181/' /opt/accumulo/current/conf/accumulo-site.xml
sed -i '/instance.secret/,+1 s/\(<value>\).*\(<\/value>\)/\1password\2/' /opt/accumulo/current/conf/accumulo-site.xml
sed -i '/trace.token.property.password/,+2 s/\(<value>\).*\(<\/value>\)/\1'"${ROOT_PASSWORD}"'\2/' /opt/accumulo/current/conf/accumulo-site.xml

sed -i 's/${memMapMax}/80M/' /opt/accumulo/current/conf/accumulo-site.xml
sed -i 's/${nativeEnabled}/false/' /opt/accumulo/current/conf/accumulo-site.xml
sed -i 's/${cacheDataSize}/7M/' /opt/accumulo/current/conf/accumulo-site.xml
sed -i 's/${cacheIndexSize}/20M/' /opt/accumulo/current/conf/accumulo-site.xml
sed -i 's/${sortBufferSize}/50M/' /opt/accumulo/current/conf/accumulo-site.xml
sed -i 's/${waLogMaxSize}/100M/' /opt/accumulo/current/conf/accumulo-site.xml

echo "default" > /opt/accumulo/current/conf/masters
echo "default" > /opt/accumulo/current/conf/slaves

chown -R accumulo:accumulo /opt/accumulo

sudo -u accumulo /opt/accumulo/current/bin/accumulo init --instance-name ezbake.local --password "${ROOT_PASSWORD}"
