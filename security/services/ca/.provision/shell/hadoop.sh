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

HADOOP_VERSION="2.2.0"

if [[ -d "/opt/hadoop/hadoop-${HADOOP_VERSION}" ]]; then
    echo "hadoop already installed"
    exit 1
fi

echo "download/installing hadoop"
HADOOP=$(curl -ks http://www.apache.org/dyn/closer.cgi/hadoop/common/ 2>&1 | grep -oE 'href="([^"#]+hadoop[^"#]+)"' | head -n 1 | cut -d'"' -f2)
(cd /tmp && curl -kO "${HADOOP}/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" && mkdir -p /opt/hadoop && tar -xzf "hadoop-${HADOOP_VERSION}.tar.gz" -C /opt/hadoop)
rm "/opt/hadoop/hadoop-${HADOOP_VERSION}/bin/"*.cmd
rm "/opt/hadoop/hadoop-${HADOOP_VERSION}/sbin/"*.cmd

echo "creating hadoop users"
groupadd hadoop
useradd -g hadoop -d /var/lib/hadoop hdfs
chown -R hdfs:hadoop /opt/hadoop

echo "updating hadoop symlink"
ln -sf "/opt/hadoop/hadoop-${HADOOP_VERSION}" /opt/hadoop/hadoop

echo "setting the hadoop configuration"
mkdir -p /etc/hadoop

cat <<EOF > /etc/profile.d/hadoop.sh
export JAVA_HOME=/etc/alternatives/jre
export PATH="${PATH}:/opt/hadoop/hadoop/bin:/opt/hadoop/hadoop/sbin"
EOF
source /etc/profile.d/hadoop.sh

cat >> /opt/hadoop/hadoop/etc/hadoop/hadoop-env.sh << 'EOF'
export JAVA_HOME=/etc/alternatives/jre
export HADOOP_INSTALL=/opt/hadoop
export HADOOP_COMMON_LIB_NATIVE_DIR=${HADOOP_INSTALL}/hadoop/lib/native
export HADOOP_OPTS="-Djava.library.path=${HADOOP_INSTALL}/hadoop/lib"
EOF

sed -i '/<configuration>/a <property>\n<name>fs.default.name</name>\n<value>hdfs://localhost:9000</value>\n</property>' /opt/hadoop/hadoop/etc/hadoop/core-site.xml
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

sed -i '/<configuration>/a <property>\n<name>dfs.datanode.data.dir</name>\n<value>/var/lib/hadoop/hdfs/datanode</value>\n</property><property>\n<name>dfs.namenode.name.dir</name>\n<value>/var/lib/hadoop/hdfs/namenode</value>\n</property>\n<property>\n<name>dfs.replication</name>\n<value>1</value>\n</property>' /opt/hadoop/hadoop/etc/hadoop/hdfs-site.xml


echo "formatting the namenode"
mkdir -p /var/lib/hadoop
chown -R hdfs:hadoop /var/lib/hadoop

sudo -Eu hdfs /opt/hadoop/hadoop/bin/hdfs namenode -format -nonInteractive

echo "setting up ssh"
rm -f /var/lib/hadoop/.ssh/id_rsa*
sudo -Eu hdfs ssh-keygen -q -t rsa -P '' -f /var/lib/hadoop/.ssh/id_rsa
sudo -Eu hdfs cat /var/lib/hadoop/.ssh/id_rsa.pub >> /var/lib/hadoop/.ssh/authorized_keys
sudo -Eu hdfs ssh-keyscan -H localhost > /var/lib/hadoop/.ssh/known_hosts
sudo -Eu hdfs ssh-keyscan -H 0.0.0.0 >> /var/lib/hadoop/.ssh/known_hosts

