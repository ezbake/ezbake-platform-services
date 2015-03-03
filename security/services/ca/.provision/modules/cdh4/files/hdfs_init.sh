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


sudo -u hdfs hdfs namenode -format -nonInteractive

for x in `cd /etc/init.d ; ls hadoop-hdfs-*` ; do
	sudo service $x restart
done

hadoop fs -ls /tmp &>/dev/null
stat=$?
if [ $stat -ne 0 ]; then
	echo "Initing hdfs"
	sudo -u hdfs hadoop fs -chmod 777 /
	sudo -u hdfs hadoop fs -mkdir /tmp
	sudo -u hdfs hadoop fs -chmod -R 1777 /tmp
	sudo -u hdfs hadoop fs -mkdir -p /var/lib/hadoop-hdfs/cache/mapred/mapred/staging
	sudo -u hdfs hadoop fs -chmod 1777 /var/lib/hadoop-hdfs/cache/mapred/mapred/staging
	sudo -u hdfs hadoop fs -mkdir -p /var/lib/hadoop-hdfs/cache/mapred/mapred/system
	sudo -u hdfs hadoop fs -chmod 1777 /var/lib/hadoop-hdfs/cache/mapred/mapred/system
	sudo -u hdfs hadoop fs -chown -R mapred /var/lib/hadoop-hdfs/cache/mapred
	sudo -u hdfs hadoop fs -mkdir /user/vagrant
	sudo -u hdfs hadoop fs -chown vagrant /user/vagrant
else
	echo "Not initing hdfs"
fi
