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


MAVEN_VERSION="3.2.3"

if hash mvn 2>/dev/null; then
	echo "mvn already installed"
	exit 0
fi

MAVEN=$(curl -ks http://maven.apache.org/download.cgi 2>&1 | grep -oE 'href="([^"#]+apache-maven-'"${MAVEN_VERSION}"'-bin.tar.gz)"' | head -n 1 | cut -d'"' -f2)

(cd /tmp && curl -kO "${MAVEN}" && mkdir -p /usr/lib/maven && tar -xzf "apache-maven-${MAVEN_VERSION}-bin.tar.gz" -C /usr/lib/maven && rm "apache-maven-${MAVEN_VERSION}-bin.tar.gz")
echo "export MAVEN_HOME=/usr/lib/maven/apache-maven-${MAVEN_VERSION}" > /etc/profile.d/maven.sh
echo 'export M2="${MAVEN_HOME}/bin"' >> /etc/profile.d/maven.sh
echo 'export PATH="$PATH:$M2"' >> /etc/profile.d/maven.sh
echo 'export JAVA_HOME=/usr/lib/jvm/jre' >> /etc/profile.d/maven.sh
