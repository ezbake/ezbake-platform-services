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


# need git to install rbenv
which git &>/dev/null || (
  yum install -qy git
);

# need zlib-devel for gems
rpm -qa | grep zlib-devel || yum -q -y install zlib-devel

# Install rbenv to /opt
if [ -d "/opt/.rbenv" ]; then
	echo "rbenv already installed"
else
	git clone https://github.com/sstephenson/rbenv.git /opt/.rbenv
fi

# make sure rbenv is on the path
if grep -qs "/opt/.rbenv/bin:/usr/local/bin:$PATH" /etc/profile.d/rbenv.sh;
then
	echo "rbenv.sh already configured"
else
	echo 'export PATH="/opt/.rbenv/bin:/usr/local/bin:$PATH"' >> /etc/profile.d/rbenv.sh
	echo 'eval "$(rbenv init -)"' >> /etc/profile.d/rbenv.sh
	# source it to use later on
	source /etc/profile.d/rbenv.sh
fi

# Install ruby build
if rbenv help install &>/dev/null; then
	echo "ruby-build already installed"
else
	echo "installing ruby-build"
	git clone https://github.com/sstephenson/ruby-build.git /tmp/ruby-build
	/tmp/ruby-build/install.sh

	# Install the most recent 1.9.3 release
	sudo -u vagrant -i sh - <<-'EOF'
	rb=$(rbenv install -l | grep -E 1.9.3-p[[:digit:]] | tail -n 1)
	rbenv install ${rb}
	rbenv global ${rb}
	gem install bundler
	EOF

	rm -rf /tmp/ruby-build
fi
