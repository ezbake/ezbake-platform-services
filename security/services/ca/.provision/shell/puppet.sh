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


PUPPET_REPO="http://yum.puppetlabs.com/el/6/products/x86_64/puppetlabs-release-6-10.noarch.rpm"

function hieradata() {
	local dir="${1}"
	if [ -d "/vagrant/${dir}" ]; then
		mkdir -p /etc/puppet
		ln -fTs "/vagrant/${dir}/hiera.yaml" /etc/puppet/hiera.yaml
		rm -rf /etc/puppet/hieradata
		ln -fs "/vagrant/${dir}/hieradata" /etc/puppet/hieradata
	else
		echo "not linking hieradata configs, /vagrant/${dir} does not exist"
	fi
}

if which puppet &>/dev/null; then
	echo "puppet is installed. just updating hieradata"
	hieradata "$1"
	exit 0
fi

# install puppet repo
repo_tmp=$(mktemp)
curl -so "${repo_tmp}" "${PUPPET_REPO}"
rpm -i --nosignature ${repo_tmp}
rpm -ql puppetlabs-release | grep GPG | xargs rpm --import
rm -rf ${repo_tmp}

rpm -qs --quiet puppet || yum -q -y install puppet

echo "installed puppet"
hieradata "$1"
