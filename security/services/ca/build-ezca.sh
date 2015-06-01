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


REPO_ROOT="$(pwd)"
BUILDROOT="${REPO_ROOT}/BUILD"
PACKAGEROOT=$(mktemp -d)
#"${BUILDROOT}/EzCA_Pkg"
APP_ROOT="${PACKAGEROOT}/opt/ezca"
mkdir -p "${BUILDROOT}" && cd "${BUILDROOT}"

PYENVV=2.7.6
PYENV=ezca2.0
BRANCH=master

function copy_to_build() {
    local src="$1"
    local dest="${2}/$(basename "${src}")"

    echo "Copying ${src}/ to ${dest}"
	rsync -r "${src}"/ "${dest}"
}

function install_package() {
    local name="$1"
    local dir="$2"

    #version=$(pip list | grep "${name}")
    #if [ $? -eq 1 ]; then
    echo "${name} not installed. Installing now"
    (cd "${dir}" && python setup.py clean -a && python setup.py install && pyenv rehash) || (echo "failed"; exit 1)
    #else
        #echo "${name} installed - ${version}"
    #fi
}

function install_maven() {
    local dir="$1"
    echo "Running maven package of ${dir}"
    (cd "${dir}" && mvn clean package) || (echo "failed to package ${dir}"; exit 1)
}

echo "cloning the repos from git"
for x in ${REPOS[@]}; do
    x=(${x[0]//;/ })
    repo="${x[0]}"
    dir="${x[1]}"
    branch="${x[2]}"

    if [ -d "${dir}" ]; then
        echo "${dir} already checked out"
        #(cd "${dir}" && git pull) 
    else
        echo "cloning ${repo} into ${dir}"
        git clone "${repo}" "${dir}"
    fi

    echo "cheking out ${branch}"
    (cd "${dir}" && git checkout "${branch}")
done

# Copy local resources to the build directory
copy_to_build "${REPO_ROOT}/eztssl" "${BUILDROOT}"
copy_to_build "${REPO_ROOT}/ezpz" "${BUILDROOT}"
copy_to_build "${REPO_ROOT}/ezthriftpool" "${BUILDROOT}"
copy_to_build "${REPO_ROOT}/ezpersist" "${BUILDROOT}"
copy_to_build "${REPO_ROOT}/service" "${BUILDROOT}"
copy_to_build "${REPO_ROOT}/ezca-bootstrap" "${BUILDROOT}"

echo "switching to pyenv virtualenv ${PYENV}"
eval "$(pyenv init -)"
pyenv shell "${PYENV}" || env PYTHON_CONFIGURE_OPTS="--enable-shared" pyenv install "${PYENV}" && pyenv shell "${PYENV}"

pip list | grep 'setuptools' || curl -L https://bootstrap.pypa.io/get-pip.py | python
pip list | grep 'pyinstaller' || pip install pyinstaller
pip list | grep 'zope.interface' || pip install zope.interface
touch ~/.pyenv/versions/${PYENV}/lib/python2.7/site-packages/zope/__init__.py

# Install main ezbake libs
# install_package EzConfiguration "${ezconfig_arr[1]}/api/python"
# install_package ezdiscovery "${ezdiscovery_arr[1]}/servicediscovery/python"

# Install EzCA packages
install_package EzTSSL "eztssl"
install_package "ezpz" "ezpz"
install_package "ezthriftpool" "ezthriftpool"
install_package "ezpersist" "ezpersist"
install_package ezca "service"

install_maven "ezca-bootstrap"

echo "Building with pyinstaller"
LD_LIBRARY_PATH=/home/vagrant/.pyenv/versions/${PYENV}/lib pyinstaller -y "service/bin/ezcaservice.py" --hidden-import=pkg_resources

echo "Packaging"
mkdir -p "${PACKAGEROOT}"/etc
mkdir -p ${APP_ROOT}/{bin,config,app}

# Copy app files
cp -r dist/ezcaservice "${APP_ROOT}/app/"
cp ezca-bootstrap/target/ezca-bootstrap-*-shaded.jar "${APP_ROOT}/bin/ezca-bootstrap"
cp "${REPO_ROOT}"/scripts/bin/* "${APP_ROOT}/bin/"
cat > "${APP_ROOT}/config/ezca.properties" <<'EOF'
ezbake.shared.secret.environment.variable=EZBAKE_ENCRYPTION_SECRET
EOF

# Copy system config files
cp -r "${REPO_ROOT}"/scripts/etc/. "${PACKAGEROOT}/etc"

# Update file permissions
chmod -R o-rwx "${APP_ROOT}"
chmod +x "${APP_ROOT}/bin/init"
chmod +x "${APP_ROOT}/bin/start"
chmod +x "${APP_ROOT}/bin/stop"
sudo chown -R root:root "${PACKAGEROOT}/etc"
sudo chown -R root:root "${PACKAGEROOT}/opt"
sudo chown -R ezca:ezca "${PACKAGEROOT}/opt/ezca"

# change back to the repo root
cd "${REPO_ROOT}"

#$(date +"%Y%m%d%H%M") \
sudo fpm -f -s dir -t rpm \
    -n EzCA -v 2.0 --iteration 1 \
    -C "${PACKAGEROOT}" \
    --rpm-use-file-permissions \
    --rpm-auto-add-directories \
    --rpm-auto-add-exclude-directories /etc/logrotate.d \
    opt etc

sudo rm -rf "${PACKAGEROOT}"
