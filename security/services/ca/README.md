# EzBake CA Service

## Building
The EzBake CA is built with pyinstaller, and packaged into an RPM with fpm, the f-ing package manager. There is a
Vagrantfile in the root of this repo that provisions a vanilla centos 6.5 VM with all of the tools needed to build the
EzCA RPM.

```bash
$ vagrant up
$ vagrant ssh
[vagrant@build vagrant]$ cd /vagrant
[vagrant@build vagrant]$ ./build-ezca.sh
```

The build script __build-ezca.sh__ should be run from the VM. It will install pyenv and a new virtualenv before installing
the packages and their dependencies with pip. If pypi is not configured for EzBake, you should do the pyenv steps manually.

```
export PYENVV=2.7.6
export PYENV=ezca2.0
eval "$(pyenv init -)"
pyenv shell "${PYENVV}" || env PYTHON_CONFIGURE_OPTS="--enable-shared" pyenv install "${PYENVV}"
pyenv virtualenv ${PYENVV} ${PYENV} && pyenv shell "${PYENV}"
pip install -r requirements.txt

```

Artifacts are output in the folder /vagrant/BUILD

## Gathering dependencies

If ezbake libraries are not available in the pypi repo, then it may be necessary to build ezbake-thrift and ezbake-common-python locally

```
export REPO_URL=<ex: https://github.com>
git clone "${REPO_URL}/ezbake/ezbake-thrift.git"
git clone "${REPO_URL}/ezbake/ezbake-common-python.git"

cd ezbake-thrift
mvn clean package

projects=( \
./base/target/thrift-gen/python \
./configuration/target/thrift-gen/python \
./security/ezsecurity-services/target/thrift-gen/python \
../ezbake-common-python/configuration \
../ezbake-common-python/discovery \
../ezbake-common-python/thrift/thrift-utils \
../ezbake-common-python/security \
)

for project in ${projects[@]}; do
    pushd $project ; pip install . ; popd
done
```
