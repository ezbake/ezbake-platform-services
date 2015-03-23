#   Copyright (C) 2013-2014 Computer Sciences Corporation
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

from setuptools import setup, find_packages
app = find_packages('lib')

setup(
    name='ezca',
    version='2.0',
    description='ezbake ca service',
    author='Jeff Hastings',
    author_email='jhastings@42six.com',
    url='none yet',
    packages=app,
    package_dir={
        '': 'lib',
    },
    include_package_data=True,
    scripts=['bin/ezcaservice.py'],
    dependency_links=[
        'git+ssh://git@<GIT SERVER LOCATION HERE>/ezbake-base-thrift.git@2.0#egg=ezbake-base-thrift-2.0',
        'git+ssh://git@<GIT SERVER LOCATION HERE>/ezbake-configuration-api.git@2.0#egg=EzConfiguration-2.0',
        'git+ssh://git@<GIT SERVER LOCATION HERE>/ezbake-discovery.git@2.0#egg=ezdiscovery-2.0'
    ],
    install_requires=[
        'thrift==0.9.1',
        'nose==1.3.0',
        'PyOpenSSL==0.13.1',
        'pycrypto==2.6.1',
        'kazoo',
        'ezbake-configuration>=2.1rc1.dev',
        'ezbake-discovery>=2.1rc1.dev',
        'ezbake-base-thrift>=2.1rc1.dev',
        'ezsecurity-services>=2.1rc1.dev',
        'ezbake-thrift-utils>=2.1rc1.dev',
        'EzPersist==2.0'
    ]
)
