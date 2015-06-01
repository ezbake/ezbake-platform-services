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

# -*- coding: utf-8 -*-
"""
Created on Mon Apr 14 12:00:45 2014

@author: jhastings
"""
from setuptools import setup, find_packages

setup(
    name='EzThriftPool',
    version='2.0',
    description='EzThriftPool provides connection pooling for thrift clients',
    author='Jeff Hastings',
    author_email='jhastings@42six.com',
    packages=find_packages('lib'),
    package_dir={'': 'lib'},
    install_requires=[
        'thrift==0.9.1',
        'nose==1.3.0',
        'EzPz==2.0'
    ]
)
