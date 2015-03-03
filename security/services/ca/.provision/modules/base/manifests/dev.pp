/*   Copyright (C) 2013-2015 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

class base::dev {

  $dev_pkgs = [
    'python-devel',
    'boost-devel',
    'libevent-devel',
    'pcre-devel',
    'openssl-devel',
    'libedit',
    'libtool',
    'byacc',
    'flex',
    'apr-devel',
    'apr-util-devel',
    'rpm-build',
    'readline-devel',
    'ruby-devel',
  ]

  $gems = [
    'fpm'
  ]

  package { $dev_pkgs:
    ensure => latest
  }

  package { $gems:
    ensure => latest,
    provider => gem,
    require => Package['ruby-devel', 'rubygems'],
  }

  file { "/usr/lib64/libboost_thread.so":
    ensure => link,
    target => "/usr/lib64/libboost_thread-mt.so"
  }

}
