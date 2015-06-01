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

class cdh4::zookeeper {
  class { 'cdh4': }

  $pkgs = ['zookeeper-server', 'zookeeper']
  $svcs = ['zookeeper-server']

  package { $pkgs:
    ensure => latest,
    require => File['cdh4_repo'],
    notify => Exec['init'],
  }

  service { $svcs:
    ensure => running,
    enable => true,
    require => [Package[$pkgs], Exec['init']]
  }

  exec { 'init':
    command => '/sbin/service zookeeper-server init',
    creates => '/var/lib/zookeeper/version-2'
  }

}
