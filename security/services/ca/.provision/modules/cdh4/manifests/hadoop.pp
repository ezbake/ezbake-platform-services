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

class cdh4::hadoop {

  class { 'cdh4::hadoop_client': }

  $pkgs = ['hadoop-0.20-conf-pseudo']
  $svcs = ['hadoop-hdfs-datanode','hadoop-hdfs-namenode','hadoop-hdfs-secondarynamenode']

  package { $pkgs:
    ensure => latest,
    require => File['cdh4_repo']
  }

  service { $svcs:
    ensure => running,
    enable => true,
    require => [Package[$pkgs], Package['hadoop-client'], Exec['hdfs_init']],
  }

  file { 'hdfs_init_script':
    ensure => present,
    path => '/tmp/hdfs_init.sh',
    mode => 0700,
    source => 'puppet:///modules/cdh4/hdfs_init.sh',
  }

  exec { 'hdfs_init':
    command => '/tmp/hdfs_init.sh',
    require => [
      File['hdfs_init_script'],
      Package[ $pkgs ]],
  }

}
