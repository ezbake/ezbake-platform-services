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

class cdh4::accumulo14(
  $accumulo_script = "/usr/bin/accumulo",
  $accumulo_zookeepers = "localhost:2181",
  $instance = "accumulo",
  $rootpass = "password",
  $rootauths = "U",
  $username = "accumulo",
  $userpass = "accumulo",
  $userauths = "U",
  $tables = "test"
) {
  class { 'cdh4::hadoop_client': }

  $hadoop_namenode = $cdh4::hadoop_client::hadoop_namenode

  $packages = [
    'accumulo-master',
    'accumulo-monitor',
    'accumulo-gc',
    'accumulo-tracer',
    'accumulo-logger',
    'accumulo-tserver',
  ]

  file { 'accumulo_repo':
    ensure => present,
    path => '/etc/yum.repos.d/cloudera-accumulo.repo',
    source => 'puppet:///modules/cdh4/cloudera-accumulo.repo'
  }

  package { $packages:
    ensure => 'installed',
    require => File['accumulo_repo']
  }

  service { $packages:
    enable => true,
    require => Package[$packages],
    subscribe => File['accumulo_defaults', '/etc/accumulo/conf/'],
  }

  file { '/etc/accumulo/conf/':
    ensure => directory,
    recurse => true,
    purge => true,
    source => 'puppet:///modules/cdh4/accumulo/conf',
    require => Package[$packages],
  }
  file { '/etc/accumulo/conf/accumulo-site.xml':
    ensure => present,
    owner  => root,
    group  => root,
    mode   => '0664',
    content => template('cdh4/accumulo/conf/accumulo-site.xml'),
    require => File['/etc/accumulo/conf/'],
  }
  file { 'accumulo_defaults':
     path   => '/etc/default/accumulo',
     ensure => file,
     owner  => root,
     group  => root,
     mode   => '0664',
     source => 'puppet:///modules/cdh4/accumulo/accumulo'
  }
  file { 'accumulo_provision_script':
    path => '/var/lib/accumulo/provision.sh',
    ensure => file,
    owner => accumulo,
    group => accumulo,
    mode => 755,
    content => template('cdh4/accumulo/init_accumulo_instance.sh.erb'),
    require => Package[$packages],
    notify => Exec['init_accumulo'],
  }
  exec { 'init_accumulo':
    command => "/var/lib/accumulo/provision.sh",
    refreshonly => true,
    logoutput   => true,
    timeout => 1800,
    require => [File['accumulo_provision_script'], Package['hadoop-client'], File['/etc/accumulo/conf/accumulo-site.xml']]
  }

}
