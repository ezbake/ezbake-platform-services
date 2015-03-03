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

class base {

 $pkgs = [
    'git',
    'vim-enhanced',
    'wget',
    'mlocate',
    'java-1.7.0-openjdk-devel',
    'tree',
    'rubygems',
    'zlib-devel',
    'puppet',
    'hiera',
  ]

  package { $pkgs:
    ensure => latest
  }

  # install puppet conf file
  file { 'puppet.conf':
    path => '/etc/puppet/puppet.conf',
    owner => puppet,
    group => puppet,
    mode => 0644,
    content => template('base/puppet/puppet.conf.erb'),
    require => Package['puppet'],
  }

  file { 'vagrant_sudo':
    ensure => present,
    path => '/etc/sudoers.d/vagrant',
    owner => root,
    group => root,
    mode => 0600,
    content => 'vagrant ALL=(ALL)NOPASSWD: ALL'
  }

  # Create puppet master on the first node
  if $::vagrant_id == 0 {

    package { 'puppet-server':
      ensure => latest,
    }

    file { 'autosign.conf':
      path => '/etc/puppet/autosign.conf',
      owner => puppet,
      group => puppet,
      mode => 0600,
      content => template('base/puppet/autosign.conf.erb'),
      require => Package['puppet-server'],
    }

    service { 'puppetmaster':
      enable => true,
      ensure => running,
      subscribe => File['autosign.conf', 'puppet.conf'],
      require => Package['puppet-server'],
    }
  }
}
