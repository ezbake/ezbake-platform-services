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

class base::hiera {

  $pkgs = ['puppet', 'hiera']

  file { 'hiera.yaml':
    path => '/etc/puppet/hiera.yaml',
    owner => 'puppet',
    group => 'puppet',
    mode => '0644',
    source => 'puppet:///modules/base/puppet/hiera.yaml',
    require => Package[$pkgs],
  }

  file { '/etc/puppet/hieradata':
    ensure => directory,
    owner => 'puppet',
    group => 'puppet',
    mode => '0755',
    require => Package[$pkgs],
  }

  file { '/etc/puppet/hieradata/common.yaml':
    owner => 'puppet',
    group => 'puppet',
    mode => '0644',
    content => template('base/puppet/common.yaml.erb'),
    require => File['/etc/puppet/hieradata']
  }

  file { '/etc/puppet/hieradata/hosts.yaml':
    owner => 'puppet',
    group => 'puppet',
    mode => '0644',
    source => '/vagrant/.provision/conf/hieradata/hosts.yaml',
    require => File['/etc/puppet/hieradata']
  }
}
