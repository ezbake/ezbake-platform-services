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

Artifacts are output in the folder /vagrant/BUILD