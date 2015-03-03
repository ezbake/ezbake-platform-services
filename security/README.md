# EzBake Security

EzBake Security is the IdAM abstraction layer in EzBake. It provides a thrift service that issues trusted security
tokens.

## Getting Started

The EzBake Security repo is made up of a number of modules

* thrift - EzBake Security Thrift definitions
* common - EzBake Security libraries shared in both client and server
* persistence - A persistence layer (backed by Accumulo) shared by the EzBake Security Service & Registration Service
* registration - The EzBake Security Registration thrift service
* client - Client libraries for EzBake Security in a variety of languages
    * client/maven - java & c++
    * client/nodejs - node js
    * client/python - python
* service - EzBake Security Thrift Service
* test - Integration and Test programs for EzBake Client & Server
* scripts - Service start / stop scripts
* examples - Helpful (hopefully) examples of how to use EzBake Security

All tests have been designed to work without any external dependencies, aside from maven and access to the EzBake
artifact repository.

### Dependencies
#### Dev
* Git
* Maven
* Thrift (0.9.1) - Optional. for compiling thrift IDL into target languages. Compiled sources are currently checked in.

#### Deploy
* Zookeeper (3.4.5)
* Redis
* Accumulo (1.6)


### Generating Thrift Libraries

The EzBake Security & Registration thrift definitions are in __thrift/src/main/thrift__. With the current configuration,
the generated thrift sources are checked in to the appropriate directory under __thrift/src/main__.

When any of the files under __thrift/src/main/thrift__ are modified, the thrift sources should be regenerated. Thrift
code generation is tied into the maven build process, so running the following command will generate language sources:

```bash
$ cd thrift
$ mvn clean compile -P gen-thrift
# don't forget to check in the updated sources
$ git add .
```

### Building EzBake Security RPMs

EzBake Security & Registration must both be deployed as RPMs to a dedicated server (or servers) prior to the deployment
of any apps on EzBake. Building the RPMs is integrated into the maven build process using the profile __rpmbuild__.
After maven finishes, the RPMs will be output to the target directory of the corresponding module under
__rpm/[RPM Name]/RPMS/noarch__.

#### EzBake Security

```bash
$ cd service
$ mvn clean package -P rpmbuild
# RPMs output to target/rpm/EzSecurity/RPMS/noarch
```

#### EzBake Security Registration
```bash
$ cd registration
$ mvn clean package -P rpmbuild
# RPMs output to target/rpm/EzSecurityRegistration/RPMS/noarch
```
