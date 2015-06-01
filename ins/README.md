Internal Name Service
===

Summary
---
This contains the implementation for the Internal Name Service (INS).

INS is responsible for keeping track of which feeds are available in the system, what prefix those feeds are using, what web applications are available, and what feeds those applications are capable of viewing.  INS also keeps track of what services answer Intent Queries and what feeds batch jobs have access to.

Dependencies 
---
ElasticSearch
EzSecurity Service
EzElastic

Running/Configuration
---
Following assumes all the dependencies are already running.
INS is a thrift service that is run using Java ThriftRunner.  The following command will start INS as a common_service, with the service name of "ins" using the security id "client" on port 13003:
```
java -jar ezbake-thrift-runner.jar -j ins-thrift-impl-jar-with-dependencies.jar -s ins -x client -D ezbake.security.app.id=client -p 13003
```
Notice the jar must be a jar-with-dependencies.

INS is also capable of running in dev mode.  This mode doesn't require any of the dependencies but all of the responses must be pre-configured in a properties file.
Not every method is available in dev mode. 
To activate dev mode:
```
ins.use.dev.mode=true
```

Example dev mode configurations (these are currently the only methods that are supported by dev mode):
```
ins.dev.mode.appsThatSupportIntent.myIntent=[{"applicationName":"MyFirstApp", "serviceName":"MyIntentService1"},{"applicationName":"MySecondApp", "serviceName":"MyIntentService2"}]
ins.dev.mode.getWebAppsForUri.NEWS//CNN=[{"appName":"MyFirstApp", "webUrl":"www.firstapp.com", "includePrefix":false},{"appName":"MySecondApp", "webUrl":"www.secondapp.com", "includePrefix":true}]
ins.dev.mode.getChloeWebApps=[{"appName":"MyFirstApp", "webUrl":"www.firstapp.com", "includePrefix":false},{"appName":"MySecondApp", "webUrl":"www.secondapp.com", "includePrefix":true}]
ins.dev.mode.getJobRegistrations.12345=[{"feedName":"CNN", "jobName":"cnnBatch"}]
ins.dev.mode.getSystemTopics=["SSR"]
ins.dev.mode.getAppByName.myTestApp={"appName":"myTestApp", "externalUri":"/myApp", "id":"12345", "poc":"test@test.com", "sponsoringOrganization":"MyOrg"}
ins.dev.mode.getPipelineFeeds=[{"feedName":"myTestFeed", "description":"A feed to test"},{"feedName":"mySecondFeed", "description":"A feed to test two"}]
```   
**`getListeningTopicsForFeed` is also available in dev mode but just returns the FeedName + "Topic1" and FeedName + "Topic2" every time
**`getURIPrefix supports` dev mode but always using a hard-coded dev prefix