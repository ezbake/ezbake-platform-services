## EzCentos Integration

#### Install YML and Config File

Ensure that the yml file and config file are in the directory named buildpacks/ezprofile, which should be located in the git repository directory. On the offset chance, this is not done for you already, then you should be able to run
the following bash command.

```sh
    cp -r /path/to/ezprofile/deployments/* /path/to/ezcentos/buildpacks/ezprofile
```

#### Prepare Maven Install

In order to run the ezprofile service on ezcentos, you must add ezprofile as a dependency to the pom.xml.  Below is a code snippet with the maven dependency code that should be added under the pom.xml dependencies tag.

```xml
    <dependency>
        <groupId>ezbake</groupId>
         <artifactId>ezprofile-service</artifactId>
         <version>${ezbake.snapshot.version}</version>
         <classifier>jar-with-dependencies</classifier>
    </dependency>
```

#### Run Maven Install

Now, you can run the following command from the /path/to/ezcentos directory, in order to get the ezprofile binary.

```sh
    mvn clean install -U
```

#### Start EzProfile Service

Finally, you ssh into ezcentos using vagrant, and run the following commands

```sh
    sudo /vagrant/scripts/startService.sh ezprofile
```

## Invoking EzProfile Thrift Service

The following is a code snippet, which demonstrates how to invoke an ezprofile thrift service via Java, that is, the searchProfileByName function.  The code snippet will lookup the user profile of the user Gary Drocella, and then print the names of the users found, and note it is possible to have two users with the name Gary Drocella.

```java
try {
    //Search for user profile for a user with the first name "Gary" and last name "Drocella"
    SearchResult up = client.searchProfileByName(ezToken, "Gary", "Drocella");
    // Do important stuff if the search status was successful  
    if(SearchStatus.OK == up.getStatusCode()) { 
        for (Map.Entry<String, UserProfile> profile : up.getProfiles().entrySet()) {
            //Print name of retrieved user profile. 
            logger.debug("Found {}", profile.getValue().getFirstName());  
        }
    }
}
catch(Exception e) {
    logger.error("Error: {}", e);
}
finally {
    //Make sure to always return the client to the thrift client pool.
    clientPool.returnToPool(client);  
}
```

Here is another Java code sample that is invoking the thrift service function searchDnByQuery, which is part of the ezprofile service.  The query format is "<first name> <last name>", and the query does support the wild character *.  The code below will search for the dns with the first names that end with "ary" since the query uses the wildcard * at the beginning and last names that begin with "Droc" since there is a trailing wild card at the end of the last name.


```java

String query = "*ary Droc*";
try {
    //get client from the thrift client pool
    client = clientPool.getClient(ezprofileConstants.SERVICE_NAME, EzProfile.Client.class);
    //search for users that satisfy the above declared query
    SearchResult result = client.searchDnByQuery(ezToken, query);
    
    // Do some asserts : In a Unit Test Scenario 
    Assert.assertEquals(SearchStatus.OK, result.getStatusCode());
    Assert.assertEquals(1, result.getPrincipalsSize());
}
catch(Exception e) {
    logger.error("Error: {}", e);
}
finally {
    // always return the client to the thrift client pool
    clientPool.returnToPool(client);
}
```
