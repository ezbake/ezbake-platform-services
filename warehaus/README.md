### Overview
The ezbake warehaus is an Accumulo backed data store that warehouses data in various formats.
It takes advantage of Accumulo's built-in Visibility control capabilities to provide authorization.

### Pre-requisites

Before starting the warehouse service, ensure Hadoop and Accumulo have been started.

The warehouse uses ezbake-permission-iterator to filter results by visibility during scans.
So you need to ensure the iterator is available in the accumulo runtime. You will need to copy the `ezbake-permission-iterator-{version}.jar` 
to accumulo's lib/ext area.

### Running the warehouse on ezcentos
Ensure Hadoop and Accumulo are running in your env.
Refer to Ezbake VM wiki on how to start those services on the Ezbake VM.

`sudo /vagrant/scripts/startService.sh warehaus` to start the warehouse.

### Logging
The application log files for the warehouse service are available on the Ezbake VM at
`/tmp/ezcentos-apps/common_services/common_services_warehaus.log`. This log includes the audit trail information.

### warehouse properties
Below are the warehouse properties and their default values.
warehaus.batch.writer.max.memory=256000000
warehaus.purge.system.visibility=TS&SI&TK
warehaus.splits=a,b,c,d,e,f,0,1,2,3,4,5,6,7,8,9