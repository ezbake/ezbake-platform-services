### Overview

Quarantine is a system which allows implementers of ingest pipelines to have a secure holding location for data that failed ingest for any reason. 
Quarantine is composed of three pieces, the Quarantine Service, QuarantineSpout,  and the Quarantine UI. 
The Quarantine Service is a common service which stores quarantined data in ElasticSearch. 
The QuarantineSpout is a Frack component that automatically reaches out to the Quarantine Service to process data 
that has been approved for reingest. 
The Quarantine UI is a web application which is used to access and control the reingest of the data
stored in the Quarantine Service.

### Motivation

The Quarantine system was created to provide a visibility controlled area for sending documents which have failed ingest. 
There was no common area provided by the platform, so before Quarantine all applications had to develop their own strategy 
for failover and error handling in Frack pipelines.

### Quarantine Service and QuarantineSpout

The Quarantine service was designed to index information in a way that facilitated queries necessary to quickly select data to be 
reingested through a Frack pipeline. Data is indexed by the following fields:
* Unique ID: Each time a piece of data is sent to Quarantine, it’s contents (raw bytes) are used to generate a unique ID.
* Event Text: When a piece of data is sent to Quarantine it is correlated with a String describing the error or status change that is occurring. 
This is useful for when an ingest pipeline may handle an exception or error a particular way, and the implementer of that pipeline wants all
errors from that specific type of event to be correlated. This also facilitates bulk approvals of objects to be reingested through the UI.
* Source Pipeline: All data that is sent to Quarantine from a particular Pipeline is correlated to facilitate grouping and queries from the UI.
* Source Pipe: All data that is sent to Quarantine from a particular Pipe (Generator, Worker, Listener) within a Pipeline is correlated.

The QuarantineSpout is a Storm Spout that is attached to all Pipelines that are submitted to Frack. 
This is done transparently without the pipeline implementer needing to write any additional code. 
The QuarantineSpout polls the Quarantine Service at a configured time interval and retrieves all objects that have been approved for reingest.
Each object must be approved by an administrator. The administrator can approve objects in bulk. 
The configuration used for the QuarantineSpout polling time can be defined in EZConfiguration with the `quarantine.spout.sleep.time` 
configuration parameter.