### Overview

The purpose of Replay is to rebroadcast old data. It sits on top of the Data Warehouse and re-broadcasts previously ingested data to 
specified topics (like SSR by data range, like 90 days, and feed name determined at registration). 
This allows a new app (that uses an existing pipeline) to ingest historic data from
the last day, week, month, etc. Note that replaying this data on an existing topic will rebroadcast the data to
any existing applications that also listen to the topic (so it's currently on the individual apps to handle duplicate data).

### Pre-requisites
EzSecurity, EzFrontend, Data Warehouse, Replay service and the EzBroadcast libraries (kafka).

### Replay-UI
The Replay service has a webapp, Replay-UI.
The UI provides an interface where the feed-name, date range, topics to be broadcast to can be specified.
In order to access the Replay-UI, you will need to be added to the EzBake Admin group.
