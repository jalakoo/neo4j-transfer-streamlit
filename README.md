# Neo4j Transfer Streamlit

Simple app for transferring all or specific Nodes and Relationships from one Neo4j database to another.

Leverages the [`neo4j-uploader`](https://pypi.org/project/neo4j-uploader/) package which batch transfers using lists and the Cypher UNWIND function. This package is faster than using only MATCH/MERGE Cypher statements, doesn't require the [APOC library](https://neo4j.com/labs/apoc/4.0/overview/apoc.load/), but is not as fast as loading a completely new database like as is the Neo4j Admin Tool's [Import](https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import/) feature.

![alt text](https://res.cloudinary.com/dqjkf4zsf/image/upload/c_scale,w_800/v1720313022/neo4j-transfer-streamlit_ieaujq.png "Neo4j Transfer Tool UI - Completed Transfer")

_Example transfer from sandbox.neo4j.com's Recommendation dataset to a local database instance_

A Streamlit Cloud version of this app exists at: https://neo4j-transfer.streamlit.app

However, the cloud version can not export from or to a local Neo4j database (ie one running from [Neo4j Desktop](https://neo4j.com/docs/desktop-manual/current/)). Instead see the ##Usage section below for running locally.

**NOTE:** The `neo4j-uploader` package doesn't yet contain a progress callback, so Streamlit's own running animation is the only indicator that a transfer is in progress.

## Requirements

[Poetry](https://python-poetry.org)

## Usage

```
poetry install
poetry run streamlit run neo4j_transfer_streamlit/main.py
```

## Options

### Default Properties

The Transfer package will automatically add the following properties to all transferred Nodes and Relationships:

- \_original_element_id
- \_transfer_timestamp

The `_original_element_id` value will be the [elementId](https://neo4j.com/docs/cypher-manual/current/functions/scalar/#functions-elementid) of the original Node or Relationship that was transferred

The `_transfer_timestamp` value will be the ISO-8601 datetime string of when the transfer request began.

Enabling Advanced Options allows for the direct modification of these source and target properties should an alternate source or target key be desired.

### Purge Target Database

There is a checkbox to purget the target database before starting a transfer. Warning, there is no way to undo this wipe once executed, so be sure to have a recent backup .dump file of your data before proceeding.

Aura instances automatically back up each day, but an immediate backup can be created if there is more recent data to record. See xxx for more details.

Local Desktop database instances will need to be shutdown, then a Clone or Dump of the database can be generated through the Desktop app. See xxx for more details.

### Undo

The expandable sidebar contains a transfer log of all the transfer executed during a session. By selecting the undo button on any of the listed transfers will wipe the transfered data from the target database. Note it may display an error if data from that transfer has already been removed (by a previous undo or if the `Purge Target Database` option above was used)
