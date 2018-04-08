# Stream

This application is meant to sync Cassandra tables with Hive tables in close to real-time.

The application mainly uses : 
- Cassandra Triggers to push PK events to Kafka topics
- Stream binary to :
    - Listen to these events, 
    - Fetch the relevant data from cassandra
    - Batch data into compressed Avro files
    - Push these files to HDFS 

![alt text](https://raw.githubusercontent.com/dminGod/Stream/PushHDFS/z_dev_reference/references/stream.jpg "Architecture Overview")

    
##### Flow of data:
1) A trigger is created on Cassandra -- this trigger inserts the primary key of the record being inserted into a Kafka topic.
2) For every table that needs to be synced from Cassandra to Hive there is one configuration file that has the mapping, target and
kafka topic details.
3) Stream takes all the configuration files and listens to the kafka topics that are going to get the cassandra primary key.
4) As soon as it gets a primary key from Kafka, it runs a query on Cassandra to fetch the record.
5) The record is mapped as per the configuration and converted to Avro using snappy compression.
6) Records are appended into an Avro file.
7) Once a batch (200 records at a time) are written to a file or if an Avro file is 20 seconds old the Avro file is moved to HDFS.
8) Records can now be queried from Hive.


- Each binary can handle multiple tables at the same time.
- You can deploy as many binaries as per your throughput needs. The distribution of work will get handled by Kafka.



This is how you would deploy this application :

1) Create a trigger on the cassandra table or tables that you want synced into Hive.
2) Add the correct references in the toml file.
3) Set the configuration in the /etc/stream.toml
4) Create your Hive tables as per the Avro schema.
4) Run one of more Stream binaries as per your throughput needs.

  
