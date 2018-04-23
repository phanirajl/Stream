#####app_config/api_level.go
- Load the base configuration for the APIs
    - Validate if the mentioned files exist
    - Do other validations on the actual files
    - Parse and populate the files into an []Structs with details
    
Todo:
    - This file only has comments and empty functions does not have actual code
    - What is the structure you want to keep when you get config from the main config file
    - Tests you want to run on the actual file file
    - What should exist per API level? 
        

#####app_config/app_config.go
- Load the initial configuration file for the application and populate the settings.

ToDo:
- There should be a folder reference to where the files are kept
- There should be an array configuration of what files should be loaded for the api level.


#####cassandra_client.go
This file is responsible for connecting to the target cassandra application and 
running the query on cassandra.

Todo:
- Cassandra query needs to come dynamically.

#####cassandra_source_fetch.go
Has only a single function FetchAndProcessCassandra<br/> 
    This does a :<br/>
- snappy decode
- Replace of the values, relaces " with \"
- Converts UUID fields to string

#####generic_factory.go
Produces a struct that takes []map[string]interface and can make a corresponding gr for it.
- Has struct on 

#####generic_record.go
Common functions to handle generic records.

#####hdfs_process.go
Is the main file that has the listener loop for kafka where :
- Each message is processed.
- Appended to the correct avro file
- Pushed to HDFS

#####local_file_manager.go
- File management for local and to push files to HDFS

#####lsr_model.go
Hardcoded bunch of fields that have functions manually mentioned on them

#####postgres.go
Functions : PostgresPush, GenerateQuery, getConnection, LoadPostgres
 
#####stream.go
The init function and main() function

