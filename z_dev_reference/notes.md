## Dev Notes

######R&D
- How do you read the .toml file in correctly? 
    - Folder name will come from the main json file
    - Array of names of the toml files will be one of the values 
- Check if the file exists
- Check if the file is a valid toml file
- How do you map a toml file to the API fields and check if you have everything

- How will you map :
    - New struct to the listening of the topics from kafka
    - Figure out what topic the message has come for 
    - Query the correct tables on cassandra for this topic
    - Use the appropriate field bifurcations for this field type 
    - Map the 2 dynamic fields
    - What will the struct of the object look like?
    - What is the structure of the whole application like?
    - What is going to be the flow of the application with the old stuff for cleanup and new stuff to pull data?
    - Where will we stop working os.Exit on what places?

    
######Notes:
- Current reference of a topic name will get removed
- The reference for the push topic should be changed
- Ask for the remote Hive table name instead so you can put that in the schema dynamically
- How are you getting the reference of the primary key?
- Reference to the specific fields has to change... Rather than changing code .. lets just write new functions 

- What are the things you want to use?
    - Query for cassandra    
    - Table to query in Cassandra
    - Schema file
    - Kafka topic to listen to
    - HDFS remote folder 
    - Field references (String, []String, integer, timestamp)
    - Primary column for the Cassandra table
    - Cassandra created date field (timestamp) 

- What is the list of topics I want to listen to on the start of the application? (Take this from the parsed config files)

- Remove the cachara stuff from the Kafka models

Actions:
    Fetch file
    Parse .toml
    Check things :
        Fields
        Primary key
        Cassandra created date
        Does the schema file exist?
        Is the schema file json?
    
       

<<Integrate the exposed configuration as part of the current listener>>

    
######Manage Tables
This will have responsibilities of :
- Check Stuff:
    - Checking if all the toml files mentioned exist
    - If the file exists, is it a valid toml file?    
    - Does it have all the fields that we are looking for?
        - Do the fields have the minimum number of values we are looking for? Of the 4 one should be there (String, []String, integer, timestamp)
        - Primary column from cassandra should be there
        - Cassandra created date field should exist            
    - Run a query on Cassandra and check if the table even exists or not
    - Does the table exist on HDFS?
        
- Populate stuff:
    - Make a struct of what you are expecting to use for each API
    - Parse the object returned by the toml and fill it into the object array for each API

- Expose the populated object as part of the Configuration object






        
    
######Requirement
- Current application just handles one type of API and all that code is written hard code -- all that
configuration should be outside of the golang application.

######Flow of data:
- Query for Cassandra is hardcoded this needs to move to a config file
- []map[string]interface{} is returned by the query
- Fields of the type []byte are compressed by snappy, need to uncompress them and if they dont have 
anything, then make them []string{""}
- UUID is converted to string
- Map the fields of the map[string]interface{} to a golang struct
- Use the golang struct with mapstructure.Decode and put the result of cass into the struct
- Use another method to write map[string(key_name)] : goavro.Union("<type>", value(of correct type)) 
for arrays of string that can have nulls it will be goavro.Union("array", []goavro.Union("string")) 
int64 is long 
- You can use these functions : 
    - returnGenericString(string) 
    - returnGenericStringArray([]string) 
    - returnGenericStringInt(int64) 
    - returnGenericTimeString(int64) 
    - returnGenericTimeDiff(time.Time, int64)
    

    - 'int_created_date' -- takes the current time and inserts data
    - 'int_created_date_str' -- takes the time in string format and adds it
    
    - 'int_cass_stream_diff' is a calculated field from 'int_created_date' (which is cast as int in the query) and the current time
    - 'int_created_date_cass' -- is the actual int_created_date in int64 selected from query (can be handled)
    - 'int_created_date_cass_str' -- is the value of int_created_date_str with field renamed 


######Requirement:
- Currently only one API is handled by this application -- it should be able to take more than one
tables -- We dont want one binary handling only one table -- this will mean running litrelly hundreds
of these binaries

######Requirement:
- graceful shutdown is not handled -- that must be put in place.



#####Done (Archived)
######RnD
- How do you get pid of the own process
- Does the time on the system always match the golang time
- How do you get a list of file names in a particular directory?
- How do you know the last time that a particular file was edited
- How do you convert the command line time into golang time
- Parse a string, and get the PID from the filename
- Given one array of text and another array of text -- remove one []b from []a
- How do you get the process IDs of all the running stream instances?
- Given a particular set of PIDs -- how do you process files of that given PID
- How do you combine the input of 2 channels so you can have a time based channel combined with the messages channel
- How do you identify which messages is from our timed channel and which one is from the actual kafka side

######Tests
- Get your own pid -- should not be blank
- []a []b -- a - b -- should work correctly
- File names return as a set of strings so you can write tests for the PID part and
- Make a struct for the file with name and time so it could be mocked and passed around

######Actions:
- 1: Get own pid
- 2: Get unique pids in existing folder
- 3: Get pids of the running processes
- Remove 3: & 1: pids from 2:
- Move file to remote (current file, remote folder, remote file)


When the Application starts:
######Requirement:
- Any files from older runs, we want to move them into hdfs
- We do not want to however touch any files that have been created by currently running processes

######Solution:
- When the process starts, it will establish its own pid
- When writing files it writes files starting with its own pid in the file name in a parsable format
- It also establishes the time that it started

allpids: - To see what remaining files seem to be left around without parents, lets look at the common directory
here we will pickup the names of all the files available and extract the unique pids from it
- Now we know what pids have things lying around in our folder -- this is going to be our
base list of pid files that we will work with -- this is a conservative approach because
there will be a possibility that between looking at currently running pids and deleting files
if some process starts in between -- we will not be able to identify the file that is an orphan
and a file that is freshly created by a new process that started after we scanned for the pids.
So a sane approach is to look at all the files and get the pids -- this is our base pids that we will
move the files for --- from this base list we will remove any running processes.

- Now we need to establish what processes are running, so we should not be touching any files that are
created by those processes. Get all the running process pids for stream.
- From the step 'allpids:'  -- remove any pids that are found in the currently running processes.
- This smaller list of pids after removing current pids will be the basis for safely moving files
that are orphans.
- In the folder get all the files that are of the orphaned processes -- before moving a file, make sure
the file is not last updated after your process started -- this would mean we could be making a mistake
with this file and it may actually not be an orphan, so don't move that file in that case -- skip it.
- Move files to hdfs

######Requirement:
- If we are writing into a file -- if we are writing to it more than x amount of time we want to move
it to hdfs.
- This should happen correctly even if we are not getting any new records to write the ocf file

######Solution:
- Have a timer based file movement in the background inside the same flow as the select so they do not overlap
