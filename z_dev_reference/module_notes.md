Common ToDo:
    - Flag to check version information -- after flag for this do the task for getting the config file also
    - Add Build details like D30    
    
Doubts:    
    - How are handling the App exits? when and how is the application exiting?
    - What about other jobs that are writing stuff to files are stuff when you do these exits
    - How convinient is it going to manage this application when you deploy 10 - 15 of these workers

#####app_config/app_config.go
- Load the initial configuration file for the application and populate the settings in the Config struct
- Handle different drives for windows

struct appConfiguration
struct cassandra
struct postgresxl
struct kafka
struct AppConfig
    
   Functions:
        init() - Call GetConfiguration()
        GetConfig() (AppConfig) -- External method that returns Config or calls GetConfiguration() and returns
        GetConfiguration() -- Gets the config file, makes viper and sets the Config object        
        getConfigFile() (retFilePath string, retErrors []error) -- Check windows, linux common folder for file and if STREAM_CONFIG_FILE is set use that (minor todo:2) 
        getAllWindowsDrives() (availDrives []string) -- Loop from A to Z and return valid drives
        fileExists(curFile string) (retBool bool) -- os.Stat a file and return back if it exists
        
Todo:
    Low- Currently there is no support to get flag variables -- some code for this is written but no flags are passed or returned -- flag management will need to happen
                 

#####app_config/api_level.go
- Load the base configuration for the APIs
    - Validate if the mentioned files exist
    - Do other validations on the actual files
    - Parse and populate the files into an []Structs with details
        
   Functions:
        CheckLoadAPIs() ( errs []error ) 
            -- Get list of API files, check for duplicates
            -- Make sure folder exists
            -- Make sure files exist
            -- Set Apis by calling LoadData
        GetApis()( a models.APIsRef ) -- return Apis 
        LoadData() ( a models.APIsRef, err error ) 
            -- Loop over all the files, call validateLoadToml for the file. 
            -- Run a checkApi() to check if the Avro and Query files actually exist
            -- If okay, set the api the temp obj and return it back
        validateLoadToml(folder string, file string)(c models.APIDetails, err error)
            -- Pass the file reference to viper and return back the APIDetails object           
        checkApi( f string, c models.APIDetails ) ( err error )
            -- Check 5 values that should not be blank and throw error if any of them are
            -- Check the 2 files QueryFile and AvroSchemaFile exist                 
        noDuplicatesCheck(files []string)(ret bool)
            -- Make a map[string]struct{} -- add stuff to it and check original and the new length
        folderExistsCheck(path string) (ret bool)
            -- Check if a folder exists by doing os.Stat on it
        filesExistCheck(folder string, files []string) (err []error)
            -- Check an array of files in a folder by os.Stat       
   
Todo:   
    - 
    - Write tests for this 

#####avro_file_manager/avro_file_manager.go

Functions:
    init() -- Init Conf, Api variables and set the StartTime
    getHdfsClient()( hd *hdfs.Client, err error ) -- Make a new client with hdfs user and return back the connection else send back error 
    MoveOldFiles(pids []string, st time.Time) (MovedList []string, err error)
        -- Read the staging folder
        -- Loop over all the files in the staging folder 
        -- Check if you should move files, passing valid pids that can be moved to shouldIMoveFile()
        -- Call moveFileToHDFS on files that have true in the previous step
        -- If file got moved then add the file to an []string for return
    InitialMoveFiles()(err error)
        -- Get the pid of the running app
        -- Get the uniquie pids from the tmp folder
        -- Get the PIDs for the applicatoins that are running right now
        -- Remove current from Unique
        -- Call MoveOldFiles() pass pids and time
    shouldIMoveFile(file os.FileInfo, pids []string, appSt time.Time) (ret bool)
        -- Take the name of the file
        -- Get PID from the name
        -- Check if the name has a Topic reference on it, if not return false
        -- Loop over all the passed pids and check the PID mentioned in this file exists in the list
        -- If so, make sure the file was last updated before this application started -- to make sure it does not clear files that are getting updated by 
            other binaries by mistake.        
    getPidFromName(fn string) (pid string, err error) -- Do a regexp match and extract the PID from its name
    getTopicFromName(fn string) (topic string, err error) -- Run a regexp in the format .*~(.*)~.* and extract the topic name
    RemoveCurFromUniq(allPids []string, currPids []string, myPid string) (retPid []string) -- From all pids, remove curr and mypid and return
    UniquePidsInTmp(f string) (pids []string) -- Get all the PIDs in the tmp directory, loop over them, set it as key to make unique then add to array and return
    currentPids(app string) (pids []string) -- Run a linux command, get the unique processes and return (if command gives error it returns an error)  
    moveFileToHDFS(f os.FileInfo) (res bool, err error) -- TODO: return errors -- File name needs to be checked something is there
    RotateFileLoop(s *time.Time, f **os.File, ow **goavro.OCFWriter, i *int) (err error) -- TODO: This is not used anywhere and has errors
            

#####models/api_details_model.go
Model of the []APIDetails and methods to populate the details object, make the initial files and some util functions

struct APIsRef
    Methods:         
            InitialPopulate()(errs []error) 
                -- If there are duplicate topics, return error
                -- For each API file Load the avro schema file and make a codec from it. Load the Query file as well
                -- Save the AvroSchema, Code and Query to the database for each API 
            MakeFirstFiles( stageFolder string, appPid string )
                -- Loop over each API, For each API make a random uuid, create a file with a certain name -- TODO: Change this to include topic -- return on error
                -- Make new OCF file with the created file -- return on error
                -- Set the Curfile and Ow on the Object from what we just created  
            GetByTopic(topic string) (api APIDetails, err error) -- Loop over all the APIs and return the API that matches the given topic -- if not found set as false
            GetTopics() (topics []string) -- Loop over all the APIs and return the unique topics in it

struct APIDetails
    Methods:
        Inc() -- Increment the RecCounter by 1 
        
struct AvroSchemaParser
Function:
NewAvroSP(s []byte) (ap AvroSchemaParser, err error) 
    -- Takes an []byte from an avro file 
    -- Parses it as JSON first, then loops over the fields and fills the type of field and isArray field
    -- Returns back the AvroSchemaParser object   
        
struct Field
struct InField        
   
   
#####cassandra_client.go
LoadPool() (err error) 
    -- Set common configuration for the cassandra cluster
    -- Connect to cassandra cluster and return on error 
     
GetSession() (retSession *gocql.Session, err error) -- If cassandraSession not loaded then call LoadPool, else return session and return errors if any
Select(pkRef string) ([]map[string]interface{}, error) -- Set consistency as one Run the actual query (This has to be passed dynamically later), return the values
    ToDo: The query here will come dynamically

#####cassandra_source_fetch.go
FetchAndProcessCassandra(message []map[string]interface{}) (gr []interface{})
    - Loop over 'message' 
    - If the Decompress the 'local_service_requests_s' field -- if there is an error in decompressing then the field will be made blank
    - If its not an []string after decompressing make it blank
    - local_service_requests_new_con5_pk is converted to string
    - A variable of lsr_record type struct is created and mapstructure.Decode is called on the values to map them into k
    - lsr_record.getGenericRecord() is called to generate a generic record

Note : 
- Track what are the compressed fields for the API
- What is the PK field for the API -- This will be used in the cassandra query
- UUID fields need to be converted to string

#####generic_factory.go
struct Grf { FieldHash map[string]string }
    Methods:
        PopulateNew(in map[string][]string) (err []error)
            -- callsCheckForDuplicates (This function is pending)
            -- Make an array [fieldname]type  (Supported types string, arr_str, int, timestamp)
        checkForDuplicates(in map[string][]string) (ret bool) - Todo: This is pending
        getTypeByName(s string) (ret string, err error) -- Check if the field exists, if so return else return error
        MakeGr(res map[string]interface{}) (gr []interface{}, errs []error)
            -- This will take a single record from the database, and add to a tmp map[field_name]generic_record<type>
            -- The tmp field will then be returned as []interface{ tmp }

Todo: 
    -- How do you want to save the field references? do you want to directly map stuff one to one on the fly?
    -- Make a chotu sa mapping table with type to *corresponding function
    -- There are some exception fields and extra fields that need to be added -- those will need to be handled


#####generic_record.go
Common functions to return the generic objects: 
    returnGenericString(s string) interface{}
    returnGenericStringArray(s []string) (r interface{})
    returnGenericStringInt(s int64) (r interface{})
    returnGenericTimeString(s int64) interface{}
    returnGenericTimeDiff(tm time.Time, tms int64) interface{}
    
Todo:
    -- No errors are returned back
        
#####kafka_listner.go
getKafkaConsumer()(consumer *cluster.Consumer, err error) 
    -- Init the kafka variables and start a new consumer  
    -- Start 2 anon functions that log error and notification messages
    -- Defer consumer close
     
KafkaListener() (err error) --
    -- Start listener for OS.signal 1 -- This is part of the inifinite loop at the bottom
    -- getKafkaConsumer()
    -- Start tick() channel
    ConsumerLoop:
        -- Start inifinite for loop
        -- Select 
            ticker
                RotateFileLoop(&s, &f, &ow, &i)
            consumer.messasge
                Check if ok
                get the pk                
                Try to fetch from cassandra
                    if err in fetching from cassandra -- continue on the loop (Skip this message) TODO: this is wrong
                If no records are found in cassandra (probably it didn't get synced soon enough)
                Wait 3 seconds and then try again TODO: The 3 seconds is hardcoded right now -- it should be dynamic
                    It does this only one time
                call FetchAndProcessCassandra(message []map[string]interface{}) (gr []interface{}) this gives back a generic record 
                append the record to the ow
                increase the counter
                If number of records reaches the record size for a file call RotateFileLoop
            signal
                Stop the consumer loop that is this loop and the function returns back                
    
TODO: There has to be a decent way to stop this application
    
#####lsr_model.go
struct lsr_record
getGenericRecord() (ret []interface{})
    make a map[string]generictype_record 
    key<string>generic<type>linkedFunction    
        int_created_date, int_created_date_str, 
        int_created_date_cass, int_created_date_cass_str
        int_cass_stream_diff
    
Postgres:
    MakePgQuery(tab string, record map[string]interface{}) (ret string) -- Makes the PG query string based on result of inWhichArr()  
    inWhichArr(str string) (ret string) -- Loops over all the 3 types of arrays to search for the field -- when it finds it -- it returns the heading

#####stream.go
init()  - Call app_config.GetConfiguration(), Populate the Conf variable
func main()
    -- InitializeApp()
    -- testHdfsClient()
    -- LoadPool() to load the Cassandra pool
    -- InitialMoveFiles() 
    -- Apis.MakeFirstFiles()
    -- Start KafkaListener
InitializeApp()
    -- Start logging
    -- setDefaultVals
    -- app_config.CheckLoadAPIs then set Apis = app_config.GetApis()
    -- Apis.InitialPopulate() -- Make the codec and load the query file into the structs
setDefaultVals()(err []error)
    -- Remove the trailing slash on Conf.Kafka.HdfsStagingFolder -- if nothing default it to /tmp
    -- If Conf.Kafka.FlushFrequencyMilliSec is 0 or less than 500 (ms) then set it default to 20000 (20 seconds)
    -- If Conf.Kafka.RecordsPerAvroFile is 0 set it to 50
    -- Make sure there are some kafka brokers mentioned and return error if not
    -- API Config folder should have some value on it and return error if not
    -- API files to load should have some values on it and return error if not 
        
initError(errs []error)
    -- Helper function to fmt.Println an error and also log it and exit 1
    
testHdfsClient()( err error )
    -- Make a connection to HDFS and close it to make sure there is a connection available