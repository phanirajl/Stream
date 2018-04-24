package main

import (
	"fmt"
	"github.com/antigloss/go/logger"
	"github.com/colinmarc/hdfs"
	"github.com/dminGod/Stream/app_config"
	"os"
	"strconv"
	"errors"
	"strings"
	"github.com/dminGod/Stream/avro_file_manager"
	"github.com/dminGod/Stream/models"
)

var Conf app_config.AppConfig
var Apis models.APIsRef

// TODO: - !! The applicatoin needs to have a graceful shutdown happen so it can complete whatever its doing and then exit
// TODO: - !! Write tests for all the main functions

// TODO: The File name should have the topic name in it -- only then will you be able to figure out what table the file belongs to when you see orphans
// TODO: There needs to be a common place to be able to get the API related stuff -- we should do it like the config module -- You will need to do lookups on the topic name from old orphans


func init() {

	app_config.GetConfiguration()
	Conf = app_config.GetConfig()

	return
}

func main() {

	InitializeApp()

	var err error

	// Connect to HDFS
	err = testHdfsClient()
	if err != nil {
		initError([]error{err})
	}

	logger.Info(fmt.Sprintf("Loading cassandra pool"))
	err = LoadPool()
	if err != nil {
		initError([]error{err})
	}

	err = avro_file_manager.InitialMoveFiles()
	if err != nil {
		initError([]error{err})
	}

	pid := strconv.Itoa( os.Getpid() )
	Apis.MakeFirstFiles( Conf.Kafka.HdfsStagingFolder, pid )

	// Start the actual listening
	err = KafkaListener()
	if err != nil {
		initError([]error{ err })
	}

	logger.Info("Exiting application")
}

func InitializeApp(){

	fmt.Println("Starting application..")
	fmt.Println("Logfolder is : " , Conf.Stream.StreamLogFolder)
	fmt.Println("Configuration is : " , Conf)

	var errs []error

	if len(Conf.Stream.StreamLogFolder) == 0 {
		Conf.Stream.StreamLogFolder = "/var/log/"
	}

	// Start the logging
	err := logger.Init( Conf.Stream.StreamLogFolder,	800, 20, 50,	false )
	if err != nil {
		fmt.Println("Couldn't initialize logger, Error : ", err)
	}
	logger.SetFilenamePrefix("stream.%P.%U", "stream.%P.%U")


	logger.Info("Setting default config values")
	errs = setDefaultVals()
	if len(errs) > 0 {
		initError(errs)
	}

	logger.Info("Loading API level configuration")
	errs = app_config.CheckLoadAPIs()
	if len(errs) > 0 {
		initError(errs)
	}

	Apis = app_config.GetApis()

	// Populate the AvroSchema and Codec
	// File and OW need to be still populated
	errs = Apis.InitialPopulate()
	if len(errs) > 0 {
		initError(errs)
	}
}

func setDefaultVals()(err []error) {

	Conf.Kafka.HdfsStagingFolder = strings.TrimRight(hdfsStagingFolder, "/")

	if Conf.Kafka.HdfsStagingFolder == "" {
		Conf.Kafka.HdfsStagingFolder = "/tmp"
	}

	// By default if the frequency is not set
	//  make it 20 seconds
	if Conf.Kafka.FlushFrequencyMilliSec == 0 || Conf.Kafka.FlushFrequencyMilliSec <= 500 {

		Conf.Kafka.FlushFrequencyMilliSec = 20000

		// Why are you flushing files so often?
		if Conf.Kafka.FlushFrequencyMilliSec <= 500 {

			logger.Warn("Flush frequency set to less than 500ms, will not allow less than 1 second -- set frequency to default")
		}
	}

	if Conf.Kafka.RecordsPerAvroFile == 0 {
		Conf.Kafka.RecordsPerAvroFile = 50
	}

	if len(Conf.Kafka.KafkaBrokers) == 0 {

		err = append(err, errors.New(fmt.Sprintf("No kafka brokers listed in config -- Entry : '%v' ", Conf.Kafka.KafkaBrokers)))
	}

	if len(Conf.Kafka.ApiConfigFolder) == 0 {

		err = append(err, errors.New(fmt.Sprintf("API Config folder is blank -- Entry : '%v' ", Conf.Kafka.ApiConfigFolder)))
	}

	if len(Conf.Kafka.ApiFilesToLoad) == 0 {

		err = append(err, errors.New(fmt.Sprintf("No API files to load configured 'ApiFilesToLoad' -- Entry : '%v' ", Conf.Kafka.ApiFilesToLoad)))
	}

	return
}

func initError(errs []error){

	for _, v := range errs {
		fmt.Println("Initialization error : ", v.Error())
		logger.Error("Error in setting initial vals : '%v'", v.Error())
	}
	os.Exit(1)
}


func testHdfsClient()( err error ) {

	hd, err := hdfs.NewClient(hdfs.ClientOptions{Addresses: []string{Conf.Kafka.HDFSConnPath}, User: "hdfs"})
	if err != nil {
		err = errors.New(fmt.Sprintf("Could not connect to HDFS, Config: %v, Error: %v", Conf.Kafka.HDFSConnPath, err))
		hd = nil
		return
	} else {
		hd.Close()
	}

	return
}

/*
Postgres connection logic -- if needed later :
	if postMode == true {

		logger.SetFilenamePrefix("post.%P.%U", "post.%P.%U")
		logger.Info("Starting postgres push from stream")
		LoadPostgres()
		time.Sleep(5 * time.Second)
		PostgresPush()

	} else {

		logger.SetFilenamePrefix("hdfs.%P.%U", "hdfs.%P.%U")

		logger.Info(fmt.Sprintf("Starting Cassandra listen from stream"))
		cassErr := LoadPool()
		if cassErr != nil {
			logger.Error(fmt.Sprintf("There was an error in loading Cassandra: %v", cassErr))
		}


 */


