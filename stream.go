package main

import (
	"fmt"
	"github.com/antigloss/go/logger"
	"github.com/colinmarc/hdfs"
	"github.com/dminGod/Stream/app_config"
	"os"
	"errors"
	"strings"
	"github.com/dminGod/Stream/avro_file_manager"
	"github.com/dminGod/Stream/models"
)

var Conf app_config.AppConfig
var Apis models.APIsRef

// TODO: - !! The applicatoin needs to have a graceful shutdown happen so it can complete whatever its doing and then exit
// TODO: - !! Write tests for all the main functions

// TODO: Get the config file from flag
// TODO: Graceful shutdown of the application

// ToDO: Errors need to be handled correctly to give more details about what is going on and what to do
// TODO: When is this application going to crash? how is that cycle managed?
// ToDO: How will they set the log level?
// ToDO: Version and build for D30 needs to be integrated with this

func init() {

	app_config.GetConfiguration()
	Conf = app_config.GetConfig()

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

	err = Apis.MakeFirstFiles( Conf.Hdfs.HdfsStagingFolder, Conf.Stream.CurrentPid )
	if err != nil {
		initError([]error{err})
	}

	// Start the actual listening
	err = KafkaListner()
	if err != nil {
		initError([]error{ err })
	}

	logger.Info("Exiting application")
}

func InitializeApp() {

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

	Conf.Hdfs.HdfsStagingFolder = strings.TrimRight(hdfsStagingFolder, "/")

	if Conf.Hdfs.HdfsStagingFolder == "" {
		Conf.Hdfs.HdfsStagingFolder = "/tmp"
	}

	// By default if the frequency is not set
	//  make it 20 seconds
	if Conf.Hdfs.FlushFrequencyMilliSec == 0 || Conf.Hdfs.FlushFrequencyMilliSec <= 500 {

		Conf.Hdfs.FlushFrequencyMilliSec = 20000

		// Why are you flushing files so often?
		if Conf.Hdfs.FlushFrequencyMilliSec <= 500 {

			logger.Warn("Flush frequency set to less than 500ms, will not allow less than 1 second -- set frequency to default")
		}
	}

	if Conf.Hdfs.RecordsPerAvroFile == 0 {
		Conf.Hdfs.RecordsPerAvroFile = 50
	}

	if len(Conf.Kafka.KafkaBrokers) == 0 {

		err = append(err, errors.New(fmt.Sprintf("No kafka brokers listed in config -- Entry : '%v' ", Conf.Kafka.KafkaBrokers)))
	}

	if len(Conf.Stream.ApiConfigFolder) == 0 {

		err = append(err, errors.New(fmt.Sprintf("API Config folder is blank -- Entry : '%v' ", Conf.Stream.ApiConfigFolder)))
	}

	if len(Conf.Stream.ApiFilesToLoad) == 0 {

		err = append(err, errors.New(fmt.Sprintf("No API files to load configured 'ApiFilesToLoad' -- Entry : '%v' ", Conf.Stream.ApiFilesToLoad)))
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

func testHdfsClient()( err error ){

	hd, err := hdfs.NewClient(hdfs.ClientOptions{Addresses: []string{Conf.Hdfs.HDFSConnPath}, User: "hdfs"})
	if err != nil {
		err = errors.New(fmt.Sprintf("Could not connect to HDFS, Config: %v, Error: %v", Conf.Hdfs.HDFSConnPath, err))
		hd = nil
		return
	} else {
		hd.Close()
	}

	return
}

