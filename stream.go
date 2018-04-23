package main

import (
	"flag"
	"fmt"
	"github.com/antigloss/go/logger"
	"github.com/colinmarc/hdfs"
	"github.com/dminGod/Stream/app_config"
	"os"
	"strconv"
	"time"
	"errors"
)

var Conf app_config.AppConfig

// TODO: - All the os.Exit(1) that is used everywhere is fine for now, but if 2 threads are running on the same binary then that will get messed up?
// TODO: - !! The applicatoin needs to have a graceful shutdown happen so it can complete whatever its doing and then exit

// TODO: - !! Write tests for all the main functions

func init() {


	app_config.GetConfiguration()
	Conf = app_config.GetConfig()

	hdfsStagingFolder = Conf.Kafka.HdfsStagingFolder
	if hdfsStagingFolder == "" {
		hdfsStagingFolder = "/tmp"
	}


	return
}

func main() {

	fmt.Println("Starting application..")

	// Set the configuration object

	logFolder := Conf.Stream.StreamLogFolder

	fmt.Println("Logfolder is : " , logFolder)

	if len(logFolder) == 0 {
		logFolder = "/var/log/"
	}

	// Start the logging
	err := logger.Init(Conf.Stream.StreamLogFolder, // specify the directory to save the logfiles
		800,   // maximum logfiles allowed under the specified log directory
		20,    // number of logfiles to delete when number of logfiles exceeds the configured limit
		50,    // maximum size of a logfile in MB
		false, // whether logs with Trace level are written down
	)
	if err != nil {
		fmt.Println("Couldn't initialize logger, Error : ", err)
	}

	var postMode bool

	hdfsClient, err = getHdfsClient()

	myPid = strconv.Itoa(os.Getpid())
	uq := UniquePidsInTmp(hdfsStagingFolder)
	curpids := currentPids(`stream`)
	finalPids := RemoveCurFromUniq(uq, curpids, myPid)
	appStartedTime = time.Now()


	_, err = MoveOldFiles(finalPids)
	if err != nil {
		logger.Error("HDFS not connected -- exiting")
		fmt.Println("HDFS not connected -- exiting")
		os.Exit(1)
	}


	errKafkaConf := app_config.CheckLoadData()
	if len(errKafkaConf) > 0 {

		logger.Error("There were errors in loading the API level config details")

		for _, e := range errKafkaConf {
			logger.Error(e.Error())
		}

		fmt.Println("Errors in loading the API config : ", err)
		os.Exit(2)
	}

	logger.Info("Loaded %v API details -- Details : %v", len(app_config.TablesConf), app_config.TablesConf)


	//Get the flag for hdfs_push mode
	var pushMode bool
	flag.BoolVar(&pushMode, "hdfs_push", false, `--hdfs_push Will start the binary in a push mode`)

	flag.BoolVar(&postMode, "post_push", false, `--post_push Will start the binary in a postgres push mode`)
	flag.Parse()

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
		//err := KafkaListener()
		//if err != nil {
		//	logger.Error(fmt.Sprintf("There was an error running the listener: %v", err))
		//}
	}

	logger.Info("Exiting application")
}


func getHdfsClient()(hd *hdfs.Client, err error){

	hd, err = hdfs.NewClient(hdfs.ClientOptions{Addresses: []string{Conf.Kafka.HDFSConnPath}, User: "hdfs"})
	if err != nil {
		logger.Error("Could not connect to HDFS, Config: %v, Error: %v", Conf.Kafka.HDFSConnPath, err)
		err = errors.New(fmt.Sprintf("Could not connect to HDFS, Config: %v, Error: %v", Conf.Kafka.HDFSConnPath, err))
		hd = nil
		// os.Exit(1)
	}

	return
}