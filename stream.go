package main

import (
	"flag"
	"fmt"
	"github.com/dminGod/Stream/app_config"
	"github.com/antigloss/go/logger"
	"time"
)

var Conf app_config.AppConfig



// TODO: - All the os.Exit(1) that is used everywhere is fine for now, but if 2 threads are running on the same binary then that will get messed up?
// TODO: - !! The applicatoin needs to have a graceful shutdown happen so it can complete whatever its doing and then exit
// TODO: - !! If a file is written to disk and then not moved to HDFS then it should get moved to HDFS

// TODO: - !! Where is the crack, where are the messages failing -- How are we handling the failing messages?
// TODO: - !! Write tests for all the main functions

// Done: - You can actually unify the whole thing

func main() {

	fmt.Println("Starting application..")

	// Set the configuration object
	app_config.GetConfiguration()
	Conf = app_config.GetConfig()

	logFolder := Conf.Stream.StreamLogFolder

	if len(logFolder) == 0 {

		logFolder = "/var/log/"
	}

	err := logger.Init(Conf.Stream.StreamLogFolder, // specify the directory to save the logfiles
		800, // maximum logfiles allowed under the specified log directory
		20,  // number of logfiles to delete when number of logfiles exceeds the configured limit
		50,     // maximum size of a logfile in MB
		false, // whether logs with Trace level are written down
	)
	if err != nil {
		fmt.Println("Error in intializing logger, is : ", err)
	}

	var postMode bool

	//Get the flag for hdfs_push mode
	var pushMode bool
	flag.BoolVar(&pushMode, "hdfs_push", false, `--hdfs_push Will start the binary in a push mode`)

	flag.BoolVar(&postMode, "post_push", false, `--post_push Will start the binary in a postgres push mode`)
	flag.Parse()

	if postMode == true{

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
		err := KafkaListener()
		if err != nil {
			logger.Error(fmt.Sprintf("There was an error running the listener: %v", err))
		}
	}

	logger.Info("Exiting application")
}
