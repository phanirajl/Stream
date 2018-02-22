package main

import (
	"flag"
	"fmt"
	"github.com/dminGod/Stream/app_config"
	"github.com/antigloss/go/logger"
)

var Conf app_config.AppConfig

	// TODO: - Change the fmt.Println to proper logging [done]
	// TODO: - Use the same logging from D30 if needed, its customized as per the requirement [done]
	// TODO: - Write tests for all the main functions
	// TODO: - Govendor the application so the libs are all integrated into the applicatoin
	// TODO: - The applicatoin needs to have a graceful shutdown happen so it can complete whatever its doing and then exit
	// TODO: - All the os.Exit(1) that is used everywhere is fine for now, but if 2 threads are running on the same binary then that will get messed up?

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

	// Get the flag for hdfs_push mode
	var pushMode bool
	flag.BoolVar(&pushMode, "hdfs_push", false, `--hdfs_push Will start the binary in a push mode`)
	flag.Parse()

	if pushMode == false {

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
	} else {

		logger.SetFilenamePrefix("cass.%P.%U", "cass.%P.%U")

		logger.Info("Starting HDFS push from stream")
		HdfsPush()
	}

	logger.Info("Exiting application")
}
