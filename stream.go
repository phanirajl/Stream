package main

import (
	"flag"
	"fmt"
	"github.com/dminGod/Stream/app_config"
)

var Conf app_config.AppConfig

	// TODO: - Change the fmt.Println to proper logging
	// TODO: - Use the same logging from D30 if needed, its customized as per the requirement
	// TODO: - Write tests for all the main functions
	// TODO: - Govendor the application so the libs are all integrated into the applicatoin

func main() {

	fmt.Println("Starting application")

	// Set the configuration object
	app_config.GetConfiguration()
	Conf = app_config.GetConfig()

	// Get the flag for hdfs_push mode
	var pushMode bool
	flag.BoolVar(&pushMode, "hdfs_push", false, `--hdfs_push Will start the binary in a push mode`)
	flag.Parse()

	if pushMode == false {

		fmt.Println("Starting Cassandra listen from stream")
		LoadPool()
		err := KafkaListener()
		if err != nil {
			fmt.Println(fmt.Sprintf("There was an error running the listener: %v", err))
		}
	} else {

		fmt.Println("Starting HDFS push from stream")
		HdfsPush()
	}

	fmt.Println("Exiting application")
}
