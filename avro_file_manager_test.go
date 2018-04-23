package main

import (
	"testing"
	"github.com/dminGod/Stream/app_config"
	"fmt"
	"time"
)

var validPids = []string{"1234", "2234"}


func TestRotateFileLoop(t *testing.T) {

	app_config.Config.Kafka.HDFSConnPath = "10.1.1.1"
	app_config.Config.Kafka.HdfsStagingFolder = "testdata/pid_files/"
	app_config.Config.Stream.StreamLogFolder = "testdata/logs/"

	fmt.Println(app_config.Config.Stream.StreamLogFolder, " <--- Log folder")

	pidsFound := UniquePidsInTmp("testdata/pid_files/")

	for _, v := range validPids {

		var ok bool

		for _, vv := range pidsFound {

			if v == vv {
				ok = true
			}
		}

		if ok == false {
			t.Errorf("Could not get the pid file we were looking for %v", v)
		}
	}

}

func TestMoveOldFiles(t *testing.T) {

	app_config.Config.Kafka.HDFSConnPath = "10.1.1.1"
	Conf.Kafka.HdfsStagingFolder = "testdata/pid_files/"
	app_config.Config.Stream.StreamLogFolder = "testdata/logs/"
	appStartedTime = time.Now()

	moved, _ := MoveOldFiles(validPids)

	if len(moved) > 0 {
		t.Error("Moving files to HDFS on test environment?")
	}
}

