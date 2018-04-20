package main

import (
	"fmt"
	"os"
	"time"
	"io/ioutil"
	"github.com/antigloss/go/logger"
	"regexp"
	"bytes"
	"os/exec"
	"strings"
	"github.com/linkedin/goavro"
	"github.com/gocql/gocql"
	"strconv"
)

func moveOldFiles(pids []string) {

	files, err := ioutil.ReadDir(fmt.Sprintf("/%v/", hdfsStagingFolder))
	if err != nil {
		logger.Error("Error reading hdfs dir for initial moving files", err)
		return
	}

	for _, file := range files {

		if shouldIMoveFile(file, pids, appStartedTime) {
			moveFileToHDFS(file)
		}
	}
}

// File should be in the PID list and the last modified time of this file should be after
// this app has started
func shouldIMoveFile(file os.FileInfo, pids []string, appSt time.Time) (ret bool) {

	fn := file.Name()
	pid := getPidFromName(fn)

	for _, v := range pids {

		if v == pid {

			// Modification of this file should be
			// before the current application started
			if file.ModTime().Before(appStartedTime) {
				ret = true
			}
		}
	}

	return
}

func getPidFromName(fn string) (pid string) {

	re := regexp.MustCompile(`^pid(\d+).*`)

	mat := re.FindAllStringSubmatch(fn, -1)

	if len(mat) != 0 {
		if len(mat[0]) > 1 {
			cv := mat[0][1]
			pid = cv
		}
	}

	return
}

func RemoveCurFromUniq(allPids []string, currPids []string, myPid string) (retPid []string) {

	// Loop over all the pids
	for _, v := range allPids {

		var inCurPids bool

		// If it is in the current pid, dont do anything
		// If it is its own pid, don't do anything -- this is weird, before starting you are not going to
		// write files, how did this happen
		for _, vv := range currPids {
			if v == vv {
				inCurPids = true
			}
		}

		if inCurPids || v == myPid {

			continue
		} else {

			// Add the PID to the ret
			retPid = append(retPid, v)
		}
	}

	return
}


func uniquePidsInTmp(f string) (pids []string) {

	fs, err := ioutil.ReadDir(hdfsStagingFolder)
	if err != nil {
		logger.Error("Error in fetching dir contents from hdfsStagingFolder : '%v' -- Error : %v", hdfsStagingFolder, err)
		return
	}

	tmp := make(map[string]struct{})

	for _, f := range fs {

		cv := getPidFromName(f.Name())

		if len(cv) > 0 {
			tmp[cv] = struct{}{}
		}
	}

	for k, _ := range tmp {
		pids = append(pids, k)
	}

	return
}

// td : Name of the Applicatoin is hardcoded -- that needs to change to dynamic from the Conf file
func currentPids(app string) (pids []string) {

	buf := bytes.NewBuffer([]byte{})

	c := exec.Command(`/bin/bash`, `-c`, `ps aux | grep "[s]tream" | awk ' { print $2 } '`)

	c.Stdout = buf
	err := c.Run()

	if err != nil {

		fmt.Println("Error", err)
	}

	outArr := strings.Split(buf.String(), "\n")
	tmp := make(map[string]struct{})

	for _, v := range outArr {

		cur := strings.TrimSpace(v)
		if len(cur) > 0 {

			tmp[cur] = struct{}{}
		}
	}

	for k, _ := range tmp {
		pids = append(pids, k)
	}

	return
}

func moveFileToHDFS(f os.FileInfo) (res bool) {

	removeFolder := "/" + hdfsStagingFolder + "/"

	fn := removeFolder + f.Name()
	rfn := strings.Replace(fn, removeFolder, "", 1)

	err := hdfsClient.CopyToRemote(fn, Conf.Kafka.HdfsDatabaseFolder+"/"+rfn)
	if err != nil {

		e := err.Error()
		logger.Error("Error moving file to HDFS, '%v'", e)

		if strings.Contains(e, " file already exists") {

			logger.Info("File exists, removing from local")
			os.Remove(fn)
		} else {

			os.Exit(1)
			res = false
		}
	} else {
		os.Remove(fn)
	}

	return
}


func rotateFileLoop(s *time.Time, f **os.File, ow **goavro.OCFWriter, i *int) {

	if *i == 0 {
		return
	}

	fn := (*f).Name()
	rfn := strings.Replace(fn, "/tmp/", "", 1)
	(*f).Close()

	err := hdfsClient.CopyToRemote(fn, Conf.Kafka.HdfsDatabaseFolder+"/"+rfn)
	if err != nil {
		logger.Error("Error moving file to HDFS, %v", err)
		os.Exit(1)
	} else {
		os.Remove(fn)
	}

	t := strconv.Itoa(int(time.Now().Unix()))
	ran, err := gocql.RandomUUID()
	if err != nil {
		logger.Error("Error generating random UUID : %v ", err)
	}

	*f, err = os.Create(fmt.Sprintf("%v/pid%v_%v_%v.avro", hdfsStagingFolder, myPid, ran.String(), t))
	if err != nil {
		logger.Error("Error creating file : %v", err)
		os.Exit(1)
	}

	*ow, err = goavro.NewOCFWriter(goavro.OCFConfig{W: *f, CompressionName: "snappy", Schema: codec.Schema()})
	if err != nil {
		logger.Error("Could not create OW file : %v  -- Error:  %v", (*f).Name(), err)
		os.Exit(1)
	}

	*i = 0
	*s = time.Now()
}

