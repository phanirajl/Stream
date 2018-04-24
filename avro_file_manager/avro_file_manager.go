package avro_file_manager

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
	"path"
	"errors"
	"github.com/dminGod/Stream/app_config"
	"github.com/colinmarc/hdfs"
	"github.com/dminGod/Stream/models"
)

var Conf app_config.AppConfig
var StartTime time.Time
var Apis models.APIsRef

func init(){

	Conf = app_config.GetConfig()
	StartTime = time.Now()
	Apis = app_config.GetApis()
}

func getHdfsClient()( hd *hdfs.Client, err error ) {

	hd, err = hdfs.NewClient(hdfs.ClientOptions{Addresses: []string{Conf.Kafka.HDFSConnPath}, User: "hdfs"})
	if err != nil {
		err = errors.New(fmt.Sprintf("Could not connect to HDFS, Config: %v, Error: %v", Conf.Kafka.HDFSConnPath, err))
		hd = nil
		return
	}

	return
}

func MoveOldFiles(pids []string) (MovedList []string, err error) {

	files, err := ioutil.ReadDir(fmt.Sprintf("%v/", Conf.Kafka.HdfsStagingFolder))
	if err != nil {
		logger.Error("Error reading hdfs dir for initial moving files", err)
		return
	}

	for _, file := range files {
		if shouldIMoveFile(file, pids, StartTime) {

			moved, err := moveFileToHDFS(file)

			if err != nil && moved {
				MovedList = append(MovedList, file.Name())
			}
		}
	}

	return
}

func InitialMoveFiles()(err error) {

	// Initial move old files
	myPid := strconv.Itoa(os.Getpid())
	uq := UniquePidsInTmp(Conf.Kafka.HdfsStagingFolder)
	curpids := currentPids(`stream`)
	finalPids := RemoveCurFromUniq(uq, curpids, myPid)
	StartTime = time.Now()

	// Where will you move the old files?? :)
	logger.Info("Attempting to move initial files with PID reference : '%v'", finalPids)
	mList, err := MoveOldFiles(finalPids)
	if err != nil {
		e := fmt.Sprintf("HDFS Error in moving old files")
		err = errors.New(e)
	}

	if len(mList) > 0 {
		logger.Info("Found and moved %v files", len(mList))
	}

	return
}

// File should be in the PID list and the last modified time of this file should be after
// this app has started
func shouldIMoveFile(file os.FileInfo, pids []string, appSt time.Time) (ret bool) {

	fn := file.Name()
	pid, err := getPidFromName( fn )
	if err != nil {
		logger.Error("Checking movement of file no PID File : %v -- Error : %v ", fn, err)
		return false
	}

	_, err = getTopicFromName( fn )
	if err != nil {
		logger.Error("Checking movement of file no File : %v -- Topic : %v", fn, err)
		return false
	}

	for _, v := range pids {

		if v == pid {

			// Modification of this file should be
			// before the current application started
			if file.ModTime().Before(appSt) {
				ret = true
			}
		}
	}

	return
}

func getPidFromName(fn string) (pid string, err error) {

	re := regexp.MustCompile(`^pid(\d+).*`)

	mat := re.FindAllStringSubmatch(fn, -1)

	if len(mat) != 0 {
		if len(mat[0]) > 1 {
			cv := mat[0][1]
			pid = cv
		}
	} else {

		err = errors.New(fmt.Sprintf("No PID reference found in file name : '%v'", fn))
	}

	return
}

func getTopicFromName(fn string) (topic string, err error) {

	re := regexp.MustCompile(`.*~(.*)~.*`)

	mat := re.FindAllStringSubmatch(fn, -1)

	if len(mat) != 0 {
		if len(mat[0]) > 1 {
			cv := mat[0][1]
			topic = cv
		}
	} else {

		err = errors.New(fmt.Sprintf("No Topic reference found in file name : '%v'", fn))
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

func UniquePidsInTmp(f string) (pids []string) {

	fs, err := ioutil.ReadDir(f)
	if err != nil {
		logger.Error("Error in fetching dir contents from hdfsStagingFolder : '%v' -- Error : %v", f, err)
		return
	}

	tmp := make(map[string]struct{})

	for _, f := range fs {

		cv, err := getPidFromName( f.Name() )
		if err != nil {
			logger.Error("Unable to get PID from name File: '%v' Error: '%v'", f.Name(), err)
			continue
		}

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

func moveFileToHDFS(f os.FileInfo) (res bool, err error) {

	removeFolder := "/" + Conf.Kafka.HdfsStagingFolder + "/"

	//fn := removeFolder + f.Name()
	fn := path.Join(Conf.Kafka.HdfsStagingFolder, f.Name())
	rfn := strings.Replace(fn, removeFolder, "", 1)

	topic, err := getTopicFromName( fn )

	a := Apis.GetByTopic(topic)
	hdfsLocation := a.HDFSLocation

	hdfsClient, err := getHdfsClient()

	if hdfsClient == nil {

		logger.Warn("Unable to connect to HDFS, trying again")
		fmt.Println("Trying to connect to HDFS again")
		hdfsClient, err = getHdfsClient()

		if err != nil || hdfsClient == nil {
			logger.Error("Error moving file to HDFS -- Not Connected")
			err = errors.New("Error moving file to HDFS -- Not Connected")
			res = false
			return
		}
	}

	rPath := path.Join(hdfsLocation, rfn)
	logger.Info("Moving initial file to remote File : %v -- Remote File : %v", fn, rPath)
	err = hdfsClient.CopyToRemote(fn, rPath)
	if err != nil {

		e := err.Error()
		logger.Error("Error moving file to HDFS, '%v'", e)

		if strings.Contains(e, " file already exists") {

			logger.Info("File exists, removing from local")
			os.Remove(fn)
		} else {

			res = false
		}
	} else {
		os.Remove(fn)
	}

	return
}


func RotateFileLoop(s *time.Time, f **os.File, ow **goavro.OCFWriter, i *int) (err error) {

	if *i == 0 {
		return
	}

	fn := (*f).Name()
	rfn := strings.Replace(fn, "/tmp/", "", 1)
	(*f).Close()

	// REMOVE Change this!
	err = hdfsClient.CopyToRemote(fn, ""+"/"+rfn)
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

	return
}
