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

	hd, err = hdfs.NewClient(hdfs.ClientOptions{Addresses: []string{Conf.Hdfs.HDFSConnPath}, User: "hdfs"})
	if err != nil {
		err = errors.New(fmt.Sprintf("Could not connect to HDFS, Config: %v, Error: %v", Conf.Hdfs.HDFSConnPath, err))
		hd = nil
		return
	}

	return
}

func moveOldFiles(pids []string, st time.Time) (MovedList []string, err error) {

	files, err := ioutil.ReadDir(fmt.Sprintf("%v/", Conf.Hdfs.HdfsStagingFolder))
	if err != nil {
		logger.Error("Error reading hdfs dir for initial moving files", err)
		return
	}

	for _, file := range files {
		if shouldIMoveFile(file, pids, st) {

			moved, e := moveFileToHDFS(file)
			if e != nil {
				err = e
				return
			}

			if moved {
				MovedList = append(MovedList, file.Name())
			}
		}
	}

	return
}

func InitialMoveFiles()(err error) {

	// Initial move old files
	myPid := strconv.Itoa(os.Getpid())
	uq := UniquePidsInTmp(Conf.Hdfs.HdfsStagingFolder)
	curpids, err := currentPids(`stream`)
	if err != nil {
		return
	}

	finalPids := RemoveCurFromUniq(uq, curpids, myPid)
	StartTime = time.Now()

	// Where will you move the old files?? :)
	logger.Info("Attempting to move initial files with PID reference : '%v'", finalPids)
	mList, err := moveOldFiles(finalPids, StartTime)
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

// ToDO : Name of the Applicatoin is hardcoded -- that needs to change to dynamic from the Conf file
func currentPids(app string) (pids []string, err error) {

	buf := bytes.NewBuffer([]byte{})

	c := exec.Command(`/bin/bash`, `-c`, `ps aux | grep "[s]tream" | awk ' { print $2 } '`)

	c.Stdout = buf
	err = c.Run()
	if err != nil {

		logger.Error("Could not run command to get the current application PID, Error : '%v'", err)
		return
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

	// removeFolder := "/" + Conf.Kafka.HdfsStagingFolder + "/"

	//fn := removeFolder + f.Name()
	fn := path.Join(Conf.Hdfs.HdfsStagingFolder, f.Name())
	rfn :=  f.Name() // strings.Replace(fn, removeFolder, "", 1)

	logger.Info("Moving file to HDFS, local rfn : '%v'", rfn)

	topic, err := getTopicFromName( fn )
	if err != nil {
		return
	}

	a, found := Apis.GetByTopic(topic)
	if found == false {
		s := fmt.Sprintf("No APIs for the topic found in the configuration -- Topic : '' -- Apis : ''")
		err = errors.New(s)
		return
	}

	hdfsLocation := a.HDFSLocation

	hdfsClient, err := getHdfsClient()
	if hdfsClient == nil || err != nil {

		logger.Warn("Unable to connect to HDFS, trying again")
		fmt.Println("Trying to connect to HDFS again")
		hdfsClient, err = getHdfsClient()

		if err != nil || hdfsClient == nil {
			logger.Error("Error moving file to HDFS -- Not Connected -- Error : ", err )
			err = errors.New(fmt.Sprintf("Error moving file to HDFS -- Not Connected", err.Error()))
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
			res = true
			ee := os.Remove(fn)
			if ee != nil {
				logger.Error("Unable to remove file : '%v' got error : err")
			}

		} else {

			res = false
		}
	} else {
		os.Remove(fn)
	}

	return
}

func RotateFileLoop(ap *models.APIDetails) (err error) {

	if ap.RecCounter == 0 {
		return
	}

	fn := (*(*ap).CurFile).Name()
	rfn := strings.Replace(fn, "/tmp/", "", 1)
	(*(*ap).CurFile).Close()

	hdfsClient, err := getHdfsClient()
	if err != nil {
		return
	}

	err = hdfsClient.CopyToRemote(fn, path.Join((*ap).HDFSLocation, rfn))
	if err != nil {
		logger.Error("Error moving file to HDFS, %v", err)
		return
	} else {

		err := os.Remove(fn)
		if err != nil {
			logger.Error(err.Error())
		}
	}

	t := strconv.Itoa(int(time.Now().Unix()))
	ran, err := gocql.RandomUUID()
	if err != nil {
		logger.Error("Error generating random UUID : %v ", err)
	}

	(*ap).CurFile, err = os.Create(fmt.Sprintf("%v/pid%v_~%v~_%v_%v.avro", Conf.Hdfs.HdfsStagingFolder, Conf.Stream.CurrentPid, ap.KafkaTopic, ran.String(), t))
	if err != nil {
		logger.Error("Error creating file : %v", err)
		os.Exit(1)
	}

	(*ap).Ow, err = goavro.NewOCFWriter(goavro.OCFConfig{W: (*ap).CurFile, CompressionName: "snappy", Schema: (*ap).Codec.Schema()})
	if err != nil {
		logger.Error("Could not create OW file : %v  -- Error:  %v", (*(*ap).CurFile).Name(), err)
		os.Exit(1)
	}

	(*ap).ResetCounter()
	(*ap).FileProcessTime = time.Now()

	return
}
