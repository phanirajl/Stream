package app_config

import (
	"os"
	"strings"
	"fmt"
	"errors"
	"github.com/spf13/viper"
	"github.com/dminGod/Stream/models"
	"io/ioutil"
)

var Apis models.APIsRef

func CheckLoadAPIs() ( errs []error ) {

	var e error

	folder := Config.Kafka.ApiConfigFolder
	files := Config.Kafka.ApiFilesToLoad

	// Make sure we don't have duplicates in the array .. if yes throw an error
	dupCheck := noDuplicatesCheck(files)
	if dupCheck {
		errs = append(errs, errors.New("There were duplicate files found in Kafka.ApiFilesToLoad -- Please check."))
	}

	// Make sure the folder mentioned for the reference exists
	// ApiConfigFolder
	// Make sure each file mentioned in the reference exists
	if folderExistsCheck(folder) == false {
		errs = append(errs, errors.New(fmt.Sprintf("The API configuration folder '%v' could not be located -- Please check.", folder)))
	}

	// If files dont exist throw an error
	for _, e := range filesExistCheck(folder, files) {
		errs = append(errs, e)
	}

	if len(errs) > 0 {
		return
	}

	Apis, e = LoadData()
	if e != nil {
		errs = append(errs, e)
	}

	return
}

func GetApis()(a models.APIsRef) {

	return Apis
}

func LoadData() (a models.APIsRef, err error) {

	folder := Config.Kafka.ApiConfigFolder
	files := Config.Kafka.ApiFilesToLoad

	a = make(map[string]*models.APIDetails)

	// For each file:

	// Open the file, get the contents, close the file
	// Pass these references into a checker to validate if its okay -- return back a base toml return from here
	// If the validator comes back without errors, send the base toml references to a populate function to fill
	// the module struct with the details of the configuration
	for _, f := range files {

		conf, e := validateLoadToml(folder, f)
		if(e != nil) {
			err = e
			break
		}

		e = checkApi(f, conf)
		if e != nil {
			err = e
			break
		}

		fc, e := ioutil.ReadFile(conf.AvroSchemaFile)
		if e != nil {
			err = e
			break
		}

		tm, e := models.NewAvroSP( fc )
		if e != nil {
			err = errors.New(fmt.Sprintf("Error when parsing avro schema to JSON -- Err : %v", e.Error()))
		}

		conf.AvroFields = tm
		a[conf.KafkaTopic] = &conf
	}

	return
}


func checkApi(f string, c models.APIDetails) (err error) {

	multi := len(c.QueryFile) == 0 || len(c.AvroSchemaFile) == 0 || len(c.KafkaTopic) == 0 || len(c.HiveTableName) == 0 || len(c.HDFSLocation) == 0

	if multi {

		err = errors.New(fmt.Sprintf("Some parameters missing from API file : '%v' -- QueryFile: '%v' - AvroSchemaFile: '%v' - KafkaTopic: '%v' - HiveTableName: '%v' - HDFSLocation: '%v'",
			f, c.QueryFile, c.AvroSchemaFile, c.KafkaTopic, c.HiveTableName, c.HDFSLocation))
		return
	}

	_, err = os.Stat(c.QueryFile)
	if err != nil {
		err = errors.New(fmt.Sprintf("Unable to locate the QueryFile : '%v'", c.QueryFile))
		return
	}

	_, err = os.Stat(c.AvroSchemaFile)
	if err != nil {
		err = errors.New(fmt.Sprintf("Unable to locate the AvroSchemaFile : '%v'", c.AvroSchemaFile))
		return
	}

	return
}

/*
	Given the toml contents -- run checks on and return errors if any

 */
func validateLoadToml(folder string, file string)(c models.APIDetails, err error) {

	v := viper.New()
	v.SetConfigName(file)
	viper.AddConfigPath(folder)
	v.SetConfigType("toml")
	err = v.ReadInConfig()
	if err != nil {
		return
	}

	err = v.Unmarshal(&c)
	return
}

func noDuplicatesCheck(files []string)(ret bool) {

	fLen := len(files)
	uq := make(map[string]struct{})

	for _, v := range files {
		uq[v] = struct{}{}
	}

	return fLen == len(uq)
}

func folderExistsCheck(path string) (ret bool) {

	if _, err := os.Stat(path); err == nil {
		// path/to/whatever exists
		ret = true
	}
	return
}

func filesExistCheck(folder string, files []string) (err []error) {

	folder = strings.TrimRight(folder, "/")

	if len(files) == 0 {
		err = append(err, errors.New("No files mentioned in the ApiFilesToLoad"))
	}

	for _, file := range files {

		if len(file) > 0 {
			curFile := fmt.Sprintf("%v/%v", folder, file)

			if _, er := os.Stat(curFile); os.IsNotExist(er) {
				err = append(err, errors.New(fmt.Sprintf("API Config file could not be loaded. File not found '%v'", curFile)))
			}
		} else{
			err = append(err, errors.New(fmt.Sprintf("Found blank filename in the API config files array.")))
		}
	}

	return
}












