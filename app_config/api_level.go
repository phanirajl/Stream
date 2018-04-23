package app_config

import (
	"os"
	"strings"
	"fmt"
	"errors"
	"github.com/spf13/viper"
)

/*
	open the file -- get its contents -- close it -- run checks on it -- if checks come out good --
	run a populate on it and add it to the main struct
	put the contents in the struct
 */

 /*
 	Actions that this module does:
 		- Make sure the object array does not have duplicates (what is the pk to check by?)
 		- Make sure the folder for the config exists (exit if fail)
 		- Make sure all the toml files mentioned actually exits (exit if fail)
 		- Open each file, check if the file is valid and the things you need for it exist
 		- Populate the toml contents into a struct
  */


type TablesRef []Table

/*
type TableState struct {
		TableSchemaName string  -- What is this?
		TopicName string		-- What topic should be listened to for this API -- most important
		AvroSchemaFile string	-- What is the full location to get the Avro schema -- only used once at start
		AvroSchema string		-- The actual schema string that is used
		Query string			-- The query string used to pull data from Cassandra

		CurFile *os.File		-- Current file passed to the ocfw to write stuff
		Ow goavro.OCFWriter		-- The ocfw object
	}
 */

type Table struct {

	TableSchemaName string
	TopicName string
	AvroSchemaFile string
	AvroSchema string
	Query string
}

var TablesConf TablesRef

/*
Does these checks:
	- Has this guy put in duplicate values in this array, if yes, then it's not good cause you'll have multiple objects
	- Do we have references for the fields to reach the toml files in the main config file?
		- If not, exit -- we wont be able to do anything with the applicatoin without this configuration
	- Does the directory mentioned for the reference exist?
	- Do all the files that are mentioned in the config file exist?

	- Open each file take contents of the file -- send it to a checker -- if all looks good then put the
		contents into the struct

 */
func CheckLoadData() (err []error){

	folder := Config.Kafka.ApiConfigFolder
	files := Config.Kafka.ApiFilesToLoad

	// Make sure we don't have duplicates in the array .. if yes throw an error
	dupCheck := noDuplicatesCheck(files)
	if dupCheck {
		err = append(err, errors.New("There were duplicate files found in Kafka.ApiFilesToLoad -- Please check."))
	}

	// Make sure the folder mentioned for the reference exists
	// ApiConfigFolder
	// Make sure each file mentioned in the reference exists
	if folderExistsCheck(folder) == false {

		err = append(err, errors.New(fmt.Sprintf("The API configuration folder '%v' could not be located -- Please check.", folder)))
	}

	// If files dont exist throw an error
	for _, e := range filesExistCheck(folder, files) {
		err = append(err, e)
	}

	if len(err) > 0 {
		return
	}

	err = append(err, LoadData())

	return
}

func LoadData() (err error) {

	folder := Config.Kafka.ApiConfigFolder
	files := Config.Kafka.ApiFilesToLoad

	// For each file:

	// Open the file, get the contents, close the file
	// Pass these references into a checker to validate if its okay -- return back a base toml return from here
	// If the validator comes back without errors, send the base toml references to a populate function to fill
	// the module struct with the details of the configuration
	for _, f := range files {

		conf, e := validateLoadToml(folder, f)
		if(e != nil) {
			err = e
			return
		}

		TablesConf = append(TablesConf, conf)
	}

	return
}

func noDuplicatesCheck(files []string)(ret bool) {

	fLen := len(files)
	uq := make(map[string]interface{})

	for _, v := range files {
		uq[v] = nil
	}

	return fLen == len(uq)
}

func folderExistsCheck(path string) (ret bool){

	_, err := os.Open(path)
	if err == nil {
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

/*
	Given the toml contents -- run checks on and return errors if any

 */
func validateLoadToml(folder string, file string)(c Table, err error) {

	v := viper.New()
	v.SetConfigName(file)
	viper.AddConfigPath(folder)
	v.SetConfigType("toml")
	err = v.ReadInConfig()
	if err != nil {
		return
	}

	v.Unmarshal(&c)
	return
}


func populateToml(confs map[string]string)(err error) {


	return
}











