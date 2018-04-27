package app_config

import (
	"os"
	"fmt"
	"errors"
	"github.com/dminGod/Stream/models"
	"io/ioutil"
)

var Apis models.APIsRef

func CheckLoadAPIs() ( errs []error ) {

	var e error

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

	a = make(map[string]*models.APIDetails)

	for _, f := range Config.Apis {

		conf := f

		e := checkApi(conf)
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


func checkApi( c models.APIDetails ) (err error) {

	multi := len(c.QueryFile) == 0 || len(c.AvroSchemaFile) == 0 || len(c.KafkaTopic) == 0 || len(c.HiveTableName) == 0 || len(c.HDFSLocation) == 0

	if multi {

		err = errors.New(fmt.Sprintf("Some parameters missing from API -- QueryFile: '%v' - AvroSchemaFile: '%v' - KafkaTopic: '%v' - HiveTableName: '%v' - HDFSLocation: '%v'",
			c.QueryFile, c.AvroSchemaFile, c.KafkaTopic, c.HiveTableName, c.HDFSLocation))
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

