package models

import (
	"github.com/linkedin/goavro"
	"os"
	"errors"
	"fmt"
	"io/ioutil"
	"time"
	"github.com/gocql/gocql"
	"strconv"
	"github.com/antigloss/go/logger"
)

type APIsRef map[string]*APIDetails

type APIDetails struct {

	KafkaTopic 		string

	AvroSchemaFile  string
	QueryFile		string

	AvroSchema 		string
	Query      		string

	HiveTableName 	string
	HDFSLocation	string

	RecCounter 		int

	Codec      		*goavro.Codec
	Ow         		*goavro.OCFWriter
	CurFile    		*os.File
}

// We take the details of the APIDetails and populate everything
func (a *APIsRef) InitialPopulate()(errs []error) {

	// If there are Duplicate or
	if len(a.GetTopics()) != len(*a) {
		errs = append(errs, errors.New(fmt.Sprintf(`Count of API references and Topics did not match 
			please check for missing or duplicate topics -- Topics List : %v -- API List : %v `, a.GetTopics(), a)))
	}

	for _, api := range *a {

		b, err := ioutil.ReadFile(api.AvroSchemaFile)
		if err != nil {
			errs = append(errs, err)
			break
		}

		s := fmt.Sprintf(string(b), api.HiveTableName)

		c, err := goavro.NewCodec( s )
		if err != nil {
			errs = append(errs, err)
			break
		}

		qb, err := ioutil.ReadFile(api.QueryFile)
		if err != nil {
			errs = append(errs, err)
			break
		}

		q := string(qb)

		(*api).AvroSchema = s
		(*api).Codec = c
		(*api).Query = q
	}

	return
}

func (a *APIsRef) MakeFirstFiles( stageFolder string, appPid string ){

	for _, api := range *a {

		// Make File and OCFW
		t := strconv.Itoa(int(time.Now().Unix()))

		// Make current file -- this has to be dynamic
		ran, err := gocql.RandomUUID()
		if err != nil {
			logger.Error("Error generating random UUID -- Error : %v", err)
		}

		// Make the first file
		f, err := os.Create(fmt.Sprintf("%v/pid%v_%v_%v.avro", stageFolder, appPid, ran.String(), t))
		if err != nil {
			logger.Error("Error creating file : %v", err)
			os.Exit(1)
		}

		// Make the ocf writer
		ow, err := goavro.NewOCFWriter(goavro.OCFConfig{W: f, CompressionName: "snappy", Codec: api.Codec})
		if err != nil {
			logger.Error("Could not create OW : %v ", err)
			os.Exit(1)
		}

		(*api).CurFile = f
		(*api).Ow = ow
	}
}


func (a *APIsRef) GetByTopic(topic string) (api APIDetails) {

	for _, v := range *a {

		if topic == v.KafkaTopic {
			api = *v
		}
	}

	return
}

func (a *APIsRef) GetTopics() (topics []string) {

	t := make(map[string]struct{})

	for _, v := range *a {

		t[v.KafkaTopic] = struct{}{}
	}

	return
}

func (a *APIDetails) Inc() {

	(*a).RecCounter += 1
}