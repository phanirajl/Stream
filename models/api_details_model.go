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
	"encoding/json"
	"github.com/mitchellh/mapstructure"
)

type APIsRef map[string]*APIDetails

type APIDetails struct {

	KafkaTopic 		string

	AvroSchemaFile  string
	QueryFile		string

	AvroSchema 		string
	AvroFields		AvroSchemaParser
	CassCompressedFields	[]string
	Query      		string

	HiveTableName 	string
	HDFSLocation	string

	RecCounter 		int

	Codec      		*goavro.Codec
	Ow         		*goavro.OCFWriter
	CurFile    		*os.File
	FileProcessTime time.Time
}

// We take the details of the APIDetails and populate everything
func (a *APIsRef) InitialPopulate()(errs []error) {

	// If there are Duplicate or
	if len(a.GetTopics()) != len(*a) {
		errs = append(errs, errors.New(fmt.Sprintf(`Count of API references and Topics did not match 
			please check for missing or duplicate topics -- Topics List : %v -- API List : %v `, a.GetTopics(), a)))

		return
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

func (a *APIsRef) MakeFirstFiles( stageFolder string, appPid string ) (err error) {

	for _, api := range *a {

		tt := time.Now()

		// Make File and OCFW
		t := strconv.Itoa(int(tt.Unix()))

		// Make current file -- this has to be dynamic
		ran, er := gocql.RandomUUID()
		if er != nil {
			logger.Error("Error generating random UUID -- Error : %v", er)
		}

		// Make the first file
		f, er := os.Create(fmt.Sprintf("%v/pid%v_~%v~_%v_%v.avro", stageFolder, appPid, api.KafkaTopic, ran.String(), t))
		if er != nil {
			logger.Error("Error creating file : '%v' -- Error : %v", f, er)
			err = er
			return
		}

		// Make the ocf writer
		ow, er := goavro.NewOCFWriter(goavro.OCFConfig{W: f, CompressionName: "snappy", Codec: api.Codec})
		if er != nil {
			logger.Error("Could not create OW file : '%v' -- Error : %v ", er)
			err = er
			return
		}

		(*api).FileProcessTime = tt
		(*api).CurFile = f
		(*api).Ow = ow
	}

	return
}


func (a *APIsRef) GetByTopic(topic string) (api APIDetails, found bool) {

	for _, v := range *a {

		if topic == v.KafkaTopic {
			api = *v
			found = true
		}
	}

	return
}

func (a *APIsRef) GetTopics() (topics []string) {

	t := make(map[string]struct{})

	for _, v := range *a {

		t[v.KafkaTopic] = struct{}{}
	}

	for k, _ := range t {

		topics = append(topics, k)
	}


	return
}

func (a *APIDetails) Inc() {

	(*a).RecCounter += 1
}


func (a *APIDetails) ResetCounter() {

	(*a).RecCounter = 0
}

type AvroSchemaParser struct {

	Namespace string `json:"namespace"`
	Name string `json:"name"`
	Fields []*Field `json:"fields"`
}

func (a *AvroSchemaParser) GetTypeByName(s string) (typ string, isArr bool, err error) {

	var found bool

	for _, v := range (*a).Fields {

		if v.Name == s {

			typ = v.Type
			isArr = v.IsArray
			found = true
		}
	}

	if found == false {

		err = errors.New(fmt.Sprintf("Could not find field in the schema by the given name, searched for %v", s))
	}

	return
}

type Field struct {

	Name string `json:"name"`
	Type string
	IsArray bool
	TypeArr []interface{} `json:"type"`
}

type InField struct {

	Type string
	Items []interface{}
}

func NewAvroSP(s []byte) (ap AvroSchemaParser, err error) {

	fmt.Println(string(s))

	json.Unmarshal(s, &ap)
	if err != nil {
		return
	}

	for _, f := range ap.Fields {

		for _, ty := range f.TypeArr {

			switch ty.(type) {
			case string:
				if ty != "null" {
					(*f).Type = ty.(string)
				}

			case map[string]interface{}:

				var tm InField
				err := mapstructure.Decode(ty, &tm)
				if err != nil {
					fmt.Println(err)
				}

				if tm.Type == "array" {
					for _, v := range tm.Items {
						if v.(string) != "null" {
							(*f).Type = fmt.Sprintf("%v", v.(string))
							(*f).IsArray = true
						}
					}
				}
			}
		}
	}

	return
}
