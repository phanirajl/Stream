package main

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/antigloss/go/logger"
	"github.com/bsm/sarama-cluster"
	"github.com/colinmarc/hdfs"
	"github.com/gocql/gocql"
	"github.com/linkedin/goavro"
	"io/ioutil"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"
)

var sProducer sarama.SyncProducer
var codec *goavro.Codec
var hdfsClient *hdfs.Client
var myPid string
var hdfsStagingFolder string
var appStartedTime time.Time


type AllTableState map[string]*TableState

func (a *AllTableState) GetTS(table string)(ts TableState) {

	for k, v := range *a {

		if k == table {
			ts = *v
		}
	}

	return
}

type TableState struct {

	TableSchemaName string
	AvroSchema string
	Query string

	CurFile *os.File
	Ow goavro.OCFWriter
}

var TS AllTableState

func KafkaListener() (err error) {

	bl := Conf.Kafka.KafkaBrokers

	// TODO: This will come from the listOfNodes pulled from the config file
	// topicName := Conf.Kafka.TopicsListForThisNode
	recordsPerAvroFile := Conf.Kafka.RecordsPerAvroFile

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	// . Stuff that should come dynamically :
	// . Table schema name -- where pushTopic is currently being used
	// . Schema file -- currently single file is loaded and used -- this can be a single string
	// . Query to fetch data -- Currently hard coded on the select section
	// . Name of the pk -- actually can be just used directly in the query as a template...
	// . File, os.File that is used for ow will be tracked by the global object
	// . Codec that is created from the schema file will be on the global object
	// . ow that is created with f and Codec will be part of the global object as well

/*
	. Initial - Parmanent:
	. 	Codec : schema string template + schema_name used to make the schema string - Parmanent
	. 	Query to fetch the data (should have the keyword for pk in it) - Parmanent

	. 	gr generator : This will take the names of the fields and put them in types
	. 		when given a response from cassandra -- this will basically take each field type
	. 		expect the correct type to come from cassandra and map is appropriately in the schema
	. 		gr type -- the response from this field will be used to add data to the ow

	. 	This guy will give back a custom generator that will take []map[string]interface and
	. 	give back a generic record -- this is what will be permanently saved with the type

	. 	File, State, ow manager:
		. 	file Current file that is being used to write data -- this will get rotated
		. 	ow - created from a new file -- This will get rotated

 */

	// Set defaults
	if recordsPerAvroFile == 0 {
		recordsPerAvroFile = 50
	}

	// Remove the last /
	hdfsStagingFolder = strings.TrimRight(hdfsStagingFolder, "/")

	// By default if the frequency is not set
	//  make it 20 seconds
	if Conf.Kafka.FlushFrequencyMilliSec == 0 || Conf.Kafka.FlushFrequencyMilliSec <= 500 {

		Conf.Kafka.FlushFrequencyMilliSec = 20000

		// Why are you flushing files so often?
		if Conf.Kafka.FlushFrequencyMilliSec <= 500 {

			logger.Warn("Flush frequency set to less than 500ms, will not allow less than 1 second -- set frequency to default")
		}
	}


	// TODO: the schemafile will come dynamically
	schemaFile := "" //Conf.Kafka.SchemaFile
	b, err := ioutil.ReadFile(fmt.Sprintf("%v", schemaFile))
	if err != nil {
		logger.Error("There was an error in opening the schema file Error : %v", err)
	}

	// Remove!!!
	pushTopic := ""
	topicName := ""
	// Remove!!!

	codec, err = goavro.NewCodec(fmt.Sprintf(string(b), pushTopic))

	logger.Info("Cassandra Listen Queue -- Current Conf is : %+v", Conf)

	if len(bl) == 0 {
		e := "No brokers specified, can not start the Kafka listener"
		logger.Error(e)
		err = errors.New(e)
		return
	}

	logger.Info("Starting Kafaka Worker Server -- Brokers : %v", bl)

	var topicsFoundInJson int
	var topicsInConfig int

	if len(topicName) == 0 {

		var s string

		if topicsFoundInJson == 0 {

			s = fmt.Sprintf("There were no topics found in the JSON file. No API has HadoopSchemaFileOk = true -- please check if schema files are okay and reachable.")
		}

		if topicsInConfig == 0 {

			s = fmt.Sprintf(`There were no topics found in the configuration toml file. 
					Please ensure the topics for this node are configured under Kafka heading as TopicsListForThisNode.`)
		}

		logger.Error(s)
		err = errors.New(s)
		return
	}

	consumer, err := cluster.NewConsumer(bl, "HectorBGWorkers", []string{topicName}, config)
	if err != nil {

		s := fmt.Sprintf("Unable to make consumer, Broker list : %v, Topics_list : %v, Config : %v, got error : %v", bl, topicName, config, err)
		logger.Error(s)
		err = errors.New(s)
		return
	}

	defer consumer.Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			logger.Error("Conusmer error : %s ", err.Error())
			return
		}
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			logger.Error("Kafka notification : Rebalanced: %+v ", ntf)
		}
	}()

	logger.Info("Starting consumer loop .. ")

	t := strconv.Itoa(int(time.Now().Unix()))
	ran, err := gocql.RandomUUID()
	if err != nil {
		logger.Error("Error generating random UUID -- Error : %v", err)
	}

	// Make the first file
	f, err := os.Create(fmt.Sprintf("%v/pid%v_%v_%v.avro", hdfsStagingFolder, myPid, ran.String(), t))
	if err != nil {
		logger.Error("Error creating file : %v", err)
		os.Exit(1)
	}

	ow, err := goavro.NewOCFWriter(goavro.OCFConfig{W: f, CompressionName: "snappy", Codec: codec})
	if err != nil {
		logger.Error("Could not create OW : %v ", err)
		os.Exit(1)
	}

	i := 0

	s := time.Now()
	fmt.Println("Starting time now : ", s)

	// TODO : The tick has to come from the configuration
	tick := time.NewTicker(time.Millisecond * 5)

ConsumerLoop:
	for {
		select {

		case <-tick.C:
			rotateFileLoop(&s, &f, &ow, &i)

		case msg, ok := <-consumer.Messages():

			logger.Info("Got a message..... msg : %v --- ok : %v ", msg, ok)

			if ok {

				// n := time.Now();
				// moreTime := n.Sub(s).Nanoseconds() / int64(time.Millisecond) > 20000
				// s, f, ow
				pk := string(msg.Value)
				cassTry := 0

			getCass:
				// Get the details from cassandra
				message, err := Select(pk)
				if err != nil {
					logger.Error("Error processing cassandra request, Error : %v -- Exiting", err)
					// Before you kill this, save the file for HDFS and try to push that a few times if that goes
					//	os.Exit(1)
					continue
				}

				if len(message) == 0 {

					logger.Warn("Didn't get anything from cassandra for the pk : %v -- Trying again ... ", pk)

					if cassTry == 0 {
						cassTry += 1
						time.Sleep(3 * time.Second)
						goto getCass
					} else {

						logger.Error("Even after second try after waiting 3 seconds, no luck on cass record :( ", pk)
						continue
					}
				} else {

					if cassTry > 0 {
						logger.Info("Got after second try! :) ", pk)
					}
				}

				gr := FetchAndProcessCassandra(message)
				err = ow.Append(gr)
				if err != nil {
					logger.Error("Error when appending to ow file : %v ", err)
					os.Exit(1)
				}

				i += 1

				if (i % recordsPerAvroFile) == 0 {

					rotateFileLoop(&s, &f, &ow, &i)
				}

				consumer.MarkOffset(msg, "")
			} else {

				logger.Error("Problem with message that came in : msg is : %v ", msg.Value)
			}

		case <-signals:
			break ConsumerLoop
		}
	}

	return
}