package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bsm/sarama-cluster"
	"github.com/colinmarc/hdfs"
	"github.com/gocql/gocql"
	"github.com/golang/snappy"
	"github.com/linkedin/goavro"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"
	"github.com/antigloss/go/logger"
	"io/ioutil"
)

var hdfsClient *hdfs.Client

func HdfsPush() {

	var err error

	bl := Conf.Kafka.KafkaBrokers
	topicName := Conf.Kafka.HivePushTopic
	recordsPerAvroFile := Conf.Kafka.RecordsPerAvroFile
	hdfsStagingFolder := Conf.Kafka.HdfsStagingFolder

	// Set defaults
	if recordsPerAvroFile == 0 {
		recordsPerAvroFile = 50
	}

	if hdfsStagingFolder == "" {
		hdfsStagingFolder = "/tmp"
	}

	// Remove the last /
	hdfsStagingFolder = strings.TrimRight(hdfsStagingFolder, "/")

	// Set consumer configuration
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	hdfsClient, err = hdfs.NewClient(hdfs.ClientOptions{Addresses: []string{Conf.Kafka.HDFSConnPath}, User: "hdfs"})
	if err != nil {
		logger.Error("Could not connect to HDFS, %v", err)
		os.Exit(1)
	}

	// TODO: Move this into a file -- put the reference to the file in the config and use from there [done]

	//r, _ := os.Open("/go/src/github.com/dminGod/Stream/schemafile")
	schemaFile := Conf.Kafka.SchemaFile

	//schemaFileLocation = strings.TrimRight(schemaFileLocation, "/")

	b, err := ioutil.ReadFile(fmt.Sprintf("%v",schemaFile))
	if err != nil {
		logger.Error("There was an error in opening the schema file",err)
	}
	codec, err = goavro.NewCodec(fmt.Sprintf(string(b), topicName))



	logger.Info("HDFS Push Mode -- Current Conf is : %+v", Conf)

	if len(bl) == 0 {

		//logger.Error("No brokers specified, exiting")
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
			s = fmt.Sprintf("There were no topics found in the configuration toml file. Please ensure the topics for this node are configured under Kafka heading as TopicsListForThisNode.")
		}

		logger.Error(s)
		err = errors.New(s)
		return
	}

	consumer, err := cluster.NewConsumer(bl, "HectorBGWorkersHDFSPush", []string{topicName}, config)
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

	i := 0

	t := strconv.Itoa(int(time.Now().Unix()))
	ran, err := gocql.RandomUUID()
	if err != nil {
		logger.Error("Error generating random UUID -- Error : %v", err)
	}

	f, err := os.Create(fmt.Sprintf("%v/%v_%v.avro", hdfsStagingFolder, ran.String(), t))
	if err != nil {
		logger.Error("Error creating file : %v", err)
		os.Exit(1)
	}

	ow, err := goavro.NewOCFWriter(goavro.OCFConfig{W: f, CompressionName: "snappy", Codec: codec})
	if err != nil {
		logger.Error("Could not create OW : %v ", err)
		os.Exit(1)
	}

	logger.Info("Starting loop")

ConsumerLoop:
	for {
		select {
		case msg, ok := <-consumer.Messages():

			i += 1

			// ToDO: P1 This number has to come from configuration (use variable : records_per_avro_file ) [done]
			if (i % recordsPerAvroFile) == 0 {

				logger.Info("\n Closing and moving file : %v to HDFS :  ", f.Name())
				fn := f.Name()
				rfn := strings.Replace(fn, "/tmp/", "", 1)
				f.Close()

				err := hdfsClient.CopyToRemote(fn, Conf.Kafka.HdfsDatabaseFolder+"/"+rfn)
				if err != nil {

					logger.Error("Error moving file to HDFS, %v", err)
					os.Exit(1)
				} else {

					logger.Info("Removing file : %v", fn)
					os.Remove(fn)
				}

				t := strconv.Itoa(int(time.Now().Unix()))
				ran, err := gocql.RandomUUID()
				if err != nil {
					logger.Error("Error generating random UUID : %v ", err)
				}

				f, err = os.Create(fmt.Sprintf("/tmp/%v_%v.avro", ran.String(), t))
				if err != nil {
					logger.Error("Error creating file : %v", err)
					os.Exit(1)
				}

				logger.Info("Changing the ow to : %v", f.Name())
				ow, err = goavro.NewOCFWriter(goavro.OCFConfig{W: f, CompressionName: "snappy", Schema: codec.Schema()})
				// ow, err = goavro.NewOCFWriter(goavro.OCFConfig{W: f, Schema : codec.Schema() })
				if err != nil {
					logger.Error("Could not create OW file : %v  -- Error:  %v", f.Name(), err)
					os.Exit(1)
				}
			}

			if ok {

				var gr []interface{}

				sdc := msg.Value
				sdc, err := snappy.Decode(nil, msg.Value)
				if err != nil {

					logger.Error("Error decoding snappy message, Error : %v", err)
					os.Exit(1)
				}

				var k lsr_record

				err = json.Unmarshal(sdc, &k)
				if err != nil {
					if len(string(sdc)) > 200 {

						logger.Error("JSON Decode error %v -- Payload : %v ", err, string(sdc)[0:200])
					} else {

						logger.Error("JSON Decode error %v -- Payload : %v ", err, string(sdc))
					}

					os.Exit(1)
				}

				gr = k.getGenericRecord()

				err = ow.Append( gr )
				if err != nil {
					logger.Error("Error when appending to ow file : %v ", err)
					os.Exit(1)
				}

				consumer.MarkOffset(msg, "")
			} else {

				logger.Error("Problem with message that came in -- Channel says ok is false")
			}

		case <-signals:
			break ConsumerLoop
		}
	}

	return
}
