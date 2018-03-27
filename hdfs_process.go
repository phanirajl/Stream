package main

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/golang/snappy"
	"github.com/linkedin/goavro"
	"os"
	"os/signal"
	"strings"
	"github.com/antigloss/go/logger"
	"io/ioutil"
	"github.com/mitchellh/mapstructure"
	"time"
	"github.com/gocql/gocql"
	"strconv"
	"github.com/colinmarc/hdfs"
)

var sProducer sarama.SyncProducer
var codec *goavro.Codec
var hdfsClient *hdfs.Client


func KafkaListener() (err error) {

	var messagesInParallel = Conf.Kafka.MessagesInParallel

	if messagesInParallel == 0 {

		messagesInParallel = 10
	}

	bl := Conf.Kafka.KafkaBrokers
	topicName := Conf.Kafka.TopicsListForThisNode
	recordsPerAvroFile := Conf.Kafka.RecordsPerAvroFile

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true


	pushTopic := Conf.Kafka.HivePushTopic

	hdfsStagingFolder := Conf.Kafka.HdfsStagingFolder

	// Connect to HDFS
	hdfsClient, err = hdfs.NewClient(hdfs.ClientOptions{Addresses: []string{Conf.Kafka.HDFSConnPath}, User: "hdfs"})
	if err != nil {
		logger.Error("Could not connect to HDFS, %v", err)
		os.Exit(1)
	}


	// Set defaults
	if recordsPerAvroFile == 0 {
		recordsPerAvroFile = 50
	}

	if hdfsStagingFolder == "" {
		hdfsStagingFolder = "/tmp"
	}

	// Remove the last /
	hdfsStagingFolder = strings.TrimRight(hdfsStagingFolder, "/")

	schemaFile := Conf.Kafka.SchemaFile
	b, err := ioutil.ReadFile(fmt.Sprintf("%v", schemaFile))
	if err != nil{
		logger.Error("There was an error in opening the schema file Error : %v", err)
	}

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

			s = fmt.Sprintf("There were no topics found in the configuration toml file. Please ensure the topics for this node are configured under Kafka heading as TopicsListForThisNode.")
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

	i := 0

	s := time.Now()
	fmt.Println("Starting time now : ", s)

ConsumerLoop:
	for {
		select {
		case msg, ok := <-consumer.Messages():

			i += 1

			// logger.Info("Processing message %v", msg.Key)

			if ok {


				n := time.Now();
				moreTime := n.Sub(s).Nanoseconds() / int64(time.Millisecond) > 20000

				if (i % recordsPerAvroFile) == 0 || moreTime {


					fn := f.Name()
					rfn := strings.Replace(fn, "/tmp/", "", 1)
					f.Close()

					logger.Info("\n Moving '%v' -- Num Rec '%v' -- Time Movement : %v ", rfn, i, moreTime)

					err := hdfsClient.CopyToRemote(fn, Conf.Kafka.HdfsDatabaseFolder+"/"+rfn)
					if err != nil {

						logger.Error("Error moving file to HDFS, %v", err)
						os.Exit(1)
					} else {

						// logger.Info("Removing file : %v", fn)
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

					// logger.Info("Changing the ow to : %v", f.Name())
					ow, err = goavro.NewOCFWriter(goavro.OCFConfig{W: f, CompressionName: "snappy", Schema: codec.Schema()})

					if err != nil {
						logger.Error("Could not create OW file : %v  -- Error:  %v", f.Name(), err)
						os.Exit(1)
					}

					s = time.Now();
				}

				pk := string(msg.Value)

				// logger.Info(pk)

				// Get the details from cassandra
				message, err := Select(pk)
				if err != nil {
					logger.Error("Error processing cassandra request, Error : %v -- Exiting", err)

					// Before you kill this, save the file for HDFS and try to push that a few times if that goes
				//	os.Exit(1)
					continue
				}

				// logger.Info("Going to send to Kafka %v", msg.Key)
				gr := FetchAndProcessCassandra( message )
				err = ow.Append( gr )
				if err != nil {
					logger.Error("Error when appending to ow file : %v ", err)
					os.Exit(1)
				}

				//fmt.Println(fmt.Println("Marking message as consumed -- Got new kafka message Message Key : %v, Offset : %v, BlockTimestamp : %v", msg.Key, msg.Offset, msg.BlockTimestamp))
				// logger.Info("cons")
				consumer.MarkOffset(msg, "")
			} else {

				logger.Error("Problem with message that came in : msg is : ")
			}

		case <-signals:
			break ConsumerLoop
		}
	}

	return
}

func FetchAndProcessCassandra( message []map[string]interface{} ) (gr []interface{}){

	for _, vv := range message {

		// Decompress the lsr_s field
		if _, ok := vv["local_service_requests_s"].([]byte); ok {

			if len(vv["local_service_requests_s"].([]byte)) > 0 {

				m, err := snappy.Decode(nil, vv["local_service_requests_s"].([]byte))
				if err != nil {
					logger.Error("There was an error decoding the compressed message, Error : %v ", err)
					os.Exit(1)
				}

				lsrs := strings.Replace(string(m), `"`, `\"`, -1)
				vv["local_service_requests_s"] = []string{lsrs}
			}
		} else {

			vv["local_service_requests_s"] = []string{""}
		}

		if _, ok := vv["local_service_requests_s"].([]string); !ok {

			vv["local_service_requests_s"] = []string{""}
		}

		// Take the lsr_pk field -- it will come as gocql.UUID -- change it to string
		if _, ok := vv["local_service_requests_new_con5_pk"].(gocql.UUID); ok {

			vv["local_service_requests_new_con5_pk"] = vv["local_service_requests_new_con5_pk"].(gocql.UUID).String()
			logger.Info(vv["local_service_requests_new_con5_pk"].(string))
		}

		var k lsr_record

		mapstructure.Decode(vv, &k)
		gr = k.getGenericRecord()
	}

	// logger.Info("Success - Pushed to kafka")

	return
}
