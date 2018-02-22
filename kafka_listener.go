package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/golang/snappy"
	"github.com/linkedin/goavro"
	"os"
	"os/signal"
	"strings"
	"sync"
	"github.com/antigloss/go/logger"
	"io/ioutil"
)

var sProducer sarama.SyncProducer
var codec *goavro.Codec


func KafkaListener() (err error) {

	var messagesInParallel = Conf.Kafka.MessagesInParallel

	if messagesInParallel == 0{
		messagesInParallel = 10
	}
	bl := Conf.Kafka.KafkaBrokers
	topicName := Conf.Kafka.TopicsListForThisNode

	config := cluster.NewConfig()

	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	pushTopic := Conf.Kafka.HivePushTopic

	// TODO: 1. Move this out to a file [done]
	// TODO: 2. Let the reference of that file come from config ( Same task for hdfs_push.go HdfsPush() ) [done]
	schemaFile := Conf.Kafka.SchemaFile

	b, err := ioutil.ReadFile(fmt.Sprintf("%v",schemaFile))
	if err != nil{
		logger.Error("There was an error in opening the schema file", err)
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

	var wg sync.WaitGroup
	i := 0

ConsumerLoop:
	for {
		select {
		case msg, ok := <-consumer.Messages():

			i += 1

			// TODO: Make the number of messages that run in parallel come from configuration [done]
			if (i % messagesInParallel) == 0 {

				// fmt.Println("Waiting after 10 messages async..")
				wg.Wait()
			}

			wg.Add(1)

			if ok {

				// fmt.Println(fmt.Sprintf("Got new kafka message Message Key : %v, Offset : %v, BlockTimestamp : %v", msg.Key, msg.Offset, msg.BlockTimestamp))
				pk := string(msg.Value)

				// Get the details from cassandra
				message, err := Select(pk)
				if err != nil {
					logger.Error("Error processing cassandra request, Error : %v -- Exiting", err)
					os.Exit(1)
				}

				go KafkaProducer(message, &wg)

				//fmt.Println(fmt.Println("Marking message as consumed -- Got new kafka message Message Key : %v, Offset : %v, BlockTimestamp : %v", msg.Key, msg.Offset, msg.BlockTimestamp))
				logger.Info("cons")
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

func KafkaProducer(message []map[string]interface{}, wg *sync.WaitGroup) {

	var err error

	pushTopic := Conf.Kafka.HivePushTopic

	if sProducer == nil {

		// Produce configuration
		config := sarama.NewConfig()
		config.Producer.Retry.Max = 10 // Retry up to 10 times to produce the message
		config.Producer.MaxMessageBytes = 80000000
		config.Producer.Return.Successes = true
		// config.Producer.Compression = sarama.CompressionGZIP

		sProducer, err = sarama.NewSyncProducer(Conf.Kafka.KafkaBrokersCassandra, config)
		if err != nil {
			logger.Error("Failed to start Sarama producer : %v", err)
			os.Exit(1)
		}
	}

	for _, vv := range message {

		buffer := &bytes.Buffer{}

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

		// Make it into JSON
		tmpJson, err := json.Marshal(vv)
		if err != nil {
			logger.Error("Could not parse JSON for message : %v", err)
			os.Exit(1)
			continue
		}

		h := snappy.Encode(nil, tmpJson)

		_, _, err = sProducer.SendMessage(&sarama.ProducerMessage{
			Topic: pushTopic,
			Value: sarama.ByteEncoder(h),
		})
		if err != nil {
			logger.Error("Failed to send message to schema producer, Size : %v, Error: %v", buffer.Len(), err)
			os.Exit(1)
		}
	}

	wg.Done()
}
