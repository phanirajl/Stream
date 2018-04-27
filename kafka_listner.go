package main

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/antigloss/go/logger"
	"github.com/bsm/sarama-cluster"
	"github.com/colinmarc/hdfs"
	"github.com/linkedin/goavro"
	"os"
	"os/signal"
	"time"
	"github.com/dminGod/Stream/avro_file_manager"
)

var sProducer sarama.SyncProducer
var codec *goavro.Codec
var hdfsClient *hdfs.Client
var myPid string
var hdfsStagingFolder string
var appStartedTime time.Time

var topicNames []string

func getKafkaConsumer()(consumer *cluster.Consumer, err error) {

	// Create Kafka Consumer
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	consumer, err = cluster.NewConsumer(Conf.Kafka.KafkaBrokers, "HectorBGWorkers", Apis.GetTopics(), config)
	if err != nil {
		s := fmt.Sprintf("Unable to make consumer, Broker list : %v, Topics_list : %v, Config : %v, got error : %v", Conf.Kafka.KafkaBrokers, topicNames, config, err)
		err = errors.New(s)
		return
	}

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

	defer consumer.Close()
	return
}

func KafkaListner() (err error) {

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumer, err := getKafkaConsumer()
	if err != nil {
		return
	}

	s := time.Now()
	logger.Info("Starting Consumer loop, time now : '%v' ", s)
	tick := time.NewTicker(time.Millisecond * time.Duration(int64(Conf.Hdfs.FlushFrequencyMilliSec)))

	// TODO: Need to manage the 5 internal fields for each record

ConsumerLoop:
	for {
		select {

		case <-tick.C:
			for _, v := range Apis {

				err := avro_file_manager.RotateFileLoop(&*v)
				if err != nil {
					logger.Error("Error in rotating file on ticker - Topic : %v -- File : %v -- Error : %v", v.KafkaTopic, v.CurFile.Name(), err)
				}
			}

		case msg, ok := <-consumer.Messages():

			logger.Info("Got a message..... msg : %v --- ok : %v ", msg, ok)

			if ok {

			if _, ok := Apis[msg.Topic]; ok {

				logger.Info("Message for topic %v -- not found in config, skipping", msg.Topic)
				continue
			}

			pk := string(msg.Value)
			ap := *Apis[msg.Topic]

			cassTry := 0

			getCass:
				// Pass the query to cassandra as well
				message, err := Select(ap.Query, pk)
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

				for _, v := range message {

					gr, e := MakeGr(v, ap)
					if e != nil {
						logger.Error("Some errors while making Avro gr -- Err : %v", err)
					}

					err = ap.Ow.Append(gr)
					if err != nil {
						logger.Error("Error when appending to ow file : %v ", err)
						os.Exit(1)
					}

					ap.Inc()
				}

				if (ap.RecCounter % Conf.Hdfs.RecordsPerAvroFile) == 0 {

					avro_file_manager.RotateFileLoop(&ap)
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
