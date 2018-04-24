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
	"github.com/dminGod/Stream/models"
	"github.com/dminGod/Stream/app_config"
	"strings"
)

var sProducer sarama.SyncProducer
var codec *goavro.Codec
var hdfsClient *hdfs.Client
var myPid string
var hdfsStagingFolder string
var appStartedTime time.Time

var topicNames []string



func loadApis() (a models.APIsRef, e error) {

	var err []error
	err = app_config.CheckLoadAPIs()
	if len(err) > 0 {
		logger.Error("There were errors in loading API details : ")

		var t []string
		for _, v := range err {

			t = append(t, v.Error())
		}
		e = errors.New(strings.Join(t, " -- "))
	}

	Apis = app_config.GetApis()

	return
}

func kafkaConsumer()(consumer *cluster.Consumer, err error) {

	Apis, err = loadApis()
	if err != nil {
		return
	}

	topicNames = Apis.GetTopics()

	// Create Kafka Consumer
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	consumer, err = cluster.NewConsumer(Conf.Kafka.KafkaBrokers, "HectorBGWorkers", topicNames, config)
	if err != nil {
		s := fmt.Sprintf("Unable to make consumer, Broker list : %v, Topics_list : %v, Config : %v, got error : %v", Conf.Kafka.KafkaBrokers, topicNames, config, err)
		err = errors.New(s)
		return
	}

	defer consumer.Close()
	return
}

func KafkaListener() (err error) {

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumer, err := kafkaConsumer()
	if err != nil {
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

	s := time.Now()
	logger.Info("Starting Consumer loop, time now : '%v' ", s)
	tick := time.NewTicker(time.Millisecond * time.Duration(int64(Conf.Kafka.FlushFrequencyMilliSec)))

ConsumerLoop:
	for {
		select {

		case <-tick.C:
			// For each topic, rotate the file
				// s - 	Time of starting listening -- will be OS level
				// f - 	OS.create file for this topic
				// ow -	OCF writer for this topic
				// i - Counter of how many records have been written
			RotateFileLoop(&s, &f, &ow, &i)

		case msg, ok := <-consumer.Messages():

			logger.Info("Got a message..... msg : %v --- ok : %v ", msg, ok)

			if ok {

				// n := time.Now();
				// moreTime := n.Sub(s).Nanoseconds() / int64(time.Millisecond) > 20000
				// s, f, ow
				pk := string(msg.Value)
				cassTry := 0

			getCass:
				// Pass the query to cassandra as well
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

				if (i % Conf.Kafka.RecordsPerAvroFile) == 0 {

					RotateFileLoop(&s, &f, &ow, &i)
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
