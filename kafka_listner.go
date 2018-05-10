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
	"github.com/dminGod/Stream/influx_writer"
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

	consumer, err = cluster.NewConsumer(Conf.Kafka.KafkaBrokers, "HectorWorkers", Apis.GetTopics(), config)
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


	return
}

func KafkaListner(writer influx_writer.InfluxWriter, influxEnabled bool) (err error) {

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill)

	consumer, err := getKafkaConsumer()
	if err != nil {
		return
	}

	defer consumer.Close()
	var hdfsTimeTaken int64
	var streamTimeTaken int64
	s := time.Now()
	logger.Info("Starting Consumer loop, time now : '%v' ", s)
	tick := time.NewTicker(time.Millisecond * time.Duration(int64(Conf.Hdfs.FlushFrequencyMilliSec)))

	// TODO: Need to manage the 5 internal fields for each record

ConsumerLoop:
	for {
		select {

		case <-tick.C:
			logger.Info("Got ticker event")
			for _, v := range Apis {

				logger.Info("Processing for topic : %v", v.KafkaTopic)
				err := avro_file_manager.RotateFileLoop( v )

				if err != nil {
					logger.Error("Error in rotating file on ticker - Topic : %v -- File : %v -- Error : %v", v.KafkaTopic, v.CurFile.Name(), err)
				}
			}

		case msg, okk := <-consumer.Messages():
			logger.Info("messages received : %v ",msg)
			if okk {

			if _, k := Apis[msg.Topic]; k == false {

				logger.Info("Message for topic %v -- not found in config, skipping", msg.Topic)
				continue
			}

			pk := string(msg.Value)
			ap := Apis[msg.Topic]

			cassTry := 0
			queryTimerStartTs := time.Now()
			getCass:
				// Pass the query to cassandra as well

				message, err := Select(ap.Query, pk)
				queryTimerEndTs := time.Now()
				timeTakenCassandra := queryTimerEndTs.Sub(queryTimerStartTs).Nanoseconds() / int64(time.Millisecond)
				logger.Info("Cassandra query for message %v takes %v ms",pk,timeTakenCassandra)
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


					if influxEnabled{
						if _,ok := gr[0].(map[string]interface{})["int_cass_stream_diff"]; !ok{
							for idx,_ := range gr{
								gr[idx].(map[string]interface{})["int_cass_stream_diff"] = 0
							}
						}
						streamTimeTaken = time.Now().Sub(queryTimerStartTs).Nanoseconds() / int64(time.Millisecond)

						go writeStatsToInflux(timeTakenCassandra,streamTimeTaken,gr,writer)
					}
					ap.Inc()
				}

				hdfsStartTimeTs := time.Now()

				if (ap.RecCounter % Conf.Hdfs.RecordsPerAvroFile) == 0 {

					avro_file_manager.RotateFileLoop(ap)
				}

				mm := fmt.Sprintf("Processed : %v", string(msg.Value))
				logger.Info(mm)
				hdpTimeEndTs := time.Now()
				hdfsTimeTaken = hdpTimeEndTs.Sub(hdfsStartTimeTs).Nanoseconds() / int64(time.Millisecond)
				logger.Info("It took %v ms for %v to be processed in hadoop ",hdfsTimeTaken,string(msg.Value))
				if influxEnabled{
					messageToBeSent := fmt.Sprintf(`%v,StreamID=%v HDFSMoveTime=%v`,writer.InfluxCfg.Measurement,getHostname(),hdfsTimeTaken)
					writer.WriteRow(messageToBeSent)
				}
				consumer.MarkOffset(msg, "")
			} else {

				logger.Error("Problem with message that came in : msg is : %v ", msg)
			}

		case ss := <-signals:

			logger.Info("Got '%v' signal, going into graceful exit : Attempting to close : %v Files -- (%v)", ss, len(Apis), Apis)

			for _, v := range Apis {

				logger.Info("Calling final close for Topic: %v, File: %v ", v.KafkaTopic, v.CurFile.Name())
				err := avro_file_manager.ExitCloseMoveFile( v )

				if err != nil {
					logger.Error("Error in rotating file on ticker - Topic : %v -- File : %v -- Error : %v", v.KafkaTopic, v.CurFile.Name(), err)
				}
			}

			logger.Info("App Exit 0")
			os.Exit(0)

			break ConsumerLoop
		}
	}

	return
}
