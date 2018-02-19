package main

import (
	"encoding/gob"
	"github.com/bsm/sarama-cluster"
	"fmt"
	"os"
	"os/signal"
	"errors"
	"github.com/golang/snappy"
	"encoding/json"
	"strings"
)

func KafkaListener() (err error) {

	bl := Conf.Kafka.KafkaBrokers
	topics_list := Conf.Kafka.TopicsListForThisNode

	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})

	config := cluster.NewConfig()

	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	if len(bl) == 0 {

		//logger.Error("No brokers specified, exiting")
		err = errors.New("No brokers specified, can not start the Kafka listener")
		return
	}

	fmt.Println("Starting Kafaka Worker Server -- Brokers : %v", bl)

	var topicsFoundInJson int
	var topicsInConfig int

	if len(topics_list) == 0 {

		var s string

		if topicsFoundInJson == 0 {

			s = fmt.Sprintf("There were no topics found in the JSON file. No API has HadoopSchemaFileOk = true -- please check if schema files are okay and reachable.")
		}

		if topicsInConfig == 0 {

			s = fmt.Sprintf("There were no topics found in the configuration toml file. Please ensure the topics for this node are configured under Kafka heading as TopicsListForThisNode.")
		}

		fmt.Println(s)
		err = errors.New(s)
		return
	}

	consumer, err := cluster.NewConsumer(bl, "HectorBGWorkers", topics_list, config)
	if err != nil {

		s := fmt.Sprintf("Unable to make consumer, Broker list : %v, Topics_list : %v, Config : %v, got error : %v", bl, topics_list, config, err)
		fmt.Println(s)
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
			fmt.Println(fmt.Sprintf("Conusmer error : %s ", err.Error()))
			return
		}
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			fmt.Println(fmt.Sprintf("Kafka notification : Rebalanced: %+v ", ntf))
		}
	}()

ConsumerLoop:
	for {
		select {
		case msg, ok := <-consumer.Messages():

			if ok {

				fmt.Println(fmt.Sprintf("Got new kafka message Message Key : %v, Offset : %v, BlockTimestamp : %v", msg.Key, msg.Offset, msg.BlockTimestamp))

				pk := string(msg.Value)

				fmt.Println( fmt.Sprintf("Got message, %v", string(msg.Value)))

				// Get the details from cassandra
				message, err := Select(pk)

				m, err := snappy.Decode(nil, message[0]["local_service_requests_s"].([]byte))
				lsrs := strings.Replace(string(m), `"`, `\"`, -1)

				message[0]["local_service_requests_s"] = lsrs

				out, err := json.Marshal(message)
				if err != nil {
					fmt.Println("Error in marshalling json ", err)
				}

				k := make(map[string]interface{})
				k["results"] = string(out)

				//fmt.Println(fmt.Sprintf("This is what you want : %v", string(message)))
				fmt.Println(fmt.Sprintf("The message that will go out : %v", k))

				if err != nil {

					fmt.Println("Request could not be processed : Error %v", err)
				}

				fmt.Println("This is the data from cassandra : %+v", message)

				if err != nil {

					fmt.Println("Request could not be processed : Error ", err)
					continue
				}


				fmt.Println(fmt.Println("Marking message as consumed -- Got new kafka message Message Key : %v, Offset : %v, BlockTimestamp : %v", msg.Key, msg.Offset, msg.BlockTimestamp))
				consumer.MarkOffset(msg, "")

			} else {

				fmt.Println("Problem with message that came in : msg is : ", msg)
			}

		case <-signals:
			break ConsumerLoop
		}
	}

	return
}