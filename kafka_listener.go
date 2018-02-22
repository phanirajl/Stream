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
)

var sProducer sarama.SyncProducer
var codec *goavro.Codec


func KafkaListener() (err error) {

	bl := Conf.Kafka.KafkaBrokers
	topicName := Conf.Kafka.TopicsListForThisNode

	config := cluster.NewConfig()

	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	pushTopic := Conf.Kafka.HivePushTopic

	// TODO: 1. Move this out to a file
	// TODO: 2. Let the reference of that file come from config ( Same task for hdfs_push.go HdfsPush() )
	codec, err = goavro.NewCodec(fmt.Sprintf(`
       {"type" : "record", "name" : "%v", "namespace" : "app_db", "fields" : [ { "name" : "local_service_requests_new_con5_pk", "type" : [ "null", "string" ], "default" : null }, { "name" : "actor", "type" : [ "null", "string" ], "default" : null }, { "name" : "application_detail", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "ba_category", "type" : [ "null", "string" ], "default" : null }, { "name" : "ba_segment", "type" : [ "null", "string" ], "default" : null }, { "name" : "bano", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "billing_system", "type" : [ "null", "string" ], "default" : null }, { "name" : "bso_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "bwo_id", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "bso_status_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "bso_error_message_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "cano", "type" : [ "null", "string" ], "default" : null }, { "name" : "channel", "type" : [ "null", "string" ], "default" : null }, { "name" : "charge_type", "type" : [ "null", "string" ], "default" : null }, { "name" : "complete_dt", "type" : [ "null", "string" ], "default" : null }, { "name" : "destination", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "error_message", "type" : [ "null", "string" ], "default" : null }, { "name" : "imsi_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "last_upd", "type" : [ "null", "string" ], "default" : null }, { "name" : "local_service_requests_s", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "location_cd", "type" : [ "null", "string" ], "default" : null }, { "name" : "msisdn_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "ne_id", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_date", "type" : [ "null", "long" ], "default" : null }, { "name" : "order_date_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_ref", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_status", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_sub_type", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "order_type", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_item_status_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "order_item_error_message_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "order_item_error_date_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "pgz_record_type", "type" : [ "null", "string" ], "default" : null }, { "name" : "pgz_task_id", "type" : [ "null", "string" ], "default" : null }, { "name" : "product_name_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "promotion_action_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "promotion_code_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "req_status", "type" : [ "null", "string" ], "default" : null }, { "name" : "request_id", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "resp_status", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "sano", "type" : [ "null", "string" ], "default" : null }, { "name" : "service_action_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "service_code_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "sim_serial_no", "type" : [ "null", "string" ], "default" : null }, { "name" : "submitted_date", "type" : [ "null", "long" ], "default" : null }, { "name" : "submitted_date_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "subscriberid1_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "subscriberid2_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "subscriberid3_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "user_id", "type" : [ "null", "string" ], "default" : null }, { "name" : "user_sys", "type" : [ "null", "string" ], "default" : null }, { "name" : "wo_header_error_message_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "wo_header_status_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "wo_header_error_date_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "bso_error_date_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "int_created_date", "type" : [ "null", "long" ], "default" : null }, { "name" : "int_created_date_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "int_updated_date", "type" : [ "null", "long" ], "default" : null }, { "name" : "int_updated_date_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "int_is_deleted", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_info_error_date_new", "type" : [ "null", "long" ], "default" : null }, { "name" : "order_info_error_date_new_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_info_error_date_time", "type" : [ "null", "long" ], "default" : null }, { "name" : "order_info_error_date_time_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_info_error_date", "type" : [ "null", "long" ], "default" : null }, { "name" : "order_info_error_date_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "request_id_int_field", "type" : [ "null", "string" ], "default" : null }, { "name" : "bwo_id_int_field", "type" : [ "null", "string" ], "default" : null }, { "name" : "retry_count", "type" : [ "null", "long" ], "default" : null }, { "name" : "val1_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val2_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val3_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val4_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val5_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val6_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val7_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val8_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val9_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val10_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val1", "type" : [ "null", "string" ], "default" : null }, { "name" : "val2", "type" : [ "null", "string" ], "default" : null }, { "name" : "val3", "type" : [ "null", "string" ], "default" : null }, { "name" : "val4", "type" : [ "null", "string" ], "default" : null }, { "name" : "val5", "type" : [ "null", "string" ], "default" : null }, { "name" : "val6", "type" : [ "null", "string" ], "default" : null }, { "name" : "val7", "type" : [ "null", "string" ], "default" : null }, { "name" : "val8", "type" : [ "null", "string" ], "default" : null }, { "name" : "val9", "type" : [ "null", "string" ], "default" : null }, { "name" : "val10", "type" : [ "null", "string" ], "default" : null }, { "name" : "resend_flag", "type" : [ "null", "string" ], "default" : null }, { "name" : "file_name", "type" : [ "null", "string" ], "default" : null }, { "name" : "ref_id", "type" : [ "null", "string" ], "default" : null }, { "name" : "lot_no", "type" : [ "null", "string" ], "default" : null }, { "name" : "val11_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val12_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val13_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val14_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val15_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val16_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val17_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val18_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val19_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val20_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null } ]}
	`, pushTopic))

	fmt.Println(fmt.Sprintf("Cassandra Listen Queue -- Current Conf is : %+v", Conf))

	if len(bl) == 0 {

		//logger.Error("No brokers specified, exiting")
		fmt.Println("No brokers specified, can not start the Kafka listener")
		err = errors.New("No brokers specified, can not start the Kafka listener")
		return
	}

	fmt.Println(fmt.Sprintf("Starting Kafaka Worker Server -- Brokers : %v", bl))

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

		fmt.Println(s)
		err = errors.New(s)
		return
	}

	consumer, err := cluster.NewConsumer(bl, "HectorBGWorkers", []string{topicName}, config)
	if err != nil {

		s := fmt.Sprintf("Unable to make consumer, Broker list : %v, Topics_list : %v, Config : %v, got error : %v", bl, topicName, config, err)
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

	fmt.Println(fmt.Sprintf("Starting consumer loop .. "))

	var wg sync.WaitGroup
	i := 0

ConsumerLoop:
	for {
		select {
		case msg, ok := <-consumer.Messages():

			i += 1

			// TODO: Make the number of messages that run in parallel come from configuration
			if (i % 10) == 0 {

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
					fmt.Println(fmt.Sprintf("Error processing cassandra request, Error : %v -- Exiting", err))
					os.Exit(1)
				}

				go KafkaProducer(message, &wg)

				//fmt.Println(fmt.Println("Marking message as consumed -- Got new kafka message Message Key : %v, Offset : %v, BlockTimestamp : %v", msg.Key, msg.Offset, msg.BlockTimestamp))
				fmt.Println("cons")
				consumer.MarkOffset(msg, "")
			} else {

				fmt.Println("Problem with message that came in : msg is : ")
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
			fmt.Println("Failed to start Sarama producer:", err)
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
					fmt.Println("There was an error decoding the compressed message, Error : ", err)
					os.Exit(1)
				}

				lsrs := strings.Replace(string(m), `"`, `\"`, -1)
				vv["local_service_requests_s"] = []string{lsrs}
			}
		}

		// Make it into JSON
		tmpJson, err := json.Marshal(vv)
		if err != nil {
			fmt.Println("Could not parse JSON for message : ", err)
			os.Exit(1)
			continue
		}

		h := snappy.Encode(nil, tmpJson)

		_, _, err = sProducer.SendMessage(&sarama.ProducerMessage{
			Topic: pushTopic,
			Value: sarama.ByteEncoder(h),
		})
		if err != nil {
			fmt.Println(fmt.Sprintf("Failed to send message to schema producer, Size : %v, Error: %v", buffer.Len(), err))
			os.Exit(1)
		}
	}

	wg.Done()
}
