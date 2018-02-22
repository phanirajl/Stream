package main

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/bsm/sarama-cluster"
	"github.com/linkedin/goavro"
	"os"
	"os/signal"
	"github.com/gocql/gocql"
	"time"
	"strconv"
	"github.com/colinmarc/hdfs"
	"strings"
	"encoding/json"
	"github.com/golang/snappy"
)


var hdfsClient *hdfs.Client

func HdfsPush() {

	var err error

	bl := Conf.Kafka.KafkaBrokers
	topicName := Conf.Kafka.HivePushTopic

	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})

	config := cluster.NewConfig()

	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	// options ClientOptions

	hdfsClient, err = hdfs.NewClient(hdfs.ClientOptions {Addresses: []string{ Conf.Kafka.HDFSConnPath }, User: "hdfs" })
	if err != nil {
		fmt.Println("Could not connect to HDFS, ", err)
		os.Exit(1)
	}

	codec, err = goavro.NewCodec(fmt.Sprintf(`
       {"type" : "record", "name" : "%v", "namespace" : "app_db", "fields" : [ { "name" : "local_service_requests_new_con5_pk", "type" : [ "null", "string" ], "default" : null }, { "name" : "actor", "type" : [ "null", "string" ], "default" : null }, { "name" : "application_detail", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "ba_category", "type" : [ "null", "string" ], "default" : null }, { "name" : "ba_segment", "type" : [ "null", "string" ], "default" : null }, { "name" : "bano", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "billing_system", "type" : [ "null", "string" ], "default" : null }, { "name" : "bso_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "bwo_id", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "bso_status_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "bso_error_message_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "cano", "type" : [ "null", "string" ], "default" : null }, { "name" : "channel", "type" : [ "null", "string" ], "default" : null }, { "name" : "charge_type", "type" : [ "null", "string" ], "default" : null }, { "name" : "complete_dt", "type" : [ "null", "string" ], "default" : null }, { "name" : "destination", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "error_message", "type" : [ "null", "string" ], "default" : null }, { "name" : "imsi_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "last_upd", "type" : [ "null", "string" ], "default" : null }, { "name" : "local_service_requests_s", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "location_cd", "type" : [ "null", "string" ], "default" : null }, { "name" : "msisdn_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "ne_id", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_date", "type" : [ "null", "long" ], "default" : null }, { "name" : "order_date_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_ref", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_status", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_sub_type", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "order_type", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_item_status_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "order_item_error_message_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "order_item_error_date_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "pgz_record_type", "type" : [ "null", "string" ], "default" : null }, { "name" : "pgz_task_id", "type" : [ "null", "string" ], "default" : null }, { "name" : "product_name_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "promotion_action_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "promotion_code_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "req_status", "type" : [ "null", "string" ], "default" : null }, { "name" : "request_id", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "resp_status", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "sano", "type" : [ "null", "string" ], "default" : null }, { "name" : "service_action_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "service_code_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "sim_serial_no", "type" : [ "null", "string" ], "default" : null }, { "name" : "submitted_date", "type" : [ "null", "long" ], "default" : null }, { "name" : "submitted_date_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "subscriberid1_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "subscriberid2_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "subscriberid3_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "user_id", "type" : [ "null", "string" ], "default" : null }, { "name" : "user_sys", "type" : [ "null", "string" ], "default" : null }, { "name" : "wo_header_error_message_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "wo_header_status_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "wo_header_error_date_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "bso_error_date_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "int_created_date", "type" : [ "null", "long" ], "default" : null }, { "name" : "int_created_date_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "int_updated_date", "type" : [ "null", "long" ], "default" : null }, { "name" : "int_updated_date_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "int_is_deleted", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_info_error_date_new", "type" : [ "null", "long" ], "default" : null }, { "name" : "order_info_error_date_new_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_info_error_date_time", "type" : [ "null", "long" ], "default" : null }, { "name" : "order_info_error_date_time_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "order_info_error_date", "type" : [ "null", "long" ], "default" : null }, { "name" : "order_info_error_date_str", "type" : [ "null", "string" ], "default" : null }, { "name" : "request_id_int_field", "type" : [ "null", "string" ], "default" : null }, { "name" : "bwo_id_int_field", "type" : [ "null", "string" ], "default" : null }, { "name" : "retry_count", "type" : [ "null", "long" ], "default" : null }, { "name" : "val1_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val2_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val3_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val4_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val5_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val6_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val7_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val8_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val9_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val10_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val1", "type" : [ "null", "string" ], "default" : null }, { "name" : "val2", "type" : [ "null", "string" ], "default" : null }, { "name" : "val3", "type" : [ "null", "string" ], "default" : null }, { "name" : "val4", "type" : [ "null", "string" ], "default" : null }, { "name" : "val5", "type" : [ "null", "string" ], "default" : null }, { "name" : "val6", "type" : [ "null", "string" ], "default" : null }, { "name" : "val7", "type" : [ "null", "string" ], "default" : null }, { "name" : "val8", "type" : [ "null", "string" ], "default" : null }, { "name" : "val9", "type" : [ "null", "string" ], "default" : null }, { "name" : "val10", "type" : [ "null", "string" ], "default" : null }, { "name" : "resend_flag", "type" : [ "null", "string" ], "default" : null }, { "name" : "file_name", "type" : [ "null", "string" ], "default" : null }, { "name" : "ref_id", "type" : [ "null", "string" ], "default" : null }, { "name" : "lot_no", "type" : [ "null", "string" ], "default" : null }, { "name" : "val11_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val12_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val13_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val14_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val15_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val16_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val17_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val18_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val19_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null }, { "name" : "val20_key", "type" : [ "null", { "type" : "array", "items" : [ "null", "string" ] } ], "default" : null } ]}
	`, topicName))

	fmt.Println(fmt.Sprintf("HDFS Push Mode -- Current Conf is : %+v", Conf))

	if len(bl) == 0 {

		//logger.Error("No brokers specified, exiting")
		fmt.Println("No brokers specified, can not start the Kafka listener")
		err = errors.New("No brokers specified, can not start the Kafka listener")
		return
	}

	fmt.Println("Starting Kafaka Worker Server -- Brokers : %v", bl)

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

	consumer, err := cluster.NewConsumer(bl, "HectorBGWorkersHDFSPush", []string{topicName}, config)
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

//	var wg sync.WaitGroup
	i := 0

	t := strconv.Itoa(int(time.Now().Unix()))
	ran, err := gocql.RandomUUID()
	if err != nil {
		fmt.Println("Error generating random UUID ", err)
	}

	f, err := os.Create(fmt.Sprintf("/tmp/%v_%v.avro", ran.String(), t ))
	if err != nil {
		fmt.Println("Error creating file", err)
	}

	ow, err := goavro.NewOCFWriter(goavro.OCFConfig{W: f, CompressionName: "snappy", Codec : codec })
	if err != nil {
		fmt.Println("Could not create OW, ", err)
		os.Exit(1)
	}

	fmt.Println("Starting loop")

	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})

ConsumerLoop:
	for {
		select {
		case msg, ok := <-consumer.Messages():

			i += 1

			if (i % 30) == 0 {

				// fmt.Println("Waiting after 10 messages async..")
		//		wg.Wait()
				fmt.Println("Going to close the file f ", f.Name())
				fn := f.Name()
				rfn := strings.Replace(fn, "/tmp/", "", 1)
				f.Close()

				err := hdfsClient.CopyToRemote(fn, Conf.Kafka.HdfsDatabaseFolder + "/" + rfn)
				if err != nil {
					fmt.Println("Error moving file to HDFS, ", err)
					os.Exit(1)
				}

				t := strconv.Itoa(int(time.Now().Unix()))
				ran, err := gocql.RandomUUID()
				if err != nil {
					fmt.Println("Error generating random UUID ", err)
				}

				f, err = os.Create(fmt.Sprintf("/tmp/%v_%v.avro", ran.String(), t ))
				if err != nil {
					fmt.Println("Error creating file", err)
					os.Exit(1)
				}

				fmt.Println("Changing the ow")
				ow, err = goavro.NewOCFWriter(goavro.OCFConfig{W: f, CompressionName: "snappy", Schema : codec.Schema() })
				// ow, err = goavro.NewOCFWriter(goavro.OCFConfig{W: f, Schema : codec.Schema() })
				if err != nil {
					fmt.Println("Could not create OW, ", err)
					os.Exit(1)
				}
			}

//			wg.Add(1)

			if ok {

				// var rdr *bytes.Reader
				var gr []interface{}

				sdc := msg.Value
				//fmt.Println("Raw message is : ", string(sdc)[0:100])
				sdc, err := snappy.Decode(nil, msg.Value)
				if err != nil {
					fmt.Println(fmt.Sprintf("Error decoding snappy message, Error : %v", err))
					consumer.MarkOffset(msg, "")
					continue
				}

				//rdr = bytes.NewReader( sdc )
				//dec := gob.NewDecoder( rdr )
				//
				//err = dec.Decode( &gr )
				//if err != nil {
				//	fmt.Println("Could not decode message ", err)
				//	os.Exit(1)
				//}

				var k lsr_record

				err = json.Unmarshal(sdc, &k)
				if err != nil {
					fmt.Println(fmt.Sprintf("JSON Decode error %v -- Payload :  ", err, string(sdc)[0:200]))
					consumer.MarkOffset(msg, "")
					continue
				}

				gr = k.getGenericRecord()

				//if len(fmt.Sprintf("Will be added to file : %+v", gr)) > 300 {
				//
				//	fmt.Println(fmt.Sprintf("Will be added to file : %+v", gr)[0:300])
				//} else {
				//
				//	fmt.Println(fmt.Sprintf("Will be added to file : %+v", gr))
				//}

				fmt.Print(".")
				err = ow.Append( gr )
				if err != nil {
					fmt.Println("Error when appending to ow file : ", err)
					consumer.MarkOffset(msg, "")
					continue
				}

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
