package main

import (
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"time"
	"github.com/antigloss/go/logger"
)

var cassandraSession *gocql.Session
var cassandraSessionError error
var cassandraHost []string

var cassandraUID string
var cassandraPass string

// TODO: This method has to return an error and it should be checked and handled where its called
func LoadPool() (err error) {

	cassandraHost = Conf.Cassandra.Host
	cassandraUID = Conf.Cassandra.Username
	cassandraPass = Conf.Cassandra.Password

	//gocql.NumConnctions = Conf.Cassandra.NumConnectionsPerHost

	cluster := gocql.NewCluster(cassandraHost...)

	// TODO: Database name for casssandra has to come from config -- will need to add an extra field for it
	//cluster.Keyspace = "alltrade_test"
	cluster.Keyspace = Conf.Cassandra.KeySpace
	cluster.ProtoVersion = 3
	cluster.Timeout = time.Duration(Conf.Cassandra.ConnectionTimeout) * time.Second
	cluster.SocketKeepalive = time.Duration(Conf.Cassandra.SocketKeepAlive) * time.Second
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: Conf.Cassandra.NumberOfQueryRetries}

	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: cassandraUID,
		Password: cassandraPass,
	}

	//var err error

	cassandraSession, err = cluster.CreateSession()

	if err != nil {
		// TODO: Return a proper error
		logger.Error("ErrorType : INFRA_ERROR - Cassandra Connection could not be established, please check! %v", err)
		cassandraSessionError = err
	} else {
		cassandraSessionError = nil
	}
	return
}

func GetSession() (retSession *gocql.Session, err error) {

	if cassandraSession == nil {
		// TODO: From LoadPool, send the actual error out, bubble that error out and use that
		LoadPool()
	}

	//if cassandraSession == nil {
	//
	//	return nil, errors.New("Could not connect to cassandra")
	//}

	retSession = cassandraSession
	err = cassandraSessionError
	return
}

func Select(pkRef string) ([]map[string]interface{}, error) {

	// Check if the array is not blank
	if len(pkRef) == 0 {
		logger.Error("Req : ErrorType : QUERY_ERROR, Query was sent blank, returning")
		return nil, errors.New("Query was sent blank, returning")
	}

	session, err := GetSession()
	if err != nil {
		logger.Error("ErrorType : INFRA_ERROR - Cassandra Connection could not be established -- Error : %v", err.Error())
		return nil, errors.New("Cassandra Connection could not be established, please check")
	}

	var q string

	q = fmt.Sprintf(`SELECT
   local_service_requests_new_con5_pk,
   blobAsBigInt(timestampAsBlob(int_created_date)) as int_created_date,
   actor,
   ba_category,
   ba_segment,
   bano,
   billing_system,
   bso_error_date_key,
   bso_error_message_key ,
   bso_key ,
   bso_status_key ,
   bwo_id ,
   cano,
   channel,
   charge_type,
   complete_dt,
   destination ,
   error_message,
   file_name,
   imsi_key ,
   blobAsBigInt(timestampAsBlob(int_updated_date)) as int_updated_date,
   last_upd,
   local_service_requests_s ,
   location_cd,
   lot_no,   
   msisdn_key ,
   ne_id,
   blobAsBigInt(timestampAsBlob(order_date)) as order_date,
   blobAsBigInt(timestampAsBlob(order_info_error_date)) as order_info_error_date,
   order_item_error_date_key ,
   order_item_error_message_key ,
   order_item_status_key ,
   order_ref,
   order_status,
   order_sub_type ,
   order_type,
   pgz_record_type,
   pgz_task_id,
   product_name_key ,
   promotion_action_key ,
   promotion_code_key ,
   ref_id,
   req_status,
   request_id ,
   resend_flag,
   resp_status ,
   retry_count,
   sano,
   service_action_key ,
   service_code_key ,
   sim_serial_no,
   blobAsBigInt(timestampAsBlob(submitted_date)) as submitted_date,
   subscriberid1_key ,
   subscriberid2_key ,
   subscriberid3_key ,
   user_id,
   user_sys,
   val1,
   val10,
   val10_key ,
   val11_key ,
   val12_key ,
   val13_key ,
   val14_key ,
   val15_key ,
   val16_key ,
   val17_key ,
   val18_key ,
   val19_key ,
   val1_key ,
   val2,
   val20_key ,
   val2_key ,
   val3,
   val3_key ,
   val4,
   val4_key ,
   val5,
   val5_key ,
   val6,
   val6_key ,
   val7,
   val7_key ,
   val8,
   val8_key ,
   val9,
   val9_key ,
   wo_header_error_date_key ,
   wo_header_error_message_key ,
   wo_header_status_key 
FROM alltrade_test.local_service_requests_new_con5
WHERE local_service_requests_new_con5_pk IN (%v) `, pkRef)

	iter := session.Query(q).Iter()
	result, err := iter.SliceMap()

	if err != nil {

		logger.Error("ErrorType : QUERY_ERROR, Error fetching details, Error: %v -- Query : %v", err.Error(), q)
		return nil, errors.New("QUERY_ERROR, Error fetching details")
	}

	return result, nil
}
