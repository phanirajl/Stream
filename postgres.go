package main

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/antigloss/go/logger"
	"github.com/golang/snappy"
	_ "github.com/lib/pq"
	"os"
	"strings"

	"github.com/bsm/sarama-cluster"
	"math/rand"
	"os/signal"
	"time"
)

//var dbHost, dbPort, dbUser, dbPass, dbName string
var dbpool []*sql.DB

// TODO : Write logic to make inserts into Postgres-XL as well.

func PostgresPush() {

	bl := Conf.Kafka.KafkaBrokers
	topicName := Conf.Kafka.TopicsListForThisNode

	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	if len(bl) == 0 {

		//logger.Error("No brokers specified, exiting")
		e := "No brokers specified, can not start the Kafka listener"
		logger.Error(e)
		//err := errors.New(e)
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

	//consume errors
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

	connection, errdb := getConnection()

	if errdb != nil {

		logger.Error(fmt.Sprintf(" ErrorType : INFRA_ERROR, Unable to reach any Postgres servers! PANIC, Error : %v", errdb.Error()))
		os.Exit(1)

	}

ConsumerLoop:
	for {
		select {
		case msg, ok := <-consumer.Messages():

			if ok {

				pk := string(msg.Value)

				// Get the details from cassandra

				//pk := "3b257f21-0725-490e-881b-a00fbf65c0a0"
				//pk := "a4596dbc-e12b-42e4-b67e-5be55c4f44cc"
				message, err := Select(pk)
				if err != nil {
					logger.Error("Error processing cassandra request, Error : %v -- Exiting", err)
					os.Exit(1)
				}

				//go KafkaProducer(message, &wg)

				q := GenerateQuery(message)

				//q := GenerateQuery(msg)
				_, err = connection.Exec(q)
				if err != nil {

					logger.Error("There was an error with executing this query", err)
					os.Exit(1)
				}

				connection.Close()

				consumer.MarkOffset(msg, "")
			} else {

				logger.Error("Problem with message that came in -- Channel says ok is false")
			}

		case <-signals:
			break ConsumerLoop
		}
	}

}
func GenerateQuery(message []map[string]interface{}) (q string) {

	var tbName = "local_service_requests_new_con5"

	//var keys, vals string

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

		q = MakePgQuery(tbName, vv)

		//q = fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v);", tbName, keys, vals)
		fmt.Printf("\ninsert stmt %v\n", q)
		//os.Stdout.Write([]byte(q))

	}
	return
}

//func GenerateQuery(msg *sarama.ConsumerMessage) (q string){
////func GenerateQuery(msg []byte) (q string){
//	//fmt.Printf("came here")
//	var tbName = "local_service_requests_new_con5"
//	sdc := msg.Value
//	sdc, err := snappy.Decode(nil, sdc)
//	if err != nil {
//
//		logger.Error("Error decoding snappy message, Error : %v", err)
//		os.Exit(1)
//	}
//
//	var k interface{}
//
//	err = json.Unmarshal(sdc, &k)
//	if err != nil {
//
//		logger.Error("Error converting JSON:", err)
//		os.Exit(1)
//	}
//
//	var keys, vals string
//
//
//	if _, ok := k.(map[string]interface{}); ok {
//
//		l := k.(map[string]interface{})
//
//		for key, v := range l {
//
//			switch vv := v.(type) {
//
//			case string:
//				if key == "local_service_requests_s"{
//					keys += fmt.Sprintf("%v,", key)
//					vals += fmt.Sprintf("'{%v}',", vv)
//					continue
//				}
//				keys += fmt.Sprintf("%v,", key)
//				vals += fmt.Sprintf("'%v',", vv)
//				//fmt.Printf("this is string %v\n", key)
//
//			case []interface{}:
//				//fmt.Printf("array of interface for %v\n", key)
//				var tmpArr []string
//
//				for _, vvv := range vv {
//
//					switch vvv.(type) {
//
//					case string:
//						tmpArr = append(tmpArr, vvv.(string))
//					}
//				}
//
//				if len(tmpArr) > 0 {
//					keys += fmt.Sprintf("%v,", key)
//					tmpVals := fmt.Sprintf("'%v'", strings.Join(tmpArr, "','"))
//					vals += fmt.Sprintf("ARRAY[%v]::text[],", tmpVals)
//				}
//
//
//			case int64:
//				//fmt.Printf("this is int64 %v\n",key)
//				keys += fmt.Sprintf("%v,", key)
//				vals += fmt.Sprintf("%v,", vv)
//
//
//			case int:
//				//fmt.Printf("this is int %v\n", key)
//				keys += fmt.Sprintf("%v,", key)
//				vals += fmt.Sprintf("%v,", vv)
//
//			case float64:
//				//fmt.Printf("this is float64 %v\n",key)
//				if key == "int_created_date_cass" { continue }
//				if key == "int_created_date_cas" { continue }
//
//				if key == "int_created_date" {
//
//					dt := int64((vv / 1000) * 1000)
//
//					keys += fmt.Sprintf("%v,", "int_created_date_cass")
//					vals += fmt.Sprintf("to_timestamp(%v),", dt)
//
//					continue
//				}
//
//				if key == "int_updated_date" {
//					dt := int64((vv / 1000) * 1000)
//
//					keys += fmt.Sprintf("%v,", "int_updated_date")
//					vals += fmt.Sprintf("to_timestamp(%v),", dt)
//
//					continue
//				}
//				if key == "order_date" {
//					dt := int64((vv / 1000) * 1000)
//
//					keys += fmt.Sprintf("%v,", "order_date")
//					vals += fmt.Sprintf("to_timestamp(%v),", dt)
//
//					continue
//				}
//
//				if key == "submitted_date" {
//					dt := int64((vv / 1000) * 1000)
//
//					keys += fmt.Sprintf("%v,", "submitted_date")
//					vals += fmt.Sprintf("to_timestamp(%v),", dt)
//
//					continue
//				}
//
//				if key == "order_info_error_date_new" {
//					dt := int64((vv / 1000) * 1000)
//
//					keys += fmt.Sprintf("%v,", "order_info_error_date_new")
//					vals += fmt.Sprintf("to_timestamp(%v),", dt)
//
//					continue
//				}
//
//				if key == "order_info_error_date_time" {
//					dt := int64((vv / 1000) * 1000)
//
//					keys += fmt.Sprintf("%v,", "order_info_error_date_time")
//					vals += fmt.Sprintf("to_timestamp(%v),", dt)
//
//					continue
//				}
//
//				if key == "order_info_error_date" {
//					dt := int64((vv / 1000) * 1000)
//
//					keys += fmt.Sprintf("%v,", "order_info_error_date")
//					vals += fmt.Sprintf("to_timestamp(%v),", dt)
//
//					continue
//				}
//
//
//
//
//				dt := int64((vv / 1000) * 1000)
//				keys += fmt.Sprintf("%v,", key)
//
//				if dt > 1000000 {
//
//					vals += fmt.Sprintf("to_timestamp(%v),", dt)
//				} else {
//
//					vals += fmt.Sprintf("%v,", dt)
//				}
//
//			default:
//				fmt.Printf("`Field : %v, Value : %v, Type : `\n", key, vv)
//
//			}
//		}
//
//		keys = strings.TrimRight(keys, ",")
//		vals = strings.TrimRight(vals, ",")
//
//
//		q = fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v);", tbName, keys, vals)
//		//os.Stdout.Write([]byte(q))
//
//
//	} else {
//
//		logger.Error("Expecting data as map[string]interface{} got : ", k)
//		os.Exit(1)
//	}
//
//	return
//
//}

func getConnection() (*sql.DB, error) {

	checkCount := 0

	//	Conf := config.Get()

getNew:
	if len(dbpool) > 0 {

		randNum := rand.Intn(len(dbpool))

		if dbpool[randNum].Ping() != nil {

			if checkCount < 10 {

				logger.Warn(fmt.Sprintf("Not able to ping server number %v for postgres connection, please remove this from config.", randNum))

				checkCount += 1

				goto getNew
			} else {

				logger.Error("ErrorType : INFRA_ERROR, Tried 9 times not able to get connection from pool, giving up. Too many tries to server")
				return nil, errors.New("Tried 9 times not able to get connection from pool, giving up. Too many tries to server")
			}
		} else {

			//if dbAbstract.Quiet == false {
			//
			logger.Info(fmt.Sprintf("Returning connection : %v , Total Size: %v , Connections : %v , Stats : %v", randNum, len(dbpool), dbpool, dbpool[randNum].Stats()))
			//}
			//
			return dbpool[randNum], nil
		}
	} else {

		logger.Error("ErrorType : INFRA_ERROR, Unable to reach any Postgres servers! PANIC, Trying to reload")
		LoadPostgres()
		goto getNew
	}

	return nil, errors.New("Error in establishing connection")
}

func LoadPostgres() {

	//time.Sleep(2 * time.Second)
	//Conf := app_config.Get()

	dbName := Conf.Postgresxl.Database
	dbUser := Conf.Postgresxl.Username
	dbPass := Conf.Postgresxl.Password

	rand.Seed(time.Now().UTC().UnixNano())

	for _, curDB := range Conf.Postgresxl.Host {

		// Get the host and port
		curDBSplit := strings.Split(curDB, ":")

		if len(curDBSplit) == 1 {

			logger.Error(fmt.Sprintf("ErrorType : CONFIG_ERROR, Got server configured without port information skipping server, Server: %v", curDB))
			continue
		}

		dbHost := curDBSplit[0]
		dbPort := curDBSplit[1]

		var dbInfo string

		if len(dbPass) == 0 {

			dbInfo = fmt.Sprintf("user=%s dbname=%s sslmode=disable host=%s port=%s",
				dbUser, dbName, dbHost, dbPort)
		} else {

			dbInfo = fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable host=%s port=%s",
				dbUser, dbPass, dbName, dbHost, dbPort)
		}

		dbpoolConn, err := sql.Open("postgres", dbInfo)

		dbpoolConn.SetMaxOpenConns(Conf.Postgresxl.MaxOpenConns)
		dbpoolConn.SetMaxIdleConns(Conf.Postgresxl.MaxIdleConns)
		dbpoolConn.SetConnMaxLifetime(time.Duration(Conf.Postgresxl.ConnMaxLifetime) * time.Second)

		if err != nil {

			logger.Error(fmt.Sprintf("ErrorType : INFRA_ERROR, Not able to connect to DB Server : %v , Got error : %v", dbHost, err.Error()))
			continue
		} else {

			logger.Info(fmt.Sprintf("Adding connection to pool, Server : %v ,Port : %v", dbHost, dbPort))
			dbpool = append(dbpool, dbpoolConn)
		}
	}

}
