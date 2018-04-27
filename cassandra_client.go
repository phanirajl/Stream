package main

import (
	"errors"
	"fmt"
	"github.com/antigloss/go/logger"
	"github.com/gocql/gocql"
	"time"
)

var cassandraSession *gocql.Session
var cassandraSessionError error
var cassandraHost []string

var cassandraUID string
var cassandraPass string


func LoadPool() (err error) {

	cassandraHost = Conf.Cassandra.Host
	cassandraUID = Conf.Cassandra.Username
	cassandraPass = Conf.Cassandra.Password

	cluster := gocql.NewCluster(cassandraHost...)

	cluster.Keyspace = Conf.Cassandra.KeySpace
	cluster.ProtoVersion = 3
	cluster.Timeout = time.Duration(Conf.Cassandra.ConnectionTimeout) * time.Second
	cluster.SocketKeepalive = time.Duration(Conf.Cassandra.SocketKeepAlive) * time.Second
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: Conf.Cassandra.NumberOfQueryRetries}

	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: cassandraUID,
		Password: cassandraPass,
	}

	cassandraSession, err = cluster.CreateSession()

	if err != nil {

		logger.Error("ErrorType : INFRA_ERROR - Cassandra Connection could not be established, please check! %v", err)
		cassandraSessionError = err
	} else {
		cassandraSessionError = nil
	}
	return
}

func GetSession() (retSession *gocql.Session, err error) {

	if cassandraSession == nil {
		LoadPool()
	}

	retSession = cassandraSession
	if cassandraSessionError != nil {
		err = cassandraSessionError
	}
	return
}

func Select(qry string, pkRef string) ([]map[string]interface{}, error) {

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

	// Set the read consistency
	// TODO: This should come from the config
	session.SetConsistency(gocql.One)

	iter := session.Query(qry, pkRef).Iter()

	result, err := iter.SliceMap()
	if err != nil {
		logger.Error("ErrorType : QUERY_ERROR, Error fetching details, Error: %v -- Query : %v -- PkRef : %v", err.Error(), qry, pkRef)
		return nil, errors.New(fmt.Sprintf("QUERY_ERROR, Error fetching details - Error : %v - Query : %v -- PkRef : %v", err.Error(), qry, pkRef))
	}

	return result, nil
}
