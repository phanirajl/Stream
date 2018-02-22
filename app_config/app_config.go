package app_config

import (
	"errors"
	"fmt"
	"github.com/spf13/viper"
	"os"
	"runtime"
	"strings"
)

// TODO : Some of the configuration items are from older structs from D50 code that was written -- Need to remove the useless ones
type appConfiguration struct {

	// How to fetch the pgxc_ctl file?
	EnableHivePush     bool // if
	EnablePostgresPush bool // if
	SchemaId           int
	StreamLogFolder		string
}

type cassandra struct {

	Host                  []string
	Username              string
	Password              string
	NumConnectionsPerHost int
	ConnectionTimeout     int
	SocketKeepAlive       int
	NumberOfQueryRetries  int
	ReadConsistency       int
	WriteConsistency      int
	KeySpace			  string
}

type postgresxl struct {

	Username              string
	Password              string
	Database              string
	Port                  string
	Host                  []string
	MaxOpenConns          int
	MaxIdleConns          int
	ConnMaxLifetime       int
	ThrottleFirstPG       int
	ThrottleFirstPGEnable bool
}

type kafka struct {

	// Used for push
	EnableKafkaPush       bool
	KafkaBrokers          []string
	KafkaBrokersCassandra []string

	// List of topics to handle from this node..
	TopicsListForThisNode string
	HivePushTopic         string

	AvroSchemaFile           string
	MessageSendRetryAttempts int
	FlushFrequencyMilliSec   int // Milliseconds

	// Enable background worker
	EnableKafkaBackgroundWorkerMode bool
	HDFSConnPath                    string
	HdfsStagingFolder               string
	RecordsPerAvroFile				int
	HdfsDatabaseFolder              string
	AvroArchivesFolder              string
	DisableSnappyCompression        bool
	MessagesInParallel				int

	// Make zip files or not
	EnableMovedFilesArchive bool

	FileMovementIntervalSec      int
	ArchiveMovedFilesIntervalSec int

	SchemaFile			 string
}

//AppConfig stores all the information regarding servers, database, applications
// received from the config file
type AppConfig struct {
	Stream     appConfiguration
	Cassandra  cassandra
	Postgresxl postgresxl
	Kafka      kafka
}

// Dont need to add the .toml in the name here
var configFile = "stream.toml"

// With trailing slash
var linuxConfigFolders = []string{"/etc/"}
var windowsConfigFolders = []string{"\\stream\\"}
var viConfig *viper.Viper
var configLoaded bool

//Config is used to load the configuration from the toml file
var Config AppConfig

/*
	This module handles the application configuration
	configuration can be passed from the configuration file
	also can be set using flags.
	location of the configuration file will either be a standard location or will using command line flags.
*/

func init() {

	GetConfiguration()
}

//GetConfiguration gets the configuration details from the .toml file
func GetConfiguration() {

	configFile, err := getConfigFile()

	if err != nil {

		fmt.Println("There were errors during fetching application config")
		for _, e := range err {
			fmt.Println(e.Error())
		}

		os.Exit(1)
	}

	viConfig = viper.New()

	viConfig.SetConfigFile(configFile)
	viConfig.AutomaticEnv()

	verr := viConfig.ReadInConfig()

	e2 := viConfig.Unmarshal(&Config)

	if e2 != nil {

		fmt.Print("Error marshaling config ", e2)
	}

	if verr != nil {

		fmt.Println("There was an error reading in configuration. Error : ", verr.Error())
	}

	configLoaded = true

}

//GetConfig returns Config that holds the configuration from the toml file
func GetConfig() AppConfig {

	if configLoaded == false {

		GetConfiguration()
	}
	return Config

}

//ShowConfig shows the configuration from the .toml file
func ShowConfig() {

	fmt.Println(viConfig.AllSettings())

}

/*
	Check in common default locations if the config file is available
	Check if there is an environment variable set
	Check if there is a config file passed
	Command line flags will override environment variables and common loctions
		-- If passed and file not found then application will exit with 1
	Environment variables will override common locations
		-- If environment variables set and file not found app will exit with 1
	If flags and env variables not found then common locations will be used
	If config file not found in any of the places then app will exit with  code 1
*/
func getConfigFile() (retFilePath string, retErrors []error) {

	var flagConfigFile string
	//	flag.StringVar(&flagConfigFile, "config_file", "", "Customize the path of the configuration file using this flag.")
	//	flag.Parse()

	envConfigFile := os.Getenv("STREAM_CONFIG_FILE")

	// If we are in windows, check the folders we generally put stuff in
	if runtime.GOOS == "windows" {

		winFolders := getAllWindowsDrives()

		if len(winFolders) == 0 {

			retErrors = append(retErrors, errors.New("windows, no drives found when searching for config"))
		}

	FileChecking:
		// Get the drive letters, loop over them and check which folder seems to be correct
		for _, curDrive := range getAllWindowsDrives() {

			// Loop over all the common windows folders
			for _, curFolder := range windowsConfigFolders {

				curFile := curDrive + ":" + curFolder + configFile

				if fileExists(curFile) {

					// Config file found
					retFilePath = curFile
					break FileChecking

				}
			}
		}
	}

	// If we are in linux, check the folders for that
	if runtime.GOOS == "linux" {

		for _, curFolder := range linuxConfigFolders {

			curFile := curFolder + configFile

			if fileExists(curFile) {

				retFilePath = curFile
				break
			}
		}
	}

	if len(flagConfigFile) > 0 {

		if fileExists(flagConfigFile) {

			retFilePath = flagConfigFile
		} else {

			retErrors = append(retErrors, errors.New("Unable to locate the config_file file '"+flagConfigFile+"' exiting."))
		}
	} else {

		if len(envConfigFile) > 0 {

			// In windows if you use set DWARA_CONFIG_FILE="C:\..." it adds the " to the string also, removing that
			envConfigFile = strings.Trim(envConfigFile, `"`)

			if fileExists(envConfigFile) {

				retFilePath = envConfigFile
			} else {

				retErrors = append(retErrors, errors.New("Unable to locate the config file that is set on environment variable DWARA_CONFIG_FILE "+envConfigFile+" exiting"))
			}
		}
	}

	// In the end if you still don't have anything you have an error
	if retFilePath == "" {

		retErrors = append(retErrors, errors.New("No configuration files named "+configFile+" found."))
	}

	return
}

func getAllWindowsDrives() (availDrives []string) {

	for _, drive := range "ABCDEFGHIJKLMNOPQRSTUVWXYZ" {

		_, err := os.Open(string(drive) + ":\\")

		if err == nil {
			availDrives = append(availDrives, string(drive))
		}
	}

	return
}

func fileExists(curFile string) (retBool bool) {

	_, err := os.Stat(curFile)

	if err == nil {

		retBool = true
	} else {

		fmt.Println("Error locating file, "+curFile, err.Error())
	}

	return
}
