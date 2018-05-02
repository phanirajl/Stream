package main

import (
	"flag"
	"fmt"
	"os"
	"github.com/dminGod/Stream/app_config"
)

func CheckFlags() {
	flag.StringVar(&app_config.FlagConfigFile, "config_file", "", `This is the config file that will be used for starting stream if a file is passed and it is not found stream will exit.`)

	version := flag.Bool("version", false, "prints current build version")
	version_status := flag.Bool("vversion", false, "prints current build version and git status")
	flag.Parse()


	if *version_status {
		fmt.Println(AppVersion)
		fmt.Println(AppVersionSection)
		os.Exit(0)
	}

	if *version {
		fmt.Println(AppVersion)
		os.Exit(0)
	}

}