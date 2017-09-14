package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/Sirupsen/logrus"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/naveego/navigator-go/subscribers/server"
)

var (
	verbose = flag.Bool("v", false, "enable verbose logging")
)

func main() {

	logrus.SetOutput(os.Stdout)

	if len(os.Args) < 2 {
		fmt.Println("Not enough arguments.")
		os.Exit(-1)
	}

	flag.Parse()

	addr := os.Args[1]

	//if *verbose {
	logrus.SetLevel(logrus.DebugLevel)
	//}

	subscriber := &mssqlSubscriber{}

	srv := server.NewSubscriberServer(addr, subscriber)

	err := srv.ListenAndServe()
	if err != nil {
		logrus.Fatal("Error shutting down server: ", err)
	}
}
