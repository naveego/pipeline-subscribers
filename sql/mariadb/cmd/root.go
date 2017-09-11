// Copyright © 2017 Naveego
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"flag"
	"fmt"
	"os"

	"github.com/Sirupsen/logrus"
	"github.com/naveego/navigator-go/subscribers/server"
	"github.com/naveego/pipeline-subscribers/shapeutils"
	"github.com/spf13/cobra"
)

var verbose *bool

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "mariadb [listen address] [broadcast address*]",
	Args:  cobra.RangeArgs(1, 2),
	Short: "A subscriber that sends all data to MariaDB",
	Long: `Settings should contain a DataSourceName property with a value 
corresponding to the standard MariaDB/MySQL connection string: "user:password@address:port/database".`,

	RunE: func(cmd *cobra.Command, args []string) error {

		logrus.SetOutput(os.Stdout)

		if len(os.Args) < 2 {
			fmt.Println("Not enough arguments.")
			os.Exit(-1)
		}

		addr := args[0]

		if *verbose {
			logrus.SetLevel(logrus.DebugLevel)
		}

		subscriber := &mariaSubscriber{
			knownShapes: shapeutils.NewShapeCache(),
		}

		srv := server.NewSubscriberServer(addr, subscriber)

		err := srv.ListenAndServe()

		return err
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {

	verbose = flag.Bool("v", false, "enable verbose logging")
}
