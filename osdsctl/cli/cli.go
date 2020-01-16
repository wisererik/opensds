// Copyright 2019 The OpenSDS Authors.
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
/*
This module implements a entry into the OpenSDS CLI service.

*/

package cli

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"

	c "github.com/opensds/opensds/client"
	"github.com/opensds/opensds/pkg/utils"
	"github.com/opensds/opensds/pkg/utils/constants"
	"github.com/spf13/cobra"
)

var (
	client      *c.Client
	rootCommand = &cobra.Command{
		Use:   "osdsctl",
		Short: "Administer the opensds storage cluster",
		Long:  `Admin utility for the opensds unified storage cluster.`,
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Usage()
			os.Exit(1)
		},
	}
	Debug bool
	//globalFlags = GlobalFlags{}
)

func init() {
	rootCommand.AddCommand(versionCommand)
	rootCommand.AddCommand(volumeCommand)
	rootCommand.AddCommand(dockCommand)
	rootCommand.AddCommand(poolCommand)
	rootCommand.AddCommand(profileCommand)
	rootCommand.AddCommand(fileShareCommand)
	rootCommand.AddCommand(hostCommand)
	flags := rootCommand.PersistentFlags()
	flags.BoolVar(&Debug, "debug", false, "shows debugging output.")
	/*flags.BoolVar(&globalFlags.Debug, "debug", false, "shows debugging output.")

	flags.StringVar(&globalFlags.Endpoint, "endpoint", "", "opensds endpoint")
	flags.StringVar(&globalFlags.AuthStrategy, "authstrategy", "", "opensds auth strategy")
	flags.StringVar(&globalFlags.TLS.CertFile, "cert", "", "secure client using TLS certificate file")
	flags.StringVar(&globalFlags.TLS.KeyFile, "key", "", "secure client using TLS key file")
	flags.StringVar(&globalFlags.TLS.TrustedCAFile, "cacert", "", "TLS-enabled secure servers certificates verification using CA bundle")
	Debug = globalFlags.Debug*/
}

type DummyWriter struct{}

// do nothing
func (writer DummyWriter) Write(data []byte) (n int, err error) {
	return len(data), nil
}

type DebugWriter struct{}

// do nothing
func (writer DebugWriter) Write(data []byte) (n int, err error) {
	Debugf("%s", string(data))
	return len(data), nil
}

func ExitWithError(err error) {
	fmt.Fprintln(os.Stderr, "Error:", err)
	os.Exit(1)
}

func getValueFromCmd(key string) string {
	var value string
	var err error
	if value, err = rootCommand.Flags().GetString(key); err != nil {
		ExitWithError(err)
	} else if key == "" && rootCommand.Flags().Changed(key) {
		ExitWithError(errors.New(fmt.Sprintf("empty string is passed to --%s  option", key)))
	}
	return value
}

// Run method indicates how to start a cli tool through cobra.
func Run() error {
	if !utils.Contained("--debug", os.Args) {
		log.SetOutput(DummyWriter{})
	} else {
		log.SetOutput(DebugWriter{})
	}

	/*endpoint := getValueFromCmd("endpoint")
	cfg := &c.Config{Endpoint: endpoint}
	u, _ := url.Parse(endpoint)
	if u.Scheme == "https" {
		cert := getValueFromCmd("cert")
		key := getValueFromCmd("key")
		cacert := getValueFromCmd("cacert")
		httpsOptions := c.NewKHttpsOptions(cert, key, cacert)
		cfg.HttpsOptions = httpsOptions
	}

	authStrategy := getValueFromCmd("authstrategy")*/


	ep, ok := os.LookupEnv(c.OpensdsEndpoint)
	if !ok {
		ep = constants.DefaultOpensdsEndpoint
		Warnf("OPENSDS_ENDPOINT is not specified, use default(%s)\n", ep)
	}

	cfg := &c.Config{Endpoint: ep}

	authStrategy, ok := os.LookupEnv(c.OpensdsAuthStrategy)
	if !ok {
		authStrategy = c.Noauth
		Warnf("Not found Env OPENSDS_AUTH_STRATEGY, use default(noauth)\n")
	}

	u, _ := url.Parse(ep)
	if u.Scheme == "https" {
		cert, ok := os.LookupEnv(c.OpensdsClientCert)
		if !ok {
			cert = constants.OpensdsClientCertFile
			Warnf("OPENSDS_CLIENT_CERT is not specified, use default(%s)\n", cert)
		}
		key, ok := os.LookupEnv(c.OpensdsClientKey)
		if !ok {
			key = constants.OpensdsClientKeyFile
			Warnf("OPENSDS_CLIENT_KEY is not specified, use default(%s)\n", key)
		}
		cacert, ok := os.LookupEnv(c.OpensdsCACert)
		if !ok {
			cacert = constants.OpensdsCaCertFile
			Warnf("OPENSDS_CA_CERT is not specified, use default(%s)\n", cacert)
		}
		httpsOptions := c.NewKHttpsOptions(cert, key, cacert)
		cfg.HttpsOptions = httpsOptions
	}

	var authOptions c.AuthOptions
	var err error

	switch authStrategy {
	case c.Keystone:
		authOptions, err = c.LoadKeystoneAuthOptionsFromEnv()
		if err != nil {
			return err
		}
	case c.Noauth:
		authOptions = c.LoadNoAuthOptionsFromEnv()
	default:
		authOptions = c.NewNoauthOptions(constants.DefaultTenantId)
	}

	cfg.AuthOptions = authOptions

	client, err = c.NewClient(cfg)
	if client == nil || err != nil {
		return err
	}

	return rootCommand.Execute()
}
