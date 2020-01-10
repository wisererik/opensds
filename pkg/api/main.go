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
This module implements a entry into the OpenSDS northbound REST service.
*/

package api

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"

	"github.com/astaxie/beego"
	"github.com/opensds/opensds/pkg/api/filter/accesslog"
	"github.com/opensds/opensds/pkg/api/filter/auth"
	"github.com/opensds/opensds/pkg/api/filter/context"
	"github.com/opensds/opensds/pkg/api/filter/validation"
	cfg "github.com/opensds/opensds/pkg/utils/config"
	"github.com/opensds/opensds/pkg/utils/constants"

	// Load the API routers
	_ "github.com/opensds/opensds/pkg/api/routers"
)

const (
	AddressIdx = iota
	PortIdx
)

func Run(apiServerCfg cfg.OsdsApiServer) {

	if apiServerCfg.HTTPSEnabled {
		if apiServerCfg.BeegoHTTPSCertFile == "" || apiServerCfg.BeegoHTTPSKeyFile == "" {
			fmt.Println("If https is enabled in hotpot, please ensure key file and cert file of the hotpot are not empty.")
			return
		}
		// beego https config
		beego.BConfig.Listen.EnableHTTP = false
		beego.BConfig.Listen.EnableHTTPS = true
		strs := strings.Split(apiServerCfg.ApiEndpoint, ":")
		beego.BConfig.Listen.HTTPSAddr = strs[AddressIdx]
		beego.BConfig.Listen.HTTPSPort, _ = strconv.Atoi(strs[PortIdx])
		beego.BConfig.Listen.HTTPSCertFile = apiServerCfg.BeegoHTTPSCertFile
		beego.BConfig.Listen.HTTPSKeyFile = apiServerCfg.BeegoHTTPSKeyFile

		cert, err := tls.LoadX509KeyPair(apiServerCfg.BeegoHTTPSCertFile, apiServerCfg.BeegoHTTPSKeyFile)
		if err != nil {
			log.Fatalf("loading key pair for server cert failed : %v", err)
		}

		clientCA, err := ioutil.ReadFile(constants.OpensdsCaCertFile)
		if err != nil {
			log.Fatalf("reading ca cert failed : %v", err)
		}
		clientCAPool := x509.NewCertPool()
		clientCAPool.AppendCertsFromPEM(clientCA)
		log.Println("ClientCA loaded")

		tlsConfig := &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    clientCAPool,
			GetCertificate: func(info *tls.ClientHelloInfo) (certificate *tls.Certificate, e error) {
				return &cert, nil
			},
		}

		beego.BeeApp.Server.TLSConfig = tlsConfig
	}

	beego.BConfig.Listen.ServerTimeOut = apiServerCfg.BeegoServerTimeOut
	beego.BConfig.CopyRequestBody = true
	beego.BConfig.EnableErrorsShow = false
	beego.BConfig.EnableErrorsRender = false
	beego.BConfig.WebConfig.AutoRender = false
	// insert some auth rules
	pattern := fmt.Sprintf("/%s/*", constants.APIVersion)
	beego.InsertFilter(pattern, beego.BeforeExec, context.Factory())
	beego.InsertFilter(pattern, beego.BeforeExec, auth.Factory())
	beego.InsertFilter("*", beego.BeforeExec, accesslog.Factory())
	beego.InsertFilter("*", beego.BeforeExec, validation.Factory(apiServerCfg.ApiSpecPath))

	// start service
	beego.Run(apiServerCfg.ApiEndpoint)
}
