// Copyright 2020 The OpenSDS Authors.
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

package client

const (
	//Opensds Certificate ENVs
	OpensdsClientCert = "OPENSDS_CLIENT_CERT"
	OpensdsClientKey  = "OPENSDS_CLIENT_KEY"
	OpensdsCACert     = "OPENSDS_CA_CERT"
)

type HttpsOptions interface {
	GetClientCertFile() string
	GetClientKeyFile() string
	GetCACertFile() string
}

type TLSOptions struct {
	CertFile      string
	KeyFile       string
	TrustedCAFile string
}

func NewKHttpsOptions(certFile string, keyFile string, caCertFile string) *TLSOptions {
	return &TLSOptions{
		CertFile:      certFile,
		KeyFile:       keyFile,
		TrustedCAFile: caCertFile,
	}
}

func (cert *TLSOptions) GetClientCertFile() string {
	return cert.CertFile
}

func (cert *TLSOptions) GetClientKeyFile() string {
	return cert.KeyFile
}

func (cert *TLSOptions) GetCACertFile() string {
	return cert.TrustedCAFile
}
