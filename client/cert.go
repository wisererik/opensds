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

type Certificates interface {
	GetClientCertFile() string
	GetClientKeyFile() string
	GetCACertFile() string
}

type TLSConfig struct {
	CertFile      string
	KeyFile       string
	TrustedCAFile string
}

func NewCertificates(certFile string, keyFile string, caCertFile string) *TLSConfig {
	return &TLSConfig{
		CertFile:      certFile,
		KeyFile:       keyFile,
		TrustedCAFile: caCertFile,
	}
}

func (cert *TLSConfig) GetClientCertFile() string {
	return cert.CertFile
}

func (cert *TLSConfig) GetClientKeyFile() string {
	return cert.KeyFile
}

func (cert *TLSConfig) GetCACertFile() string {
	return cert.TrustedCAFile
}
