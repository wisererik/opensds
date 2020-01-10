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

package client

import (
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"

	"github.com/opensds/opensds/pkg/model"
	. "github.com/opensds/opensds/testutils/collection"
)

var (
	fakeClient *Client
	once       sync.Once
	TestEp     = "TestEndPoint"
)

func NewFakeClient(config *Config) *Client {
	once.Do(func() {
		os.Setenv("OPENSDS_ENDPOINT", config.Endpoint)
		fakeClient = &Client{
			ProfileMgr: &ProfileMgr{
				Receiver: NewFakeProfileReceiver(),
				Endpoint: config.Endpoint,
			},
			DockMgr: &DockMgr{
				Receiver: NewFakeDockReceiver(),
				Endpoint: config.Endpoint,
			},
			PoolMgr: &PoolMgr{
				Receiver: NewFakePoolReceiver(),
				Endpoint: config.Endpoint,
			},
			VolumeMgr: &VolumeMgr{
				Receiver: NewFakeVolumeReceiver(),
				Endpoint: config.Endpoint,
			},
			ReplicationMgr: &ReplicationMgr{
				Receiver: NewFakeReplicationReceiver(),
				Endpoint: config.Endpoint,
			},
			VersionMgr: &VersionMgr{
				Receiver: NewFakeVersionReceiver(),
				Endpoint: config.Endpoint,
			},
			FileShareMgr: &FileShareMgr{
				Receiver: NewFakeFileShareReceiver(),
				Endpoint: config.Endpoint,
			},
			HostMgr: &HostMgr{
				Receiver: NewFakeHostReceiver(),
				Endpoint: config.Endpoint,
			},
		}
	})
	return fakeClient
}

func NewFakeDockReceiver() Receiver {
	return &fakeDockReceiver{}
}

type fakeDockReceiver struct{
	tlsConfig *TLSConfig
}

func (*fakeDockReceiver) Recv(
	string,
	method string,
	in interface{},
	out interface{},
) error {
	if strings.ToUpper(method) != "GET" {
		return errors.New("method not supported")
	}

	switch out.(type) {
	case *model.DockSpec:
		if err := json.Unmarshal([]byte(ByteDock), out); err != nil {
			return err
		}
		break
	case *[]*model.DockSpec:
		if err := json.Unmarshal([]byte(ByteDocks), out); err != nil {
			return err
		}
		break
	default:
		return errors.New("output format not supported")
	}

	return nil
}

// sets tls connection configurations for https requests
func (f *fakeDockReceiver) SetTLSConfig(tlsConfig *TLSConfig) {
	f.tlsConfig = tlsConfig
}

func NewFakePoolReceiver() Receiver {
	return &fakePoolReceiver{}
}

type fakePoolReceiver struct{
	tlsConfig *TLSConfig
}

func (*fakePoolReceiver) Recv(
	string,
	method string,
	in interface{},
	out interface{},
) error {
	if strings.ToUpper(method) != "GET" {
		return errors.New("method not supported")
	}

	switch out.(type) {
	case *model.StoragePoolSpec:
		if err := json.Unmarshal([]byte(BytePool), out); err != nil {
			return err
		}
		break
	case *[]*model.StoragePoolSpec:
		if err := json.Unmarshal([]byte(BytePools), out); err != nil {
			return err
		}
		break
	default:
		return errors.New("output format not supported")
	}

	return nil
}

// sets tls connection configurations for https requests
func (f *fakePoolReceiver) SetTLSConfig(tlsConfig *TLSConfig) {
	f.tlsConfig = tlsConfig
}

func NewFakeProfileReceiver() Receiver {
	return &fakeProfileReceiver{}
}

type fakeProfileReceiver struct{
	tlsConfig *TLSConfig
}

func (*fakeProfileReceiver) Recv(
	string,
	method string,
	in interface{},
	out interface{},
) error {
	switch strings.ToUpper(method) {
	case "POST":
		switch out.(type) {
		case *model.ProfileSpec:
			if err := json.Unmarshal([]byte(ByteProfile), out); err != nil {
				return err
			}
			break
		case *model.CustomPropertiesSpec:
			if err := json.Unmarshal([]byte(ByteCustomProperties), out); err != nil {
				return err
			}
			break
		default:
			return errors.New("output format not supported")
		}
		break
	case "GET":
		switch out.(type) {
		case *model.ProfileSpec:
			if err := json.Unmarshal([]byte(ByteProfile), out); err != nil {
				return err
			}
			break
		case *[]*model.ProfileSpec:
			if err := json.Unmarshal([]byte(ByteProfiles), out); err != nil {
				return err
			}
			break
		case *model.CustomPropertiesSpec:
			if err := json.Unmarshal([]byte(ByteCustomProperties), out); err != nil {
				return err
			}
			break
		default:
			return errors.New("output format not supported")
		}
		break
	case "PUT":
		switch out.(type) {
		case *model.ProfileSpec:
			if err := json.Unmarshal([]byte(ByteProfile), out); err != nil {
				return err
			}
			break
		default:
			return errors.New("output format not supported")
		}
		break
	case "DELETE":
		break
	default:
		return errors.New("inputed method format not supported")
	}

	return nil
}

// sets tls connection configurations for https requests
func (f *fakeProfileReceiver) SetTLSConfig(tlsConfig *TLSConfig) {
	f.tlsConfig = tlsConfig
}

func NewFakeVolumeReceiver() Receiver {
	return &fakeVolumeReceiver{}
}

type fakeVolumeReceiver struct{
	tlsConfig *TLSConfig
}

func (*fakeVolumeReceiver) Recv(
	string,
	method string,
	in interface{},
	out interface{},
) error {
	switch strings.ToUpper(method) {
	case "POST", "PUT":
		switch out.(type) {
		case *model.VolumeSpec:
			if err := json.Unmarshal([]byte(ByteVolume), out); err != nil {
				return err
			}
			break
		case *model.VolumeAttachmentSpec:
			if err := json.Unmarshal([]byte(ByteAttachment), out); err != nil {
				return err
			}
			break
		case *model.VolumeSnapshotSpec:
			if err := json.Unmarshal([]byte(ByteSnapshot), out); err != nil {
				return err
			}
			break
		case *model.VolumeGroupSpec:
			if err := json.Unmarshal([]byte(ByteVolumeGroup), out); err != nil {
				return err
			}
			break
		default:
			return errors.New("output format not supported")
		}
		break
	case "GET":
		switch out.(type) {
		case *model.VolumeSpec:
			if err := json.Unmarshal([]byte(ByteVolume), out); err != nil {
				return err
			}
			break
		case *[]*model.VolumeSpec:
			if err := json.Unmarshal([]byte(ByteVolumes), out); err != nil {
				return err
			}
			break
		case *model.VolumeAttachmentSpec:
			if err := json.Unmarshal([]byte(ByteAttachment), out); err != nil {
				return err
			}
			break
		case *[]*model.VolumeAttachmentSpec:
			if err := json.Unmarshal([]byte(ByteAttachments), out); err != nil {
				return err
			}
			break
		case *model.VolumeSnapshotSpec:
			if err := json.Unmarshal([]byte(ByteSnapshot), out); err != nil {
				return err
			}
			break
		case *[]*model.VolumeSnapshotSpec:
			if err := json.Unmarshal([]byte(ByteSnapshots), out); err != nil {
				return err
			}
			break
		case *model.VolumeGroupSpec:
			if err := json.Unmarshal([]byte(ByteVolumeGroup), out); err != nil {
				return err
			}
			break
		case *[]*model.VolumeGroupSpec:
			if err := json.Unmarshal([]byte(ByteVolumeGroups), out); err != nil {
				return err
			}
			break
		default:
			return errors.New("output format not supported")
		}
		break
	case "DELETE":
		break
	default:
		return errors.New("inputed method format not supported")
	}

	return nil
}

// sets tls connection configurations for https requests
func (f *fakeVolumeReceiver) SetTLSConfig(tlsConfig *TLSConfig) {
	f.tlsConfig = tlsConfig
}

func NewFakeReplicationReceiver() Receiver {
	return &fakeReplicationReceiver{}
}

type fakeReplicationReceiver struct{
	tlsConfig *TLSConfig
}

func (*fakeReplicationReceiver) Recv(
	url string,
	method string,
	in interface{},
	out interface{},
) error {
	switch strings.ToUpper(method) {
	case "POST":
		if out != nil {
			return json.Unmarshal([]byte(ByteReplication), out)
		}
		return nil
	case "PUT":
		return json.Unmarshal([]byte(ByteReplication), out)
	case "GET":
		switch out.(type) {
		case *model.ReplicationSpec:
			return json.Unmarshal([]byte(ByteReplication), out)
		case *[]*model.ReplicationSpec:
			return json.Unmarshal([]byte(ByteReplications), out)
		default:
			return errors.New("output format not supported")
		}
	case "DELETE":
		return nil
	}
	return errors.New("input method format not supported")
}

// sets tls connection configurations for https requests
func (f *fakeReplicationReceiver) SetTLSConfig(tlsConfig *TLSConfig) {
	f.tlsConfig = tlsConfig
}

func NewFakeVersionReceiver() Receiver {
	return &fakeVersionReceiver{}
}

type fakeVersionReceiver struct{
	tlsConfig *TLSConfig
}

func (*fakeVersionReceiver) Recv(
	string,
	method string,
	in interface{},
	out interface{},
) error {
	switch strings.ToUpper(method) {
	case "GET":
		switch out.(type) {
		case *model.VersionSpec:
			if err := json.Unmarshal([]byte(ByteVersion), out); err != nil {
				return err
			}
			break
		case *[]*model.VersionSpec:
			if err := json.Unmarshal([]byte(ByteVersions), out); err != nil {
				return err
			}
			break
		default:
			return errors.New("output format not supported")
		}
		break
	case "DELETE":
		break
	default:
		return errors.New("inputed method format not supported")
	}

	return nil
}

// sets tls connection configurations for https requests
func (f *fakeVersionReceiver) SetTLSConfig(tlsConfig *TLSConfig) {
	f.tlsConfig = tlsConfig
}

func NewFakeFileShareReceiver() Receiver {
	return &fakeFileShareReceiver{}
}

type fakeFileShareReceiver struct{
	tlsConfig *TLSConfig
}

func (*fakeFileShareReceiver) Recv(
	string,
	method string,
	in interface{},
	out interface{},
) error {
	switch strings.ToUpper(method) {
	case "POST", "PUT":
		switch out.(type) {
		case *model.FileShareSpec:
			if err := json.Unmarshal([]byte(ByteFileShare), out); err != nil {
				return err
			}
		case *model.FileShareSnapshotSpec:
			if err := json.Unmarshal([]byte(ByteFileShareSnapshot), out); err != nil {
				return err
			}
		case *model.FileShareAclSpec:
			if err := json.Unmarshal([]byte(ByteFileShareAcl), out); err != nil {
				return err
			}
		default:
			return errors.New("output format not supported")
		}
	case "GET":
		switch out.(type) {
		case *model.FileShareSpec:
			if err := json.Unmarshal([]byte(ByteFileShare), out); err != nil {
				return err
			}
		case *[]*model.FileShareSpec:
			if err := json.Unmarshal([]byte(ByteFileShares), out); err != nil {
				return err
			}
		case *model.FileShareSnapshotSpec:
			if err := json.Unmarshal([]byte(ByteFileShareSnapshot), out); err != nil {
				return err
			}
		case *[]*model.FileShareSnapshotSpec:
			if err := json.Unmarshal([]byte(ByteFileShareSnapshots), out); err != nil {
				return err
			}
		case *model.FileShareAclSpec:
			if err := json.Unmarshal([]byte(ByteFileShareAcl), out); err != nil {
				return err
			}
		case *[]*model.FileShareAclSpec:
			if err := json.Unmarshal([]byte(ByteFileSharesAcls), out); err != nil {
				return err
			}
		default:
			return errors.New("output format not supported")
		}
	case "DELETE":
		break
	default:
		return errors.New("inputed method format not supported")
	}

	return nil
}

// sets tls connection configurations for https requests
func (f *fakeFileShareReceiver) SetTLSConfig(tlsConfig *TLSConfig) {
	f.tlsConfig = tlsConfig
}

func NewFakeHostReceiver() Receiver {
	return &fakeHostReceiver{}
}

type fakeHostReceiver struct{
	tlsConfig *TLSConfig
}

func (*fakeHostReceiver) Recv(
	string,
	method string,
	in interface{},
	out interface{},
) error {
	switch strings.ToUpper(method) {
	case "POST", "PUT":
		switch out.(type) {
		case *model.HostSpec:
			if err := json.Unmarshal([]byte(ByteHost), out); err != nil {
				return err
			}
		default:
			return errors.New("output format not supported")
		}
	case "GET":
		switch out.(type) {
		case *model.HostSpec:
			if err := json.Unmarshal([]byte(ByteHost), out); err != nil {
				return err
			}
		case *[]*model.HostSpec:
			if err := json.Unmarshal([]byte(ByteHosts), out); err != nil {
				return err
			}
		default:
			return errors.New("output format not supported")
		}
	case "DELETE":
		break
	default:
		return errors.New("inputed method format not supported")
	}

	return nil
}

// sets tls connection configurations for https requests
func (f *fakeHostReceiver) SetTLSConfig(tlsConfig *TLSConfig) {
	f.tlsConfig = tlsConfig
}