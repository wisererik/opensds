// Copyright 2017 The OpenSDS Authors.
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
	"time"

	log "github.com/golang/glog"
	pb "github.com/opensds/opensds/pkg/model/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/keepalive"
)

// Client interface provides an abstract description about how to interact
// with gRPC client. Besides some nested methods defined in pb.DockClient,
// Client also exposes two methods: Connect() and Close(), for which callers
// can easily open and close gRPC connection.
type Client interface {
	pb.ProvisionDockClient
	pb.AttachDockClient
	pb.FileShareDockClient

	Connect(edp string) error

	Close()
}

// DockClient structure is one implementation of Client interface and will be
// called in real environment. There would be more other kind of connection
// in the long run.
type DockClient struct {
	pb.ProvisionDockClient
	pb.AttachDockClient
	pb.FileShareDockClient
	*grpc.ClientConn
}

func NewClient() Client { return &DockClient{} }

func (c *DockClient) Connect(edp string) error {
	// Set up a connection to the Dock server.
	if c.ClientConn != nil && c.ClientConn.GetState() == connectivity.Ready {
		return nil
	}
	var kacp = keepalive.ClientParameters{
		Time:                10 * time.Second, // send pings every 10 seconds if there is no activity
		Timeout:             time.Second,      // wait 1 second for ping ack before considering the connection dead
		PermitWithoutStream: true,             // send pings even without active streams
	}
	conn, err := grpc.Dial(edp, grpc.WithInsecure(), grpc.WithKeepaliveParams(kacp))
	if err != nil {
		log.Errorf("did not connect: %+v\n", err)
		return err
	}
	// Create dock client via the connection.
	c.FileShareDockClient = pb.NewFileShareDockClient(conn)
	c.ProvisionDockClient = pb.NewProvisionDockClient(conn)
	c.AttachDockClient = pb.NewAttachDockClient(conn)
	c.ClientConn = conn

	return nil
}
