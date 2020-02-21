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

/*
This module implements the entry into operations of storageDock module.

*/

package discovery

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/opensds/opensds/contrib/connector"
	"github.com/opensds/opensds/contrib/drivers"
	fd "github.com/opensds/opensds/contrib/drivers/filesharedrivers"
	"github.com/opensds/opensds/contrib/drivers/utils/config"
	c "github.com/opensds/opensds/pkg/context"
	"github.com/opensds/opensds/pkg/db"
	"github.com/opensds/opensds/pkg/model"
	"github.com/opensds/opensds/pkg/utils"
	. "github.com/opensds/opensds/pkg/utils/config"
	uuid "github.com/satori/go.uuid"
)

const (
	availableStatus   = "available"
	unavailableStatus = "unavailable"
)

var configPaths = make(map[string]string)

type Context struct {
	StopChan chan bool
	ErrChan  chan error
	MetaChan chan string
}

func DiscoveryAndReport(dd DockDiscoverer, ctx *Context) {
	for {
		select {
		case <-ctx.StopChan:
			return
		default:
			if err := dd.Discover(); err != nil {
				ctx.ErrChan <- err
			}

			if err := dd.Report(); err != nil {
				ctx.ErrChan <- err
			}
		}

		time.Sleep(60 * time.Second)
	}
}

type DockDiscoverer interface {
	Init() error

	Discover() error

	Report() error
}

// NewDockDiscoverer method creates a new DockDiscoverer.
func NewDockDiscoverer(dockType string) DockDiscoverer {
	switch dockType {
	case model.DockTypeProvioner:
		return &provisionDockDiscoverer{
			DockRegister: NewDockRegister(),
		}
	case model.DockTypeAttacher:
		return &attachDockDiscoverer{
			DockRegister: NewDockRegister(),
		}
	}
	return nil
}

// provisionDockDiscoverer is a struct for exposing some operations of provision
// dock service discovery.
type provisionDockDiscoverer struct {
	*DockRegister

	dcks []*model.DockSpec
	pols []*model.StoragePoolSpec
}

func (pdd *provisionDockDiscoverer) Init() error {
	// Load resource from specified file
	bm := GetBackendsMap()
	host, err := os.Hostname()
	if err != nil {
		log.Error("When get os hostname:", err)
		return err
	}

	for _, v := range CONF.EnabledBackends {
		b := bm[v]
		if b.Name == "" {
			continue
		}
		configPaths[b.Name] = b.ConfigPath
		dck := &model.DockSpec{
			BaseModel: &model.BaseModel{
				Id: uuid.NewV5(uuid.NamespaceOID, host+":"+b.Name).String(),
			},
			Name:        b.Name,
			Description: b.Description,
			DriverName:  b.DriverName,
			Endpoint:    CONF.OsdsDock.ApiEndpoint,
			NodeId:      host,
			Type:        model.DockTypeProvioner,
			Metadata:    map[string]string{"HostReplicationDriver": CONF.OsdsDock.HostBasedReplicationDriver},
		}
		// Update the id if the dock is already in etcd
		name := map[string][]string{
			"Name": {dck.Name},
		}
		docks, err := pdd.DockRegister.c.ListDocksWithFilter(c.NewAdminContext(), name)
		if err == nil && len(docks) != 0 {
			dck.Id = docks[0].Id
		}
		pdd.dcks = append(pdd.dcks, dck)
	}
	return nil
}

var filesharedrivers = []string{config.NFSDriverType, config.HuaweiOceanStorFileDriverType, config.ManilaDriverType, config.ChubaofsDriverType}

func (pdd *provisionDockDiscoverer) Discover() error {
	// Clear existing pool info
	pdd.pols = pdd.pols[:0]
	var pols []*model.StoragePoolSpec
	var err error
	var polsInDb []*model.StoragePoolSpec
	ctx := c.NewAdminContext()
	polsInDb, err = pdd.c.ListPools(ctx)
	if err != nil {
		return fmt.Errorf("can not read pools in db")
	}
	dbPolsMap := make(map[string]map[string]*model.StoragePoolSpec)
	for _, dck := range pdd.dcks {
		dbPolsMap[dck.Id] = make(map[string]*model.StoragePoolSpec)
	}
	for _, polInDb := range polsInDb {
		if dbPolsMap[polInDb.DockId] != nil {
			polInDb.Status = unavailableStatus
			dbPolsMap[polInDb.DockId][polInDb.Id] = polInDb
		}
	}
	for _, dck := range pdd.dcks {
		// Call function of StorageDrivers configured by storage drivers.
		if utils.Contains(filesharedrivers, dck.DriverName) {
			d := fd.Init(dck.DriverName, configPaths[dck.Name])
			defer fd.Clean(d)
			pols, err = d.ListPools()
			for _, pol := range pols {
				log.Infof("Backend %s discovered pool %s", dck.DriverName, pol.Name)
				delete(dbPolsMap[dck.Id], pol.Id)
				pol.DockId = dck.Id
				pol.Status = availableStatus
			}
		} else {
			d, err := drivers.Init(dck.DriverName, configPaths[dck.Name], dck.Name)
			if err != nil {
				log.Errorf("Init driver %s failed: %v", dck.DriverName, err)
				continue
			}

			defer drivers.Clean(d)
			pols, err = d.ListPools()

			replicationDriverName := dck.Metadata["HostReplicationDriver"]
			replicationType := model.ReplicationTypeHost
			if drivers.IsSupportArrayBasedReplication(dck.Name) {
				replicationType = model.ReplicationTypeArray
				replicationDriverName = dck.DriverName
			}
			for _, pol := range pols {
				log.Infof("Backend %s discovered pool %s", dck.DriverName, pol.Name)
				name := map[string][]string{
					"Name":   {pol.Name},
					"DockId": {dck.Id},
				}
				pools, err := pdd.c.ListPoolsWithFilter(ctx, name)
				if err == nil && len(pools) != 0 {
					pol.Id = pools[0].Id
				}

				delete(dbPolsMap[dck.Id], pol.Id)
				pol.DockId = dck.Id
				pol.ReplicationType = replicationType
				pol.ReplicationDriverName = replicationDriverName
				pol.Status = availableStatus
			}
		}
		if err != nil {
			log.Error("Call driver to list pools failed:", err)
			continue
		}

		if len(pols) == 0 {
			log.Warningf("The pool of dock %s is empty!\n", dck.Id)
		}

		pdd.pols = append(pdd.pols, pols...)
		for _, pol := range dbPolsMap[dck.Id] {
			pdd.pols = append(pdd.pols, pol)
		}

	}
	if len(pdd.pols) == 0 {
		return fmt.Errorf("there is no pool can be found")
	}

	return nil
}

func (pdd *provisionDockDiscoverer) Report() error {
	var err error

	// Store dock resources in database.
	for _, dck := range pdd.dcks {
		if err = pdd.Register(dck); err != nil {
			break
		}
	}

	// Store pool resources in database.
	for _, pol := range pdd.pols {
		if err != nil {
			break
		}
		err = pdd.Register(pol)
	}

	return err
}

// attachDockDiscoverer is a struct for exposing some operations of attach
// dock service discovery.
type attachDockDiscoverer struct {
	*DockRegister

	dck *model.DockSpec
}

func (add *attachDockDiscoverer) Init() error { return nil }

func (add *attachDockDiscoverer) Discover() error {
	host, err := os.Hostname()
	if err != nil {
		log.Error("When get os hostname:", err)
		return err
	}

	localIqn, err := connector.NewConnector(connector.IscsiDriver).GetInitiatorInfo()
	if err != nil {
		log.Warning("get initiator failed, ", err)
	}

	bindIp := CONF.BindIp
	if bindIp == "" {
		bindIp = connector.GetHostIP()
	}

	fcInitiator, err := connector.NewConnector(connector.FcDriver).GetInitiatorInfo()
	if err != nil {
		log.Warning("get initiator failed, ", err)
	}

	var wwpns []string
	for _, v := range fcInitiator {
		if strings.Contains(v, "node_name") {
			wwpns = append(wwpns, strings.Split(v, ":")[1])
		}
	}

	segments := strings.Split(CONF.OsdsDock.ApiEndpoint, ":")
	endpointIp := segments[len(segments)-2]
	add.dck = &model.DockSpec{
		BaseModel: &model.BaseModel{
			Id: uuid.NewV5(uuid.NamespaceOID, host+":"+endpointIp).String(),
		},
		Endpoint: CONF.OsdsDock.ApiEndpoint,
		NodeId:   host,
		Type:     model.DockTypeAttacher,
		Metadata: map[string]string{
			"Platform":  runtime.GOARCH,
			"OsType":    runtime.GOOS,
			"HostIp":    bindIp,
			"Initiator": localIqn[0],
			"WWPNS":     strings.Join(wwpns, ","),
		},
	}
	return nil
}

func (add *attachDockDiscoverer) Report() error {
	return add.Register(add.dck)
}

func NewDockRegister() *DockRegister {
	return &DockRegister{c: db.C}
}

type DockRegister struct {
	c db.Client
}

func (dr *DockRegister) Register(in interface{}) error {
	ctx := c.NewAdminContext()

	switch in.(type) {
	case *model.DockSpec:
		dck := in.(*model.DockSpec)
		// Call db module to create dock resource.
		if _, err := dr.c.CreateDock(ctx, dck); err != nil {
			log.Errorf("When create dock %s in db: %v\n", dck.Id, err)
			return err
		}
		break
	case *model.StoragePoolSpec:
		pol := in.(*model.StoragePoolSpec)
		// Call db module to create pool resource.
		if _, err := dr.c.CreatePool(ctx, pol); err != nil {
			log.Errorf("When create pool %s in db: %v\n", pol.Id, err)
			return err
		}
		break
	default:
		return fmt.Errorf("Resource type is not supported!")
	}

	return nil
}

func (dr *DockRegister) Unregister(in interface{}) error {
	ctx := c.NewAdminContext()

	switch in.(type) {
	case *model.DockSpec:
		dck := in.(*model.DockSpec)
		// Call db module to delete dock resource.
		if err := dr.c.DeleteDock(ctx, dck.Id); err != nil {
			log.Errorf("When delete dock %s in db: %v\n", dck.Id, err)
			return err
		}
		break
	case *model.StoragePoolSpec:
		pol := in.(*model.StoragePoolSpec)
		// Call db module to delete pool resource.
		if err := dr.c.DeletePool(ctx, pol.Id); err != nil {
			log.Errorf("When delete pool %s in db: %v\n", pol.Id, err)
			return err
		}
		break
	default:
		return fmt.Errorf("Resource type is not supported!")
	}

	return nil
}
