// Copyright 2017 The OpenSDS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package oceanstor

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	log "github.com/golang/glog"
	. "github.com/opensds/opensds/contrib/drivers/utils"
	. "github.com/opensds/opensds/contrib/drivers/utils/config"
	"github.com/opensds/opensds/pkg/model"
	pb "github.com/opensds/opensds/pkg/model/proto"
	"github.com/opensds/opensds/pkg/utils"
	"github.com/opensds/opensds/pkg/utils/config"
	uuid "github.com/satori/go.uuid"
)

type Driver struct {
	conf   *OceanStorConfig
	client *OceanStorClient

	limitLunIdRange bool
	availableLunIds map[int64]bool

	createVolumeMutex sync.Mutex
	attachDetachMutex sync.Mutex
}

func (d *Driver) Setup() error {
	if d.client != nil {
		// Login already, return
		return nil
	}

	// Read huawei oceanstor config file
	conf := &OceanStorConfig{}
	d.conf = conf
	path := config.CONF.OsdsDock.Backends.HuaweiOceanStorBlock.ConfigPath

	if "" == path {
		path = defaultConfPath
	}

	Parse(conf, path)

	if d.conf.LunIdRangeMin < 0 || d.conf.LunIdRangeMax < 0 {
		msg := fmt.Sprintf("specified lun ID range [%d-%d] is invalid", d.conf.LunIdRangeMin, d.conf.LunIdRangeMax)
		log.Error(msg)
		return errors.New(msg)
	}

	client, err := NewClient(&d.conf.AuthOptions)
	if err != nil {
		log.Errorf("Get new client failed, %v", err)
		return err
	}

	err = client.login()
	if err != nil {
		log.Errorf("Client login failed, %v", err)
		return err
	}

	d.client = client

	if d.conf.LunIdRangeMax > 0 {
		d.limitLunIdRange = true
		d.availableLunIds = make(map[int64]bool)

		err := d.getAvailableLunIds(d.conf.LunIdRangeMin, d.conf.LunIdRangeMax)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Driver) Unset() error {
	if d.client != nil {
		d.client.logout()
	}
	return nil
}

func (d *Driver) createVolumeFromSnapshot(opt *pb.CreateVolumeOpts) (*model.VolumeSpec, error) {
	metadata := opt.GetMetadata()
	if metadata["hypermetro"] == "true" && metadata["replication_enabled"] == "true" {
		msg := "Hypermetro and Replication can not be used in the same volume_type"
		log.Error(msg)
		return nil, errors.New(msg)
	}
	snapshot, e1 := d.client.GetSnapshotByName(EncodeName(opt.GetSnapshotId()))
	if e1 != nil {
		log.Infof("Get Snapshot failed : %v", e1)
		return nil, e1
	}
	volumeDesc := TruncateDescription(opt.GetDescription())
	poolId, err1 := d.client.GetPoolIdByName(opt.GetPoolName())
	if err1 != nil {
		return nil, err1
	}

	provPolicy := d.conf.Pool[opt.GetPoolName()].Extras.DataStorage.ProvisioningPolicy
	if provPolicy == "" {
		provPolicy = "Thick"
	}

	var lun *Lun
	var err error

	if !d.limitLunIdRange {
		lun, err = d.client.CreateVolume(EncodeName(opt.GetId()), opt.GetSize(), volumeDesc, poolId, provPolicy, "")
	} else {
		lun, err = d.createVolumeWithId(EncodeName(opt.GetId()), opt.GetSize(), volumeDesc, poolId, provPolicy)
	}

	if err != nil {
		log.Error("Create Volume Failed:", err)
		return nil, err
	}

	log.Infof("Create Volume from snapshot, source_lun_id : %s , target_lun_id : %s", snapshot.Id, lun.Id)
	err = utils.WaitForCondition(func() (bool, error) {
		getVolumeResult, getVolumeErr := d.client.GetVolume(lun.Id)
		if nil == getVolumeErr {
			if getVolumeResult.HealthStatus == StatusHealth && getVolumeResult.RunningStatus == StatusVolumeReady {
				return true, nil
			}
			log.V(5).Infof("Current lun HealthStatus : %s , RunningStatus : %s",
				getVolumeResult.HealthStatus, getVolumeResult.RunningStatus)
			return false, nil
		}
		return false, getVolumeErr

	}, LunReadyWaitInterval, LunReadyWaitTimeout)

	if err != nil {
		log.Error(err)
		d.client.DeleteVolume(lun.Id)
		return nil, err
	}
	err = d.copyVolume(opt, snapshot.Id, lun.Id)
	if err != nil {
		d.client.DeleteVolume(lun.Id)
		return nil, err
	}
	return &model.VolumeSpec{
		BaseModel: &model.BaseModel{
			Id: opt.GetId(),
		},
		Name:             opt.GetName(),
		Size:             Sector2Gb(lun.Capacity),
		Description:      volumeDesc,
		AvailabilityZone: opt.GetAvailabilityZone(),
		Metadata: map[string]string{
			KLunId: lun.Id,
		},
	}, nil

}
func (d *Driver) copyVolume(opt *pb.CreateVolumeOpts, srcid, tgtid string) error {
	metadata := opt.GetMetadata()
	copyspeed := metadata["copyspeed"]
	luncopyid, err := d.client.CreateLunCopy(EncodeName(opt.GetId()), srcid,
		tgtid, copyspeed)

	if err != nil {
		log.Error("Create Lun Copy failed,", err)
		return err
	}

	err = d.client.StartLunCopy(luncopyid)
	if err != nil {
		log.Errorf("Start lun: %s copy failed :%v,", luncopyid, err)
		d.client.DeleteLunCopy(luncopyid)
		return err
	}

	err = utils.WaitForCondition(func() (bool, error) {
		deleteLunCopyErr := d.client.DeleteLunCopy(luncopyid)
		if nil == deleteLunCopyErr {
			return true, nil
		}

		return false, nil
	}, LunCopyWaitInterval, LunCopyWaitTimeout)

	if err != nil {
		log.Error(err)
		return err
	}

	log.Infof("Copy Volume %s success", tgtid)
	return nil
}

func (d *Driver) CreateVolume(opt *pb.CreateVolumeOpts) (*model.VolumeSpec, error) {
	d.createVolumeMutex.Lock()
	defer d.createVolumeMutex.Unlock()

	if opt.GetSnapshotId() != "" {
		return d.createVolumeFromSnapshot(opt)
	}

	name := EncodeName(opt.GetId())
	desc := TruncateDescription(opt.GetDescription())
	poolId, err := d.client.GetPoolIdByName(opt.GetPoolName())
	if err != nil {
		return nil, err
	}

	provPolicy := d.conf.Pool[opt.GetPoolName()].Extras.DataStorage.ProvisioningPolicy
	if provPolicy == "" {
		provPolicy = "Thick"
	}

	var lun *Lun
	if !d.limitLunIdRange {
		lun, err = d.client.CreateVolume(name, opt.GetSize(), desc, poolId, provPolicy, "")
	} else {
		lun, err = d.createVolumeWithId(name, opt.GetSize(), desc, poolId, provPolicy)
	}

	if err != nil {
		log.Error("Create Volume Failed:", err)
		return nil, err
	}

	log.Infof("Create volume %s (%s) success.", opt.GetName(), lun.Id)
	return &model.VolumeSpec{
		BaseModel: &model.BaseModel{
			Id: opt.GetId(),
		},
		Name:             opt.GetName(),
		Size:             Sector2Gb(lun.Capacity),
		Description:      opt.GetDescription(),
		AvailabilityZone: opt.GetAvailabilityZone(),
		Identifier:       &model.Identifier{DurableName: lun.Wwn, DurableNameFormat: "NAA"},
		Metadata: map[string]string{
			KLunId: lun.Id,
		},
	}, nil
}

func (d *Driver) PullVolume(volID string) (*model.VolumeSpec, error) {
	name := EncodeName(volID)
	lun, err := d.client.GetVolumeByName(name)
	if err != nil {
		return nil, err
	}

	return &model.VolumeSpec{
		BaseModel: &model.BaseModel{
			Id: volID,
		},
		Size:             Sector2Gb(lun.Capacity),
		Description:      lun.Description,
		AvailabilityZone: lun.ParentName,
	}, nil
}

func (d *Driver) DeleteVolume(opt *pb.DeleteVolumeOpts) error {
	lunId := opt.GetMetadata()[KLunId]
	err := d.client.DeleteVolume(lunId)
	if err != nil {
		log.Errorf("Delete volume failed, volume id =%s , Error:%s", opt.GetId(), err)
		return err
	}

	log.Info("Remove volume success, volume id =", opt.GetId())
	if d.limitLunIdRange {
		id, _ := strconv.ParseInt(lunId, 10, 64)
		d.removeAvailableLunId(id)
	}

	return nil
}

// ExtendVolume ...
func (d *Driver) ExtendVolume(opt *pb.ExtendVolumeOpts) (*model.VolumeSpec, error) {
	lunId := opt.GetMetadata()[KLunId]
	err := d.client.ExtendVolume(opt.GetSize(), lunId)
	if err != nil {
		log.Error("Extend Volume Failed:", err)
		return nil, err
	}

	log.Infof("Extend volume %s (%s) success.", opt.GetName(), opt.GetId())
	return &model.VolumeSpec{
		BaseModel: &model.BaseModel{
			Id: opt.GetId(),
		},
		Name:             opt.GetName(),
		Size:             opt.GetSize(),
		Description:      opt.GetDescription(),
		AvailabilityZone: opt.GetAvailabilityZone(),
	}, nil
}

func (d *Driver) getTargetInfo() (string, string, error) {
	tgtIp := d.conf.TargetIp
	resp, err := d.client.ListTgtPort()
	if err != nil {
		return "", "", err
	}
	for _, itp := range resp.Data {
		items := strings.Split(itp.Id, ",")
		iqn := strings.Split(items[0], "+")[1]
		items = strings.Split(iqn, ":")
		ip := items[len(items)-1]
		if tgtIp == ip {
			return iqn, ip, nil
		}
	}
	msg := fmt.Sprintf("Not find configuration targetIp: %v in device", tgtIp)
	return "", "", errors.New(msg)
}

func (d *Driver) InitializeConnection(opt *pb.CreateVolumeAttachmentOpts) (*model.ConnectionInfo, error) {
	d.attachDetachMutex.Lock()
	defer d.attachDetachMutex.Unlock()

	if opt.GetAccessProtocol() == ISCSIProtocol {
		return d.InitializeConnectionIscsi(opt)
	}
	if opt.GetAccessProtocol() == FCProtocol {
		return d.InitializeConnectionFC(opt)
	}
	return nil, fmt.Errorf("not supported protocol type: %s", opt.GetAccessProtocol())
}

func (d *Driver) InitializeConnectionIscsi(opt *pb.CreateVolumeAttachmentOpts) (*model.ConnectionInfo, error) {

	lunId := opt.GetMetadata()[KLunId]
	hostInfo := opt.GetHostInfo()
	// Create host if not exist.
	hostId, err := d.client.AddHostWithCheck(hostInfo)
	if err != nil {
		log.Errorf("Add host failed, host name =%s, error: %v", hostInfo.Host, err)
		return nil, err
	}

	// Add initiator to the host.
	initiatorName := GetInitiatorName(hostInfo.GetInitiators(), opt.GetAccessProtocol())
	if err = d.client.AddInitiatorToHostWithCheck(hostId, initiatorName); err != nil {
		log.Errorf("Add initiator to host failed, host id=%s, initiator=%s, error: %v", hostId, initiatorName, err)
		return nil, err
	}

	// Add host to hostgroup.
	hostGrpId, err := d.client.AddHostToHostGroup(hostId)
	if err != nil {
		log.Errorf("Add host to group failed, host id=%s, error: %v", hostId, err)
		return nil, err
	}

	// Mapping lungroup and hostgroup to view.
	if err = d.client.DoMapping(lunId, hostGrpId, hostId); err != nil {
		log.Errorf("Do mapping failed, lun id=%s, hostGrpId=%s, hostId=%s, error: %v",
			lunId, hostGrpId, hostId, err)
		return nil, err
	}

	tgtIqn, tgtIp, err := d.getTargetInfo()
	if err != nil {
		log.Error("Get the target info failed,", err)
		return nil, err
	}
	tgtLun, err := d.client.GetHostLunId(hostId, lunId)
	if err != nil {
		log.Error("Get the get host lun id failed,", err)
		return nil, err
	}
	connInfo := &model.ConnectionInfo{
		DriverVolumeType: opt.GetAccessProtocol(),
		ConnectionData: map[string]interface{}{
			"targetDiscovered": true,
			"targetIQN":        []string{tgtIqn},
			"targetPortal":     []string{tgtIp + ":3260"},
			"discard":          false,
			"targetLun":        tgtLun,
		},
	}
	return connInfo, nil
}

func (d *Driver) TerminateConnection(opt *pb.DeleteVolumeAttachmentOpts) error {
	d.attachDetachMutex.Lock()
	defer d.attachDetachMutex.Unlock()

	if opt.GetAccessProtocol() == ISCSIProtocol {
		return d.TerminateConnectionIscsi(opt)
	}
	if opt.GetAccessProtocol() == FCProtocol {
		return d.TerminateConnectionFC(opt)
	}
	return fmt.Errorf("not supported protocal type: %s", opt.GetAccessProtocol())
}

func (d *Driver) TerminateConnectionIscsi(opt *pb.DeleteVolumeAttachmentOpts) error {
	hostId, err := d.client.GetHostIdByName(opt.GetHostInfo().GetHost())
	if err != nil {
		// host id has been delete already, ignore the host not found error
		if IsNotFoundError(err) {
			log.Warningf("host(%s) has been removed already, ignore it. "+
				"Delete volume attachment(%s)success.", hostId, opt.GetId())
			return nil
		}
		return err
	}
	// the name format of there objects blow is: xxxPrefix + hostId
	// the empty xxId means that the specified object has been removed already.
	lunGrpId, err := d.client.FindLunGroup(PrefixLunGroup + hostId)
	if err != nil && !IsNotFoundError(err) {
		return err
	}
	hostGrpId, err := d.client.FindHostGroup(PrefixHostGroup + hostId)
	if err != nil && !IsNotFoundError(err) {
		return err
	}
	viewId, err := d.client.FindMappingView(PrefixMappingView + hostId)
	if err != nil && !IsNotFoundError(err) {
		return err
	}

	lunId := opt.GetMetadata()[KLunId]
	if lunGrpId != "" {
		if d.client.IsLunGroupContainLun(lunGrpId, lunId) {
			if err := d.client.RemoveLunFromLunGroup(lunGrpId, lunId); err != nil {
				return err
			}
		}

		//  if lun group still contains other lun(s), ignore the all the operations blow,
		// and goes back with success status.
		var leftObjectCount = 0
		if leftObjectCount, err = d.client.getObjectCountFromLungroup(lunGrpId); err != nil {
			return err
		}
		if leftObjectCount > 0 {
			log.Infof("Lun group(%s) still contains %d lun(s). "+
				"Delete volume attachment(%s)success.", lunGrpId, leftObjectCount, opt.GetId())
			return nil
		}
	}

	if viewId != "" {
		if d.client.IsMappingViewContainLunGroup(viewId, lunGrpId) {
			if err := d.client.RemoveLunGroupFromMappingView(viewId, lunGrpId); err != nil {
				return err
			}
		}
		if d.client.IsMappingViewContainHostGroup(viewId, hostGrpId) {
			if err := d.client.RemoveHostGroupFromMappingView(viewId, hostGrpId); err != nil {
				return err
			}
		}
		if err := d.client.DeleteMappingView(viewId); err != nil {
			return err
		}
	}

	if lunGrpId != "" {
		if err := d.client.DeleteLunGroup(lunGrpId); err != nil {
			return err
		}
	}

	if hostGrpId != "" {
		if d.client.IsHostGroupContainHost(hostGrpId, hostId) {
			if err := d.client.RemoveHostFromHostGroup(hostGrpId, hostId); err != nil {
				return err
			}
		}
		if err := d.client.DeleteHostGroup(hostGrpId); err != nil {
			return err
		}
	}

	initiatorName := GetInitiatorName(opt.GetHostInfo().GetInitiators(), opt.GetAccessProtocol())
	if d.client.IsHostContainInitiator(hostId, initiatorName) {
		if err := d.client.RemoveIscsiFromHost(initiatorName); err != nil {
			return err
		}
	}

	fcExist, err := d.client.checkFCInitiatorsExistInHost(hostId)
	if err != nil {
		return err
	}
	iscsiExist, err := d.client.checkIscsiInitiatorsExistInHost(hostId)
	if err != nil {
		return err
	}
	if fcExist || iscsiExist {
		log.Warningf("host (%s) still contains initiator(s), ignore delete it. "+
			"Delete volume attachment(%s)success.", hostId, opt.GetId())
		return nil
	}

	if err := d.client.DeleteHost(hostId); err != nil {
		return err
	}
	log.Infof("Delete volume attachment(%s)success.", opt.GetId())
	return nil
}

func (d *Driver) CreateSnapshot(opt *pb.CreateVolumeSnapshotOpts) (*model.VolumeSnapshotSpec, error) {
	lunId := opt.GetMetadata()[KLunId]
	name := EncodeName(opt.GetId())
	desc := TruncateDescription(opt.GetDescription())
	snap, err := d.client.CreateSnapshot(lunId, name, desc)
	if err != nil {
		return nil, err
	}
	return &model.VolumeSnapshotSpec{
		BaseModel: &model.BaseModel{
			Id: opt.GetId(),
		},
		Name:        opt.GetName(),
		Description: opt.GetDescription(),
		VolumeId:    opt.GetVolumeId(),
		Size:        0,
		Metadata: map[string]string{
			KSnapId: snap.Id,
		},
	}, nil
}

func (d *Driver) PullSnapshot(id string) (*model.VolumeSnapshotSpec, error) {
	name := EncodeName(id)
	snap, err := d.client.GetSnapshotByName(name)
	if err != nil {
		return nil, err
	}
	return &model.VolumeSnapshotSpec{
		BaseModel: &model.BaseModel{
			Id: snap.Id,
		},
		Name:        snap.Name,
		Description: snap.Description,
		Size:        0,
		VolumeId:    snap.ParentId,
	}, nil
}

func (d *Driver) DeleteSnapshot(opt *pb.DeleteVolumeSnapshotOpts) error {
	id := opt.GetMetadata()[KSnapId]
	err := d.client.DeleteSnapshot(id)
	if err != nil {
		log.Errorf("Delete volume snapshot failed, volume snapshot id = %s , error: %v", opt.GetId(), err)
		return err
	}
	log.Info("Remove volume snapshot success, volume snapshot id =", opt.GetId())
	return nil
}

func (d *Driver) ListPools() ([]*model.StoragePoolSpec, error) {
	var pols []*model.StoragePoolSpec
	sp, err := d.client.ListStoragePools()
	if err != nil {
		return nil, err
	}
	for _, p := range sp {
		c := d.conf
		if _, ok := c.Pool[p.Name]; !ok {
			continue
		}
		host, _ := os.Hostname()
		name := fmt.Sprintf("%s:%s:%s", host, d.conf.Endpoints, p.Id)
		pol := &model.StoragePoolSpec{
			BaseModel: &model.BaseModel{
				Id: uuid.NewV5(uuid.NamespaceOID, name).String(),
			},
			Name:             p.Name,
			TotalCapacity:    Sector2Gb(p.UserTotalCapacity),
			FreeCapacity:     Sector2Gb(p.UserFreeCapacity),
			StorageType:      c.Pool[p.Name].StorageType,
			Extras:           c.Pool[p.Name].Extras,
			AvailabilityZone: c.Pool[p.Name].AvailabilityZone,
			MultiAttach:      c.Pool[p.Name].MultiAttach,
		}
		if pol.AvailabilityZone == "" {
			pol.AvailabilityZone = defaultAZ
		}
		pols = append(pols, pol)
	}
	return pols, nil
}

func (d *Driver) InitializeConnectionFC(opt *pb.CreateVolumeAttachmentOpts) (*model.ConnectionInfo, error) {
	lunId := opt.GetMetadata()[KLunId]
	hostInfo := opt.GetHostInfo()
	// Create host if not exist.
	hostId, err := d.client.AddHostWithCheck(hostInfo)
	if err != nil {
		log.Errorf("Add host failed, host name =%s, error: %v", hostInfo.Host, err)
		return nil, err
	}

	// Add host to hostgroup.
	hostGrpId, err := d.client.AddHostToHostGroup(hostId)
	if err != nil {
		log.Errorf("Add host to group failed, host id=%s, error: %v", hostId, err)
		return nil, err
	}

	// Not use FC switch
	initiators := GetInitiatorsByProtocol(opt.GetHostInfo().GetInitiators(), opt.GetAccessProtocol())
	tgtPortWWNs, initTargMap, err := d.connectFCUseNoSwitch(opt, initiators, hostId)
	if err != nil {
		return nil, err
	}

	// Mapping lungroup and hostgroup to view.
	if err = d.client.DoMapping(lunId, hostGrpId, hostId); err != nil {
		log.Errorf("Do mapping failed, lun id=%s, hostGrpId=%s, hostId=%s, error: %v",
			lunId, hostGrpId, hostId, err)
		return nil, err
	}

	tgtLun, err := d.client.GetHostLunId(hostId, lunId)
	if err != nil {
		log.Error("Get the get host lun id failed,", err)
		return nil, err
	}

	fcInfo := &model.ConnectionInfo{
		DriverVolumeType: opt.GetAccessProtocol(),
		ConnectionData: map[string]interface{}{
			"targetDiscovered":     true,
			"targetWWNs":           tgtPortWWNs,
			"volumeId":             opt.GetVolumeId(),
			"initiator_target_map": initTargMap,
			"description":          "huawei",
			"hostName":             opt.GetHostInfo().Host,
			"targetLun":            tgtLun,
		},
	}
	return fcInfo, nil
}

func (d *Driver) connectFCUseNoSwitch(opt *pb.CreateVolumeAttachmentOpts, initiators []string, hostId string) ([]string, map[string][]string, error) {
	var addWWNs []string
	var hostWWNs []string

	for _, wwn := range initiators {
		initiator, err := d.client.GetFCInitiatorByWWN(wwn)
		if err != nil {
			log.Errorf("Get FC initiator %s error: %v", wwn, err)
			return nil, nil, err
		}
		if initiator == nil {
			log.Warningf("FC initiator %s does not exist", wwn)
			continue
		}

		if initiator.RunningStatus != "27" {
			log.Warningf("FC initiator %s is not online", wwn)
			continue
		}

		if len(initiator.ParentId) == 0 {
			addWWNs = append(addWWNs, wwn)
		} else if initiator.ParentId != hostId {
			msg := fmt.Sprintf("FC initiator %s is already associated to another host %s", wwn, initiator.ParentId)
			log.Error(msg)
			return nil, nil, errors.New(msg)
		}

		hostWWNs = append(hostWWNs, wwn)
	}

	for _, wwn := range addWWNs {
		if err := d.client.AddFCPortTohost(hostId, wwn); err != nil {
			log.Errorf("Add initiator %s to host %s error: %v", wwn, hostId, err)
			return nil, nil, err
		}
	}

	tgtPortWWNs, initTargMap, err := d.client.GetIniTargMap(hostWWNs)
	if err != nil {
		return nil, nil, err
	}

	return tgtPortWWNs, initTargMap, nil

}

func (d *Driver) isInStringArray(s string, source []string) bool {
	for _, i := range source {
		if s == i {
			return true
		}
	}
	return false
}

func (d *Driver) TerminateConnectionFC(opt *pb.DeleteVolumeAttachmentOpts) error {
	// Detach lun
	fcInfo, err := d.detachVolumeFC(opt)
	if err != nil {
		return err
	}
	log.Info(fmt.Sprintf("terminate connection fc, return data is: %s", fcInfo))
	return nil
}

func (d *Driver) detachVolumeFC(opt *pb.DeleteVolumeAttachmentOpts) (string, error) {
	wwns := GetInitiatorsByProtocol(opt.GetHostInfo().GetInitiators(), opt.GetAccessProtocol())
	lunId := opt.GetMetadata()[KLunId]

	log.Infof("terminate connection, wwpns: %s,lun id: %s", wwns, lunId)

	hostId, lunGrpId, hostGrpId, viewId, err := d.getMappedInfo(opt.GetHostInfo().GetHost())
	if err != nil {
		return "", err
	}
	if len(hostId) == 0 {
		return "", nil
	}

	if lunId != "" && lunGrpId != "" {
		if err := d.client.RemoveLunFromLunGroup(lunGrpId, lunId); err != nil {
			return "", err
		}
	}

	var leftObjectCount = -1
	if lunGrpId != "" {
		if leftObjectCount, err = d.client.getObjectCountFromLungroup(lunGrpId); err != nil {
			return "", err
		}
	}

	var fcInfo string
	if leftObjectCount > 0 {
		fcInfo = "driver_volume_type: fibre_channel, data: {}"
	} else {
		if fcInfo, err = d.deleteZoneAndRemoveFCInitiators(wwns, hostId, hostGrpId, viewId); err != nil {
			return "", err
		}

		if err := d.clearHostRelatedResource(lunGrpId, viewId, hostId, hostGrpId); err != nil {
			return "", err
		}
	}

	log.Info(fmt.Sprintf("Return target backend FC info is: %s", fcInfo))
	return fcInfo, nil
}

func (d *Driver) deleteZoneAndRemoveFCInitiators(wwns []string, hostId, hostGrpId, viewId string) (string, error) {
	tgtPortWWNs, initTargMap, err := d.client.GetIniTargMap(wwns)
	if err != nil {
		return "", err
	}

	// Remove the initiators from host if need.
	hostGroupNum, err := d.client.getHostGroupNumFromHost(hostId)
	if err != nil {
		return "", err
	}
	if hostGrpId != "" && hostGroupNum <= 1 || (hostGrpId == "" && hostGroupNum <= 0) {
		fcInitiators, err := d.client.GetHostFCInitiators(hostId)
		if err != nil {
			return "", err
		}
		for _, wwn := range wwns {
			if d.isInStringArray(wwn, fcInitiators) {
				if err := d.client.removeFCFromHost(wwn); err != nil {
					return "", err
				}
			}
		}
	}

	return fmt.Sprintf("driver_volume_type: fibre_channel, target_wwns: %s, initiator_target_map: %s", tgtPortWWNs, initTargMap), nil
}

func (d *Driver) getMappedInfo(hostName string) (string, string, string, string, error) {
	hostId, err := d.client.GetHostIdByName(hostName)
	if err != nil {
		if IsNotFoundError(err) {
			log.Warningf("host(%s) has been removed already, ignore it.", hostName)
			return "", "", "", "", nil
		}

		return "", "", "", "", err
	}

	lunGrpId, err := d.client.FindLunGroup(PrefixLunGroup + hostId)
	if err != nil && !IsNotFoundError(err) {
		return "", "", "", "", err
	}

	hostGrpId, err := d.client.FindHostGroup(PrefixHostGroup + hostId)
	if err != nil && !IsNotFoundError(err) {
		return "", "", "", "", err
	}

	viewId, err := d.client.FindMappingView(PrefixMappingView + hostId)
	if err != nil && !IsNotFoundError(err) {
		return "", "", "", "", err
	}

	return hostId, lunGrpId, hostGrpId, viewId, nil
}

func (d *Driver) clearHostRelatedResource(lunGrpId, viewId, hostId, hostGrpId string) error {
	if lunGrpId != "" {
		if viewId != "" {
			d.client.RemoveLunGroupFromMappingView(viewId, lunGrpId)
		}
		d.client.DeleteLunGroup(lunGrpId)
	}
	if hostId != "" {
		if hostGrpId != "" {

			if viewId != "" {
				d.client.RemoveHostGroupFromMappingView(viewId, hostGrpId)
			}

			views, err := d.client.getHostgroupAssociatedViews(hostGrpId)
			if err != nil {
				return err
			}

			if len(views) <= 0 {
				if err := d.client.RemoveHostFromHostGroup(hostGrpId, hostId); err != nil {
					return err
				}
				hosts, err := d.client.getHostsInHostgroup(hostGrpId)
				if err != nil {
					return err
				}

				if len(hosts) <= 0 {
					if err := d.client.DeleteHostGroup(hostGrpId); err != nil {
						return err
					}
				}
			}
		}

		flag, err := d.client.checkFCInitiatorsExistInHost(hostId)
		if err != nil {
			return err
		}
		if !flag {
			if err := d.client.DeleteHost(hostId); err != nil {
				return err
			}
		}
	}

	if viewId != "" {
		if err := d.client.DeleteMappingView(viewId); err != nil {
			return err
		}
	}

	return nil
}

func (d *Driver) InitializeSnapshotConnection(opt *pb.CreateSnapshotAttachmentOpts) (*model.ConnectionInfo, error) {
	return nil, &model.NotImplementError{S: "method InitializeSnapshotConnection has not been implemented yet."}
}

func (d *Driver) TerminateSnapshotConnection(opt *pb.DeleteSnapshotAttachmentOpts) error {
	return &model.NotImplementError{S: "method TerminateSnapshotConnection has not been implemented yet."}
}

func (d *Driver) CreateVolumeGroup(opt *pb.CreateVolumeGroupOpts) (*model.VolumeGroupSpec, error) {
	return nil, &model.NotImplementError{"method CreateVolumeGroup has not been implemented yet"}
}

func (d *Driver) UpdateVolumeGroup(opt *pb.UpdateVolumeGroupOpts) (*model.VolumeGroupSpec, error) {
	return nil, &model.NotImplementError{"method UpdateVolumeGroup has not been implemented yet"}
}

func (d *Driver) DeleteVolumeGroup(opt *pb.DeleteVolumeGroupOpts) error {
	return &model.NotImplementError{"method DeleteVolumeGroup has not been implemented yet"}
}

func (d *Driver) getAvailableLunIds(lunIdRangeMin, lunIdRangeMax int64) error {
	for i := lunIdRangeMin; i <= lunIdRangeMax; i++ {
		d.addAvailableLunId(i)
	}

	var i int64 = 0
	for ; ; i++ {
		luns, err := d.client.GetVolumesByRange(i*500, (i+1)*500)
		if err != nil {
			log.Errorf("Batch query volumes error: %v", err)
			return err
		}

		if len(luns) == 0 {
			return nil
		}

		lastIndex := len(luns) - 1
		lastId, _ := strconv.ParseInt(luns[lastIndex].Id, 10, 64)
		firstId, _ := strconv.ParseInt(luns[0].Id, 10, 64)

		if lastId <= lunIdRangeMin {
			continue
		} else if firstId >= lunIdRangeMax {
			return nil
		}

		for _, lun := range luns {
			id, _ := strconv.ParseInt(lun.Id, 10, 64)
			if _, exist := d.availableLunIds[id]; exist {
				d.removeAvailableLunId(id)
			} else if id >= lunIdRangeMax {
				return nil
			}
		}
	}
}

func (d *Driver) createVolumeWithId(name string, size int64, desc, poolId, provPolicy string) (*Lun, error) {
	var lunIdsRefreshed bool

	for i := 0; i < 2; i++ {
		if len(d.availableLunIds) == 0 {
			err := d.getAvailableLunIds(d.conf.LunIdRangeMin, d.conf.LunIdRangeMax)
			if err != nil {
				return nil, err
			}

			lunIdsRefreshed = true
		}

		lun, err := d.tryCreateVolumeWithId(name, size, desc, poolId, provPolicy)
		if err != nil {
			return nil, err
		}
		if lun != nil {
			return lun, nil
		}

		// if already called getAvailableLunIds above, won't try to refresh availableLunIds again
		if lunIdsRefreshed {
			msg := fmt.Sprintf("cannot find available id between [%d-%d]",
				d.conf.LunIdRangeMin, d.conf.LunIdRangeMax)
			log.Error(msg)
			return nil, errors.New(msg)
		}
	}

	// shouldn't come here, just in case
	msg := fmt.Sprintf("something odd happened while creating volume %s", name)
	log.Error(msg)
	return nil, errors.New(msg)
}

func (d *Driver) tryCreateVolumeWithId(name string, size int64, desc, poolId, provPolicy string) (*Lun, error) {
	for id, _ := range d.availableLunIds {
		lunId := strconv.FormatInt(id, 10)
		lun, err := d.client.CreateVolume(name, size, desc, poolId, provPolicy, lunId)
		if err == nil {
			d.removeAvailableLunId(id)
			return lun, nil
		}

		if _, ok := err.(*IdInUseError); ok {
			// if oceanstor returns IdInUseError, continue to try next id
			log.Warningf("Id %d already in use, try next one.", id)
			d.removeAvailableLunId(id)
			continue
		} else {
			return nil, err
		}
	}

	return nil, nil
}

func (d *Driver) addAvailableLunId(lunId int64) {
	d.availableLunIds[lunId] = true
}

func (d *Driver) removeAvailableLunId(lunId int64) {
	delete(d.availableLunIds, lunId)
}
