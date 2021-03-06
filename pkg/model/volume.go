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
This module implements the common data structure.

*/

package model

// VolumeSpec is an block device created by storage service, it can be attached
// to physical machine or virtual machine instance.
type VolumeSpec struct {
	*BaseModel

	// The uuid of the project that the volume belongs to.
	TenantId string `json:"tenantId,omitempty"`

	// The uuid of the user that the volume belongs to.
	// +optional
	UserId string `json:"userId,omitempty"`

	// The name of the volume.
	Name string `json:"name,omitempty"`

	// The description of the volume.
	// +optional
	Description string `json:"description,omitempty"`

	// The group id of the volume.
	GroupId string `json:"groupId,omitempty"`

	// The size of the volume requested by the user.
	// Default unit of volume Size is GB.
	Size int64 `json:"size,omitempty"`

	// The locality that volume belongs to.
	AvailabilityZone string `json:"availabilityZone,omitempty"`

	// The status of the volume.
	// One of: "available", "error", "in-use", etc.
	Status string `json:"status,omitempty"`

	// The uuid of the pool which the volume belongs to.
	// +readOnly
	PoolId string `json:"poolId,omitempty"`

	// The uuid of the profile which the volume belongs to.
	ProfileId string `json:"profileId,omitempty"`

	// Metadata should be kept until the scemantics between opensds volume
	// and backend storage resouce description are clear.
	// +optional
	Metadata map[string]string `json:"metadata,omitempty"`

	// The uuid of the snapshot which the volume is created
	SnapshotId string `json:"snapshotId,omitempty"`

	// Download Snapshot From Cloud
	SnapshotFromCloud bool `json:"snapshotFromCloud,omitempty"`

	// The uuid of the replication which the volume belongs to.
	ReplicationId string `json:"replicationId,omitempty"`

	// The uuid of the replication which the volume belongs to.
	ReplicationDriverData map[string]string `json:"replicationDriverData,omitempty"`
	// Attach status of the volume.
	Attached *bool `json:"attached,omitempty"`

	// Whether the volume can be attached more than once, default value is false.
	MultiAttach bool        `json:"multiAttach,omitempty"`
	Identifier  *Identifier `json:"identifier,omitempty"`
}

// This type describes any additional identifiers for a resource which is used to uniquly identify the resource.
type Identifier struct {
	//This indicates the world wide, persistent name of the resource
	DurableName       string `json:"durableName,omitempty"`
	DurableNameFormat string `json:"durableNameFormat,omitempty"`
}

// VolumeSnapshotSpec is a description of volume snapshot resource.
type VolumeSnapshotSpec struct {
	*BaseModel

	// The uuid of the project that the volume snapshot belongs to.
	TenantId string `json:"tenantId,omitempty"`

	// The uuid of the user that the volume snapshot belongs to.
	// +optional
	UserId string `json:"userId,omitempty"`

	// The name of the volume snapshot.
	Name string `json:"name,omitempty"`

	// The description of the volume snapshot.
	// +optional
	Description string `json:"description,omitempty"`

	// The uuid of the profile which the volume belongs to.
	ProfileId string `json:"profileId,omitempty"`

	// The size of the volume which the snapshot belongs to.
	// Default unit of volume Size is GB.
	Size int64 `json:"size,omitempty"`

	// The status of the volume snapshot.
	// One of: "available", "error", etc.
	Status string `json:"status,omitempty"`

	// The uuid of the volume which the snapshot belongs to.
	VolumeId string `json:"volumeId,omitempty"`

	// Metadata should be kept until the scemantics between opensds volume
	// snapshot and backend storage resouce snapshot description are clear.
	// +optional
	Metadata map[string]string `json:"metadata,omitempty"`
}

// ExtendVolumeSpec ...
type ExtendVolumeSpec struct {
	NewSize int64 `json:"newSize,omitempty"`
}

type VolumeGroupSpec struct {
	*BaseModel
	// The name of the volume group.
	Name string `json:"name,omitempty"`

	Status string `json:"status,omitempty"`

	// The uuid of the project that the volume snapshot belongs to.
	TenantId string `json:"tenantId,omitempty"`

	// The uuid of the user that the volume snapshot belongs to.
	// +optional
	UserId string `json:"userId,omitempty"`

	// The description of the volume group.
	// +optional
	Description string `json:"description,omitempty"`

	// The uuid of the profile which the volume group belongs to.
	Profiles []string `json:"profiles,omitempty"`

	// The locality that volume group belongs to.
	// +optional
	AvailabilityZone string `json:"availabilityZone,omitempty"`

	// The addVolumes contain UUIDs of volumes to be added to the group.
	AddVolumes []string `json:"addVolumes,omitempty"`

	// The removeVolumes contains the volumes to be removed from the group.
	RemoveVolumes []string `json:"removeVolumes,omitempty"`

	// The uuid of the pool which the volume belongs to.
	// +readOnly
	PoolId string `json:"poolId,omitempty"`

	GroupSnapshots []string `json:"groupSnapshots,omitempty"`
}
