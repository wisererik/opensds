// Copyright 2019 NetApp, Inc. All Rights Reserved.

package storagedrivers

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	trident "github.com/netapp/trident/config"
	"github.com/netapp/trident/utils"
)

// ValidateCommonSettings attempts to "partially" decode the JSON into just the settings in CommonStorageDriverConfig
func ValidateCommonSettings(configJSON string) (*CommonStorageDriverConfig, error) {

	config := &CommonStorageDriverConfig{}

	// Decode configJSON into config object
	err := json.Unmarshal([]byte(configJSON), &config)
	if err != nil {
		return nil, fmt.Errorf("could not parse JSON configuration: %v", err)
	}

	// Load storage drivers and validate the one specified actually exists
	if config.StorageDriverName == "" {
		return nil, errors.New("missing storage driver name in configuration file")
	}

	// Validate config file version information
	if config.Version != ConfigVersion {
		return nil, fmt.Errorf("unexpected config file version; found %d, expected %d", config.Version, ConfigVersion)
	}

	// Warn about ignored fields in common config if any are set
	if config.DisableDelete {
		log.WithFields(log.Fields{
			"driverName": config.StorageDriverName,
		}).Warn("disableDelete set in backend config.  This will be ignored.")
	}
	if config.Debug {
		log.Warnf("The debug setting in the configuration file is now ignored; " +
			"use the command line --debug switch instead.")
	}

	// The storage prefix may have three states: nil (no prefix specified, drivers will use
	// a default prefix), "" (specified as an empty string, drivers will use no prefix), and
	// "<value>" (a prefix specified in the backend config file).  For historical reasons,
	// the value is serialized as a raw JSON string (a byte array), and it may take multiple
	// forms.  An empty byte array, or an array with the ASCII values {} or null, is interpreted
	// as nil (no prefix specified).  A byte array containing two double-quote characters ("")
	// is an empty string.  A byte array containing characters enclosed in double quotes is
	// a specified prefix.  Anything else is rejected as invalid.  The storage prefix is exposed
	// to the rest of the code in StoragePrefix; only serialization code such as this should
	// be concerned with StoragePrefixRaw.

	if len(config.StoragePrefixRaw) > 0 {
		rawPrefix := string(config.StoragePrefixRaw)
		if rawPrefix == "{}" || rawPrefix == "null" {
			config.StoragePrefix = nil
			log.Debugf("Storage prefix is %s, will use default prefix.", rawPrefix)
		} else if rawPrefix == "\"\"" {
			empty := ""
			config.StoragePrefix = &empty
			log.Debug("Storage prefix is empty, will use no prefix.")
		} else if strings.HasPrefix(rawPrefix, "\"") && strings.HasSuffix(rawPrefix, "\"") {
			prefix := string(config.StoragePrefixRaw[1 : len(config.StoragePrefixRaw)-1])
			config.StoragePrefix = &prefix
			log.WithField("storagePrefix", prefix).Debug("Parsed storage prefix.")
		} else {
			return nil, fmt.Errorf("invalid value for storage prefix: %v", config.StoragePrefixRaw)
		}
	} else {
		config.StoragePrefix = nil
		log.Debug("Storage prefix is absent, will use default prefix.")
	}

	// Validate volume size limit (if set)
	if config.LimitVolumeSize != "" {
		if _, err = utils.ConvertSizeToBytes(config.LimitVolumeSize); err != nil {
			return nil, fmt.Errorf("invalid value for limitVolumeSize: %v", config.LimitVolumeSize)
		}
	}

	log.Debugf("Parsed commonConfig: %+v", *config)

	return config, nil
}

func GetDefaultStoragePrefix(context trident.DriverContext) string {
	switch context {
	default:
		return ""
	case trident.ContextKubernetes, trident.ContextCSI:
		return DefaultTridentStoragePrefix
	case trident.ContextDocker:
		return DefaultDockerStoragePrefix
	}
}

func GetDefaultIgroupName(context trident.DriverContext) string {
	switch context {
	default:
		fallthrough
	case trident.ContextKubernetes, trident.ContextCSI:
		return DefaultTridentIgroupName
	case trident.ContextDocker:
		return DefaultDockerIgroupName
	}
}

func SanitizeCommonStorageDriverConfig(c *CommonStorageDriverConfig) {
	if c != nil && c.StoragePrefixRaw == nil {
		c.StoragePrefixRaw = json.RawMessage("{}")
	}
}

func GetCommonInternalVolumeName(c *CommonStorageDriverConfig, name string) string {

	prefixToUse := trident.OrchestratorName

	// If a prefix was specified in the configuration, use that.
	if c.StoragePrefix != nil {
		prefixToUse = *c.StoragePrefix
	}

	// Special case an empty prefix so that we don't get a delimiter in front.
	if prefixToUse == "" {
		return name
	}

	return fmt.Sprintf("%s-%s", prefixToUse, name)
}

// CheckVolumeSizeLimits if a limit has been set, ensures the requestedSize is under it.
func CheckVolumeSizeLimits(requestedSizeInt uint64, config *CommonStorageDriverConfig) (bool, uint64, error) {

	requestedSize := float64(requestedSizeInt)
	// If the user specified a limit for volume size, parse and enforce it
	limitVolumeSize := config.LimitVolumeSize
	log.WithFields(log.Fields{
		"limitVolumeSize": limitVolumeSize,
	}).Debugf("Limits")
	if limitVolumeSize == "" {
		log.Debugf("No limits specified, not limiting volume size")
		return false, 0, nil
	}

	volumeSizeLimit := uint64(0)
	volumeSizeLimitStr, parseErr := utils.ConvertSizeToBytes(limitVolumeSize)
	if parseErr != nil {
		return false, 0, fmt.Errorf("error parsing limitVolumeSize: %v", parseErr)
	}
	volumeSizeLimit, _ = strconv.ParseUint(volumeSizeLimitStr, 10, 64)

	log.WithFields(log.Fields{
		"limitVolumeSize":    limitVolumeSize,
		"volumeSizeLimit":    volumeSizeLimit,
		"requestedSizeBytes": requestedSize,
	}).Debugf("Comparing limits")

	if requestedSize > float64(volumeSizeLimit) {
		return true, volumeSizeLimit, fmt.Errorf("requested size: %1.f > the size limit: %d", requestedSize, volumeSizeLimit)
	}

	return true, volumeSizeLimit, nil
}

// Clone will create a copy of the source object and store it into the destination object (which must be a pointer)
func Clone(source, destination interface{}) {
	if reflect.TypeOf(destination).Kind() != reflect.Ptr {
		log.Error("storage_drivers.Clone, destination parameter must be a pointer")
	}

	buff := new(bytes.Buffer)
	enc := gob.NewEncoder(buff)
	dec := gob.NewDecoder(buff)
	enc.Encode(source)
	dec.Decode(destination)
}

// CheckSupportedFilesystem checks for a supported file system type
func CheckSupportedFilesystem(fs string, volumeInternalName string) (string, error) {
	fstype := strings.ToLower(fs)
	switch fstype {
	case FsXfs, FsExt3, FsExt4, FsRaw:
		log.WithFields(log.Fields{"fileSystemType": fstype, "name": volumeInternalName}).Debug("Filesystem format.")
		return fstype, nil
	default:
		return fstype, fmt.Errorf("unsupported fileSystemType option: %s", fstype)
	}
}
