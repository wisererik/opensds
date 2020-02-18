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

package config

import (
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-ini/ini"
	"github.com/opensds/opensds/pkg/utils/constants"
)

const (
	ConfKeyName = iota
	ConfDefaultValue
)

var allSections []string

func setSlice(v reflect.Value, str string) error {
	sList := strings.Split(str, ",")
	s := reflect.MakeSlice(v.Type(), 0, 5)
	switch v.Type().Elem().Kind() {
	case reflect.Bool:
		for _, elm := range sList {
			val, err := strconv.ParseBool(elm)
			if err != nil {
				return fmt.Errorf("cann't convert slice item %s to Bool, %v", elm, err)
			}
			s = reflect.Append(s, reflect.ValueOf(val))
		}
	case reflect.Int:
		for _, elm := range sList {
			val, err := strconv.Atoi(elm)
			if err != nil {
				return fmt.Errorf("cann't convert slice item %s to Int, %v", elm, err)
			}
			s = reflect.Append(s, reflect.ValueOf(val))
		}
	case reflect.Int8:
		for _, elm := range sList {
			val, err := strconv.ParseInt(elm, 10, 64)
			if err != nil {
				return fmt.Errorf("cann't convert slice item %s to Int8, %v", elm, err)
			}
			s = reflect.Append(s, reflect.ValueOf(int8(val)))
		}
	case reflect.Int16:
		for _, elm := range sList {
			val, err := strconv.ParseInt(elm, 10, 64)
			if err != nil {
				return fmt.Errorf("cann't convert slice item %s to Int16, %v", elm, err)
			}
			s = reflect.Append(s, reflect.ValueOf(int16(val)))
		}
	case reflect.Int32:
		for _, elm := range sList {
			val, err := strconv.ParseInt(elm, 10, 64)
			if err != nil {
				return fmt.Errorf("cann't convert slice item %s to Int32, %v", elm, err)
			}
			s = reflect.Append(s, reflect.ValueOf(int32(val)))
		}
	case reflect.Int64:
		for _, elm := range sList {
			val, err := strconv.ParseInt(elm, 10, 64)
			if err != nil {
				return fmt.Errorf("cann't convert slice item %s to Int64, %v", elm, err)
			}
			s = reflect.Append(s, reflect.ValueOf(int64(val)))
		}
	case reflect.Uint:
		for _, elm := range sList {
			val, err := strconv.ParseUint(elm, 10, 64)
			if err != nil {
				return fmt.Errorf("cann't convert slice item %s to Uint, %v", elm, err)
			}
			s = reflect.Append(s, reflect.ValueOf(uint(val)))
		}
	case reflect.Uint8:
		for _, elm := range sList {
			val, err := strconv.ParseUint(elm, 10, 64)
			if err != nil {
				return fmt.Errorf("cann't convert slice item %s to Uint8, %v", elm, err)
			}
			s = reflect.Append(s, reflect.ValueOf(uint8(val)))
		}
	case reflect.Uint16:
		for _, elm := range sList {
			val, err := strconv.ParseUint(elm, 10, 64)
			if err != nil {
				return fmt.Errorf("cann't convert slice item %s to Uint16, %v", elm, err)
			}
			s = reflect.Append(s, reflect.ValueOf(uint16(val)))
		}
	case reflect.Uint32:
		for _, elm := range sList {
			val, err := strconv.ParseUint(elm, 10, 64)
			if err != nil {
				return fmt.Errorf("cann't convert slice item %s to Uint32, %v", elm, err)
			}
			s = reflect.Append(s, reflect.ValueOf(uint32(val)))
		}
	case reflect.Uint64:
		for _, elm := range sList {
			val, err := strconv.ParseUint(elm, 10, 64)
			if err != nil {
				return fmt.Errorf("cann't convert slice item %s to Uint64, %v", elm, err)
			}
			s = reflect.Append(s, reflect.ValueOf(uint64(val)))
		}
	case reflect.Float32:
		for _, elm := range sList {
			val, err := strconv.ParseFloat(elm, 64)
			if err != nil {
				return fmt.Errorf("cann't convert slice item %s to Float32, %v", elm, err)
			}
			s = reflect.Append(s, reflect.ValueOf(float32(val)))
		}
	case reflect.Float64:
		for _, elm := range sList {
			val, err := strconv.ParseFloat(elm, 64)
			if err != nil {
				return fmt.Errorf("cann't convert slice item %s to Float54, %v", elm, err)
			}
			s = reflect.Append(s, reflect.ValueOf(val))
		}
	case reflect.String:
		for _, elm := range sList {
			s = reflect.Append(s, reflect.ValueOf(elm))
		}
	default:
		log.Printf("[ERROR] Does not support this type of slice.")
	}
	v.Set(s)
	return nil
}

func parseItems(section string, v reflect.Value, cfg *ini.File) error {
	if v.Kind() == reflect.Slice {
		return nil
	}
	for i := 0; i < v.Type().NumField(); i++ {
		field := v.Field(i)
		tag := v.Type().Field(i).Tag.Get("conf")
		if "" == tag {
			parseSections(cfg, field.Type(), field)
		}
		tags := strings.SplitN(tag, ",", 2)
		if !field.CanSet() {
			continue
		}
		var strVal = ""
		if len(tags) > 1 {
			strVal = tags[ConfDefaultValue]
		}
		if cfg != nil {
			key, err := cfg.Section(section).GetKey(tags[ConfKeyName])
			if err == nil {
				strVal = key.Value()
			}
		}
		switch field.Kind() {
		case reflect.Bool:
			val, err := strconv.ParseBool(strVal)
			if err != nil {
				return fmt.Errorf("cann't convert %s:%s to Bool, %v", tags[0], strVal, err)
			}
			field.SetBool(val)
		case reflect.ValueOf(time.Second).Kind():
			if field.Type().String() == "time.Duration" {
				v, err := time.ParseDuration(strVal)
				if err != nil {
					return fmt.Errorf("cann't convert %s:%s to Duration, %v", tags[0], strVal, err)
				}
				field.SetInt(int64(v))
				break
			}
			fallthrough
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			val, err := strconv.ParseInt(strVal, 10, 64)
			if err != nil {
				return fmt.Errorf("cann't convert %s:%s to Int, %v", tags[0], strVal, err)
			}
			field.SetInt(val)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			val, err := strconv.ParseUint(strVal, 10, 64)
			if err != nil {
				return fmt.Errorf("cann't convert %s:%s to Uint, %v", tags[0], strVal, err)
			}
			field.SetUint(val)
		case reflect.Float32, reflect.Float64:
			val, err := strconv.ParseFloat(strVal, 64)
			if err != nil {
				return fmt.Errorf("cann't convert %s:%s to Float, %v", tags[0], strVal, err)
			}
			field.SetFloat(val)
		case reflect.String:
			field.SetString(strVal)
		case reflect.Slice:
			setSlice(field, strVal)
		default:
		}
	}
	return nil
}

func parseSections(cfg *ini.File, t reflect.Type, v reflect.Value) error {
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
		t = t.Elem()
	}
	// parse struct
	for i := 0; i < t.NumField(); i++ {
		field := v.Field(i)
		section := t.Field(i).Tag.Get("conf")
		if "" == section {
			if err := parseSections(cfg, field.Type(), field); err != nil {
				return err
			}
		}
		if err := parseItems(section, field, cfg); err != nil {
			return err
		}
		if len(allSections) != 0 {
			for i := 0; i < len(allSections); i++ {
				if strings.EqualFold(section, allSections[i]) {
					if i == len(allSections)-1 {
						allSections = allSections[:i]
					} else {
						allSections = append(allSections[:i], allSections[i+1:]...)
					}
				}
			}
		}
	}
	return nil
}

func parseBackends(cfg *ini.File, sections []string, v reflect.Value) {
	var backends Backends
	for i := 0; i < len(sections); i++ {
		var backend BackendProperties
		key, err := cfg.Section(sections[i]).GetKey("name")
		if err == nil {
			backend.Name = key.Value()
		}
		key, err = cfg.Section(sections[i]).GetKey("description")
		if err == nil {
			backend.Description = key.Value()
		}
		key, err = cfg.Section(sections[i]).GetKey("driver_name")
		if err == nil {
			backend.DriverName = key.Value()
		}
		key, err = cfg.Section(sections[i]).GetKey("config_path")
		if err == nil {
			backend.ConfigPath = key.Value()
		}
		backends.backends = append(backends.backends, backend)
	}
	backendsReflect := reflect.ValueOf(backends)
	v.Set(backendsReflect)
}

func initConf(confFile string, conf interface{}) {
	cfg, err := ini.Load(confFile)
	if err != nil && confFile != "" {
		log.Printf("[ERROR] Read configuration failed, use default value")
	}
	t := reflect.TypeOf(conf)
	v := reflect.ValueOf(conf)
	allSections = make([]string, 0)
	if cfg != nil {
		allSections = cfg.SectionStrings()
	}
	if err := parseSections(cfg, t, v); err != nil {
		log.Fatalf("[ERROR] parse configure file failed: %v", err)
	}
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
		t = t.Elem()
	}
	if len(allSections) != 0 {
		parseBackends(cfg, allSections, v.FieldByName("Backends"))
	}
}

// Global Configuration Variable
var CONF *Config = GetDefaultConfig()

//Create a Config and init default value.
func GetDefaultConfig() *Config {
	var conf *Config = new(Config)
	initConf("", conf)
	return conf
}

func GetConfigPath() string {
	path := constants.OpensdsConfigPath
	for i := 1; i < len(os.Args)-1; i++ {
		if m, _ := regexp.MatchString(`^-{1,2}config-file$`, os.Args[i]); m {
			if !strings.HasSuffix(os.Args[i+1], "-") {
				path = os.Args[i+1]
			}
		}
	}
	return path
}

func (c *Config) Load() {
	var dummyConfigPath string
	// Flag 'config-file' is set here for usage show and parameter check, the config path will be parsed by GetConfigPath
	flag.StringVar(&dummyConfigPath, "config-file", constants.OpensdsConfigPath, "OpenSDS config file path")
	initConf(GetConfigPath(), CONF)
}

func GetBackendsMap() map[string]BackendProperties {
	backendsMap := map[string]BackendProperties{}

	for i := 0; i < len(CONF.backends); i++ {
		backendsMap[CONF.backends[i].Name] = CONF.backends[i]
	}
	return backendsMap
}
