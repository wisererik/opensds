package azgo

import (
	"encoding/xml"
	"reflect"

	log "github.com/sirupsen/logrus"
)

// VolumeAutosizeAttributesType is a structure to represent a volume-autosize-attributes ZAPI object
type VolumeAutosizeAttributesType struct {
	XMLName                   xml.Name `xml:"volume-autosize-attributes"`
	GrowThresholdPercentPtr   *int     `xml:"grow-threshold-percent"`
	IsEnabledPtr              *bool    `xml:"is-enabled"`
	MaximumSizePtr            *int     `xml:"maximum-size"`
	MinimumSizePtr            *int     `xml:"minimum-size"`
	ModePtr                   *string  `xml:"mode"`
	ResetPtr                  *bool    `xml:"reset"`
	ShrinkThresholdPercentPtr *int     `xml:"shrink-threshold-percent"`
}

// NewVolumeAutosizeAttributesType is a factory method for creating new instances of VolumeAutosizeAttributesType objects
func NewVolumeAutosizeAttributesType() *VolumeAutosizeAttributesType {
	return &VolumeAutosizeAttributesType{}
}

// ToXML converts this object into an xml string representation
func (o *VolumeAutosizeAttributesType) ToXML() (string, error) {
	output, err := xml.MarshalIndent(o, " ", "    ")
	if err != nil {
		log.Errorf("error: %v", err)
	}
	return string(output), err
}

// String returns a string representation of this object's fields and implements the Stringer interface
func (o VolumeAutosizeAttributesType) String() string {
	return ToString(reflect.ValueOf(o))
}

// GrowThresholdPercent is a 'getter' method
func (o *VolumeAutosizeAttributesType) GrowThresholdPercent() int {
	r := *o.GrowThresholdPercentPtr
	return r
}

// SetGrowThresholdPercent is a fluent style 'setter' method that can be chained
func (o *VolumeAutosizeAttributesType) SetGrowThresholdPercent(newValue int) *VolumeAutosizeAttributesType {
	o.GrowThresholdPercentPtr = &newValue
	return o
}

// IsEnabled is a 'getter' method
func (o *VolumeAutosizeAttributesType) IsEnabled() bool {
	r := *o.IsEnabledPtr
	return r
}

// SetIsEnabled is a fluent style 'setter' method that can be chained
func (o *VolumeAutosizeAttributesType) SetIsEnabled(newValue bool) *VolumeAutosizeAttributesType {
	o.IsEnabledPtr = &newValue
	return o
}

// MaximumSize is a 'getter' method
func (o *VolumeAutosizeAttributesType) MaximumSize() int {
	r := *o.MaximumSizePtr
	return r
}

// SetMaximumSize is a fluent style 'setter' method that can be chained
func (o *VolumeAutosizeAttributesType) SetMaximumSize(newValue int) *VolumeAutosizeAttributesType {
	o.MaximumSizePtr = &newValue
	return o
}

// MinimumSize is a 'getter' method
func (o *VolumeAutosizeAttributesType) MinimumSize() int {
	r := *o.MinimumSizePtr
	return r
}

// SetMinimumSize is a fluent style 'setter' method that can be chained
func (o *VolumeAutosizeAttributesType) SetMinimumSize(newValue int) *VolumeAutosizeAttributesType {
	o.MinimumSizePtr = &newValue
	return o
}

// Mode is a 'getter' method
func (o *VolumeAutosizeAttributesType) Mode() string {
	r := *o.ModePtr
	return r
}

// SetMode is a fluent style 'setter' method that can be chained
func (o *VolumeAutosizeAttributesType) SetMode(newValue string) *VolumeAutosizeAttributesType {
	o.ModePtr = &newValue
	return o
}

// Reset is a 'getter' method
func (o *VolumeAutosizeAttributesType) Reset() bool {
	r := *o.ResetPtr
	return r
}

// SetReset is a fluent style 'setter' method that can be chained
func (o *VolumeAutosizeAttributesType) SetReset(newValue bool) *VolumeAutosizeAttributesType {
	o.ResetPtr = &newValue
	return o
}

// ShrinkThresholdPercent is a 'getter' method
func (o *VolumeAutosizeAttributesType) ShrinkThresholdPercent() int {
	r := *o.ShrinkThresholdPercentPtr
	return r
}

// SetShrinkThresholdPercent is a fluent style 'setter' method that can be chained
func (o *VolumeAutosizeAttributesType) SetShrinkThresholdPercent(newValue int) *VolumeAutosizeAttributesType {
	o.ShrinkThresholdPercentPtr = &newValue
	return o
}
