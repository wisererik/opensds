package azgo

import (
	"encoding/xml"
	"reflect"

	log "github.com/sirupsen/logrus"
)

// JobInfoType is a structure to represent a job-info ZAPI object
type JobInfoType struct {
	XMLName            xml.Name         `xml:"job-info"`
	IsRestartedPtr     *bool            `xml:"is-restarted"`
	JobCategoryPtr     *string          `xml:"job-category"`
	JobCompletionPtr   *string          `xml:"job-completion"`
	JobDescriptionPtr  *string          `xml:"job-description"`
	JobDropdeadTimePtr *int             `xml:"job-dropdead-time"`
	JobEndTimePtr      *int             `xml:"job-end-time"`
	JobIdPtr           *int             `xml:"job-id"`
	JobNamePtr         *string          `xml:"job-name"`
	JobNodePtr         *NodeNameType    `xml:"job-node"`
	JobPriorityPtr     *JobPriorityType `xml:"job-priority"`
	JobProgressPtr     *string          `xml:"job-progress"`
	JobQueueTimePtr    *int             `xml:"job-queue-time"`
	JobSchedulePtr     *string          `xml:"job-schedule"`
	JobStartTimePtr    *int             `xml:"job-start-time"`
	JobStatePtr        *JobStateType    `xml:"job-state"`
	JobStatusCodePtr   *int             `xml:"job-status-code"`
	JobTypePtr         *string          `xml:"job-type"`
	JobUsernamePtr     *string          `xml:"job-username"`
	JobUuidPtr         *UuidType        `xml:"job-uuid"`
	JobVserverPtr      *VserverNameType `xml:"job-vserver"`
}

// NewJobInfoType is a factory method for creating new instances of JobInfoType objects
func NewJobInfoType() *JobInfoType {
	return &JobInfoType{}
}

// ToXML converts this object into an xml string representation
func (o *JobInfoType) ToXML() (string, error) {
	output, err := xml.MarshalIndent(o, " ", "    ")
	if err != nil {
		log.Errorf("error: %v", err)
	}
	return string(output), err
}

// String returns a string representation of this object's fields and implements the Stringer interface
func (o JobInfoType) String() string {
	return ToString(reflect.ValueOf(o))
}

// IsRestarted is a 'getter' method
func (o *JobInfoType) IsRestarted() bool {
	r := *o.IsRestartedPtr
	return r
}

// SetIsRestarted is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetIsRestarted(newValue bool) *JobInfoType {
	o.IsRestartedPtr = &newValue
	return o
}

// JobCategory is a 'getter' method
func (o *JobInfoType) JobCategory() string {
	r := *o.JobCategoryPtr
	return r
}

// SetJobCategory is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetJobCategory(newValue string) *JobInfoType {
	o.JobCategoryPtr = &newValue
	return o
}

// JobCompletion is a 'getter' method
func (o *JobInfoType) JobCompletion() string {
	r := *o.JobCompletionPtr
	return r
}

// SetJobCompletion is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetJobCompletion(newValue string) *JobInfoType {
	o.JobCompletionPtr = &newValue
	return o
}

// JobDescription is a 'getter' method
func (o *JobInfoType) JobDescription() string {
	r := *o.JobDescriptionPtr
	return r
}

// SetJobDescription is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetJobDescription(newValue string) *JobInfoType {
	o.JobDescriptionPtr = &newValue
	return o
}

// JobDropdeadTime is a 'getter' method
func (o *JobInfoType) JobDropdeadTime() int {
	r := *o.JobDropdeadTimePtr
	return r
}

// SetJobDropdeadTime is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetJobDropdeadTime(newValue int) *JobInfoType {
	o.JobDropdeadTimePtr = &newValue
	return o
}

// JobEndTime is a 'getter' method
func (o *JobInfoType) JobEndTime() int {
	r := *o.JobEndTimePtr
	return r
}

// SetJobEndTime is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetJobEndTime(newValue int) *JobInfoType {
	o.JobEndTimePtr = &newValue
	return o
}

// JobId is a 'getter' method
func (o *JobInfoType) JobId() int {
	r := *o.JobIdPtr
	return r
}

// SetJobId is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetJobId(newValue int) *JobInfoType {
	o.JobIdPtr = &newValue
	return o
}

// JobName is a 'getter' method
func (o *JobInfoType) JobName() string {
	r := *o.JobNamePtr
	return r
}

// SetJobName is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetJobName(newValue string) *JobInfoType {
	o.JobNamePtr = &newValue
	return o
}

// JobNode is a 'getter' method
func (o *JobInfoType) JobNode() NodeNameType {
	r := *o.JobNodePtr
	return r
}

// SetJobNode is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetJobNode(newValue NodeNameType) *JobInfoType {
	o.JobNodePtr = &newValue
	return o
}

// JobPriority is a 'getter' method
func (o *JobInfoType) JobPriority() JobPriorityType {
	r := *o.JobPriorityPtr
	return r
}

// SetJobPriority is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetJobPriority(newValue JobPriorityType) *JobInfoType {
	o.JobPriorityPtr = &newValue
	return o
}

// JobProgress is a 'getter' method
func (o *JobInfoType) JobProgress() string {
	r := *o.JobProgressPtr
	return r
}

// SetJobProgress is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetJobProgress(newValue string) *JobInfoType {
	o.JobProgressPtr = &newValue
	return o
}

// JobQueueTime is a 'getter' method
func (o *JobInfoType) JobQueueTime() int {
	r := *o.JobQueueTimePtr
	return r
}

// SetJobQueueTime is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetJobQueueTime(newValue int) *JobInfoType {
	o.JobQueueTimePtr = &newValue
	return o
}

// JobSchedule is a 'getter' method
func (o *JobInfoType) JobSchedule() string {
	r := *o.JobSchedulePtr
	return r
}

// SetJobSchedule is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetJobSchedule(newValue string) *JobInfoType {
	o.JobSchedulePtr = &newValue
	return o
}

// JobStartTime is a 'getter' method
func (o *JobInfoType) JobStartTime() int {
	r := *o.JobStartTimePtr
	return r
}

// SetJobStartTime is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetJobStartTime(newValue int) *JobInfoType {
	o.JobStartTimePtr = &newValue
	return o
}

// JobState is a 'getter' method
func (o *JobInfoType) JobState() JobStateType {
	r := *o.JobStatePtr
	return r
}

// SetJobState is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetJobState(newValue JobStateType) *JobInfoType {
	o.JobStatePtr = &newValue
	return o
}

// JobStatusCode is a 'getter' method
func (o *JobInfoType) JobStatusCode() int {
	r := *o.JobStatusCodePtr
	return r
}

// SetJobStatusCode is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetJobStatusCode(newValue int) *JobInfoType {
	o.JobStatusCodePtr = &newValue
	return o
}

// JobType is a 'getter' method
func (o *JobInfoType) JobType() string {
	r := *o.JobTypePtr
	return r
}

// SetJobType is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetJobType(newValue string) *JobInfoType {
	o.JobTypePtr = &newValue
	return o
}

// JobUsername is a 'getter' method
func (o *JobInfoType) JobUsername() string {
	r := *o.JobUsernamePtr
	return r
}

// SetJobUsername is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetJobUsername(newValue string) *JobInfoType {
	o.JobUsernamePtr = &newValue
	return o
}

// JobUuid is a 'getter' method
func (o *JobInfoType) JobUuid() UuidType {
	r := *o.JobUuidPtr
	return r
}

// SetJobUuid is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetJobUuid(newValue UuidType) *JobInfoType {
	o.JobUuidPtr = &newValue
	return o
}

// JobVserver is a 'getter' method
func (o *JobInfoType) JobVserver() VserverNameType {
	r := *o.JobVserverPtr
	return r
}

// SetJobVserver is a fluent style 'setter' method that can be chained
func (o *JobInfoType) SetJobVserver(newValue VserverNameType) *JobInfoType {
	o.JobVserverPtr = &newValue
	return o
}
