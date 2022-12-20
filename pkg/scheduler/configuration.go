package scheduler

import (
	"context"
	"errors"
	"github.com/rs/xid"
	"time"
)

type Version string

const (
	JobConfigurationVersion1 Version = "1"
	JobConfigurationVersion2 Version = "2"
)

type JobConfiguration struct {
	Id              JobId                                          `json:"id" yaml:"id"`
	Title           string                                         `json:"title" yaml:"title"`
	Version         Version                                        `json:"version" yaml:"version"`
	Description     string                                         `json:"description" yaml:"description"`
	Tags            []string                                       `json:"tags,omitempty" yaml:"tags"`
	Enabled         bool                                           `json:"enabled" yaml:"enabled"`
	BatchSize       int                                            `json:"batchSize" yaml:"batchSize"`
	ResumeOnRestart bool                                           `json:"resumeOnRestart" yaml:"resumeOnRestart"`
	OnError         []string                                       `json:"onError,omitempty" yaml:"onError"`
	OnSuccess       []string                                       `json:"onSuccess,omitempty" yaml:"onSuccess"`
	Schedule        string                                         `json:"schedule" yaml:"schedule"`
	Topic           string                                         `json:"topic" yaml:"topic"`
	Tasks           []*TaskConfiguration                           `json:"tasks,omitempty" yaml:"tasks"`
	DefaultFunc     func(ctx context.Context, task *JobTask) error `json:"-"`
}

type TaskConfiguration struct {
	Id          string         `json:"id" yaml:"id"`
	Name        string         `json:"name" yaml:"name"`
	Description string         `json:"description" yaml:"description"`
	BatchSize   int            `json:"batchSize" yaml:"batchSize"`
	DependsOn   []string       `json:"dependsOn" yaml:"dependsOn"`
	Type        string         `json:"type" yaml:"type"`
	Source      map[string]any `json:"source" yaml:"source"`
	Sink        map[string]any `json:"sink" yaml:"sink"`
	Transform   map[string]any `json:"transform" yaml:"transform"`
}

type JobRunState struct {
	JobId    JobId        `json:"jobId"`
	JobTitle string       `json:"jobTitle"`
	State    string       `json:"state"`
	Started  time.Time    `json:"started"`
	Tasks    []*TaskState `json:"tasks"`
}

type JobHistory struct {
	Id        string       `json:"id"`
	JobId     JobId        `json:"jobId"`
	Title     string       `json:"title"`
	State     string       `json:"state"`
	Start     time.Time    `json:"start"`
	End       time.Time    `json:"end"`
	LastError string       `json:"lastError"`
	Tasks     []*TaskState `json:"tasks"`
}

func (config *JobConfiguration) ToJob(addTasks bool) (*Job, error) {
	job := &Job{
		Id:              config.Id,
		Title:           config.Title,
		Description:     config.Description,
		Tags:            config.Tags,
		Enabled:         config.Enabled,
		BatchSize:       config.BatchSize,
		ResumeOnRestart: config.ResumeOnRestart,
		OnError:         nil,
		OnSuccess:       nil,
		Schedule:        config.Schedule,
		Topic:           config.Topic,
		Tasks:           nil,
	}
	if addTasks {
		taskSet := make(map[string]*JobTask)
		tasks := make([]*JobTask, 0)
		for _, taskConfig := range config.Tasks {
			id := taskConfig.Id
			if id == "" {
				id = xid.New().String()
			}
			task := &JobTask{
				Id:          id,
				Name:        taskConfig.Name,
				Description: taskConfig.Description,
				BatchSize:   taskConfig.BatchSize,
				DependsOn:   make([]*JobTask, 0),
			}
			if config.DefaultFunc != nil {
				task.Fn = config.DefaultFunc
			}
			taskSet[taskConfig.Id] = task

			tasks = append(tasks, task)
		}
		for _, taskConfig := range config.Tasks {
			if taskConfig.DependsOn != nil {
				for _, d := range taskConfig.DependsOn {
					dependsOn, ok := taskSet[d]
					if !ok {
						return nil, errors.New("missing task dependency")
					}
					task := taskSet[taskConfig.Id]
					task.DependsOn = append(task.DependsOn, dependsOn)
				}

			}
		}
		job.Tasks = tasks
		err := job.Verify()
		if err != nil {
			return nil, err
		}
	}

	// map up success state:
	success := make([]NamedTask, 0)
	for _, s := range config.OnSuccess {
		if s == "SuccessReport" {
			success = append(success, &SuccessReportTask{})
		}
	}
	job.OnSuccess = success

	errTasks := make([]NamedTask, 0)
	for _, s := range config.OnError {
		if s == "SuccessReport" {
			errTasks = append(errTasks, &SuccessReportTask{})
		}
	}
	job.OnError = errTasks

	return job, nil
}
