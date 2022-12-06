package scheduler

import (
	"context"
	"errors"
	"github.com/rs/xid"
	"time"
)

type JobConfiguration struct {
	Id              string               `json:"id"`
	Title           string               `json:"title"`
	Description     string               `json:"description"`
	Tags            []string             `json:"tags"`
	Paused          bool                 `json:"paused"`
	BatchSize       int                  `json:"batchSize"`
	ResumeOnRestart bool                 `json:"resumeOnRestart"`
	OnError         []string             `json:"onError"`
	OnSuccess       []string             `json:"onSuccess"`
	Schedule        string               `json:"schedule"`
	Topic           string               `json:"topic"`
	Tasks           []*TaskConfiguration `json:"tasks"`
	DefaultFunc     func(ctx context.Context, task *JobTask) error
}

type TaskConfiguration struct {
	Id          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	BatchSize   int      `json:"batchSize"`
	DependsOn   []string `json:"dependsOn"`
}

type JobRunState struct {
	JobId    JobId        `json:"jobId"`
	JobTitle string       `json:"jobTitle"`
	State    string       `json:"state"`
	Started  time.Time    `json:"started"`
	Tasks    []*TaskState `json:"tasks"`
}

type JobHistory struct {
	Id        JobId        `json:"id"`
	Title     string       `json:"title"`
	State     string       `json:"state"`
	Start     time.Time    `json:"start"`
	End       time.Time    `json:"end"`
	LastError string       `json:"lastError"`
	Tasks     []*TaskState `json:"tasks"`
}

func (config *JobConfiguration) ToJob() (*Job, error) {
	job := &Job{
		Id:              JobId(config.Id),
		Title:           config.Title,
		Description:     config.Description,
		Tags:            config.Tags,
		Paused:          config.Paused,
		BatchSize:       config.BatchSize,
		ResumeOnRestart: config.ResumeOnRestart,
		OnError:         nil,
		OnSuccess:       nil,
		Schedule:        config.Schedule,
		Topic:           config.Topic,
		Tasks:           nil,
	}
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
