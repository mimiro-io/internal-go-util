package scheduler

import (
	"context"
	"errors"
	"github.com/rs/xid"
	"time"
)

type (
	JobStateStatus int
	JobStateType   int
)

const (
	TypeJob JobStateType = iota
	TypeTask
)

type JobTask struct {
	Id          string     `json:"id"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	BatchSize   int        `json:"batchSize"`
	DependsOn   []*JobTask `json:"dependsOn"`
	state       *TaskState
	store       Store
	Fn          func(ctx context.Context, task *JobTask) error `json:"-"`
}

var DefaultFn = func(ctx context.Context, task *JobTask) error {
	return errors.New("missing runner func")
}

func NewJobTask(name string) *JobTask {
	return &JobTask{
		Id:        xid.New().String(),
		Name:      name,
		DependsOn: make([]*JobTask, 0),
	}
}

func (task *JobTask) AddDependency(jobs ...*JobTask) {
	task.DependsOn = append(task.DependsOn, jobs...)
}

func (task *JobTask) Run(ctx context.Context) error {
	if task.Fn == nil {
		return DefaultFn(ctx, task)
	}

	return task.Fn(ctx, task)
}

type TaskState struct {
	Id     string         `json:"id"`
	Type   JobStateType   `json:"type"`
	Status JobStateStatus `json:"status"`
	Error  string         `json:"error"`
	Start  time.Time      `json:"start"`
	End    time.Time      `json:"end"`
}

func (t JobStateType) String() string {
	switch t {
	case TypeJob:
		return "job"
	case TypeTask:
		return "task"
	}
	return "unknown"
}

const (
	StatusUndefined JobStateStatus = iota
	StatusPlanned
	StatusRunning
	StatusSuccess
	StatusFailed
)

func (s JobStateStatus) String() string {
	switch s {
	case StatusPlanned:
		return "planned"
	case StatusRunning:
		return "running"
	case StatusSuccess:
		return "success"
	case StatusFailed:
		return "failed"
	}
	return "undefined"
}

func (task *JobTask) setFailed(jobId JobId, errUnderHandle error, start, end *time.Time) error {
	existing, err := task.store.GetTaskState(jobId, task.Id)
	if err != nil {
		return err
	}
	var t *TaskState
	if existing == nil {
		t = &TaskState{
			Id:     task.Id,
			Type:   TypeTask,
			Status: StatusUndefined,
		}
	} else {
		t = existing
	}
	t.Status = StatusFailed
	t.Error = errUnderHandle.Error()
	if start != nil {
		t.Start = *start
	}
	if end != nil {
		t.End = *end
	}
	return task.store.SaveTaskState(jobId, t)
}

func (task *JobTask) updateState(jobId JobId, status JobStateStatus, start, end *time.Time) error {
	existing, err := task.store.GetTaskState(jobId, task.Id)
	if err != nil {
		return err
	}
	var t *TaskState
	if existing == nil {
		t = &TaskState{
			Id:     task.Id,
			Type:   TypeTask,
			Status: StatusUndefined,
		}
	} else {
		t = existing
	}
	if start != nil {
		t.Start = *start
	}
	if end != nil {
		t.End = *end
	}
	t.Status = status
	return task.store.SaveTaskState(jobId, t)
}
