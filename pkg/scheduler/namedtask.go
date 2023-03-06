package scheduler

import (
	"context"
	"fmt"
	"strings"
)

type NamedTask interface {
	Name() string
	Run(ctx context.Context, job *Job, history *JobHistory) error
}

type SuccessReportTask struct {
}

func (*SuccessReportTask) Name() string {
	return "SuccessReport"
}

func (t *SuccessReportTask) Run(_ context.Context, job *Job, history *JobHistory) error {
	job.logger.Infof(fmt.Sprintf("Job %s finnished with the following task state:", job.Id))
	errState := ""
	for i, state := range history.Tasks {
		if state.Status == StatusFailed {
			job.logger.Errorf(fmt.Sprintf("  - Task %s (%v of %v) state is %s ->%s", state.Id, i+1, len(history.Tasks), strings.ToUpper(state.Status.String()), state.Error))
		} else {
			job.logger.Infof(fmt.Sprintf("  - Task %s (%v of %v) state is %s", state.Id, i+1, len(history.Tasks), strings.ToUpper(state.Status.String())))
		}

		if state.Status != StatusSuccess {
			errState = state.Error
		}
	}

	job.logger.Infof("Job %s - [%s] %s", job.Id, history.State, errState)
	return nil
}

type RunTask struct {
	JobId string
}

func (*RunTask) Name() string {
	return "RunTask"
}

func (t *RunTask) Run(ctx context.Context, job *Job, _ *JobHistory) error {
	return job.runner.RunJob(ctx, JobId(t.JobId))
}
