package scheduler

import (
	"context"
	"fmt"
	"github.com/dominikbraun/graph"
	"go.uber.org/zap"
	"strings"
	"time"
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

func (*SuccessReportTask) MarshalJSON() ([]byte, error) {
	return []byte("SuccessReport"), nil
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

type JobId string

type Job struct {
	Id              JobId       `json:"id"`
	Title           string      `json:"title"`
	Description     string      `json:"description"`
	Tags            []string    `json:"tags"`
	Enabled         bool        `json:"enabled"`
	BatchSize       int         `json:"batchSize"`
	ResumeOnRestart bool        `json:"resumeOnRestart"`
	OnError         []NamedTask `json:"onError"`
	OnSuccess       []NamedTask `json:"onSuccess"`
	Schedule        string      `json:"schedule"`
	Topic           string      `json:"topic"`
	Tasks           []*JobTask  `json:"tasks"`
	store           Store
	runner          *JobRunner
	chain           *jobChain
	logger          *zap.SugaredLogger
}

func (job *Job) AddTask(task *JobTask, fn func(ctx context.Context, task *JobTask) error) {
	if job.Tasks == nil {
		job.Tasks = make([]*JobTask, 0)
	}
	task.Fn = fn
	job.Tasks = append(job.Tasks, task)
}

func (job *Job) Verify() error {
	jobHash := func(c *JobTask) string {
		return c.Id
	}
	g := graph.New(jobHash, graph.PreventCycles(), graph.Directed(), graph.Acyclic())
	for _, task := range job.Tasks {
		err := g.AddVertex(task, graph.VertexAttribute("label", task.Name), graph.VertexAttribute("id", task.Id))
		if err != nil {
			return err
		}
	}
	for _, task := range job.Tasks {
		if task.DependsOn != nil {
			for _, d := range task.DependsOn {
				err := g.AddEdge(d.Id, task.Id)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Run is what starts the job. A job consists of a jobChain with a DAG of
// JobTask's associated with it.
func (job *Job) Run(ctx context.Context) {
	job.logger = job.logger.Named(fmt.Sprintf("job-%s", job.Id))

	history := &JobHistory{
		JobId: job.Id,
		Title: job.Title,
		Start: time.Now(),
	}

	// set up the jobChain
	chain := newJobChain(job)
	for _, task := range job.Tasks {
		err := chain.add(task) // TODO: might have to do some sorting to load the root first
		if err != nil {
			history.End = time.Now()
			job.handleError(ctx, history, err)
			return
		}
	}
	err := chain.Run(ctx)
	history.End = time.Now()

	// let's fetch the state
	tasks, _ := job.store.GetTasks(job.Id) // ignore the error
	history.Tasks = tasks

	if err != nil {
		history.LastError = fmt.Sprintf("%v", err)
		job.handleError(ctx, history, err)
		return
	}
	job.handleSuccess(ctx, history)
}

func (job *Job) handleSuccess(ctx context.Context, history *JobHistory) {
	// log to activity log
	history.State = "SUCCESS"
	_ = job.store.SaveJobHistory(job.Id, history)

	// run OnSuccess tasks
	for _, handler := range job.OnSuccess {
		_ = handler.Run(ctx, job, history)
	}

	// clean up the run state
	_ = job.store.DeleteTasks(job.Id)

}

func (job *Job) handleError(ctx context.Context, history *JobHistory, err error) {
	if err == nil {
		return
	}

	// log the error(s) to the activity log
	history.State = "ERROR"
	_ = job.store.SaveJobHistory(job.Id, history)

	// run OnError handler tasks, if any
	for _, handler := range job.OnError {
		_ = handler.Run(ctx, job, history)
	}

}
