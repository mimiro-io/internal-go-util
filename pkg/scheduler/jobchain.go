package scheduler

import (
	"context"
	"errors"
	"fmt"
	"github.com/dominikbraun/graph"
	"github.com/dominikbraun/graph/draw"
	"go.uber.org/zap"
	"os"
	"sync"
)

type jobChain struct {
	g               graph.Graph[string, *JobTask]
	tasks           map[string]*JobTask
	store           Store
	jobId           JobId
	logger          *zap.SugaredLogger
	runner          *JobRunner
	wg              sync.WaitGroup
	lastErr         error
	resumeOnRestart bool // is this is set, then tasks will not fail if they are in a failed start
}

func newJobChain(job *Job) *jobChain {
	jobHash := func(c *JobTask) string {
		return c.Id
	}
	g := graph.New(jobHash, graph.PreventCycles(), graph.Directed(), graph.Acyclic())

	return &jobChain{
		store:           job.store,
		jobId:           job.Id,
		g:               g,
		logger:          job.logger,
		tasks:           make(map[string]*JobTask),
		runner:          job.runner,
		resumeOnRestart: job.ResumeOnRestart,
	}
}

func (chain *jobChain) add(task *JobTask) error {
	if _, ok := chain.tasks[task.Id]; ok {
		return nil
	}
	task.store = chain.store

	// we must load any previous state, and merge if existing, this is to allow to recover dead jobs
	taskState, err := chain.store.GetTaskState(chain.jobId, task.Id)
	if err != nil {
		return err
	}
	task.state = taskState

	chain.tasks[task.Id] = task
	return nil
}

func (chain *jobChain) done() {
	chain.wg.Done()
}

func (chain *jobChain) stop(err error) {
	chain.lastErr = err
}

func (chain *jobChain) Run(ctx context.Context) error {
	// we build the graph when we run it
	for _, task := range chain.tasks {
		err := chain.g.AddVertex(task, graph.VertexAttribute("label", task.Name), graph.VertexAttribute("id", task.Id))
		if err != nil {
			return err
		}
	}
	for _, task := range chain.tasks {
		if task.DependsOn != nil {
			for _, d := range task.DependsOn {
				err := chain.g.AddEdge(d.Id, task.Id)
				if err != nil {
					return err
				}
			}
		}
	}

	order, _ := graph.TopologicalSort(chain.g)

	// go through once, and set the planned state if tasks are missing
	// by doing this, we have the full set of tasks, even if one is failing
	for _, id := range order {
		task := chain.tasks[id]
		if task.state == nil {
			_ = task.updateState(chain.jobId, StatusPlanned)
		}
	}

	// TODO: we must figure out what to run next, as there might be more than 1 that we can run in parallel
	for i, id := range order {
		// need to do some task state managing
		task := chain.tasks[id]
		if task.state == nil { // this is probably not needed
			_ = task.updateState(chain.jobId, StatusPlanned)
		} else if task.state.Status == StatusSuccess {
			chain.logger.Info(fmt.Sprintf("Skipping Job Task %s-%s in state %s (%v of %v)", chain.jobId, task.Id, task.state.Status, i+1, len(order)))
			continue
		} else if task.state.Status == StatusFailed {
			if !chain.resumeOnRestart { // stop if failed and resume is off
				chain.logger.Info(fmt.Sprintf("Stopping Job Task %s-%s in state %s (%v of %v)", chain.jobId, task.Id, task.state.Status, i+1, len(order)))
				return errors.New("task in failed state")
			}
		}

		// we need to add the jobs to the queue, and let the queue runner run the tasks
		// this is a blocking operation
		_ = task.updateState(chain.jobId, StatusRunning)
		chain.logger.Info(fmt.Sprintf("Running Job Task %s-%s (%v of %v)", chain.jobId, task.Id, i+1, len(order)))
		chain.wg.Add(1)
		var err error
		go func() {
			err2 := chain.runner.runTask(ctx, chain, task)
			if err2 != nil {
				err = err2
				chain.wg.Done()
			}
		}()
		chain.wg.Wait()

		if err != nil {
			_ = task.setFailed(chain.jobId, err)
			chain.logger.Info(fmt.Sprintf("Failed Job task %s-%s: %v (%v of %v)", chain.jobId, task.Id, err, i+1, len(order)))
			return err
		}
		if chain.lastErr != nil {
			_ = task.setFailed(chain.jobId, chain.lastErr)
			chain.logger.Info(fmt.Sprintf("Failed Job task %s-%s: %v (%v of %v)", chain.jobId, task.Id, chain.lastErr, i+1, len(order)))
			lastErr := chain.lastErr
			chain.lastErr = nil

			return lastErr
		}

		_ = task.updateState(chain.jobId, StatusSuccess)
	}
	return nil
}

func (chain *jobChain) Print() {
	file, _ := os.Create("./mygraph.gv")
	_ = draw.DOT(chain.g, file)
}
