package scheduler

import (
	"context"
	"errors"
	"fmt"
	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/robfig/cron/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"sync"
	"time"
)

const (
	defaultJobPoolSize = 10

	WorkerStateIdle    = "IDLE"
	WorkerStateRunning = "RUNNING"
	WorkerStateError   = "ERROR"
	WorkerStateQueued  = "QUEUED"
)

var (
	ErrMissingTask = errors.New("tasks are missing on job")
)

type JobScheduler struct {
	name          string
	logger        *zap.SugaredLogger
	cron          *cron.Cron
	workPermits   chan struct{}            // This limits the number of tasks allowed to run concurrently.
	jobs          map[JobId]*worker        // keep a map of registered workers and their ids
	scheduledJobs map[cron.EntryID]*worker // this allows us to find the job if we have the cron entry
	lock          sync.Mutex
	count         *atomic.Int32 // this atomic counter allows us to keep track of current running tasks to use in metrics
	store         Store
	m             statsd.ClientInterface
}

type TaskScheduler struct {
	name        string
	logger      *zap.SugaredLogger
	workPermits chan struct{} // This limits the number of tasks allowed to run concurrently.
	lock        sync.Mutex
	count       *atomic.Int32 // this atomic counter allows us to keep track of current running tasks to use in metrics
	store       Store
	m           statsd.ClientInterface
}

func NewJobScheduler(logger *zap.SugaredLogger, name string, store Store, concurrency int32) *JobScheduler {
	log := logger.Named("scheduler-" + name)
	l := jobLogger{logger: log.Desugar()}

	if concurrency <= 0 {
		concurrency = defaultJobPoolSize
	}

	s := &JobScheduler{
		logger: log,
		name:   name,
		cron: cron.New(cron.WithLogger(l), cron.WithChain(
			cron.Recover(l),
		)),
		workPermits:   make(chan struct{}, concurrency),
		jobs:          make(map[JobId]*worker),
		scheduledJobs: make(map[cron.EntryID]*worker),
		count:         atomic.NewInt32(concurrency),
		store:         store,
	}
	s.start()
	return s
}
func NewTaskScheduler(logger *zap.SugaredLogger, name string, store Store, concurrency int32) *TaskScheduler {
	log := logger.Named("scheduler-" + name)

	if concurrency <= 0 {
		concurrency = defaultJobPoolSize
	}
	return &TaskScheduler{
		logger:      log,
		name:        name,
		workPermits: make(chan struct{}, concurrency),
		count:       atomic.NewInt32(concurrency),
		store:       store,
	}
}

func (scheduler *JobScheduler) start() {
	scheduler.logger.Info("Starting scheduler")
	if scheduler.count.Load() > 0 {
		scheduler.cron.Start()
		return
	}
	scheduler.logger.Warn("Scheduler has less than 1 concurrency and will be disabled")
}

func (scheduler *JobScheduler) stop() {
	scheduler.logger.Info("Stopping scheduler")
	scheduler.cron.Stop()
}

func (scheduler *JobScheduler) doWork(runner *JobRunner, work *worker) {
	work.State = WorkerStateQueued // only queued so far, not yet running, might hang in the queue for a while
	defer func() {
		work.State = WorkerStateIdle
	}()

	// load the config, some tings needs to be figured out at runtime
	config, _ := scheduler.store.GetConfiguration(work.job.Id)
	if config != nil {
		if !config.Enabled { // if the job is not enabled, then just exit
			return
		}
	}

	// maybe the enabled state has changed, then the scheduled job needs to reflect that for the future, so update it
	work.job.Enabled = config.Enabled

	if scheduler.workPermits != nil { // limit the no of parallel jobs with a chan
		scheduler.workPermits <- struct{}{}
		scheduler.count.Dec()
		defer func() {
			<-scheduler.workPermits
			scheduler.count.Inc()
		}()
	}
	defer func() {
		if work.once {
			scheduler.cron.Remove(work.Id)
		}
	}()
	tags := []string{
		fmt.Sprintf("jobs:job-%s", work.Name),
		fmt.Sprintf("jobtype:%v", scheduler.name),
	}

	work.State = WorkerStateRunning
	started := time.Now()
	scheduler.logger.Infow(fmt.Sprintf("Starting job with id '%s' (%s)", work.Name, work.job.Title),
		"job.jobId", work.job.Id,
		"job.jobTitle", work.job.Title,
		"job.state", "Starting")
	defer func() {
		work.ctx = nil
		work.cancel = nil
		timed := time.Since(started)
		_ = scheduler.m.Timing("jobs.duration", timed, tags, 1)
		scheduler.logger.Infow(fmt.Sprintf("Finished job with id '%s' (%s) - duration was %s", work.Name, work.job.Title, timed),
			"job.jobId", work.Name,
			"job.jobTitle", work.job.Title,
			"job.state", "Finished",
			"job.jobType", scheduler.name)

	}()
	_ = scheduler.m.Count("jobs.count", 1, tags, 1)
	_ = scheduler.m.Gauge("jobs.free", float64(scheduler.count.Load()), tags, 1)
	work.started = started
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	work.ctx = ctx
	work.cancel = cancel
	work.job.runner = runner
	work.job.store = runner.jobScheduler.store
	work.job.logger = runner.jobScheduler.logger
	work.job.Run(ctx)
}

type JobRunner struct {
	jobScheduler  *JobScheduler
	taskScheduler *TaskScheduler
}

func NewJobRunner(jobScheduler *JobScheduler, taskScheduler *TaskScheduler, m statsd.ClientInterface) *JobRunner {
	stats := m
	if m == nil {
		stats = &statsd.NoOpClient{}
	}

	jobScheduler.m = stats
	taskScheduler.m = stats
	return &JobRunner{
		jobScheduler:  jobScheduler,
		taskScheduler: taskScheduler,
	}
}

func (runner *JobRunner) ForceReload(jobId JobId) error {
	if state, ok := runner.jobScheduler.jobs[jobId]; ok {
		// it does exist, we are interested in 2 changes; Enabled, and Schedule
		jobConfiguration, err := runner.jobScheduler.store.GetConfiguration(jobId)
		if err != nil {
			return err
		}
		if jobConfiguration.Enabled != state.job.Enabled {
			runner.jobScheduler.lock.Lock()
			state.job.Enabled = jobConfiguration.Enabled
			runner.jobScheduler.lock.Unlock()
		}
		if jobConfiguration.Schedule != state.Schedule {
			// this has changed, stop the running job, and re-add
			err := runner.RemoveJob(jobId, false)
			if err != nil {
				return err
			}
			_, err = runner.Schedule(jobConfiguration.Schedule, false, state.job)
			return err
		}
	}
	return nil
}

func (runner *JobRunner) RunJob(ctx context.Context, jobId JobId) error {
	var job *Job
	if state, ok := runner.jobScheduler.jobs[jobId]; ok {
		if state.State == WorkerStateRunning || state.State == WorkerStateQueued {
			return nil
		}
		job = state.job // we use the already existing job
	} else {
		return errors.New(fmt.Sprintf("job with id '%s' is missing from the job list", job.Id))
	}

	work := &worker{
		Name:  job.Title,
		State: WorkerStateIdle,
		job:   job,
		once:  true,
	}
	s := runner.jobScheduler

	go func() {
		work.State = WorkerStateQueued
		defer func() {
			work.State = WorkerStateIdle
		}()
		if s.workPermits != nil { // limit the no of parallel jobs with a chan
			s.workPermits <- struct{}{}
			s.count.Dec()
			defer func() {
				<-s.workPermits
				s.count.Inc()
			}()
		}

		work.State = WorkerStateRunning
		started := time.Now()
		s.logger.Infow(fmt.Sprintf("Starting job with id '%s' (%s)", work.Name, work.job.Title),
			"job.jobId", work.job.Id,
			"job.jobTitle", work.job.Title,
			"job.state", "Starting")
		defer func() {
			work.ctx = nil
			work.cancel = nil
			timed := time.Since(started)
			//_ = work.m.Timing("jobs.duration", timed, tags, 1)
			s.logger.Infow(fmt.Sprintf("Finished job with id '%s' (%s) - duration was %s", work.Name, work.job.Title, timed),
				"job.jobId", work.Name,
				"job.jobTitle", work.job.Title,
				"job.state", "Finished",
				"job.jobType", s.name)

		}()
		work.started = started
		//ctx = context.WithValue(ctx, ctxContinuationToken, state.ContinuationToken) // add continuation token to context
		ctx, cancel := context.WithCancel(ctx)
		work.ctx = ctx
		work.cancel = cancel
		job.runner = runner
		job.store = runner.jobScheduler.store
		job.logger = runner.jobScheduler.logger
		job.Run(ctx)
	}()

	runner.jobScheduler.lock.Lock()
	defer runner.jobScheduler.lock.Unlock()
	runner.jobScheduler.jobs[job.Id] = work

	return nil
}

type worker struct {
	Id       cron.EntryID
	Name     string
	Schedule string
	State    string
	once     bool
	ctx      context.Context
	cancel   context.CancelFunc
	job      *Job
	started  time.Time
}

func (runner *JobRunner) Schedule(schedule string, once bool, job *Job) (cron.EntryID, error) {
	if job.Tasks == nil || len(job.Tasks) == 0 {
		return 0, ErrMissingTask
	}

	work := &worker{
		Name:     job.Title,
		Schedule: schedule,
		State:    WorkerStateIdle,
		job:      job,
		once:     once,
	}

	s := runner.jobScheduler
	state, ok := s.jobs[job.Id]
	if ok { // it's already present, no need to touch it
		if state.Id > 0 {
			return state.Id, nil
		}
	}

	if work.Schedule != "" { // we only add the work to the schedule if a schedule is set (but we still add the job to the list of jobs)
		id, err := s.cron.AddFunc(work.Schedule, func() {
			state, ok := s.jobs[work.job.Id]
			if ok && (state.State == WorkerStateRunning || state.State == WorkerStateQueued) { // cannot start more instances
				return
			}

			work.State = WorkerStateQueued
			defer func() {
				work.State = WorkerStateIdle
			}()

			s.doWork(runner, work)
		})
		work.Id = id
		s.logger.Infof("Adding work with id '%v' (%s) to schedule '%s'", work.job.Id, work.Name, work.Schedule)
		if err != nil {
			return 0, err
		}
	}

	// keep track of registered jobs to use in returning a list of registered jobs
	s.lock.Lock()
	s.jobs[job.Id] = work
	s.scheduledJobs[work.Id] = work
	s.lock.Unlock()
	return work.Id, nil
}

func (runner *JobRunner) runTask(ctx context.Context, chain *jobChain, task *JobTask) error {
	// do a quick test to see if the job id already exists, remove it if it does
	s := runner.taskScheduler

	go func() {

		if s.workPermits != nil { // limit the no of parallel jobs with a chan
			s.workPermits <- struct{}{}
			s.count.Dec()
			defer func() {
				<-s.workPermits
				s.count.Inc()
			}()
		}
		tags := []string{
			fmt.Sprintf("tasks:task-%s", task.Name),
			fmt.Sprintf("jobtype:%v", s.name),
		}
		started := time.Now()
		defer func() {
			timed := time.Since(started)
			_ = s.m.Timing("jobs.duration", timed, tags, 1)
			chain.done() // make sure to return control
		}()
		_ = s.m.Count("tasks.count", 1, tags, 1)
		_ = s.m.Gauge("tasks.free", float64(s.count.Load()), tags, 1)

		err2 := task.Run(ctx)
		if err2 != nil {
			chain.stop(err2)
		}
	}()
	return nil
}

// RemoveJob will remove the Job from future running, but will not stop or cancel the job
func (runner *JobRunner) RemoveJob(jobId JobId, del bool) error {
	for id, v := range runner.jobScheduler.jobs {
		if id == jobId {
			runner.jobScheduler.cron.Remove(v.Id)
			runner.jobScheduler.lock.Lock()
			delete(runner.jobScheduler.jobs, id)
			delete(runner.jobScheduler.scheduledJobs, v.Id)
			runner.jobScheduler.lock.Unlock()
		}
	}
	if del {
		return runner.jobScheduler.store.DeleteConfiguration(jobId)
	}
	return nil
}

func (runner *JobRunner) CancelJob(jobId JobId) {
	if state, ok := runner.jobScheduler.jobs[jobId]; ok {
		if state.State == WorkerStateRunning && state.ctx != nil {
			state.job.logger.Infof("Killing job with id '%s'", jobId)
			state.cancel() // hopefully won't bork too hard
		}
	}
}

type JobEntry struct {
	EntryID cron.EntryID `json:"entryId"`
	//Schedule cron.Schedule     `json:"-"`
	Next    time.Time         `json:"next"`
	Prev    time.Time         `json:"prev"`
	Started time.Time         `json:"started"`
	Job     *JobConfiguration `json:"job"`
	State   string            `json:"state"`
	Tasks   []*TaskEntry      `json:"tasks"`
}

type TaskEntry struct {
	Id    string `json:"id"`
	Name  string `json:"name"`
	State string `json:"state"`
}

// RunningState will get the current scheduler JobEntry, if one exists for the id.
// The JobEntry contains the full running state of the Job, and the JobTask's if any
func (runner *JobRunner) RunningState(jobId JobId) (*JobEntry, error) {
	if state, ok := runner.jobScheduler.jobs[jobId]; ok {
		jobConfiguration, err := runner.jobScheduler.store.GetConfiguration(jobId)
		if err != nil {
			return nil, err
		}

		entry := &JobEntry{
			Job:     jobConfiguration,
			Started: state.started,
			State:   state.State,
		}
		// is this job scheduled as well?
		worker, ok := runner.jobScheduler.scheduledJobs[state.Id]
		if ok {
			e := runner.jobScheduler.cron.Entry(worker.Id)
			entry.EntryID = e.ID
			entry.Next = e.Next
			entry.Prev = e.Prev
		}
		tasks := make([]*TaskEntry, 0)
		// if a job is scheduled, but not running, then the chain is nil
		if state.job.chain != nil {
			for _, t := range state.job.chain.tasks {
				tasks = append(tasks, &TaskEntry{
					Id:    t.Id,
					Name:  t.Name,
					State: t.state.Status.String(),
				})
			}
		}
		entry.Tasks = tasks
		return entry, nil
	} else {
		return nil, nil
	}
}

// Schedules will list jobs currently scheduled in the cron scheduler
func (runner *JobRunner) Schedules() ([]JobEntry, error) {
	entries := make([]JobEntry, 0)
	for _, e := range runner.jobScheduler.cron.Entries() {
		if v, ok := runner.jobScheduler.scheduledJobs[e.ID]; ok {
			jobConfiguration, err := runner.jobScheduler.store.GetConfiguration(v.job.Id)
			if err != nil {
				return nil, err
			}
			entry := JobEntry{
				EntryID: e.ID,
				Next:    e.Next,
				Prev:    e.Prev,
				Job:     jobConfiguration,
				Started: v.started,
				State:   v.State,
			}
			tasks := make([]*TaskEntry, 0)

			// if a job is scheduled, but not running, then the chain is nil
			if v.job.chain != nil {
				for _, t := range v.job.chain.tasks {
					tasks = append(tasks, &TaskEntry{
						Id:    t.Id,
						Name:  t.Name,
						State: t.state.Status.String(),
					})
				}
			}
			entry.Tasks = tasks
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// JobEntries will return a list of all jobs that the scheduler is aware of, with their current running state
func (runner *JobRunner) JobEntries() ([]JobEntry, error) {
	entries := make([]JobEntry, 0)
	for _, v := range runner.jobScheduler.jobs {
		jobConfiguration, err := runner.jobScheduler.store.GetConfiguration(v.job.Id)
		if err != nil {
			return nil, err
		}

		entry := JobEntry{
			Job:     jobConfiguration,
			State:   v.State,
			Started: v.started,
		}

		// is this job scheduled as well?
		worker, ok := runner.jobScheduler.scheduledJobs[v.Id]
		if ok {
			e := runner.jobScheduler.cron.Entry(worker.Id)
			entry.EntryID = e.ID
			entry.Next = e.Next
			entry.Prev = e.Prev
		}

		tasks := make([]*TaskEntry, 0)
		// if a job is scheduled, but not running, then the chain is nil
		if v.job.chain != nil {
			for _, t := range v.job.chain.tasks {
				tasks = append(tasks, &TaskEntry{
					Id:    t.Id,
					Name:  t.Name,
					State: t.state.Status.String(),
				})
			}
		}
		entry.Tasks = tasks
		entries = append(entries, entry)

	}
	return entries, nil
}

func (runner *JobRunner) Stop() {
	for _, worker := range runner.jobScheduler.jobs {
		worker.cancel()
	}
}
