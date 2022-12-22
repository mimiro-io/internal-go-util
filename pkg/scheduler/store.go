package scheduler

import (
	"fmt"
	"github.com/dgraph-io/badger/v3"
	"github.com/rs/xid"
	"sort"
	"strings"
	"sync"
)

// We need to store 3 states:
// 1. The job configuration itself
// 2. The jobChain + it's JobTasks (this is the running info)
// 3. Exit status, aka the job + task log

type Store interface {
	GetConfiguration(id JobId) (*JobConfiguration, error)
	SaveConfiguration(id JobId, config *JobConfiguration) error
	DeleteConfiguration(id JobId) error
	ListConfigurations() ([]*JobConfiguration, error)
	GetTaskState(jobId JobId, taskId string) (*TaskState, error)
	SaveTaskState(jobId JobId, state *TaskState) error
	GetTasks(jobId JobId) ([]*TaskState, error)
	DeleteTasks(jobId JobId) error
	DeleteTask(jobId JobId, taskId string) error
	GetLastJobHistory(jobId JobId) ([]*JobHistory, error)
	GetJobHistory(jobId JobId, limit int) ([]*JobHistory, error)
	ListJobHistory(limit int) ([]*JobHistory, error)
	SaveJobHistory(jobId JobId, history *JobHistory) error
	DeleteJobHistory(jobId JobId) error
}

const (
	INDEX_JOB_CONFIG = "INDEX_JOB_CONFIG"
	INDEX_JOB_STATE  = "INDEX_JOB_STATE"
	INDEX_JOB_LOG    = "INDEX_JOB_LOG"
)

type BadgerStore struct {
	db       *badger.DB
	location string
}

func (b BadgerStore) DeleteTask(jobId JobId, taskId string) error {
	//TODO implement me
	panic("implement me")
}

func (b BadgerStore) ListJobHistory(limit int) ([]*JobHistory, error) {
	//TODO implement me
	panic("implement me")
}

func (b BadgerStore) GetLastJobHistory(jobId JobId) ([]*JobHistory, error) {
	//TODO implement me
	panic("implement me")
}

func (b BadgerStore) GetJobHistory(jobId JobId, limit int) ([]*JobHistory, error) {
	//TODO implement me
	panic("implement me")
}

func (b BadgerStore) SaveJobHistory(jobId JobId, history *JobHistory) error {
	//TODO implement me
	panic("implement me")
}

func (b BadgerStore) DeleteJobHistory(jobId JobId) error {
	//TODO implement me
	panic("implement me")
}

func (b BadgerStore) GetConfiguration(id JobId) (*JobConfiguration, error) {
	return FindOne[JobConfiguration](b.db, INDEX_JOB_CONFIG, string(id))
}

func (b BadgerStore) SaveConfiguration(id JobId, config *JobConfiguration) error {
	return Save[JobConfiguration](b.db, INDEX_JOB_CONFIG, string(id), config)
}

func (b BadgerStore) DeleteConfiguration(id JobId) error {
	return Delete(b.db, INDEX_JOB_CONFIG, string(id))
}

func (b BadgerStore) ListConfigurations() ([]*JobConfiguration, error) {
	return FindAll[JobConfiguration](b.db, INDEX_JOB_CONFIG)
}

func (b BadgerStore) GetTaskState(jobId JobId, taskId string) (*TaskState, error) {
	id := fmt.Sprintf("%s::%s", jobId, taskId)
	return FindOne[TaskState](b.db, INDEX_JOB_STATE, id)
}

func (b BadgerStore) SaveTaskState(jobId JobId, state *TaskState) error {
	id := fmt.Sprintf("%s::%s", jobId, state.Id)
	return Save[TaskState](b.db, INDEX_JOB_STATE, id, state)
}

func (b BadgerStore) GetTasks(jobId JobId) ([]*TaskState, error) {
	return FindSome[TaskState](b.db, INDEX_JOB_STATE, string(jobId))
}

func (b BadgerStore) DeleteTasks(jobId JobId) error {
	all, err := FindSome[TaskState](b.db, INDEX_JOB_STATE, string(jobId))
	if err != nil {
		return err
	}
	for _, item := range all { // this is not optimal, but should work
		err := Delete(b.db, INDEX_JOB_STATE, fmt.Sprintf("%s::%s", jobId, item.Id))
		if err != nil {
			return err
		}
	}
	return nil
}

func NewBadgerStore(location string) (Store, error) {
	opts := badger.DefaultOptions("/home/bcintra/.datahub/jobs")
	opts.NumVersionsToKeep = 1

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &BadgerStore{
		db:       db,
		location: location,
	}, nil
}

var _ Store = (*BadgerStore)(nil)
var _ Store = (*InMemoryStore)(nil)

type InMemoryStore struct {
	lock    sync.Mutex
	configs map[JobId]*JobConfiguration
	tasks   map[string]*TaskState
	history map[string]*JobHistory
}

func (i *InMemoryStore) ListJobHistory(limit int) ([]*JobHistory, error) {
	items := make([]*JobHistory, 0)
	for _, v := range i.history {
		items = append(items, v)
	}
	sort.SliceStable(items, func(i, j int) bool {
		return items[i].End.Unix() < items[j].End.Unix()
	})

	if limit == -1 {
		return items, nil
	}
	if len(items) <= limit {
		return items, nil
	}

	return items[:limit], nil
}

func (i *InMemoryStore) GetLastJobHistory(jobId JobId) ([]*JobHistory, error) {
	return i.GetJobHistory(jobId, 1)
}

func (i *InMemoryStore) GetJobHistory(jobId JobId, limit int) ([]*JobHistory, error) {
	items := make([]*JobHistory, 0)
	for k, v := range i.history {
		if strings.HasPrefix(k, string(jobId)) {
			items = append(items, v)
		}
	}

	sort.SliceStable(items, func(i, j int) bool {
		return items[i].End.Unix() < items[j].End.Unix()
	})

	if limit == -1 {
		return items, nil
	}
	if len(items) <= limit {
		return items, nil
	}

	return items[:limit], nil
}

func (i *InMemoryStore) SaveJobHistory(jobId JobId, history *JobHistory) error {
	runId := xid.New()
	id := fmt.Sprintf("%s::%s", jobId, runId)
	history.Id = id
	i.history[id] = history
	return nil
}

func (i *InMemoryStore) DeleteJobHistory(jobId JobId) error {
	for k, _ := range i.history {
		if strings.HasPrefix(k, string(jobId)) {
			delete(i.history, k)
		}
	}
	return nil
}

func NewInMemoryStore() Store {
	return &InMemoryStore{
		configs: make(map[JobId]*JobConfiguration),
		tasks:   make(map[string]*TaskState),
		history: make(map[string]*JobHistory),
	}
}

func (i *InMemoryStore) GetConfiguration(id JobId) (*JobConfiguration, error) {
	i.lock.Lock()
	defer i.lock.Unlock()
	if v, ok := i.configs[id]; ok {
		return v, nil
	}
	return nil, nil
}

func (i *InMemoryStore) SaveConfiguration(id JobId, config *JobConfiguration) error {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.configs[id] = config
	return nil
}

func (i *InMemoryStore) DeleteConfiguration(id JobId) error {
	i.lock.Lock()
	defer i.lock.Unlock()
	delete(i.configs, id)
	return nil
}

func (i *InMemoryStore) ListConfigurations() ([]*JobConfiguration, error) {
	i.lock.Lock()
	defer i.lock.Unlock()
	configs := make([]*JobConfiguration, 0)
	for _, v := range i.configs {
		configs = append(configs, v)
	}
	return configs, nil
}

func (i *InMemoryStore) GetTaskState(jobId JobId, taskId string) (*TaskState, error) {

	i.lock.Lock()
	defer i.lock.Unlock()
	id := fmt.Sprintf("%s::%s", jobId, taskId)
	if v, ok := i.tasks[id]; ok {
		return v, nil
	}
	return nil, nil
}

func (i *InMemoryStore) SaveTaskState(jobId JobId, state *TaskState) error {
	i.lock.Lock()
	defer i.lock.Unlock()
	id := fmt.Sprintf("%s::%s", jobId, state.Id)
	i.tasks[id] = state
	return nil
}

func (i *InMemoryStore) GetTasks(jobId JobId) ([]*TaskState, error) {
	i.lock.Lock()
	defer i.lock.Unlock()
	tasks := make([]*TaskState, 0)
	for k, v := range i.tasks {
		if strings.HasPrefix(k, string(jobId)) {
			tasks = append(tasks, v)
		}
	}
	return tasks, nil
}

func (i *InMemoryStore) DeleteTask(jobId JobId, taskId string) error {
	i.lock.Lock()
	defer i.lock.Unlock()
	id := fmt.Sprintf("%s::%s", jobId, taskId)
	for k, _ := range i.tasks {
		if k == id {
			delete(i.tasks, k)
		}
	}
	return nil
}

func (i *InMemoryStore) DeleteTasks(jobId JobId) error {
	i.lock.Lock()
	defer i.lock.Unlock()
	for k, _ := range i.tasks {
		if strings.HasPrefix(k, string(jobId)) {
			delete(i.tasks, k)
		}
	}
	return nil
}
