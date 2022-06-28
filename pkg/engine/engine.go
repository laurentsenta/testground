package engine

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/xid"
	"github.com/testground/testground/pkg/api"
	"github.com/testground/testground/pkg/build"
	"github.com/testground/testground/pkg/config"
	"github.com/testground/testground/pkg/logging"
	"github.com/testground/testground/pkg/rpc"
	"github.com/testground/testground/pkg/runner"
	"github.com/testground/testground/pkg/task"
)

// AllBuilders enumerates all builders known to the system.
var AllBuilders = []api.Builder{
	&build.DockerGoBuilder{},
	&build.ExecGoBuilder{},
	&build.DockerGenericBuilder{},
	&build.DockerNodeBuilder{},
}

// AllRunners enumerates all runners known to the system.
var AllRunners = []api.Runner{
	&runner.LocalDockerRunner{},
	&runner.LocalExecutableRunner{},
	&runner.ClusterSwarmRunner{},
	&runner.ClusterK8sRunner{},
}

// Engine is the central runtime object of the system. It knows about all test
// plans, builders, and runners. It is supposed to be instantiated as a
// singleton in all runtimes, whether the testground is run as a CLI tool, or as
// a daemon. In the latter mode, the GitHub bridge will trigger commands and
// perform queries on the Engine.
//
// TODO: the Engine should also centralise all system state and make it
// queryable, e.g. what tests are running, or have run, such that we can easily
// query test plans that ran for a particular commit of an upstream.
type Engine struct {
	lk sync.RWMutex
	// builders binds builders to their identifying key.
	builders map[string]api.Builder
	// runners binds runners to their identifying key.
	runners map[string]api.Runner
	envcfg  *config.EnvConfig
	ctx     context.Context
	store   *task.Storage
	queue   *task.Queue
	// signals contains a channel for each running task
	// by closing a channel, the task is canceled
	signals   map[string]chan int
	signalsLk sync.RWMutex
}

var _ api.Engine = (*Engine)(nil)

type EngineConfig struct {
	Builders  []api.Builder
	Runners   []api.Runner
	EnvConfig *config.EnvConfig
}

func NewEngine(cfg *EngineConfig) (*Engine, error) {
	var (
		store *task.Storage
		err   error
	)

	trt := cfg.EnvConfig.Daemon.Scheduler.TaskRepoType
	switch trt {
	case "memory":
		store, err = task.NewMemoryTaskStorage()
		if err != nil {
			return nil, err
		}
	case "disk":
		path := filepath.Join(cfg.EnvConfig.Dirs().Home(), "tasks.db")
		logging.S().Infow("init leveldb task storage", "path", path)
		store, err = task.NewTaskStorage(path)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unknown task repo type: %s", trt)
	}

	queue, err := task.NewQueue(store, cfg.EnvConfig.Daemon.Scheduler.QueueSize, UnmarshalTask)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		builders: make(map[string]api.Builder, len(cfg.Builders)),
		runners:  make(map[string]api.Runner, len(cfg.Runners)),
		envcfg:   cfg.EnvConfig,
		ctx:      context.Background(),
		store:    store,
		queue:    queue,
		signals:  make(map[string]chan int),
	}

	for _, b := range cfg.Builders {
		e.builders[b.ID()] = b
	}

	for _, r := range cfg.Runners {
		e.runners[r.ID()] = r
	}

	for i := 0; i < cfg.EnvConfig.Daemon.Scheduler.Workers; i++ {
		go e.worker(i)
	}

	return e, nil
}

func NewDefaultEngine(ecfg *config.EnvConfig) (*Engine, error) {
	cfg := &EngineConfig{
		Builders:  AllBuilders,
		Runners:   AllRunners,
		EnvConfig: ecfg,
	}

	e, err := NewEngine(cfg)
	if err != nil {
		return nil, err
	}

	return e, nil
}

func (e *Engine) BuilderByName(name string) (api.Builder, bool) {
	e.lk.RLock()
	defer e.lk.RUnlock()

	m, ok := e.builders[name]
	return m, ok
}

func (e *Engine) RunnerByName(name string) (api.Runner, bool) {
	e.lk.RLock()
	defer e.lk.RUnlock()

	m, ok := e.runners[name]
	return m, ok
}

func (e *Engine) ListBuilders() map[string]api.Builder {
	e.lk.RLock()
	defer e.lk.RUnlock()

	m := make(map[string]api.Builder, len(e.builders))
	for k, v := range e.builders {
		m[k] = v
	}
	return m
}

func (e *Engine) ListRunners() map[string]api.Runner {
	e.lk.RLock()
	defer e.lk.RUnlock()

	m := make(map[string]api.Runner, len(e.runners))
	for k, v := range e.runners {
		m[k] = v
	}
	return m
}

func (e *Engine) QueueBuild(request *api.BuildRequest, sources *api.UnpackedSources) (string, error) {
	id := xid.New().String()
	err := e.queue.Push(&task.Task{
		Version:  0,
		Priority: request.Priority,
		ID:       id,
		Type:     task.TypeBuild,
		Input: &BuildInput{
			BuildRequest: request,
			Sources:      sources,
		},
		States: []task.DatedState{
			{
				State:   task.StateScheduled,
				Created: time.Now().UTC(),
			},
		},
		CreatedBy: task.CreatedBy(request.CreatedBy),
	})

	return id, err
}

func (e *Engine) QueueRun(request *api.RunRequest, sources *api.UnpackedSources) (string, error) {
	var (
		builders = request.Composition.ListBuilders()
		runner   = request.Composition.Global.Runner
	)

	// Get the runner.
	run, ok := e.runners[runner]
	if !ok {
		return "", fmt.Errorf("unknown runner: %s", runner)
	}

	// Check if builders and runner are compatible
	for _, builder := range builders {
		if !stringInSlice(builder, run.CompatibleBuilders()) {
			return "", fmt.Errorf("runner %s is incompatible with builder %s", runner, builder)
		}
	}

	id := xid.New().String()
	err := e.queue.Push(&task.Task{
		Version:     0,
		Priority:    request.Priority,
		Plan:        request.Composition.Global.Plan,
		Case:        request.Composition.Global.Case,
		ID:          id,
		Runner:      runner,
		Type:        task.TypeRun,
		Composition: request.Composition,
		Input: &RunInput{
			RunRequest: request,
			Sources:    sources,
		},
		States: []task.DatedState{
			{
				State:   task.StateScheduled,
				Created: time.Now().UTC(),
			},
		},
		CreatedBy: task.CreatedBy(request.CreatedBy),
	})

	return id, err
}

func (e *Engine) DoCollectOutputs(ctx context.Context, runID string, ow *rpc.OutputWriter) error {
	t, err := e.GetTask(runID)
	if err != nil {
		return fmt.Errorf("could not get task %s: %s", runID, err.Error())
	}

	runner := t.Runner
	run, ok := e.runners[runner]
	if !ok {
		return fmt.Errorf("unknown runner: %s", runner)
	}

	var cfg config.CoalescedConfig

	// Get the env config for the runner.
	cfg = cfg.Append(e.envcfg.Runners[runner])

	// Coalesce all configurations and deserialize into the config type
	// mandated by the builder.
	obj, err := cfg.CoalesceIntoType(run.ConfigType())
	if err != nil {
		return fmt.Errorf("error while coalescing configuration values: %w", err)
	}

	input := &api.CollectionInput{
		RunnerID:     runner,
		RunID:        runID,
		EnvConfig:    *e.envcfg,
		RunnerConfig: obj,
	}

	return run.CollectOutputs(ctx, input, ow)
}

func (e *Engine) DoTerminate(ctx context.Context, ctype api.ComponentType, ref string, ow *rpc.OutputWriter) error {
	var component interface{}
	var ok bool
	switch ctype {
	case api.RunnerType:
		component, ok = e.runners[ref]
	case api.BuilderType:
		component, ok = e.builders[ref]
	}

	if !ok {
		return fmt.Errorf("unknown component: %s (type: %s)", ref, ctype)
	}

	terminatable, ok := component.(api.Terminatable)
	if !ok {
		return fmt.Errorf("component %s is not terminatable", ref)
	}

	ow.Infof("terminating all jobs on component: %s", ref)

	err := terminatable.TerminateAll(ctx, ow)
	if err != nil {
		return err
	}

	ow.Infof("all jobs terminated on component: %s", ref)
	return nil
}

func (e *Engine) DoHealthcheck(ctx context.Context, runner string, fix bool, ow *rpc.OutputWriter) (*api.HealthcheckReport, error) {
	run, ok := e.runners[runner]
	if !ok {
		return nil, fmt.Errorf("unknown runner: %s", runner)
	}

	hc, ok := run.(api.Healthchecker)
	if !ok {
		return nil, fmt.Errorf("runner %s does not support healthchecks", runner)
	}

	ow.Infof("checking runner: %s", runner)

	return hc.Healthcheck(ctx, e, ow, fix)
}

func (e *Engine) DoBuildPurge(ctx context.Context, builder, plan string, ow *rpc.OutputWriter) error {
	bm, ok := e.builders[builder]
	if !ok {
		return fmt.Errorf("unrecognized builder: %s", builder)
	}
	return bm.Purge(ctx, plan, ow)
}

// EnvConfig returns the EnvConfig for this Engine.
func (e *Engine) EnvConfig() config.EnvConfig {
	return *e.envcfg
}

func (e *Engine) Context() context.Context {
	return e.ctx
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

// Tasks returns a list of tasks that match the filters argument
func (e *Engine) Tasks(filters api.TasksFilters) ([]task.Task, error) {
	var (
		res    []task.Task
		before time.Time
		after  time.Time
	)

	if filters.Before != nil {
		before = filters.Before.UTC()
	}

	if filters.After != nil {
		after = filters.After.UTC()
	} else {
		after = time.Now().UTC()
	}

	e.signalsLk.RLock()

	for _, state := range filters.States {
		var ires []task.Task

		tsks, err := e.store.Filter(state, before, after)
		if err != nil {
			return nil, err
		}

		for _, tsk := range tsks {
			if filters.TestPlan != "" && tsk.Plan != filters.TestPlan {
				continue
			}

			if filters.TestCase != "" && tsk.Case != filters.TestCase {
				continue
			}

			for _, tp := range filters.Types {
				if tsk.Type == tp {
					ires = append([]task.Task{*tsk}, ires...)
					break
				}
			}
		}

		res = append(res, ires...)
	}

	e.signalsLk.RUnlock()
	return res, nil
}

// DeleteTask removes a task from the Testground daemon database
func (e *Engine) DeleteTask(id string) error {
	return e.store.Delete(id)
}

func (e *Engine) GetTask(id string) (*task.Task, error) {
	return e.store.Get(id)
}

// Kill closes the signal channel for a given task, which signals to the runner to stop it
func (e *Engine) Kill(id string) error {
	e.signalsLk.RLock()
	if ch, ok := e.signals[id]; ok {
		close(ch)
	}
	e.signalsLk.RUnlock()

	return nil
}

// UnmarshalTask converts the given byte array into a valid task
func UnmarshalTask(taskData []byte) (*task.Task, error) {
	finalTask := &task.Task{}

	// unmarshal task once, so we can read its type
	unmarshaledValue := &task.Task{}
	err := json.Unmarshal(taskData, unmarshaledValue)

	if err != nil {
		return nil, err
	}

	// unmarshal task again, based on its type
	switch unmarshaledValue.Type {
	case task.TypeRun:
		finalTask.Input = &RunInput{}
		err = json.Unmarshal(taskData, finalTask)
	case task.TypeBuild:
		finalTask.Input = &BuildInput{}
		err = json.Unmarshal(taskData, finalTask)
	default:
		err = fmt.Errorf("invalid task type: %s", unmarshaledValue.Type)
	}

	if err != nil {
		return nil, err
	}
	return finalTask, nil
}

// Logs writes the Testground daemon logs for a given task to the passed writer.
// It is used when using the `--follow` option with `testground run`
func (e *Engine) Logs(ctx context.Context, id string, follow bool, cancel bool, w io.Writer) (*task.Task, error) {
	ow := rpc.NewFileOutputWriter(w)

	path := filepath.Join(e.EnvConfig().Dirs().Daemon(), id+".out")

	if !follow {
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("error while os.Open, err: %w", err)
		}
		defer file.Close()

		// copy logs to responseWriter, they are already json marshaled
		_, err = io.Copy(w, file)
		if err != nil {
			return nil, fmt.Errorf("error while io.Copy, err: %w", err)
		}

		return e.GetTask(id)
	}

	// wait for the task to start
	for {
		tsk, err := e.GetTask(id)
		if err != nil {
			return nil, fmt.Errorf("error while e.Status, err: %w", err)
		}

		if tsk.State().State == task.StateScheduled {
			time.Sleep(time.Millisecond * 500)
		} else {
			break
		}
	}

	stop := make(chan struct{})
	file, err := newTailReader(path, stop)
	if err != nil {
		return nil, fmt.Errorf("error when newTailReader, err: %w", err)
	}
	defer file.Close()

	go func() {
		for {
			e.signalsLk.RLock()
			_, running := e.signals[id]
			e.signalsLk.RUnlock()
			if !running {
				time.Sleep(2 * time.Second)
				close(stop)
				return
			}

			time.Sleep(1 * time.Second)
		}
	}()

	// helps with very long progress lines
	// (i.e. marshalled stdout logs into a chunk)
	// unlike bufio reader
	dec := json.NewDecoder(file)

Outer:
	for {
		select {
		case <-ctx.Done():
			if cancel {
				e.signalsLk.RLock()
				if ch, ok := e.signals[id]; ok {
					close(ch)
				}
				e.signalsLk.RUnlock()
			}
			break Outer
		default:
			var chunk rpc.Chunk
			err := dec.Decode(&chunk)
			if err != nil {
				if err == io.EOF {
					break Outer
				}
				return nil, fmt.Errorf("error when decoding chunk, err: %w", err)
			}

			m, err := base64.StdEncoding.DecodeString(chunk.Payload.(string))
			if err != nil {
				return nil, fmt.Errorf("error when base64 decoding string, err: %w", err)
			}

			_, err = ow.WriteProgress([]byte(m))
			if err != nil {
				return nil, fmt.Errorf("error on ow.WriteProgress, err: %w", err)
			}
		}
	}

	return e.GetTask(id)
}

type tailReader struct {
	io.ReadCloser
	stop chan struct{}
}

func (t tailReader) Read(b []byte) (int, error) {
	for {
		select {
		case <-t.stop:
			return 0, io.EOF
		default:
			n, err := t.ReadCloser.Read(b)
			if n > 0 {
				return n, nil
			} else if err != io.EOF {
				return n, err
			}
			time.Sleep(1000 * time.Millisecond)
		}
	}
}

func newTailReader(fileName string, stop chan struct{}) (tailReader, error) {
	f, err := os.Open(fileName)
	if err != nil {
		return tailReader{}, err
	}

	if _, err := f.Seek(0, 0); err != nil {
		return tailReader{}, err
	}
	return tailReader{f, stop}, nil
}
