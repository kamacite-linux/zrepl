package cmd

import (
	"time"

	"context"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"github.com/zrepl/zrepl/rpc"
	"github.com/zrepl/zrepl/zfs"
	"sync"
)

type LocalJob struct {
	Name              string
	Mapping           *DatasetMapFilter
	SnapshotPrefix    string
	Interval          time.Duration
	InitialReplPolicy InitialReplPolicy
	PruneLHS          PrunePolicy
	PruneRHS          PrunePolicy
	Debug             JobDebugSettings
	snapperTask       *Task
	replTask          *Task
	pruneRHSTask      *Task
	pruneLHSTask      *Task
}

func parseLocalJob(c JobParsingContext, name string, i map[string]interface{}) (j *LocalJob, err error) {

	var asMap struct {
		Mapping           map[string]string
		SnapshotPrefix    string `mapstructure:"snapshot_prefix"`
		Interval          string
		InitialReplPolicy string                 `mapstructure:"initial_repl_policy"`
		PruneLHS          map[string]interface{} `mapstructure:"prune_lhs"`
		PruneRHS          map[string]interface{} `mapstructure:"prune_rhs"`
		Debug             map[string]interface{}
	}

	if err = mapstructure.Decode(i, &asMap); err != nil {
		err = errors.Wrap(err, "mapstructure error")
		return nil, err
	}

	j = &LocalJob{Name: name}

	if j.Mapping, err = parseDatasetMapFilter(asMap.Mapping, false); err != nil {
		return
	}

	if j.SnapshotPrefix, err = parseSnapshotPrefix(asMap.SnapshotPrefix); err != nil {
		return
	}

	if j.Interval, err = parsePostitiveDuration(asMap.Interval); err != nil {
		err = errors.Wrap(err, "cannot parse interval")
		return
	}

	if j.InitialReplPolicy, err = parseInitialReplPolicy(asMap.InitialReplPolicy, DEFAULT_INITIAL_REPL_POLICY); err != nil {
		return
	}

	if j.PruneLHS, err = parsePrunePolicy(asMap.PruneLHS); err != nil {
		err = errors.Wrap(err, "cannot parse 'prune_lhs'")
		return
	}
	if j.PruneRHS, err = parsePrunePolicy(asMap.PruneRHS); err != nil {
		err = errors.Wrap(err, "cannot parse 'prune_rhs'")
		return
	}

	if err = mapstructure.Decode(asMap.Debug, &j.Debug); err != nil {
		err = errors.Wrap(err, "cannot parse 'debug'")
		return
	}

	return
}

func (j *LocalJob) JobName() string {
	return j.Name
}

func (j *LocalJob) JobStart(ctx context.Context) {

	log := ctx.Value(contextKeyLog).(Logger)
	defer log.Info("exiting")

	j.snapperTask = NewTask("snapshot", log)
	j.replTask = NewTask("repl", log)
	j.pruneRHSTask = NewTask("prune_rhs", log)
	j.pruneLHSTask = NewTask("prune_lhs", log)

	local := rpc.NewLocalRPC()
	// Allow access to any dataset since we control what mapping
	// is passed to the pull routine.
	// All local datasets will be passed to its Map() function,
	// but only those for which a mapping exists will actually be pulled.
	// We can pay this small performance penalty for now.
	handler := NewHandler(log.WithField(logTaskField, "handler"), localPullACL{}, NewPrefixFilter(j.SnapshotPrefix))

	registerEndpoints(local, handler)

	snapper := IntervalAutosnap{
		task:             j.snapperTask,
		DatasetFilter:    j.Mapping.AsFilter(),
		Prefix:           j.SnapshotPrefix,
		SnapshotInterval: j.Interval,
	}

	plhs, err := j.Pruner(j.pruneLHSTask, PrunePolicySideLeft, false)
	if err != nil {
		log.WithError(err).Error("error creating lhs pruner")
		return
	}
	prhs, err := j.Pruner(j.pruneRHSTask, PrunePolicySideRight, false)
	if err != nil {
		log.WithError(err).Error("error creating rhs pruner")
		return
	}

	makeCtx := func(parent context.Context, taskName string) (ctx context.Context) {
		return context.WithValue(parent, contextKeyLog, log.WithField(logTaskField, taskName))
	}
	var snapCtx, plCtx, prCtx, pullCtx context.Context
	snapCtx = makeCtx(ctx, "autosnap")
	plCtx = makeCtx(ctx, "prune_lhs")
	prCtx = makeCtx(ctx, "prune_rhs")
	pullCtx = makeCtx(ctx, "repl")

	didSnaps := make(chan struct{})
	go snapper.Run(snapCtx, didSnaps)

outer:
	for {

		select {
		case <-ctx.Done():
			break outer
		case <-didSnaps:
			log.Debug("finished taking snapshots")
			log.Info("starting replication procedure")
		}

		{
			log := pullCtx.Value(contextKeyLog).(Logger)
			log.Debug("replicating from lhs to rhs")
			puller := Puller{j.replTask, local, log, j.Mapping, j.InitialReplPolicy}
			if err := puller.doPull(); err != nil {
				log.WithError(err).Error("error replicating lhs to rhs")
			}
			// use a ctx as soon as doPull gains ctx support
			select {
			case <-ctx.Done():
				break outer
			default:
			}
		}

		var wg sync.WaitGroup

		log.Info("pruning lhs")
		wg.Add(1)
		go func() {
			plhs.Run(plCtx)
			wg.Done()
		}()

		log.Info("pruning rhs")
		wg.Add(1)
		go func() {
			prhs.Run(prCtx)
			wg.Done()
		}()

		wg.Wait()

	}

	log.WithError(ctx.Err()).Info("context")

}

func (j *LocalJob) JobStatus(ctxt context.Context) (*JobStatus, error) {
	return &JobStatus{Tasks: []*TaskStatus{
		j.snapperTask.Status(),
		j.pruneLHSTask.Status(),
		j.pruneRHSTask.Status(),
		j.replTask.Status(),
	}}, nil
}

func (j *LocalJob) Pruner(task *Task, side PrunePolicySide, dryRun bool) (p Pruner, err error) {

	var dsfilter zfs.DatasetFilter
	var pp PrunePolicy
	switch side {
	case PrunePolicySideLeft:
		pp = j.PruneLHS
		dsfilter = j.Mapping.AsFilter()
	case PrunePolicySideRight:
		pp = j.PruneRHS
		dsfilter, err = j.Mapping.InvertedFilter()
		if err != nil {
			err = errors.Wrap(err, "cannot invert mapping for prune_rhs")
			return
		}
	default:
		err = errors.Errorf("must be either left or right side")
		return
	}

	p = Pruner{
		task,
		time.Now(),
		dryRun,
		dsfilter,
		j.SnapshotPrefix,
		pp,
	}

	return
}
