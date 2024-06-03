package daemon

import (
	"context"
	"fmt"

	"github.com/godbus/dbus/v5"
	"github.com/godbus/dbus/v5/introspect"
	"github.com/godbus/dbus/v5/prop"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/zrepl/zrepl/daemon/job"
	"github.com/zrepl/zrepl/endpoint"
	"github.com/zrepl/zrepl/zfs"
)

type dbusJob struct {
	jobs     *jobs
}

func newDBusJob(jobs *jobs) *dbusJob {
	return &dbusJob{jobs: jobs}
}

func (j *dbusJob) Name() string {
	return jobNameDBus
}

func (j *dbusJob) Status() *job.Status {
	return &job.Status{Type: job.TypeInternal}
}

func (j *dbusJob) OwnedDatasetSubtreeRoot() (*zfs.DatasetPath, bool) {
	return nil, false
}

func (j *dbusJob) SenderConfig() *endpoint.SenderConfig {
	return nil
}

func (j *dbusJob) RegisterMetrics(registerer prometheus.Registerer) {
	// promControl.requestBegin = prometheus.NewCounterVec(prometheus.CounterOpts{
	// 	Namespace: "zrepl",
	// 	Subsystem: "control",
	// 	Name:      "request_begin",
	// 	Help:      "number of request we started to handle",
	// }, []string{"endpoint"})

	// promControl.requestFinished = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	// 	Namespace: "zrepl",
	// 	Subsystem: "control",
	// 	Name:      "request_finished",
	// 	Help:      "time it took a request to finish",
	// 	Buckets:   []float64{1e-6, 10e-6, 100e-6, 500e-6, 1e-3, 10e-3, 100e-3, 200e-3, 400e-3, 800e-3, 1, 10, 20},
	// }, []string{"endpoint"})
	// registerer.MustRegister(promControl.requestBegin)
	// registerer.MustRegister(promControl.requestFinished)
}

func (j *dbusJob) Run(ctx context.Context) {
	log := job.GetLogger(ctx)
	defer log.Info("dbus job finished")
	conn, err := dbus.ConnectSessionBus(dbus.WithContext(ctx))
	if err != nil {
		log.WithError(err).Error("error connecting to bus")
		return
	}
	defer conn.Close()
	reply, err := conn.RequestName(dbusServiceName, dbus.NameFlagDoNotQueue)
	if err != nil {
		log.WithError(err).Error("failed to acquire service name")
		return
	}
	if reply != dbus.RequestNameReplyPrimaryOwner {
		log.Error("service name is already taken")
		return
	}
	if err = j.exportIntrospectable(conn); err != nil {
		log.WithError(err).Error("failed to export introspection interfaces")
	}
	c := make(chan *dbus.Signal)
	conn.Signal(c)
	for range c {
		log.WithField("signal", c).Debug("received signal")
	}
	log.Info("dbus connection closed")
}

func (j *dbusJob) exportIntrospectable(conn *dbus.Conn) error {
	// /io/github/zrepl
	//   /io/github/zrepl/jobs
	//     /io/github/zrepl/jobs/<name1>
	//     /io/github/zrepl/jobs/<name2>
	node := &introspect.Node{
		Interfaces: []introspect.Interface{
			// 	peerIntrospectData,
			prop.IntrospectData,
			objectManagerIntrospectData,
		},
		Children: []introspect.Node{{Name: "jobs"}},
	}
	err := conn.Export(introspect.NewIntrospectable(node), dbusRootPath,
		introspectableInterface)
	if err != nil {
		return fmt.Errorf("export of '%s' at '%s' failed: %w",
			introspectableInterface, dbusRootPath, err)
	}
	for name, entry := range j.jobs.jobs {
		if IsInternalJobName(name) {
			continue
		}
		// Need to check the job status to determine which properties to
		// advertise via introspection.
		//
		// - Job types with a connect/serve parameter get a Transport
		//   property.
		// - Sink and pull jobs get a RootFilesystem property.
		// - Job types that support snapshotting get snapshot-related
		//   properties.
		iface := introspect.Interface{Name: zreplJobInterface}
		status := entry.Status()
		switch status.Type {
		case job.TypeSnap:
			iface.Properties = []introspect.Property{
				propType, propLastSnapshot, propNextSnapshot,
			}
		case job.TypePush:
			iface.Properties = []introspect.Property{
				propType, propTransport, propLastSnapshot,
				propNextSnapshot,
			}
		case job.TypeSink:
			iface.Properties = []introspect.Property{
				propType, propRootFs, propTransport,
			}
		case job.TypePull:
			iface.Properties = []introspect.Property{
				propType, propRootFs, propTransport,
			}
		case job.TypeSource:
			iface.Properties = []introspect.Property{
				propType, propTransport, propLastSnapshot,
				propNextSnapshot,
			}
		}
		subPath := dbusRootPath + "/jobs/" + name
		node = &introspect.Node{
			Interfaces: []introspect.Interface{
				iface, prop.IntrospectData,
			},
		}
		err = conn.Export(introspect.NewIntrospectable(node),
			dbus.ObjectPath(subPath), introspectableInterface)
		if err != nil {
			return fmt.Errorf("export of '%s' at '%s' failed: %w",
				introspectableInterface, subPath, err)
		}
		methods := map[string]interface{}{
			// "Get": s.cloneBE,
			"GetAll": j.getAllJobProps(name),
			// "Set": s.destroyBE,
		}
		err = conn.ExportMethodTable(methods, dbus.ObjectPath(subPath),
			propInterface)
		if err != nil {
			return fmt.Errorf("export of '%s' at '%s' failed: %w",
				introspectableInterface, subPath, err)
		}
	}
	return nil
}

// getAllJobProps returns a function that implements the
// org.freedesktop.DBus.Properties.GetAll method for the given job.
func (j *dbusJob) getAllJobProps(name string) func(string) (map[string]dbus.Variant, *dbus.Error) {
	return func(iface string) (map[string]dbus.Variant, *dbus.Error) {
		if iface != zreplJobInterface {
			return nil, prop.ErrIfaceNotFound
		}
		status, err := j.jobs.jobStatus(name)
		if err != nil {
			return nil, dbus.MakeFailedError(err)
		}
		out := make(map[string]dbus.Variant)
		out["Type"] = dbus.MakeVariant(status.Type)
		switch v := status.JobSpecific.(type) {
		case *job.ActiveSideStatus:
			// v.Snapshotting.Periodic.SleepUntil
			fmt.Printf("status: %#v\n", v)
		default:
			fmt.Printf("I don't know about type %T!\n", v)
		}
		switch status.Type {
		case job.TypeSnap:
			out["LastSnapshotTimestamp"] = dbus.MakeVariant(uint64(0))
			out["NextSnapshotTimestamp"] = dbus.MakeVariant(uint64(0))
		case job.TypePush:
			out["Transport"] = dbus.MakeVariant("placeholder")
			out["LastSnapshotTimestamp"] = dbus.MakeVariant(uint64(0))
			out["NextSnapshotTimestamp"] = dbus.MakeVariant(uint64(0))
		case job.TypeSink:
			out["RootFilesystem"] = dbus.MakeVariant("placeholder")
			out["Transport"] = dbus.MakeVariant("placeholder")
		case job.TypePull:
			out["RootFilesystem"] = dbus.MakeVariant("placeholder")
			out["Transport"] = dbus.MakeVariant("placeholder")
		case job.TypeSource:
			out["Transport"] = dbus.MakeVariant("placeholder")
			out["LastSnapshotTimestamp"] = dbus.MakeVariant(uint64(0))
			out["NextSnapshotTimestamp"] = dbus.MakeVariant(uint64(0))
		}
		return out, nil
	}
}

var promDBus struct {
	requestBegin    *prometheus.CounterVec
	requestFinished *prometheus.HistogramVec
}

const dbusServiceName = "io.github.zrepl1"
const dbusRootPath = "/io/github/zrepl"

const zreplJobInterface = "io.github.zrepl.Job"

var (
	propType = introspect.Property{
		Name:   "Type",
		Type:   "s",
		Access: "read",
		Annotations: []introspect.Annotation{
			{
				Name:  emitsChangedSignalAnn,
				Value: prop.EmitConst.String(),
			},
		},
	}
	propRootFs = introspect.Property{
		Name:   "RootFilesystem",
		Type:   "s",
		Access: "read",
		Annotations: []introspect.Annotation{
			{
				Name:  emitsChangedSignalAnn,
				Value: prop.EmitConst.String(),
			},
		},
	}
	propTransport = introspect.Property{
		Name:   "Transport",
		Type:   "s",
		Access: "read",
		Annotations: []introspect.Annotation{
			{
				Name:  emitsChangedSignalAnn,
				Value: prop.EmitConst.String(),
			},
		},
	}
	propLastSnapshot = introspect.Property{
		Name:   "LastSnapshotTimestamp",
		Type:   "t",
		Access: "read",
		Annotations: []introspect.Annotation{
			{
				Name:  emitsChangedSignalAnn,
				Value: prop.EmitFalse.String(),
			},
		},
	}
	propNextSnapshot = introspect.Property{
		Name:   "NextSnapshotTimestamp",
		Type:   "t",
		Access: "read",
		Annotations: []introspect.Annotation{
			{
				Name:  emitsChangedSignalAnn,
				Value: prop.EmitFalse.String(),
			},
		},
	}
)

// Standard D-Bus interfaces.
const (
	introspectableInterface = "org.freedesktop.DBus.Introspectable"
	propInterface           = "org.freedesktop.DBus.Properties"
	objectManagerInterface  = "org.freedesktop.DBus.ObjectManager"
)

const emitsChangedSignalAnn = "org.freedesktop.DBus.Property.EmitsChangedSignal"

// Introspection data for the standard org.freedesktop.DBus.ObjectManager
// interface.
var objectManagerIntrospectData = introspect.Interface{
	Name: objectManagerInterface,
	Methods: []introspect.Method{
		{
			Name: "GetManagedObjects",
			Args: []introspect.Arg{
				{
					Name: "object_paths_interfaces_and_properties",
					Type: "a{oa{sa{sv}}}",
					Direction: "out",
				},
			},
		},
	},
	Signals: []introspect.Signal{
		{
			Name: "InterfacesAdded",
			Args: []introspect.Arg{
				{
					Name: "object_path",
					Type: "o",
					Direction: "out",
				},
				{
					Name: "interfaces_and_properties",
					Type: "a{sa{sv}}",
					Direction: "out",
				},
			},
		},
		{
			Name: "InterfacesRemoved",
			Args: []introspect.Arg{
				{
					Name: "object_path",
					Type: "o",
					Direction: "out",
				},
				{
					Name: "interfaces",
					Type: "as",
					Direction: "out",
				},
			},
		},
	},
}
