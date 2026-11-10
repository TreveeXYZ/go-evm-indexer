package internal_bus

import (
	"github.com/mustafaturan/bus/v3"
	"github.com/mustafaturan/monoton/v2"
	"github.com/mustafaturan/monoton/v2/sequencer"
)

const (
	NewLogTopic string = "logs.new"
	// LogsIndexedTopic is emitted once a source at the head has stored a batch
	// of logs AND its cursor is persisted; the payload is the pipeline id (uint). It
	// wakes the pipeline's exporters instead of letting them sleep a full
	// PullInterval.
	LogsIndexedTopic     string = "logs.indexed"
	SourceUpdateTopic    string = "source.update"
	EnableSourceTopic    string = "source.enable"
	DisableSourceTopic   string = "source.disable"
	ExporterUpdateTopic  string = "exporter.update"
	ExporterCursorTopic  string = "exporter.cursor"
	EnableExporterTopic  string = "exporter.enable"
	DisableExporterTopic string = "exporter.disable"
	ShutdownSignalTopic  string = "signal.shutwown"
)

func InitializeBus() *bus.Bus {
	// Create message bus
	// configure id generator (it doesn't have to be monoton)
	node := uint64(1)
	initialTime := uint64(1577865600000) // set 2020-01-01 PST as initial time
	m, err := monoton.New(sequencer.NewMillisecond(), node, initialTime)
	if err != nil {
		panic(err)
	}

	// init an id generator
	var idGenerator bus.Next = m.Next

	// create a new bus instance
	b, err := bus.NewBus(idGenerator)
	if err != nil {
		panic(err)
	}

	b.RegisterTopics(NewLogTopic)
	b.RegisterTopics(LogsIndexedTopic)
	b.RegisterTopics(SourceUpdateTopic)
	b.RegisterTopics(EnableSourceTopic)
	b.RegisterTopics(DisableSourceTopic)
	b.RegisterTopics(ExporterUpdateTopic)
	b.RegisterTopics(ExporterCursorTopic)
	b.RegisterTopics(EnableExporterTopic)
	b.RegisterTopics(DisableExporterTopic)
	b.RegisterTopics(ShutdownSignalTopic)

	return b
}
