package exporter

import (
	"context"
	"sync"
	"testing"
	"time"

	internal_bus "github.com/evmi-cloud/go-evm-indexer/internal/bus"
	"github.com/evmi-cloud/go-evm-indexer/internal/types"
	pluginsdk "github.com/evmi-cloud/go-evm-indexer/pkg/exporter"
)

// lockedStore is a fakeStore a test can add logs to while the exporter runs.
type lockedStore struct {
	mu sync.Mutex
	fakeStore
}

func (s *lockedStore) add(l types.EvmLog) {
	s.mu.Lock()
	s.logs = append(s.logs, l)
	s.mu.Unlock()
}

func (s *lockedStore) GetLogsAfter(sourceIds []uint64, afterBlock uint64, afterLogIndex uint64, toBlock uint64) ([]types.EvmLog, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fakeStore.GetLogsAfter(sourceIds, afterBlock, afterLogIndex, toBlock)
}

// chanPlugin hands every delivered log id to the test.
type chanPlugin struct{ delivered chan string }

func (p *chanPlugin) Name() string                 { return "chan" }
func (p *chanPlugin) Init(pluginsdk.Context) error { return nil }
func (p *chanPlugin) Close() error                 { return nil }
func (p *chanPlugin) NewLogEvent(l pluginsdk.LogEvent) error {
	p.delivered <- l.Id
	return nil
}

// An idle exporter delivers freshly indexed logs as soon as its pipeline says
// so, without waiting for its pull interval; other pipelines do not wake it.
func TestExporterWakesOnIndexedLogs(t *testing.T) {
	store := &lockedStore{}
	plug := &chanPlugin{delivered: make(chan string, 16)}
	svc := newTestService(t, store, plug)
	svc.bus = internal_bus.InitializeBus()
	svc.chain.PullInterval = 30 // the fallback must not be what delivers

	// One connection: the test and the exporter share the in-memory database.
	sqlDB, err := svc.db.Conn.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)

	source := addSource(t, svc, 10, 11)
	store.add(logAt(source, 11, 0))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- svc.run(ctx, map[string]interface{}{}) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("the exporter loop did not stop")
		}
	})

	expect := func(want string) {
		t.Helper()
		select {
		case got := <-plug.delivered:
			if got != want {
				t.Fatalf("delivered %s, want %s", got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s was not delivered", want)
		}
	}

	// The backlog goes out at once; after it the exporter has nothing to do and
	// sleeps for its pull interval.
	expect(idKey(source, 11, 0))
	time.Sleep(300 * time.Millisecond)

	// A new block is indexed while it sleeps.
	store.add(logAt(source, 12, 0))
	setSourceHead(t, svc, source, 12)

	// Another pipeline's logs are not this exporter's business.
	if err := svc.bus.Emit(context.Background(), internal_bus.LogsIndexedTopic, svc.pipeline.ID+1); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-plug.delivered:
		t.Fatalf("%s delivered although only another pipeline indexed logs", got)
	case <-time.After(3 * wakeSettleDelay):
	}

	start := time.Now()
	if err := svc.bus.Emit(context.Background(), internal_bus.LogsIndexedTopic, svc.pipeline.ID); err != nil {
		t.Fatal(err)
	}
	expect(idKey(source, 12, 0))
	if waited := time.Since(start); waited < wakeSettleDelay {
		t.Fatalf("delivered after %s: the settle delay (%s) was skipped", waited, wakeSettleDelay)
	}
}
