package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	evmi_database "github.com/evmi-cloud/go-evm-indexer/internal/database/evmi-database"
	"github.com/rs/zerolog"
)

// fakeNode answers the read of the latest block with a fixed head and counts
// the calls.
func fakeNode(t *testing.T, head uint64) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Method != "eth_getBlockByNumber" {
			http.Error(w, "unexpected method "+req.Method, http.StatusBadRequest)
			return
		}
		calls.Add(1)
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, latestHeader(head))
	}))
	return srv, &calls
}

// latestHeader is the header a node answers for its latest block.
func latestHeader(number uint64) string {
	h, _ := json.Marshal(blockHead{Number: hexutil.Uint64(number), Hash: hashOf(number, 0), ParentHash: hashOf(number-1, 0)})
	return string(h)
}

// One watcher, many readers: the node hears one question per interval no
// matter how many sources read the answer.
func TestHeadWatcherSharesOneRequest(t *testing.T) {
	srv, calls := fakeNode(t, 12345)
	defer srv.Close()

	w := NewHeadWatcher(evmi_database.EvmBlockchain{RpcUrl: srv.URL, PullInterval: 1}, nil, zerolog.Nop())
	w.interval = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Serve(ctx) }()

	// Wait for the first answer.
	deadline := time.Now().Add(2 * time.Second)
	for {
		if n, ok := w.Head(); ok {
			if n != 12345 {
				t.Fatalf("head = %d, want 12345", n)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("watcher never learned the head")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Thirty "sources" hammering Head() for a while.
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				w.Head()
				time.Sleep(time.Millisecond)
			}
		}()
	}
	wg.Wait()

	// ~200ms elapsed at a 50ms interval: a handful of calls, not thousands.
	if got := calls.Load(); got > 10 {
		t.Fatalf("node answered %d reads of the latest block; readers must not add any", got)
	}
}

// Before the first successful read there is no head to act on.
func TestHeadWatcherUnknownUntilFirstRead(t *testing.T) {
	w := NewHeadWatcher(evmi_database.EvmBlockchain{RpcUrl: "http://127.0.0.1:1", PullInterval: 1}, nil, zerolog.Nop())
	if _, ok := w.Head(); ok {
		t.Fatal("a fresh watcher must not report a head")
	}
}

// A dead node must not stop the watcher: it keeps the last head and retries.
func TestHeadWatcherSurvivesNodeErrors(t *testing.T) {
	var fail atomic.Bool
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if fail.Load() {
			http.Error(w, "boom", http.StatusBadGateway)
			return
		}
		var req struct{ ID json.RawMessage }
		_ = json.NewDecoder(r.Body).Decode(&req)
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, latestHeader(16))
	}))
	defer srv.Close()

	w := NewHeadWatcher(evmi_database.EvmBlockchain{RpcUrl: srv.URL, PullInterval: 1}, nil, zerolog.Nop())
	w.interval = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Serve(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := w.Head(); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("watcher never learned the head")
		}
		time.Sleep(5 * time.Millisecond)
	}
	fail.Store(true)
	before := calls.Load()
	time.Sleep(120 * time.Millisecond)
	if n, ok := w.Head(); !ok || n != 16 {
		t.Fatalf("head must survive node errors, got %d/%v", n, ok)
	}
	if calls.Load() <= before {
		t.Fatal("watcher must keep retrying while the node fails")
	}
}
