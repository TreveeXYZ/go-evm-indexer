package indexer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	internal_bus "github.com/evmi-cloud/go-evm-indexer/internal/bus"
	evmi_database "github.com/evmi-cloud/go-evm-indexer/internal/database/evmi-database"
	"github.com/lmittmann/w3"
	"github.com/mustafaturan/bus/v3"
	"github.com/rs/zerolog"
)

// --- fake chain node --------------------------------------------------------

var (
	addrA      = common.HexToAddress("0x00000000000000000000000000000000000000a1")
	addrB      = common.HexToAddress("0x00000000000000000000000000000000000000b2")
	addrAbsent = common.HexToAddress("0x00000000000000000000000000000000000000c3")
)

type nodeBlock struct {
	number uint64
	hash   common.Hash
	parent common.Hash
	logs   []ethTypes.Log
}

// chainNode is a JSON-RPC node holding a small chain. It answers the three
// methods the indexer uses to follow the head and counts each kind of call.
// The fault hooks turn it into a node that lies or fails.
type chainNode struct {
	srv *httptest.Server

	mu     sync.Mutex
	head   uint64
	blocks map[uint64]*nodeBlock
	calls  map[string]int

	// header may alter the header of a block or fail the call.
	header func(number uint64, h *blockHead) error
	// byHash may alter the logs answered for a block hash or fail the call.
	byHash func(number uint64, logs []ethTypes.Log) ([]ethTypes.Log, error)
	// byRange may alter the logs answered for a block range or fail the call.
	byRange func(logs []ethTypes.Log) ([]ethTypes.Log, error)
}

const (
	callHead    = "eth_blockNumber"
	callHeader  = "eth_getBlockByNumber"
	callByHash  = "eth_getLogs(blockHash)"
	callByRange = "eth_getLogs(range)"
)

func newChainNode(t *testing.T) *chainNode {
	t.Helper()
	n := &chainNode{blocks: map[uint64]*nodeBlock{}, calls: map[string]int{}}
	n.srv = httptest.NewServer(http.HandlerFunc(n.serve))
	t.Cleanup(n.srv.Close)
	return n
}

// hashOf is the hash of block number on a given fork (0 = the main chain).
func hashOf(number, fork uint64) common.Hash {
	return common.BigToHash(new(big.Int).SetUint64((fork+1)<<32 | number))
}

// add appends a block to the main chain; each emitter logs once in it.
func (n *chainNode) add(number uint64, emitters ...common.Address) {
	n.addOn(number, 0, emitters...)
}

// addOn puts block number of the given fork on the node, its parent on the
// same fork.
func (n *chainNode) addOn(number, fork uint64, emitters ...common.Address) {
	b := &nodeBlock{number: number, hash: hashOf(number, fork), parent: hashOf(number-1, fork)}
	for i, addr := range emitters {
		b.logs = append(b.logs, ethTypes.Log{
			Address:     addr,
			Topics:      []common.Hash{common.BigToHash(new(big.Int).SetUint64(number*1000 + uint64(i) + 1))},
			Data:        []byte{},
			BlockNumber: number,
			BlockHash:   b.hash,
			TxHash:      common.BigToHash(new(big.Int).SetUint64(number*7919 + uint64(i) + 1)),
			TxIndex:     uint(i),
			Index:       uint(i),
		})
	}
	n.mu.Lock()
	n.blocks[number] = b
	n.mu.Unlock()
}

func (n *chainNode) setHead(head uint64) {
	n.mu.Lock()
	n.head = head
	n.mu.Unlock()
}

func (n *chainNode) count(kind string) int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.calls[kind]
}

func bloomOf(logs []ethTypes.Log) ethTypes.Bloom {
	var bloom ethTypes.Bloom
	for _, l := range logs {
		bloom.Add(l.Address.Bytes())
		for _, topic := range l.Topics {
			bloom.Add(topic.Bytes())
		}
	}
	return bloom
}

func (n *chainNode) serve(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID     json.RawMessage   `json:"id"`
		Method string            `json:"method"`
		Params []json.RawMessage `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "the fake node only answers single requests", http.StatusBadRequest)
		return
	}

	result, err := n.answer(req.Method, req.Params)
	resp := map[string]any{"jsonrpc": "2.0", "id": req.ID}
	if err != nil {
		resp["error"] = map[string]any{"code": -32000, "message": err.Error()}
	} else {
		resp["result"] = result
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (n *chainNode) answer(method string, params []json.RawMessage) (any, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	switch method {
	case "eth_blockNumber":
		n.calls[callHead]++
		return hexutil.Uint64(n.head), nil

	case "eth_getBlockByNumber":
		n.calls[callHeader]++
		var tag string
		if err := json.Unmarshal(params[0], &tag); err != nil {
			return nil, err
		}
		number, err := hexutil.DecodeUint64(tag)
		if err != nil {
			return nil, err
		}
		b, ok := n.blocks[number]
		if !ok {
			return nil, nil // a real node answers null for a block it does not have
		}
		h := &blockHead{Number: hexutil.Uint64(b.number), Hash: b.hash, ParentHash: b.parent, Bloom: bloomOf(b.logs)}
		if n.header != nil {
			if err := n.header(number, h); err != nil {
				return nil, err
			}
		}
		return h, nil

	case "eth_getLogs":
		var f struct {
			BlockHash *common.Hash     `json:"blockHash"`
			FromBlock string           `json:"fromBlock"`
			ToBlock   string           `json:"toBlock"`
			Address   []common.Address `json:"address"`
		}
		if err := json.Unmarshal(params[0], &f); err != nil {
			return nil, err
		}
		if f.BlockHash != nil {
			n.calls[callByHash]++
			for _, b := range n.blocks {
				if b.hash != *f.BlockHash {
					continue
				}
				logs := append([]ethTypes.Log{}, b.logs...)
				if n.byHash != nil {
					return n.byHash(b.number, logs)
				}
				return logs, nil
			}
			return nil, errors.New("unknown block")
		}

		n.calls[callByRange]++
		from, err := hexutil.DecodeUint64(f.FromBlock)
		if err != nil {
			return nil, err
		}
		to, err := hexutil.DecodeUint64(f.ToBlock)
		if err != nil {
			return nil, err
		}
		logs := []ethTypes.Log{}
		for number := from; number <= to; number++ {
			b, ok := n.blocks[number]
			if !ok {
				continue
			}
			for _, l := range b.logs {
				for _, addr := range f.Address {
					if l.Address == addr {
						logs = append(logs, l)
					}
				}
			}
		}
		if n.byRange != nil {
			return n.byRange(logs)
		}
		return logs, nil
	}
	return nil, errors.New("unexpected method " + method)
}

// newSummaryWatcher is a watcher shared by enough sources to read block
// summaries (slice 1, three sources), whose first head read was block 100.
func newSummaryWatcher(t *testing.T, n *chainNode) (*HeadWatcher, *w3.Client) {
	t.Helper()
	w := NewHeadWatcher(evmi_database.EvmBlockchain{RpcUrl: n.srv.URL, BlockSlice: 1, PullInterval: 1}, nil, zerolog.Nop())
	w.refs.Store(3)
	client := w3.MustDial(n.srv.URL)
	t.Cleanup(func() { client.Close() })
	w.summarise(context.Background(), client, 100)
	return w, client
}

func summaryCount(w *HeadWatcher) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.summaries)
}

// --- what a summary says ----------------------------------------------------

func TestSummaryRecordsWhoEmitted(t *testing.T) {
	n := newChainNode(t)
	w, client := newSummaryWatcher(t, n)

	n.add(101, addrA, addrB)
	n.add(102)
	w.summarise(context.Background(), client, 102)

	if w.Quiet(101, 101, addrA) || w.Quiet(101, 102, addrB) {
		t.Fatal("an address that logged in block 101 must not be reported quiet")
	}
	if !w.Quiet(102, 102, addrA) {
		t.Fatal("block 102 is empty: every address is quiet in it")
	}
	if !w.Quiet(101, 102, addrAbsent) {
		t.Fatal("an address with no log in 101-102 must be reported quiet")
	}
	if got := w.EmittedIn(101, 102, addrA); len(got) != 1 || got[0] != 101 {
		t.Fatalf("EmittedIn = %v, want [101]", got)
	}
	if got := w.EmittedIn(101, 102, addrAbsent); len(got) != 0 {
		t.Fatalf("EmittedIn for an absent address = %v, want none", got)
	}
	// One header and one unfiltered read per block, whatever the sources.
	if h, l := n.count(callHeader), n.count(callByHash); h != 2 || l != 2 {
		t.Fatalf("2 blocks cost %d headers and %d log reads, want 2 and 2", h, l)
	}
}

func TestSummaryQuietNeedsEveryBlock(t *testing.T) {
	n := newChainNode(t)
	w, client := newSummaryWatcher(t, n)
	n.add(101)
	n.add(102)
	w.summarise(context.Background(), client, 102)

	if w.Quiet(102, 101, addrAbsent) {
		t.Fatal("an inverted range is never quiet")
	}
	if w.Quiet(100, 102, addrAbsent) {
		t.Fatal("block 100 has no summary: the range cannot be quiet")
	}
	if w.Quiet(101, 103, addrAbsent) {
		t.Fatal("block 103 has no summary: the range cannot be quiet")
	}
	if w.Quiet(101, 101+summaryWindow, addrAbsent) {
		t.Fatal("a range wider than the window is never quiet")
	}

	w.Forget(102)
	if w.Quiet(101, 102, addrAbsent) {
		t.Fatal("a forgotten block has no summary any more")
	}
	if !w.Quiet(101, 101, addrAbsent) {
		t.Fatal("forgetting one block must keep the others")
	}
}

// --- when summaries are read ------------------------------------------------

// The first head read has nothing before it: no block is summarised, the
// sources catch up on their own as they always did.
func TestSummaryNoneOnFirstHead(t *testing.T) {
	n := newChainNode(t)
	n.add(100, addrA)
	w, _ := newSummaryWatcher(t, n) // reads head 100

	if got := n.count(callHeader) + n.count(callByHash); got != 0 {
		t.Fatalf("first head cost %d requests, want 0", got)
	}
	if summaryCount(w) != 0 {
		t.Fatal("first head must not be summarised")
	}
}

// Summaries are read only when they are cheaper than the sources reading
// alone: two requests per block against one request per source per slice.
func TestSummaryGating(t *testing.T) {
	cases := []struct {
		name  string
		slice uint64
		refs  int32
		want  bool
	}{
		{"slice 1, two sources", 1, 2, false},
		{"slice 1, three sources", 1, 3, true},
		{"slice 5, ten sources", 5, 10, false},
		{"slice 5, eleven sources", 5, 11, true},
		{"no source", 1, 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n := newChainNode(t)
			n.add(101)
			w := NewHeadWatcher(evmi_database.EvmBlockchain{RpcUrl: n.srv.URL, BlockSlice: c.slice, PullInterval: 1}, nil, zerolog.Nop())
			w.refs.Store(c.refs)
			client := w3.MustDial(n.srv.URL)
			defer client.Close()

			w.summarise(context.Background(), client, 100)
			w.summarise(context.Background(), client, 101)

			if got := summaryCount(w) == 1; got != c.want {
				t.Fatalf("summarised = %v, want %v", got, c.want)
			}
			if !c.want && n.count(callHeader)+n.count(callByHash) != 0 {
				t.Fatal("summaries off must not cost a request")
			}
		})
	}
}

func TestSummaryKillSwitch(t *testing.T) {
	t.Setenv(summariesEnv, "0")
	n := newChainNode(t)
	n.add(101)
	w, client := newSummaryWatcher(t, n)
	w.summarise(context.Background(), client, 101)

	if summaryCount(w) != 0 || n.count(callHeader)+n.count(callByHash) != 0 {
		t.Fatal(summariesEnv + "=0 must turn block summaries off")
	}
}

// A head jump is summarised block by block up to summaryMaxGap blocks; a
// larger one is left to the sources, and summaries resume right after it.
func TestSummaryGap(t *testing.T) {
	t.Run("largest jump summarised", func(t *testing.T) {
		n := newChainNode(t)
		w, client := newSummaryWatcher(t, n)
		last := 100 + summaryMaxGap
		for b := uint64(101); b <= last; b++ {
			n.add(b)
		}
		w.summarise(context.Background(), client, last)
		if !w.Quiet(101, last, addrAbsent) {
			t.Fatalf("a jump of %d blocks must be summarised", summaryMaxGap)
		}
	})

	t.Run("jump too large", func(t *testing.T) {
		n := newChainNode(t)
		w, client := newSummaryWatcher(t, n)
		last := 101 + summaryMaxGap
		for b := uint64(101); b <= last+1; b++ {
			n.add(b)
		}
		w.summarise(context.Background(), client, last)
		if summaryCount(w) != 0 || n.count(callHeader) != 0 {
			t.Fatalf("a jump of %d blocks must be left to the sources", summaryMaxGap+1)
		}
		w.summarise(context.Background(), client, last+1)
		if !w.Quiet(last+1, last+1, addrAbsent) {
			t.Fatal("summaries must resume on the block after the jump")
		}
	})
}

// A lagging node reporting an older head, or the same head again, costs
// nothing and does not rewind what was summarised.
func TestSummaryIgnoresOlderHead(t *testing.T) {
	n := newChainNode(t)
	w, client := newSummaryWatcher(t, n)
	for b := uint64(101); b <= 105; b++ {
		n.add(b)
	}
	w.summarise(context.Background(), client, 105)
	before := n.count(callHeader) + n.count(callByHash)

	w.summarise(context.Background(), client, 103)
	w.summarise(context.Background(), client, 105)

	if got := n.count(callHeader) + n.count(callByHash); got != before {
		t.Fatalf("an older or repeated head cost %d requests, want 0", got-before)
	}
	if w.next != 106 {
		t.Fatalf("next = %d, want 106", w.next)
	}
}

func TestSummaryWindowIsBounded(t *testing.T) {
	n := newChainNode(t)
	w, client := newSummaryWatcher(t, n)
	last := 100 + summaryWindow + 16
	for b := uint64(101); b <= last; b++ {
		n.add(b)
		w.summarise(context.Background(), client, b)
	}

	if got := summaryCount(w); got != int(summaryWindow) {
		t.Fatalf("%d summaries kept, want %d", got, summaryWindow)
	}
	first := last - summaryWindow + 1
	if !w.Quiet(first, last, addrAbsent) {
		t.Fatal("the last window of blocks must be summarised")
	}
	if w.Quiet(first-1, last-1, addrAbsent) {
		t.Fatal("a block older than the window must be pruned")
	}
}

// --- answers that must not be trusted ---------------------------------------

// Whatever a node answers wrong for a block, that block gets no summary and
// its sources read it themselves.
func TestSummaryRejected(t *testing.T) {
	failing := errors.New("node error")
	cases := []struct {
		name string
		rig  func(n *chainNode)
		// calls is the kind of request retried summaryAttempts times.
		calls string
	}{
		{"truncated logs", func(n *chainNode) {
			n.byHash = func(_ uint64, logs []ethTypes.Log) ([]ethTypes.Log, error) { return logs[:1], nil }
		}, callByHash},
		{"empty answer for a block with logs", func(n *chainNode) {
			n.byHash = func(uint64, []ethTypes.Log) ([]ethTypes.Log, error) { return []ethTypes.Log{}, nil }
		}, callByHash},
		{"log of another block hash", func(n *chainNode) {
			n.byHash = func(_ uint64, logs []ethTypes.Log) ([]ethTypes.Log, error) {
				logs[0].BlockHash = hashOf(101, 9)
				return logs, nil
			}
		}, callByHash},
		{"log of another block number", func(n *chainNode) {
			n.byHash = func(_ uint64, logs []ethTypes.Log) ([]ethTypes.Log, error) {
				logs[0].BlockNumber = 100
				return logs, nil
			}
		}, callByHash},
		{"removed log", func(n *chainNode) {
			n.byHash = func(_ uint64, logs []ethTypes.Log) ([]ethTypes.Log, error) {
				logs[0].Removed = true
				return logs, nil
			}
		}, callByHash},
		{"extra log", func(n *chainNode) {
			n.byHash = func(_ uint64, logs []ethTypes.Log) ([]ethTypes.Log, error) {
				extra := logs[0]
				extra.Address = addrAbsent
				return append(logs, extra), nil
			}
		}, callByHash},
		{"log read fails", func(n *chainNode) {
			n.byHash = func(uint64, []ethTypes.Log) ([]ethTypes.Log, error) { return nil, failing }
		}, callByHash},
		{"header of another block", func(n *chainNode) {
			n.header = func(_ uint64, h *blockHead) error { h.Number++; return nil }
		}, callHeader},
		{"header read fails", func(n *chainNode) {
			n.header = func(uint64, *blockHead) error { return failing }
		}, callHeader},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			n := newChainNode(t)
			w, client := newSummaryWatcher(t, n)
			n.add(101, addrA, addrB)
			c.rig(n)

			w.summarise(context.Background(), client, 101)

			if summaryCount(w) != 0 {
				t.Fatal("a wrong answer must not become a summary")
			}
			if w.Quiet(101, 101, addrAbsent) {
				t.Fatal("a block without summary must not be reported quiet")
			}
			if got := n.count(c.calls); got != summaryAttempts {
				t.Fatalf("%d attempts, want %d", got, summaryAttempts)
			}
		})
	}

	t.Run("block the node does not have", func(t *testing.T) {
		t.Parallel()
		n := newChainNode(t)
		w, client := newSummaryWatcher(t, n)
		w.summarise(context.Background(), client, 101)
		if summaryCount(w) != 0 || n.count(callByHash) != 0 {
			t.Fatal("a missing block must not be summarised")
		}
	})
}

// A node that does not have the new block yet is asked again.
func TestSummaryRetriesTransientFailure(t *testing.T) {
	n := newChainNode(t)
	w, client := newSummaryWatcher(t, n)
	n.add(101, addrA)
	asked := 0
	n.byHash = func(_ uint64, logs []ethTypes.Log) ([]ethTypes.Log, error) {
		asked++
		if asked == 1 {
			return []ethTypes.Log{}, nil // behind: the block looks empty
		}
		return logs, nil
	}

	w.summarise(context.Background(), client, 101)

	if got := w.EmittedIn(101, 101, addrA); len(got) != 1 {
		t.Fatal("the second, complete answer must be kept")
	}
	if got := n.count(callByHash); got != 2 {
		t.Fatalf("%d log reads, want 2", got)
	}
}

// A round stops at the first block it cannot verify: the following blocks of
// that round have no summary either, and the next head starts a fresh round.
func TestSummaryRoundStopsAtFirstFailure(t *testing.T) {
	n := newChainNode(t)
	w, client := newSummaryWatcher(t, n)
	for b := uint64(101); b <= 103; b++ {
		n.add(b, addrA)
	}
	n.byHash = func(number uint64, logs []ethTypes.Log) ([]ethTypes.Log, error) {
		if number == 102 {
			return []ethTypes.Log{}, nil
		}
		return logs, nil
	}

	w.summarise(context.Background(), client, 103)

	if !w.Quiet(101, 101, addrAbsent) {
		t.Fatal("block 101 was verified and must be summarised")
	}
	if w.Quiet(102, 102, addrAbsent) || w.Quiet(103, 103, addrAbsent) {
		t.Fatal("blocks 102 and 103 must have no summary")
	}

	n.add(104)
	w.summarise(context.Background(), client, 104)
	if !w.Quiet(104, 104, addrAbsent) {
		t.Fatal("the next head must be summarised again")
	}
	if w.Quiet(101, 104, addrAbsent) {
		t.Fatal("a range across the unverified blocks must not be quiet")
	}
}

// A block that does not build on the one summarised before it means the chain
// reorganised: what was summarised before describes blocks that are gone.
func TestSummaryDroppedOnReorg(t *testing.T) {
	n := newChainNode(t)
	w, client := newSummaryWatcher(t, n)
	n.add(101)
	n.add(102)
	w.summarise(context.Background(), client, 102)

	n.addOn(103, 1) // its parent is another block 102
	w.summarise(context.Background(), client, 103)

	if w.Quiet(101, 102, addrAbsent) {
		t.Fatal("summaries from before the reorg must be dropped")
	}
	if !w.Quiet(103, 103, addrAbsent) {
		t.Fatal("the new block must be summarised")
	}
}

// --- waking the sources -----------------------------------------------------

func TestWaitReturnsOnNewHead(t *testing.T) {
	w := NewHeadWatcher(evmi_database.EvmBlockchain{PullInterval: 30}, nil, zerolog.Nop())
	w.publish(10)

	// A head the caller has not acted on yet: no wait at all.
	start := time.Now()
	if err := w.Wait(context.Background(), 9); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("Wait must return at once when the head already moved")
	}

	// Same head: Wait blocks until the head moves, not for the interval.
	done := make(chan error, 1)
	go func() { done <- w.Wait(context.Background(), 10) }()
	select {
	case <-done:
		t.Fatal("Wait returned although the head did not move")
	case <-time.After(100 * time.Millisecond):
	}
	w.publish(10) // the same head again wakes nobody
	select {
	case <-done:
		t.Fatal("publishing the same head must not wake the sources")
	case <-time.After(100 * time.Millisecond):
	}
	w.publish(11)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait must return as soon as a new head is published")
	}
}

func TestWaitFallsBackToTheInterval(t *testing.T) {
	w := NewHeadWatcher(evmi_database.EvmBlockchain{PullInterval: 30}, nil, zerolog.Nop())
	w.interval = 40 * time.Millisecond

	for _, seen := range []uint64{0, 10} { // head unknown, then head unchanged
		start := time.Now()
		if err := w.Wait(context.Background(), seen); err != nil {
			t.Fatal(err)
		}
		if waited := time.Since(start); waited < 30*time.Millisecond || waited > 2*time.Second {
			t.Fatalf("Wait took %s, want one interval", waited)
		}
		w.publish(10)
	}
}

func TestWaitStopsWithTheContext(t *testing.T) {
	w := NewHeadWatcher(evmi_database.EvmBlockchain{PullInterval: 30}, nil, zerolog.Nop())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Wait(ctx, 0) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Wait = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait must return when its context is canceled")
	}
}

// Serve stores the summaries of a new head before it publishes that head: a
// source woken by the head always finds them.
func TestServeSummarisesBeforePublishing(t *testing.T) {
	n := newChainNode(t)
	n.setHead(100)
	w := NewHeadWatcher(evmi_database.EvmBlockchain{RpcUrl: n.srv.URL, BlockSlice: 1, PullInterval: 1}, nil, zerolog.Nop())
	w.refs.Store(3)
	w.interval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Serve(ctx) }()

	seen := uint64(0)
	for _, head := range []uint64{100, 103, 104} {
		for b := seen + 1; seen != 0 && b <= head; b++ {
			n.add(b, addrA)
		}
		n.setHead(head)
		deadline := time.Now().Add(3 * time.Second)
		for {
			if got, _ := w.Head(); got == head {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("head %d never published", head)
			}
			if err := w.Wait(ctx, seen); err != nil {
				t.Fatal(err)
			}
		}
		if seen != 0 && !w.Quiet(seen+1, head, addrAbsent) {
			t.Fatalf("head %d published before its summaries were stored", head)
		}
		seen = head
	}
	if h, l := n.count(callHeader), n.count(callByHash); h != 4 || l != 4 {
		t.Fatalf("4 new blocks cost %d headers and %d log reads, want 4 and 4", h, l)
	}
}

// --- a source using the summaries -------------------------------------------

// contractSource is a CONTRACT source on addrA, indexed up to block 100, wired
// to the node and to a watcher that already read head 100.
func contractSource(t *testing.T, n *chainNode, refs int32) (*SourceIndexerService, *HeadWatcher, *w3.Client) {
	t.Helper()
	s := newSourceIndexerForTest(t, evmi_database.EvmLogSource{
		Enabled:          true,
		Type:             string(evmi_database.ContractLogSourceType),
		Address:          sql.NullString{String: addrA.Hex(), Valid: true},
		SyncBlock:        100,
		EvmLogPipelineID: 3,
		EvmBlockchainID:  2,
	})
	s.chain = evmi_database.EvmBlockchain{RpcUrl: n.srv.URL, BlockSlice: 1, BlockRange: 500, PullInterval: 1}

	w := NewHeadWatcher(s.chain, nil, zerolog.Nop())
	w.refs.Store(refs)
	w.interval = 20 * time.Millisecond
	client := w3.MustDial(n.srv.URL)
	t.Cleanup(func() { client.Close() })
	w.summarise(context.Background(), client, 100)
	w.publish(100)
	s.head = w
	return s, w, client
}

// runSource runs the source's loop and reports each cursor it persists.
func runSource(t *testing.T, s *SourceIndexerService) (cursors <-chan uint64, stop func() error) {
	t.Helper()
	moved := make(chan uint64, 64)
	last := s.source.SyncBlock
	key := "test-cursor-" + t.Name()
	s.bus.RegisterHandler(key, bus.Handler{
		Matcher: internal_bus.SourceUpdateTopic,
		Handle: func(_ context.Context, e bus.Event) {
			if src, ok := e.Data.(evmi_database.EvmLogSource); ok && src.SyncBlock != last {
				last = src.SyncBlock
				moved <- src.SyncBlock
			}
		},
	})
	t.Cleanup(func() { s.bus.DeregisterHandler(key) })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- s.serveIndexation(ctx, func(from, to *big.Int) ethereum.FilterQuery {
			return ethereum.FilterQuery{FromBlock: from, ToBlock: to, Addresses: []common.Address{addrA}}
		})
	}()
	var once sync.Once
	var result error
	return moved, func() error {
		once.Do(func() {
			cancel()
			select {
			case result = <-done:
			case <-time.After(3 * time.Second):
				t.Error("the source loop did not stop")
			}
		})
		return result
	}
}

func storedCursor(t *testing.T, s *SourceIndexerService) uint64 {
	t.Helper()
	var row evmi_database.EvmLogSource
	if err := s.db.Conn.First(&row, s.source.ID).Error; err != nil {
		t.Fatal(err)
	}
	return row.SyncBlock
}

func waitCursor(t *testing.T, cursors <-chan uint64, want uint64) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case got := <-cursors:
			if got == want {
				return
			}
		case <-deadline:
			t.Fatalf("the cursor never reached block %d", want)
		}
	}
}

// Blocks in which the source's contract emitted nothing move its cursor
// without a request, even when other contracts emitted in them.
func TestSourceSkipsQuietBlocksWithoutRequest(t *testing.T) {
	n := newChainNode(t)
	s, w, client := contractSource(t, n, 3)
	cursors, stop := runSource(t, s)

	n.add(101, addrB)
	n.add(102)
	w.summarise(context.Background(), client, 102)
	w.publish(102)
	waitCursor(t, cursors, 102)

	n.add(103, addrB)
	w.summarise(context.Background(), client, 103)
	w.publish(103)
	waitCursor(t, cursors, 103)

	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := storedCursor(t, s); got != 103 {
		t.Fatalf("stored cursor = %d, want 103", got)
	}
	if got := n.count(callByRange); got != 0 {
		t.Fatalf("the source sent %d eth_getLogs for blocks with nothing for it, want 0", got)
	}
}

// Without summaries the source reads its range itself, as before.
func TestSourceReadsItselfWithoutSummaries(t *testing.T) {
	n := newChainNode(t)
	s, w, client := contractSource(t, n, 1) // one source: summaries are not worth it
	cursors, stop := runSource(t, s)

	n.add(101, addrB)
	w.summarise(context.Background(), client, 101)
	w.publish(101)
	waitCursor(t, cursors, 101)

	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := storedCursor(t, s); got != 101 {
		t.Fatalf("stored cursor = %d, want 101", got)
	}
	if got := n.count(callByRange); got != 1 {
		t.Fatalf("the source sent %d eth_getLogs, want 1", got)
	}
	if got := n.count(callHeader) + n.count(callByHash); got != 0 {
		t.Fatalf("summaries were read (%d requests) for a single source", got)
	}
}

// A block whose summary could not be verified is read by the source itself.
func TestSourceReadsItselfWhenSummaryRejected(t *testing.T) {
	n := newChainNode(t)
	s, w, client := contractSource(t, n, 3)
	cursors, stop := runSource(t, s)

	n.add(101, addrB)
	n.byHash = func(uint64, []ethTypes.Log) ([]ethTypes.Log, error) { return []ethTypes.Log{}, nil }
	w.summarise(context.Background(), client, 101)
	w.publish(101)
	waitCursor(t, cursors, 101)

	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := n.count(callByRange); got != 1 {
		t.Fatalf("the source sent %d eth_getLogs, want 1", got)
	}
}

// The watcher saw the source's contract emit in a block, but the source's own
// read comes back empty (a node that is behind): the read is refused, the
// cursor stays, and the block is replayed.
func TestSourceRefusesEmptyAnswerForBlockWithItsLogs(t *testing.T) {
	n := newChainNode(t)
	s, w, client := contractSource(t, n, 3)
	_, stop := runSource(t, s)
	t.Cleanup(func() { _ = stop() })

	n.add(101, addrA)
	n.byRange = func([]ethTypes.Log) ([]ethTypes.Log, error) { return []ethTypes.Log{}, nil }
	w.summarise(context.Background(), client, 101)
	w.publish(101)

	deadline := time.Now().Add(3 * time.Second)
	for n.count(callByRange) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the source never read block 101")
		}
		time.Sleep(5 * time.Millisecond)
	}
	err := stop()
	if err == nil || !strings.Contains(err.Error(), "returned nothing for block 101") {
		t.Fatalf("serveIndexation = %v, want the empty answer refused", err)
	}
	if got := storedCursor(t, s); got != 100 {
		t.Fatalf("stored cursor = %d, want 100 (block 101 must be replayed)", got)
	}
	// The summary is dropped so that a stale summary cannot block the replay.
	if got := w.EmittedIn(101, 101, addrA); len(got) != 0 {
		t.Fatal("the summary of the refused block must be forgotten")
	}
	if err := s.checkAgainstSummaries(101, 101, nil); err != nil {
		t.Fatalf("the replay must not be checked against a forgotten summary: %v", err)
	}
}

func TestCheckAgainstSummaries(t *testing.T) {
	n := newChainNode(t)
	s, w, client := contractSource(t, n, 3)
	n.add(101, addrA)
	n.add(102, addrB)
	n.add(103, addrA)
	w.summarise(context.Background(), client, 103)

	at := func(blocks ...uint64) []ethTypes.Log {
		var logs []ethTypes.Log
		for _, b := range blocks {
			logs = append(logs, ethTypes.Log{Address: addrA, BlockNumber: b})
		}
		return logs
	}

	if err := s.checkAgainstSummaries(101, 103, at(101, 103)); err != nil {
		t.Fatalf("a complete answer was refused: %v", err)
	}
	if err := s.checkAgainstSummaries(102, 102, nil); err != nil {
		t.Fatalf("an empty answer for a block without this source's logs was refused: %v", err)
	}
	if err := s.checkAgainstSummaries(101, 103, at(101)); err == nil {
		t.Fatal("an answer missing block 103 must be refused")
	}

	// Only CONTRACT and FACTORY sources are checked: the others do not filter
	// on one address.
	s.source.Type = string(evmi_database.TopicLogSourceType)
	if _, ok := s.watchedAddress(); ok {
		t.Fatal("a TOPIC source has no watched address")
	}
	if err := s.checkAgainstSummaries(101, 101, nil); err != nil {
		t.Fatalf("a TOPIC source must not be checked: %v", err)
	}
}
