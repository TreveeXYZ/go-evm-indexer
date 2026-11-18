package indexer

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	evmi_database "github.com/evmi-cloud/go-evm-indexer/internal/database/evmi-database"
	"github.com/rs/zerolog"
)

// --- a node that announces its blocks ---------------------------------------

// announcer is the WebSocket side of a chainNode: it serves eth_subscribe
// newHeads and pushes the headers the test tells it to.
type announcer struct {
	mu     sync.Mutex
	server *rpc.Server
	subs   []chan *blockHead
	opened int
}

// NewHeads is eth_subscribe("newHeads").
func (a *announcer) NewHeads(ctx context.Context) (*rpc.Subscription, error) {
	notifier, ok := rpc.NotifierFromContext(ctx)
	if !ok {
		return nil, rpc.ErrNotificationsUnsupported
	}
	sub := notifier.CreateSubscription()
	feed := make(chan *blockHead, 16)
	a.mu.Lock()
	a.subs = append(a.subs, feed)
	a.opened++
	a.mu.Unlock()
	go func() {
		for {
			select {
			case head := <-feed:
				_ = notifier.Notify(sub.ID, head)
			case <-sub.Err():
				return
			}
		}
	}()
	return sub, nil
}

// start (re)opens the WebSocket side with no subscriber.
func (a *announcer) start(t *testing.T) {
	t.Helper()
	srv := rpc.NewServer()
	if err := srv.RegisterName("eth", a); err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	a.server, a.subs = srv, nil
	a.mu.Unlock()
}

// drop closes every connection, as a node that restarts does.
func (a *announcer) drop() {
	a.mu.Lock()
	srv := a.server
	a.mu.Unlock()
	srv.Stop()
}

func (a *announcer) handler() http.Handler {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.server.WebsocketHandler([]string{"*"})
}

func (a *announcer) subscriptions() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.opened
}

// announce pushes a header to every open subscription.
func (a *announcer) announce(head *blockHead) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, feed := range a.subs {
		select {
		case feed <- head:
		default: // a subscription nobody reads any more
		}
	}
}

// listeningNode is a chainNode that also serves subscriptions, and a watcher
// on it shared by enough sources to read block summaries. The watcher's
// periodic read is an hour away unless the test shortens the interval.
func listeningNode(t *testing.T) (*chainNode, *announcer, *HeadWatcher) {
	t.Helper()
	n := newChainNode(t)
	a := &announcer{}
	a.start(t)
	n.ws = a.handler
	t.Cleanup(a.drop)

	w := NewHeadWatcher(evmi_database.EvmBlockchain{RpcUrl: n.srv.URL, BlockSlice: 1, PullInterval: 1}, nil, zerolog.Nop())
	w.refs.Store(3)
	w.interval = time.Hour
	return n, a, w
}

func serve(t *testing.T, w *HeadWatcher) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = w.Serve(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("Serve did not return with its context: the listener was left running")
		}
	})
}

// eventually waits for cond, for two seconds at most.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func headIs(w *HeadWatcher, want uint64) func() bool {
	return func() bool { n, _ := w.Head(); return n == want }
}

// shrink sets a timing variable for the length of one test.
func shrink(t *testing.T, v *time.Duration, to time.Duration) {
	t.Helper()
	old := *v
	*v = to
	t.Cleanup(func() { *v = old })
}

// --- the subscription -------------------------------------------------------

func TestListenURL(t *testing.T) {
	for in, want := range map[string]string{
		"https://node.example/v2/key": "wss://node.example/v2/key",
		"http://127.0.0.1:8545":       "ws://127.0.0.1:8545",
		"wss://node.example/ws":       "wss://node.example/ws",
		"ws://127.0.0.1:8546":         "ws://127.0.0.1:8546",
	} {
		if got, ok := listenURL(in); !ok || got != want {
			t.Errorf("listenURL(%q) = %q, %v; want %q", in, got, ok, want)
		}
	}
	for _, in := range []string{"", "node.example", "ipc:///tmp/geth.ipc", "https://"} {
		if got, ok := listenURL(in); ok {
			t.Errorf("listenURL(%q) = %q; want none", in, got)
		}
	}
}

// A block the node announces is the head at once, long before the periodic
// read, and its announced header is the one its summary is built on.
func TestAnnouncedBlockIsTakenAtOnce(t *testing.T) {
	n, a, w := listeningNode(t)
	n.add(100)
	n.setHead(100)
	serve(t, w)
	eventually(t, "the first read never gave a head", headIs(w, 100))
	eventually(t, "the watcher never subscribed", func() bool { return a.subscriptions() == 1 })

	n.add(101, addrA)
	a.announce(headOf(n.blocks[101]))
	eventually(t, "an announced block never became the head", headIs(w, 101))

	if w.Quiet(101, 101, addrA) || !w.Quiet(101, 101, addrAbsent) {
		t.Fatal("the announced block must be summarised before it is the head")
	}
	if got := n.count(callLatest); got != 1 {
		t.Fatalf("%d reads of the latest block, want the first one only", got)
	}
	if h, l := n.count(callHeader), n.count(callByHash); h != 0 || l != 1 {
		t.Fatalf("an announced block cost %d headers and %d log reads, want 0 and 1", h, l)
	}
}

// A block the subscription skipped is read for itself when the next one is
// announced: nothing is left without a summary.
func TestAnnouncedBlockAfterAMissedOne(t *testing.T) {
	n, a, w := listeningNode(t)
	n.add(100)
	n.setHead(100)
	serve(t, w)
	eventually(t, "the first read never gave a head", headIs(w, 100))
	eventually(t, "the watcher never subscribed", func() bool { return a.subscriptions() == 1 })

	n.add(101, addrA)
	n.add(102, addrB)
	a.announce(headOf(n.blocks[102]))
	eventually(t, "an announced block never became the head", headIs(w, 102))

	if w.Quiet(101, 101, addrA) || w.Quiet(102, 102, addrB) || !w.Quiet(101, 102, addrAbsent) {
		t.Fatal("both blocks must be summarised")
	}
	if h, l := n.count(callHeader), n.count(callByHash); h != 1 || l != 2 {
		t.Fatalf("one skipped block cost %d headers and %d log reads, want 1 and 2", h, l)
	}
}

// The head does not go back: a header older than the head, or the head again,
// changes nothing and costs nothing.
func TestOlderAnnouncementIsIgnored(t *testing.T) {
	n, a, w := listeningNode(t)
	n.add(100)
	n.setHead(100)
	serve(t, w)
	eventually(t, "the first read never gave a head", headIs(w, 100))
	eventually(t, "the watcher never subscribed", func() bool { return a.subscriptions() == 1 })

	n.add(101)
	n.add(102)
	a.announce(headOf(n.blocks[101]))
	a.announce(headOf(n.blocks[102]))
	eventually(t, "an announced block never became the head", headIs(w, 102))
	before := n.count(callHeader) + n.count(callByHash)

	a.announce(headOf(n.blocks[101]))
	a.announce(headOf(n.blocks[102]))
	n.add(103)
	a.announce(headOf(n.blocks[103]))
	eventually(t, "an announced block never became the head", headIs(w, 103))

	if got := n.count(callHeader) + n.count(callByHash) - before; got != 1 {
		t.Fatalf("two stale headers and a new one cost %d requests, want 1", got)
	}
}

// An announcement far ahead of the head is not believed: the head would stay
// there, the head does not go back.
func TestAnnouncementFarAheadIsIgnored(t *testing.T) {
	n, a, w := listeningNode(t)
	n.add(100)
	n.setHead(100)
	serve(t, w)
	eventually(t, "the first read never gave a head", headIs(w, 100))
	eventually(t, "the watcher never subscribed", func() bool { return a.subscriptions() == 1 })

	a.announce(&blockHead{Number: 100 + hexutil.Uint64(summaryMaxGap) + 1, Hash: hashOf(999, 0), ParentHash: hashOf(998, 0)})
	n.add(101)
	a.announce(headOf(n.blocks[101]))
	eventually(t, "the block after a wild announcement never became the head", headIs(w, 101))
}

// A young block the provider cannot give the logs of yet is not made the head:
// sources sent to read it now could get the same empty answer. It is the head
// once it can be read.
func TestYoungUnreadableBlockIsHeldBack(t *testing.T) {
	n, a, w := listeningNode(t)
	n.add(100)
	n.setHead(100)
	var behind atomic.Bool
	behind.Store(true)
	n.byHash = func(number uint64, logs []ethTypes.Log) ([]ethTypes.Log, error) {
		if behind.Load() {
			return nil, errors.New("unknown block")
		}
		return logs, nil
	}
	serve(t, w)
	eventually(t, "the first read never gave a head", headIs(w, 100))
	eventually(t, "the watcher never subscribed", func() bool { return a.subscriptions() == 1 })

	n.add(101, addrA)
	a.announce(headOf(n.blocks[101]))
	eventually(t, "the block was never asked for", func() bool { return n.count(callByHash) >= summaryAttempts })
	time.Sleep(50 * time.Millisecond)
	if got, _ := w.Head(); got != 100 {
		t.Fatalf("head = %d: a block that cannot be read yet must not be the head", got)
	}

	behind.Store(false)
	a.announce(headOf(n.blocks[101]))
	eventually(t, "the block never became the head once readable", headIs(w, 101))
	if w.Quiet(101, 101, addrA) || !w.Quiet(101, 101, addrAbsent) {
		t.Fatal("the block must be summarised when it becomes the head")
	}
}

// An old block that cannot be read is the head all the same, without a
// summary: the sources read it themselves, as they always did.
func TestOldUnreadableBlockIsNotHeldBack(t *testing.T) {
	n, a, w := listeningNode(t)
	n.add(100)
	n.setHead(100)
	n.byHash = func(uint64, []ethTypes.Log) ([]ethTypes.Log, error) { return nil, errors.New("unknown block") }
	serve(t, w)
	eventually(t, "the first read never gave a head", headIs(w, 100))
	eventually(t, "the watcher never subscribed", func() bool { return a.subscriptions() == 1 })

	n.add(101, addrA)
	old := headOf(n.blocks[101])
	old.Timestamp = hexutil.Uint64(time.Now().Add(-2 * time.Hour).Unix())
	a.announce(old)
	eventually(t, "an old block must be the head even without a summary", headIs(w, 101))
	if w.Quiet(101, 101, addrAbsent) {
		t.Fatal("a block that was not read has no summary")
	}
}

// Another block at the height of the head: its summary describes a block that
// is gone and is dropped.
func TestReplacedHeadLosesItsSummary(t *testing.T) {
	n, a, w := listeningNode(t)
	n.add(100)
	n.setHead(100)
	serve(t, w)
	eventually(t, "the first read never gave a head", headIs(w, 100))
	eventually(t, "the watcher never subscribed", func() bool { return a.subscriptions() == 1 })

	n.add(101, addrA)
	a.announce(headOf(n.blocks[101]))
	eventually(t, "an announced block never became the head", headIs(w, 101))
	if !w.Quiet(101, 101, addrAbsent) {
		t.Fatal("the head must be summarised")
	}

	n.addOn(101, 1, addrB)
	a.announce(headOf(n.blocks[101]))
	eventually(t, "the summary of a replaced block was kept", func() bool { return !w.Quiet(101, 101, addrAbsent) })
	if got, _ := w.Head(); got != 101 {
		t.Fatalf("head = %d, want 101", got)
	}
}

// Without a subscription the periodic read finds the blocks, as it always did.
func TestReadCarriesOnWithoutSubscription(t *testing.T) {
	n := newChainNode(t) // no WebSocket side
	n.add(100)
	n.setHead(100)
	w := NewHeadWatcher(evmi_database.EvmBlockchain{RpcUrl: n.srv.URL, BlockSlice: 1, PullInterval: 1}, nil, zerolog.Nop())
	w.interval = 10 * time.Millisecond
	serve(t, w)
	eventually(t, "the first read never gave a head", headIs(w, 100))

	n.add(101)
	n.setHead(101)
	eventually(t, "the periodic read never found the new block", headIs(w, 101))
}

// A subscription the node closes is opened again, and announcements are
// taken again.
func TestSubscriptionIsOpenedAgainAfterItBreaks(t *testing.T) {
	shrink(t, &headReopen, 10*time.Millisecond)
	n, a, w := listeningNode(t)
	n.add(100)
	n.setHead(100)
	serve(t, w)
	eventually(t, "the first read never gave a head", headIs(w, 100))
	eventually(t, "the watcher never subscribed", func() bool { return a.subscriptions() == 1 })

	a.drop()
	a.start(t)
	eventually(t, "the subscription was not opened again", func() bool { return a.subscriptions() == 2 })

	n.add(101)
	a.announce(headOf(n.blocks[101]))
	eventually(t, "a block announced on the new subscription never became the head", headIs(w, 101))
}

// A subscription that stays open but announces nothing new is opened again:
// the blocks keep coming from the periodic read meanwhile.
func TestSilentSubscriptionIsOpenedAgain(t *testing.T) {
	shrink(t, &headReopen, 10*time.Millisecond)
	shrink(t, &headQuietMin, 60*time.Millisecond)
	n, a, w := listeningNode(t)
	w.interval = 10 * time.Millisecond
	n.add(100)
	n.setHead(100)
	serve(t, w)
	eventually(t, "the first read never gave a head", headIs(w, 100))
	eventually(t, "the watcher never subscribed", func() bool { return a.subscriptions() >= 1 })

	// Headers the periodic read already had are not news.
	stale := headOf(n.blocks[100])
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(10 * time.Millisecond):
				a.announce(stale)
			}
		}
	}()
	eventually(t, "a subscription with nothing new was kept", func() bool { return a.subscriptions() >= 2 })

	n.add(101)
	n.setHead(101)
	eventually(t, "the periodic read never found the new block", headIs(w, 101))
}

// --- when the periodic read asks --------------------------------------------

func TestNextRead(t *testing.T) {
	const interval = 12 * time.Second
	blockTime := time.Unix(1_800_000_000, 0)
	w := NewHeadWatcher(evmi_database.EvmBlockchain{PullInterval: 12}, nil, zerolog.Nop())

	// Nothing known yet: one interval.
	if got := w.nextRead(blockTime); got != interval {
		t.Fatalf("with no head known, next read in %s, want %s", got, interval)
	}

	w.latest = &blockHead{Number: 1, Timestamp: hexutil.Uint64(blockTime.Unix())}
	for _, c := range []struct {
		name  string
		after time.Duration // since the block's own timestamp
		want  time.Duration
	}{
		{"just learned, a second into the slot", 1 * time.Second, 14 * time.Second},
		{"learned late in the slot", 9 * time.Second, 6 * time.Second},
		{"the next block is due now", 15 * time.Second, 2 * time.Second},
		{"still missing two seconds later", 17 * time.Second, 2 * time.Second},
		{"still missing four seconds later: wait for the next one", 19 * time.Second, 8 * time.Second},
		{"an empty slot: the block after is due", 27 * time.Second, 2 * time.Second},
		{"deep into a missed slot", 34 * time.Second, 5 * time.Second},
		{"a minute without a block: one read per interval", 15*time.Second + 5*interval, interval},
		{"a minute without a block, mid interval", 16*time.Second + 5*interval, 11 * time.Second},
		{"a block dated a little ahead of this clock", -20 * time.Second, 15 * time.Second},
		{"a block dated far ahead of this clock", -10 * time.Minute, interval},
	} {
		if got := w.nextRead(blockTime.Add(c.after)); got != c.want {
			t.Errorf("%s: next read in %s, want %s", c.name, got, c.want)
		}
	}

	// Whatever the timestamp, the read never spins and never sleeps past one
	// interval and its margin.
	for _, stamp := range []uint64{
		0, 1, uint64(blockTime.Unix()) - 1_000_000, uint64(blockTime.Unix()) + 1_000_000,
		uint64(blockTime.UnixMilli()), 1 << 62, 1<<63 + 5, 1<<64 - 1, // not seconds at all
	} {
		w.latest.Timestamp = hexutil.Uint64(stamp)
		for after := time.Duration(0); after < 3*interval; after += 700 * time.Millisecond {
			if got := w.nextRead(blockTime.Add(after)); got <= 0 || got > interval+headMarginMax {
				t.Fatalf("timestamp %d, %s on: next read in %s", stamp, after, got)
			}
		}
	}
}

// --- what is logged ---------------------------------------------------------

func TestScrubKeepsTheNodeURLOut(t *testing.T) {
	const node = "https://eth.example/v2/s3cr3t-key"
	for _, msg := range []string{
		`Post "https://eth.example/v2/s3cr3t-key": context deadline exceeded`,
		`dial wss://eth.example/v2/s3cr3t-key: bad handshake`,
		`GET /v2/s3cr3t-key: 401`,
	} {
		for _, url := range []string{node, strings.Replace(node, "https", "wss", 1)} {
			if got := scrub(errors.New(msg), url); strings.Contains(got, "s3cr3t-key") {
				t.Errorf("scrub(%q) = %q: the key is still there", msg, got)
			}
		}
	}
	if scrub(nil, node) != "" {
		t.Error("no error, nothing to say")
	}
}
