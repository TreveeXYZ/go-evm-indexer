package indexer

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	evmi_database "github.com/evmi-cloud/go-evm-indexer/internal/database/evmi-database"
	"github.com/evmi-cloud/go-evm-indexer/internal/metrics"
	"github.com/lmittmann/w3"
	"github.com/lmittmann/w3/module/eth"
	"github.com/rs/zerolog"
)

const (
	// summaryWindow is how many recent block summaries are kept. A source
	// further behind than this reads its range itself.
	summaryWindow uint64 = 64
	// summaryMaxGap is the largest head jump summarised block by block. Past
	// it (first read, long RPC outage) the sources read their ranges
	// themselves, which costs fewer requests than one summary per block.
	summaryMaxGap uint64 = 16
	// summaryAttempts is how many times one block is tried: right after a new
	// head, a load-balanced provider may answer from a node that does not
	// have the block yet.
	summaryAttempts = 3
	// summaryRetryPause separates two attempts on the same block.
	summaryRetryPause = 250 * time.Millisecond
	// summaryRoundTimeout bounds the summaries of one head change, so a slow
	// node delays the new head by this much at most.
	summaryRoundTimeout = 5 * time.Second
	// summariesEnv set to "0" turns block summaries off: every source then
	// reads its own logs, the behaviour before summaries existed.
	summariesEnv = "EVMI_HEAD_SUMMARIES"
)

// How the periodic read is timed. These are variables so a test can shrink
// them.
var (
	// headMarginMax caps the time a block is given to reach the provider
	// before the periodic read asks for it: a quarter of the interval, this
	// at most.
	headMarginMax = 3 * time.Second
	// headRetryMax caps the pause between two reads while a block that is due
	// is not there yet: a sixth of the interval, this at most.
	headRetryMax = 2 * time.Second
	// headPatience is how many intervals in a row a missing block is asked
	// for more than once. Past it the chain or the node is stuck, and one
	// read per interval is enough to see it come back.
	headPatience time.Duration = 5
	// headReportEvery is how often the watcher logs what it did.
	headReportEvery = 10 * time.Minute
)

// blockSummary is what one block holds: its hash and the set of addresses
// that emitted at least one log in it.
type blockSummary struct {
	hash     common.Hash
	emitters map[common.Address]struct{}
}

// HeadWatcher follows the chain head on behalf of every source of a
// blockchain. Without it each source polled eth_blockNumber on its own: N
// sources meant N identical requests per interval for one answer.
//
// It learns of a new block in two ways. The node announces it (a newHeads
// subscription over WebSocket, see head_listen.go), which is the quick way.
// And it reads the latest block itself once per PullInterval, timed on the
// last block's own timestamp so the read lands just after the next block is
// due rather than anywhere in the interval. The read is what remains when the
// node offers no subscription or the subscription is down.
//
// When enough sources share it, it also reads every new block's logs once
// (one unfiltered eth_getLogs by block hash) and keeps who emitted in it, so a
// CONTRACT or FACTORY source with nothing in a block moves its cursor without
// a request of its own. A block whose summary could not be read or verified
// has none: its sources read it themselves, exactly as without the watcher.
//
// It is a suture service owned by the IndexerService, which shares one
// watcher per blockchain among the sources of that chain and retires it when
// the last source is removed.
type HeadWatcher struct {
	chain    evmi_database.EvmBlockchain
	interval time.Duration
	metrics  *metrics.MetricService
	logger   zerolog.Logger

	// head is the latest block number; 0 until the first successful read.
	head atomic.Uint64
	// refs counts the sources relying on this watcher.
	refs atomic.Int32

	// summariesOff is the summariesEnv kill switch, read once at creation.
	summariesOff bool
	// next is the first block not summarised yet; 0 until the first head
	// read. Only the Serve goroutine touches it.
	next uint64
	// latest is the header of the head, as the node last gave it; nil until
	// the first one. Only the Serve goroutine touches it.
	latest *blockHead
	// stats is what the next report says. Only the Serve goroutine touches it.
	stats headStats
	// opens counts the subscriptions the listener opened.
	opens atomic.Int64

	// mu guards summaries and changed.
	mu        sync.Mutex
	summaries map[uint64]blockSummary
	// changed is closed, then replaced, each time the head moves.
	changed chan struct{}
}

func NewHeadWatcher(chain evmi_database.EvmBlockchain, m *metrics.MetricService, logger zerolog.Logger) *HeadWatcher {
	interval := time.Duration(chain.PullInterval) * time.Second
	if interval <= 0 {
		interval = time.Second
	}
	return &HeadWatcher{
		chain:        chain,
		interval:     interval,
		metrics:      m,
		logger:       logger,
		summariesOff: os.Getenv(summariesEnv) == "0",
		summaries:    map[uint64]blockSummary{},
		changed:      make(chan struct{}),
	}
}

// Head returns the latest known block number and whether one is known yet.
func (h *HeadWatcher) Head() (uint64, bool) {
	n := h.head.Load()
	return n, n > 0
}

// Wait returns when the head differs from seen, the head the caller last
// acted on, or after one poll interval at the latest. It returns the context
// error when ctx is canceled first.
func (h *HeadWatcher) Wait(ctx context.Context, seen uint64) error {
	// The channel is taken before the head is read: a head published in
	// between closes this very channel, so the change cannot be missed.
	h.mu.Lock()
	changed := h.changed
	h.mu.Unlock()

	if n := h.head.Load(); n != 0 && n != seen {
		return nil
	}

	timer := time.NewTimer(h.interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-changed:
		return nil
	case <-timer.C:
		return nil
	}
}

// Quiet reports whether addr is known to have emitted no log in any block of
// [from, to]. It answers false as soon as one block has no summary: the
// caller then reads the range itself.
func (h *HeadWatcher) Quiet(from, to uint64, addr common.Address) bool {
	if from > to || to-from >= summaryWindow {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for b := from; ; b++ {
		s, ok := h.summaries[b]
		if !ok {
			return false
		}
		if _, emitted := s.emitters[addr]; emitted {
			return false
		}
		if b == to {
			return true
		}
	}
}

// EmittedIn lists the summarised blocks of [from, to] in which addr emitted
// at least one log.
func (h *HeadWatcher) EmittedIn(from, to uint64, addr common.Address) []uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	var blocks []uint64
	for b, s := range h.summaries {
		if b < from || b > to {
			continue
		}
		if _, emitted := s.emitters[addr]; emitted {
			blocks = append(blocks, b)
		}
	}
	return blocks
}

// Forget drops the summary of one block, so its sources read it themselves.
func (h *HeadWatcher) Forget(block uint64) {
	h.mu.Lock()
	delete(h.summaries, block)
	h.mu.Unlock()
}

// Serve follows the head until ctx is canceled. A failed read keeps the last
// known head: sources simply see no progress until the next block is learned.
func (h *HeadWatcher) Serve(ctx context.Context) error {
	client, err := w3.Dial(h.chain.RpcUrl)
	if err != nil {
		return err
	}
	defer client.Close()

	// The listener lives as long as this call.
	announced := make(chan *blockHead, headBacklog)
	var listener sync.WaitGroup
	defer listener.Wait()
	lctx, stop := context.WithCancel(ctx)
	defer stop()
	if wsURL, ok := listenURL(h.chain.RpcUrl); ok {
		listener.Add(1)
		go func() {
			defer listener.Done()
			h.listen(lctx, wsURL, announced)
		}()
	}

	h.stats = headStats{since: time.Now()}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case head := <-announced:
			h.take(ctx, client, head, true)
		case <-timer.C:
			if head := h.readLatest(ctx, client); head != nil {
				h.take(ctx, client, head, false)
			}
		}
		h.report(time.Now())
		timer.Reset(h.nextRead(time.Now()))
	}
}

// readLatest asks the node for its latest block header, nil when it fails.
func (h *HeadWatcher) readLatest(ctx context.Context, client *w3.Client) *blockHead {
	// Bounded: a node that hangs must not hold the announced blocks back.
	rctx, cancel := context.WithTimeout(ctx, summaryRoundTimeout)
	defer cancel()
	head := new(blockHead)
	start := time.Now()
	err := client.CallCtx(rctx, headerCall{latest: true, out: head})
	h.recordRPC("eth_getBlockByNumber", start, err)
	h.stats.reads++
	if err != nil {
		if ctx.Err() == nil {
			h.logger.Warn().Str("error", scrub(err, h.chain.RpcUrl)).Msg("head watcher: reading the latest block failed")
		}
		return nil
	}
	return head
}

// take makes head the chain head: its summary first, then the sources are
// woken. A head already known changes nothing, and neither does an older one
// (a node behind the one that spoke last): the head does not go back.
func (h *HeadWatcher) take(ctx context.Context, client *w3.Client, head *blockHead, announced bool) {
	n := uint64(head.Number)
	known := h.head.Load()
	if n == known && h.latest != nil && head.Hash != h.latest.Hash {
		h.replaced(head)
		return
	}
	if n <= known {
		return
	}
	// An announcement far ahead of the head is not believed on its own: the
	// periodic read says where the chain is.
	if announced && known != 0 && n-known > summaryMaxGap {
		return
	}

	prev := h.latest
	h.latest = head
	// Summaries first: a source woken by the new head must find them.
	failed, err := h.summarise(ctx, client, n)
	now := time.Now()
	if err != nil && ctx.Err() == nil {
		// Right after a block, a load-balanced provider may answer from a
		// node that does not have it yet. Sources sent to read it now could
		// get that same empty answer, and nothing would tell them. The head
		// is held back instead and tried again at the next read, for as long
		// as the block is young.
		if age := now.Sub(time.Unix(int64(head.Timestamp), 0)); age >= 0 && age < h.interval/2 {
			h.next, h.latest = failed, prev
			h.stats.held++
			h.logger.Debug().Uint64("block", failed).Uint64("head", n).Str("error", scrub(err, h.chain.RpcUrl)).
				Msg("head watcher: block not readable yet, head held back")
			return
		}
		h.logger.Warn().Str("error", scrub(err, h.chain.RpcUrl)).Uint64("block", failed).Uint64("head", n).
			Msg("head watcher: no block summary, sources read these blocks themselves")
	}
	h.publish(n)
	h.stats.count(head, announced, now)
	h.logger.Debug().Uint64("block", n).Bool("announced", announced).
		Int64("after_block_ms", now.Sub(time.Unix(int64(head.Timestamp), 0)).Milliseconds()).
		Msg("head watcher: new head")
}

// replaced is called when the node gives another block at the height of the
// head: the chain reorganised and the block the sources were told about is
// gone. Its summary is dropped, so a source that has not read that height yet
// reads it for itself. A source that already has is not sent back: the
// indexer does not undo what it stored (true before this was ever noticed
// here). The line below is how often that happens.
func (h *HeadWatcher) replaced(head *blockHead) {
	n := uint64(head.Number)
	h.logger.Warn().Uint64("block", n).
		Str("was", h.latest.Hash.Hex()).Str("now", head.Hash.Hex()).
		Msg("head watcher: the head block was replaced, logs already indexed from it are not undone")
	h.Forget(n)
	h.latest = head
	h.stats.replaced++
}

// nextRead is how long the periodic read waits. A block is due one interval
// after the last one, and is given a margin to reach the provider. One that
// is not there then is asked for again shortly, a few times, then at the
// next time a block is due.
//
// PullInterval is meant to be the chain's block time. Set shorter, the read
// asks a few times per block; set longer, it reads once per interval as it
// always did.
func (h *HeadWatcher) nextRead(now time.Time) time.Duration {
	if h.latest == nil {
		return h.interval
	}
	margin := min(h.interval/4, headMarginMax)
	// A timestamp that is no time at all (not in seconds, or far from now)
	// times nothing: one read per interval.
	stamp := h.latest.Timestamp
	if stamp == 0 || uint64(stamp) > uint64(now.Unix())+uint64(h.interval/time.Second)+60 {
		return h.interval
	}
	due := time.Unix(int64(stamp), 0).Add(h.interval + margin)
	late := now.Sub(due)
	if late < 0 {
		return min(-late, h.interval+margin)
	}
	// into is how far past the latest time a block was due.
	into := late % h.interval
	if late < headPatience*h.interval && into < h.interval/3 {
		if retry := min(h.interval/6, headRetryMax); retry > 0 {
			return retry
		}
	}
	return h.interval - into
}

// publish records the new head and wakes the sources waiting for it.
func (h *HeadWatcher) publish(n uint64) {
	prev := h.head.Swap(n)
	if h.metrics != nil {
		h.metrics.SetChainHead(h.chain.ChainId, n)
	}
	if prev == n {
		return
	}
	h.mu.Lock()
	close(h.changed)
	h.changed = make(chan struct{})
	h.mu.Unlock()
}

// wantSummaries reports whether reading every block once is cheaper than
// letting each source read its own slice: two requests per block (header and
// logs) against one request per source every BlockSlice blocks.
func (h *HeadWatcher) wantSummaries() bool {
	if h.summariesOff {
		return false
	}
	refs := h.refs.Load()
	return refs > 0 && uint64(refs) > 2*h.chain.BlockSlice
}

// summarise reads the summary of every block from the last one summarised up
// to n. It stops at the first block that cannot be read or verified: that
// block and the following ones up to n get no summary, and it is returned
// with the error.
func (h *HeadWatcher) summarise(ctx context.Context, client *w3.Client, n uint64) (failed uint64, err error) {
	from := h.next
	if n < from {
		return 0, nil // a lagging node reported an older head
	}
	h.next = n + 1
	if from == 0 || !h.wantSummaries() || n-from >= summaryMaxGap {
		return 0, nil
	}

	roundCtx, cancel := context.WithTimeout(ctx, summaryRoundTimeout)
	defer cancel()
	for b := from; b <= n; b++ {
		s, parent, err := h.readSummary(roundCtx, client, b)
		if err != nil {
			return b, err
		}
		h.store(b, s, parent)
	}
	return 0, nil
}

// store keeps the summary of block b and prunes the ones out of the window.
// A summary whose parent is not the block summarised just before means the
// chain reorganised: the older summaries describe blocks that no longer
// exist and are dropped.
func (h *HeadWatcher) store(b uint64, s blockSummary, parent common.Hash) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if prev, ok := h.summaries[b-1]; ok && prev.hash != parent {
		h.logger.Warn().Uint64("block", b).
			Msg("head watcher: reorg detected, older block summaries dropped")
		h.summaries = map[uint64]blockSummary{}
	}
	for old := range h.summaries {
		if old+summaryWindow <= b {
			delete(h.summaries, old)
		}
	}
	h.summaries[b] = s
}

// readSummary tries a block summaryAttempts times.
func (h *HeadWatcher) readSummary(ctx context.Context, client *w3.Client, b uint64) (blockSummary, common.Hash, error) {
	var lastErr error
	for attempt := 0; attempt < summaryAttempts; attempt++ {
		if attempt > 0 {
			pause := time.NewTimer(summaryRetryPause)
			select {
			case <-ctx.Done():
				pause.Stop()
				return blockSummary{}, common.Hash{}, ctx.Err()
			case <-pause.C:
			}
		}
		s, parent, err := h.readSummaryOnce(ctx, client, b)
		if err == nil {
			return s, parent, nil
		}
		lastErr = err
	}
	return blockSummary{}, common.Hash{}, lastErr
}

// readSummaryOnce takes block b's header, then reads all its logs by block hash,
// and checks the logs against the header before trusting them: every log
// must belong to that block, and the bloom filter rebuilt from the logs must
// be the one in the header. An answer that is empty or truncated for a block
// that has logs fails that check.
func (h *HeadWatcher) readSummaryOnce(ctx context.Context, client *w3.Client, b uint64) (blockSummary, common.Hash, error) {
	// The head's header is already here: the node gave it with the head.
	head := h.latest
	if head == nil || uint64(head.Number) != b {
		head = new(blockHead)
		start := time.Now()
		err := client.CallCtx(ctx, headerCall{number: b, out: head})
		h.recordRPC("eth_getBlockByNumber", start, err)
		if err != nil {
			return blockSummary{}, common.Hash{}, err
		}
		if uint64(head.Number) != b {
			return blockSummary{}, common.Hash{}, fmt.Errorf("asked for block %d, got block %d", b, uint64(head.Number))
		}
	}

	var logs []ethTypes.Log
	hash := head.Hash
	start := time.Now()
	err := client.CallCtx(ctx, eth.Logs(ethereum.FilterQuery{BlockHash: &hash}).Returns(&logs))
	h.recordRPC("eth_getLogs", start, err)
	if err != nil {
		return blockSummary{}, common.Hash{}, err
	}

	var bloom ethTypes.Bloom
	emitters := make(map[common.Address]struct{})
	for i := range logs {
		l := &logs[i]
		if l.BlockHash != hash || l.BlockNumber != b || l.Removed {
			return blockSummary{}, common.Hash{}, fmt.Errorf("block %d: a returned log belongs to another block", b)
		}
		bloom.Add(l.Address.Bytes())
		for _, topic := range l.Topics {
			bloom.Add(topic.Bytes())
		}
		emitters[l.Address] = struct{}{}
	}
	if bloom != head.Bloom {
		return blockSummary{}, common.Hash{}, fmt.Errorf("block %d: returned logs do not match the header bloom", b)
	}

	h.logger.Debug().Uint64("block", b).Int("logs", len(logs)).Int("emitters", len(emitters)).
		Msg("head watcher: block summary")
	return blockSummary{hash: hash, emitters: emitters}, head.ParentHash, nil
}

func (h *HeadWatcher) recordRPC(method string, start time.Time, err error) {
	if h.metrics != nil {
		h.metrics.RecordRPC(h.chain.ChainId, method, time.Since(start), err)
	}
}

// blockHead is the part of a block header a summary needs. The hash is taken
// as the node gives it, not recomputed from the header fields: a network
// upgrade that adds a header field cannot make it wrong.
type blockHead struct {
	Number     hexutil.Uint64 `json:"number"`
	Hash       common.Hash    `json:"hash"`
	ParentHash common.Hash    `json:"parentHash"`
	Bloom      ethTypes.Bloom `json:"logsBloom"`
	Timestamp  hexutil.Uint64 `json:"timestamp"`
}

// headerCall is eth_getBlockByNumber without the transactions, for one block
// number or for the latest block.
type headerCall struct {
	number uint64
	latest bool
	out    *blockHead
}

func (c headerCall) CreateRequest() (rpc.BatchElem, error) {
	tag := hexutil.EncodeUint64(c.number)
	if c.latest {
		tag = "latest"
	}
	return rpc.BatchElem{
		Method: "eth_getBlockByNumber",
		Args:   []any{tag, false},
		Result: c.out,
	}, nil
}

func (c headerCall) HandleResponse(elem rpc.BatchElem) error {
	if elem.Error != nil {
		return elem.Error
	}
	if c.out.Hash == (common.Hash{}) {
		return errors.New("block not found")
	}
	return nil
}
