package indexer

import (
	"context"
	"errors"
	"fmt"
	"math/big"
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

// blockSummary is what one block holds: its hash and the set of addresses
// that emitted at least one log in it.
type blockSummary struct {
	hash     common.Hash
	emitters map[common.Address]struct{}
}

// HeadWatcher polls the chain head once per PullInterval on behalf of every
// source of a blockchain. Without it each source polled eth_blockNumber on
// its own: N sources meant N identical requests per interval for one answer.
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

// Serve polls eth_blockNumber every interval until ctx is canceled. A failed
// read keeps the last known head: sources simply see no progress until the
// next successful read.
func (h *HeadWatcher) Serve(ctx context.Context) error {
	client, err := w3.Dial(h.chain.RpcUrl)
	if err != nil {
		return err
	}
	defer client.Close()

	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}

		var block *big.Int
		start := time.Now()
		err := client.Call(eth.BlockNumber().Returns(&block))
		if h.metrics != nil {
			h.metrics.RecordRPC(h.chain.ChainId, "eth_blockNumber", time.Since(start), err)
		}
		if err != nil {
			h.logger.Warn().Err(err).Msg("head watcher: eth_blockNumber failed")
		} else {
			// Summaries first: a source woken by the new head must find them.
			h.summarise(ctx, client, block.Uint64())
			h.publish(block.Uint64())
		}
		timer.Reset(h.interval)
	}
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
// block and the following ones up to n get no summary.
func (h *HeadWatcher) summarise(ctx context.Context, client *w3.Client, n uint64) {
	from := h.next
	if n < from {
		return // a lagging node reported an older head
	}
	h.next = n + 1
	if from == 0 || !h.wantSummaries() || n-from >= summaryMaxGap {
		return
	}

	roundCtx, cancel := context.WithTimeout(ctx, summaryRoundTimeout)
	defer cancel()
	for b := from; b <= n; b++ {
		s, parent, err := h.readSummary(roundCtx, client, b)
		if err != nil {
			if ctx.Err() == nil {
				h.logger.Warn().Err(err).Uint64("block", b).Uint64("head", n).
					Msg("head watcher: no block summary, sources read these blocks themselves")
			}
			return
		}
		h.store(b, s, parent)
	}
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

// readSummaryOnce reads block b's header, then all its logs by block hash,
// and checks the logs against the header before trusting them: every log
// must belong to that block, and the bloom filter rebuilt from the logs must
// be the one in the header. An answer that is empty or truncated for a block
// that has logs fails that check.
func (h *HeadWatcher) readSummaryOnce(ctx context.Context, client *w3.Client, b uint64) (blockSummary, common.Hash, error) {
	head := new(blockHead)
	start := time.Now()
	err := client.CallCtx(ctx, headerCall{number: b, out: head})
	h.recordRPC("eth_getBlockByNumber", start, err)
	if err != nil {
		return blockSummary{}, common.Hash{}, err
	}
	if uint64(head.Number) != b {
		return blockSummary{}, common.Hash{}, fmt.Errorf("asked for block %d, got block %d", b, uint64(head.Number))
	}

	var logs []ethTypes.Log
	hash := head.Hash
	start = time.Now()
	err = client.CallCtx(ctx, eth.Logs(ethereum.FilterQuery{BlockHash: &hash}).Returns(&logs))
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
}

// headerCall is eth_getBlockByNumber without the transactions.
type headerCall struct {
	number uint64
	out    *blockHead
}

func (c headerCall) CreateRequest() (rpc.BatchElem, error) {
	return rpc.BatchElem{
		Method: "eth_getBlockByNumber",
		Args:   []any{hexutil.EncodeUint64(c.number), false},
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
