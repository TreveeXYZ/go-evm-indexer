package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"
)

// Listening for new blocks: the node pushes each new header over a WebSocket
// (eth_subscribe newHeads) and the watcher takes it at once, instead of
// finding the block at its next read.
//
// The subscription only makes the head quicker. Nothing depends on it: the
// periodic read goes on beside it and supplies any block it did not announce,
// so a header that is lost or late delays the sources by a few seconds and no
// more. A subscription that breaks, or that brings nothing new for a while,
// is opened again.

// These are variables so a test can shrink them.
var (
	// headBacklog is how many announced headers may wait for Serve. One more
	// is dropped: the next header or the periodic read covers it.
	headBacklog = 16
	// headOpenTimeout bounds the connection and the subscription request.
	headOpenTimeout = 10 * time.Second
	// headReopen is the pause before the subscription is opened again after
	// it broke, doubled at each failure in a row up to headReopenMax.
	headReopen    = time.Second
	headReopenMax = time.Minute
	// headSteady is how long a subscription has to have lasted for its end to
	// count as a fresh failure rather than one more in a row.
	headSteady = time.Minute
	// headQuietMin is the least time a subscription may go without a new
	// block before it is opened again: five intervals, this at least.
	headQuietMin = 30 * time.Second
)

// listenURL is the WebSocket form of the node's URL: the same host and path,
// which is where providers serve subscriptions.
func listenURL(rpcURL string) (string, bool) {
	u, err := url.Parse(rpcURL)
	if err != nil || u.Host == "" {
		return "", false
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	case "ws", "wss":
	default:
		return "", false
	}
	return u.String(), true
}

// listen keeps the subscription open until ctx ends, and hands each announced
// header to out.
func (h *HeadWatcher) listen(ctx context.Context, wsURL string, out chan<- *blockHead) {
	pause := headReopen
	warned := false
	for ctx.Err() == nil {
		delivered, err := h.listenOnce(ctx, wsURL, out)
		if ctx.Err() != nil {
			return
		}
		// Only a subscription that brought blocks counts as having worked.
		// One that opened and stayed silent is one more failure in a row: it
		// is retried less and less often, and not logged again.
		if delivered {
			pause, warned = headReopen, false
		}
		if !warned {
			// Once per outage, not once per attempt.
			h.logger.Warn().Str("error", scrub(err, wsURL)).
				Msg("head watcher: not listening for new blocks, the periodic read carries on alone")
			warned = true
		}
		wait := time.NewTimer(pause)
		select {
		case <-ctx.Done():
			wait.Stop()
			return
		case <-wait.C:
		}
		pause = min(pause*2, headReopenMax)
	}
}

// listenOnce holds one subscription until it breaks, and always returns an
// error then. delivered says whether it announced at least one block the
// watcher did not have yet.
func (h *HeadWatcher) listenOnce(ctx context.Context, wsURL string, out chan<- *blockHead) (delivered bool, err error) {
	dctx, cancel := context.WithTimeout(ctx, headOpenTimeout)
	conn, err := rpc.DialContext(dctx, wsURL)
	cancel()
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
	}
	// Closing the connection ends the subscription with it. It is not
	// unsubscribed first: that is a request, and on a connection that went
	// silent it would wait for an answer.
	defer conn.Close()

	headers := make(chan json.RawMessage, headBacklog)
	sctx, cancel := context.WithTimeout(ctx, headOpenTimeout)
	sub, err := conn.EthSubscribe(sctx, headers, "newHeads")
	cancel()
	if err != nil {
		return false, fmt.Errorf("subscribe: %w", err)
	}
	h.opens.Add(1)

	quietFor := max(5*h.interval, headQuietMin)
	quiet := time.NewTimer(quietFor)
	defer quiet.Stop()
	for {
		select {
		case <-ctx.Done():
			return delivered, ctx.Err()
		case err := <-sub.Err():
			if err == nil {
				err = errors.New("closed by the node")
			}
			return delivered, err
		case raw := <-headers:
			head := new(blockHead)
			if json.Unmarshal(raw, head) != nil || head.Hash == (common.Hash{}) {
				continue // one unreadable header is not worth the subscription
			}
			// Only a block the watcher did not have yet says the subscription
			// is delivering. One the periodic read had first proves nothing:
			// counted as news, a subscription that is always late would
			// never be opened again.
			if uint64(head.Number) > h.head.Load() {
				quiet.Reset(quietFor)
				if !delivered {
					delivered = true
					h.logger.Info().Uint64("block", uint64(head.Number)).Msg("head watcher: listening for new blocks")
				}
			}
			select {
			case out <- head:
			default:
			}
		case <-quiet.C:
			return delivered, fmt.Errorf("no new block announced for %s", quietFor)
		}
	}
}

// scrub keeps the node's URL, which carries the provider key, out of an
// error.
func scrub(err error, nodeURL string) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	for _, secret := range []string{nodeURL, httpForm(nodeURL)} {
		if secret != "" {
			msg = strings.ReplaceAll(msg, secret, "<node>")
		}
	}
	if u, perr := url.Parse(nodeURL); perr == nil && len(u.Path) > 1 {
		msg = strings.ReplaceAll(msg, u.Path, "/<path>")
	}
	return msg
}

// httpForm is the http(s) form of a ws(s) URL, "" for any other.
func httpForm(nodeURL string) string {
	switch {
	case strings.HasPrefix(nodeURL, "wss://"):
		return "https://" + strings.TrimPrefix(nodeURL, "wss://")
	case strings.HasPrefix(nodeURL, "ws://"):
		return "http://" + strings.TrimPrefix(nodeURL, "ws://")
	}
	return ""
}

// headStats is what the watcher did since its last report.
type headStats struct {
	since time.Time
	// announced and read count the new heads by who brought them first: the
	// subscription or the periodic read.
	announced, read int
	// reads counts the periodic reads made, whatever they found.
	reads int
	// held counts the heads held back because their block was not readable
	// yet, replaced the head blocks the node replaced by another.
	held, replaced int
	// opens is the listener's count at the last report.
	opens int64
	// delays is, per new head, the time from the block's own timestamp to
	// the sources being woken.
	delays []time.Duration
}

func (s *headStats) count(head *blockHead, announced bool, wokenAt time.Time) {
	if announced {
		s.announced++
	} else {
		s.read++
	}
	// A timestamp that makes no sense as a delay (a test chain, a clock that
	// is off) is left out rather than averaged in.
	if d := wokenAt.Sub(time.Unix(int64(head.Timestamp), 0)); d >= 0 && d < 10*time.Minute {
		s.delays = append(s.delays, d)
	}
}

// report logs, every headReportEvery, how the head was followed: who brought
// the new blocks and how long after a block the sources were woken.
func (h *HeadWatcher) report(now time.Time) {
	s := &h.stats
	if now.Sub(s.since) < headReportEvery {
		return
	}
	opens := h.opens.Load()
	if s.announced+s.read > 0 || opens != s.opens {
		ev := h.logger.Info().
			Int("heads_announced", s.announced).
			Int("heads_from_the_read", s.read).
			Int("reads", s.reads).
			Int("heads_held_back", s.held).
			Int("heads_replaced", s.replaced).
			Int64("subscriptions_opened", opens-s.opens)
		if len(s.delays) > 0 {
			slices.Sort(s.delays)
			ev = ev.
				Int64("woken_after_ms_median", s.delays[len(s.delays)/2].Milliseconds()).
				Int64("woken_after_ms_max", s.delays[len(s.delays)-1].Milliseconds())
		}
		ev.Msg("head watcher: summary")
	}
	h.stats = headStats{since: now, opens: opens}
}
