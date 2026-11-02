// Package chaincheck refuses to start the indexer against a node that is not
// the chain the configuration describes. Every configured blockchain carries a
// chain id and an RPC URL; nothing else in the indexer compares the two, so a
// mainnet RPC behind a Sepolia configuration would index mainnet blocks and
// store them under the wrong chain id without a single error.
package chaincheck

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/evmi-cloud/go-evm-indexer/internal/types"
)

// Timeout bounds each eth_chainId call.
const Timeout = 10 * time.Second

// Verify asks every configured blockchain's node for its chain id and returns
// an error naming the first one that does not match its configuration, or that
// cannot be reached: an indexer that cannot read its node has nothing to do,
// and a configuration error must stop the boot rather than corrupt the stores.
// Blockchains without an RPC URL are skipped.
func Verify(ctx context.Context, chains []types.ConfigBlockchain) error {
	for _, c := range chains {
		if c.RpcUrl == "" {
			continue
		}
		got, err := chainID(ctx, c.RpcUrl)
		if err != nil {
			return fmt.Errorf("blockchain %q: cannot read the node's chain id: %w", c.Name, err)
		}
		if got != c.ChainId {
			return fmt.Errorf("blockchain %q is configured with chain id %d but its node answers %d", c.Name, c.ChainId, got)
		}
	}
	return nil
}

// chainID is a plain eth_chainId call: no client dependency, so the check runs
// before anything else is mounted.
func chainID(ctx context.Context, rpcURL string) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	body := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rpcURL, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP %d", res.StatusCode)
	}
	var out struct {
		Result string `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 1<<16)).Decode(&out); err != nil {
		return 0, err
	}
	if out.Error != nil {
		return 0, errors.New(out.Error.Message)
	}
	hex := strings.TrimPrefix(strings.ToLower(out.Result), "0x")
	if hex == "" {
		return 0, errors.New("empty eth_chainId result")
	}
	return strconv.ParseUint(hex, 16, 64)
}
