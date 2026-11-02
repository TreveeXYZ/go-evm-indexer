package chaincheck

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/evmi-cloud/go-evm-indexer/internal/types"
)

func node(t *testing.T, answer string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(answer))
	}))
}

func TestVerifyAcceptsAMatchingNode(t *testing.T) {
	srv := node(t, `{"jsonrpc":"2.0","id":1,"result":"0xaa36a7"}`) // 11155111
	defer srv.Close()
	err := Verify(context.Background(), []types.ConfigBlockchain{{Name: "sepolia", ChainId: 11155111, RpcUrl: srv.URL}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestVerifyRefusesAMismatch(t *testing.T) {
	srv := node(t, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`)
	defer srv.Close()
	err := Verify(context.Background(), []types.ConfigBlockchain{{Name: "sepolia", ChainId: 11155111, RpcUrl: srv.URL}})
	if err == nil || !strings.Contains(err.Error(), "configured with chain id 11155111 but its node answers 1") {
		t.Fatalf("expected a mismatch error, got %v", err)
	}
}

func TestVerifyRefusesAnUnreachableOrBrokenNode(t *testing.T) {
	srv := node(t, `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"}}`)
	defer srv.Close()
	if err := Verify(context.Background(), []types.ConfigBlockchain{{Name: "x", ChainId: 1, RpcUrl: srv.URL}}); err == nil {
		t.Fatal("expected an error on a JSON-RPC error")
	}
	if err := Verify(context.Background(), []types.ConfigBlockchain{{Name: "x", ChainId: 1, RpcUrl: "http://127.0.0.1:1"}}); err == nil {
		t.Fatal("expected an error on an unreachable node")
	}
}

func TestVerifySkipsBlockchainsWithoutRPC(t *testing.T) {
	if err := Verify(context.Background(), []types.ConfigBlockchain{{Name: "x", ChainId: 1}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
