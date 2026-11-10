package sql_store

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/evmi-cloud/go-evm-indexer/internal/types"
	"github.com/rs/zerolog"
)

func mkLog(sourceId uint, block, idx uint64) types.EvmLog {
	return types.EvmLog{
		Id: fmt.Sprintf("1:%d:%d", block, idx), SourceId: sourceId, ChainId: 1, Address: "0xabc",
		Topics: []string{"0xt0", "0xt1"}, Data: "beef", BlockNumber: block, BlockTimestamp: block * 1000, LogIndex: idx,
		Metadata: types.EvmMetadata{ContractName: "C", Data: map[string]string{"k": "v"}},
	}
}

func ids(logs []types.EvmLog) string {
	out := make([]string, len(logs))
	for i, l := range logs {
		out[i] = l.Id
	}
	return fmt.Sprint(out)
}

func newStore(t *testing.T) *SQLStore {
	t.Helper()
	s, _ := NewSQLStore("sqlite", zerolog.Nop())
	if err := s.Init(map[string]string{"dsn": filepath.Join(t.TempDir(), "test.db")}); err != nil {
		t.Fatalf("init: %v", err)
	}
	return s
}

func TestSQLLogsRoundTrip(t *testing.T) {
	s := newStore(t)

	logs := []types.EvmLog{mkLog(1, 10, 0), mkLog(1, 10, 1), mkLog(1, 12, 0), mkLog(2, 11, 0)}
	if err := s.InsertLogs(logs); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Re-insert the same logs: dedup on conflict, count must stay 4.
	if err := s.InsertLogs(logs); err != nil {
		t.Fatalf("re-insert: %v", err)
	}

	if c, err := s.GetLogsCount(); err != nil || c != 4 {
		t.Fatalf("count = %d, err %v (want 4)", c, err)
	}

	got, err := s.GetLogs(1, 10, 11)
	if err != nil {
		t.Fatal(err)
	}
	if ids(got) != fmt.Sprint([]string{"1:10:0", "1:10:1"}) {
		t.Errorf("GetLogs = %s", ids(got))
	}
	if len(got) > 0 && (len(got[0].Topics) != 2 || got[0].Metadata.Data["k"] != "v") {
		t.Errorf("complex fields not preserved: %+v", got[0])
	}
	if len(got) > 0 && got[0].BlockTimestamp != got[0].BlockNumber*1000 {
		t.Errorf("block_timestamp not round-tripped: got %d for block %d", got[0].BlockTimestamp, got[0].BlockNumber)
	}

	after, err := s.GetLogsAfter([]uint64{1, 2}, 10, 0, 12)
	if err != nil {
		t.Fatal(err)
	}
	if ids(after) != fmt.Sprint([]string{"1:10:1", "1:11:0", "1:12:0"}) {
		t.Errorf("GetLogsAfter = %s", ids(after))
	}

	latest, err := s.GetLatestLogs(1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if ids(latest) != fmt.Sprint([]string{"1:12:0", "1:10:1"}) {
		t.Errorf("GetLatestLogs = %s", ids(latest))
	}
}

// A cursor with no block in progress asks for "everything after afterBlock":
// the logs of afterBlock itself were already delivered and must not come back.
func TestSQLGetLogsAfterCompletedBlock(t *testing.T) {
	s := newStore(t)
	if err := s.InsertLogs([]types.EvmLog{mkLog(1, 10, 0), mkLog(1, 10, 1), mkLog(1, 11, 0), mkLog(1, 11, 2)}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	cases := []struct {
		name          string
		afterBlock    uint64
		afterLogIndex uint64
		want          []string
	}{
		{"block 10 complete", 10, ^uint64(0), []string{"1:11:0", "1:11:2"}},
		{"block 11 complete", 11, ^uint64(0), []string{}},
		{"nothing delivered yet", 9, ^uint64(0), []string{"1:10:0", "1:10:1", "1:11:0", "1:11:2"}},
		{"block 11 in progress", 11, 0, []string{"1:11:2"}},
	}
	for _, c := range cases {
		got, err := s.GetLogsAfter([]uint64{1}, c.afterBlock, c.afterLogIndex, 11)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if ids(got) != fmt.Sprint(c.want) {
			t.Errorf("%s: GetLogsAfter = %s, want %v", c.name, ids(got), c.want)
		}
	}
}

func TestSQLTransactionsRoundTrip(t *testing.T) {
	s := newStore(t)
	if err := s.InsertTransactions([]types.EvmTransaction{
		{Id: "1:tx", SourceId: 1, BlockNumber: 10, BlockTimestamp: 1700000000, ChainId: 1, From: "0xf", To: "0xt", Value: "5", Hash: "0xh"},
	}); err != nil {
		t.Fatalf("insert txs: %v", err)
	}
	txs, err := s.GetTransactions(1, 0, 100)
	if err != nil || len(txs) != 1 || txs[0].Value != "5" || txs[0].From != "0xf" || txs[0].To != "0xt" {
		t.Fatalf("GetTransactions = %+v, err %v", txs, err)
	}
	if txs[0].BlockTimestamp != 1700000000 {
		t.Errorf("block_timestamp not round-tripped: got %d", txs[0].BlockTimestamp)
	}
}

func TestSQLDeleteSourceData(t *testing.T) {
	s := newStore(t)
	// mkLog's Id derives from (block, idx) only, so keep block/idx distinct across
	// sources to avoid a primary-key collision that DoNothing would swallow.
	if err := s.InsertLogs([]types.EvmLog{mkLog(1, 10, 0), mkLog(1, 11, 0), mkLog(2, 20, 0)}); err != nil {
		t.Fatalf("insert logs: %v", err)
	}
	if err := s.InsertTransactions([]types.EvmTransaction{
		{Id: "1:tx", SourceId: 1, BlockNumber: 10, ChainId: 1, Hash: "0xh1"},
		{Id: "2:tx", SourceId: 2, BlockNumber: 10, ChainId: 1, Hash: "0xh2"},
	}); err != nil {
		t.Fatalf("insert txs: %v", err)
	}

	if err := s.DeleteSourceData(1); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Source 1's logs and transactions are gone.
	if logs, _ := s.GetLogs(1, 0, 100); len(logs) != 0 {
		t.Errorf("source 1 logs not deleted: %d", len(logs))
	}
	if txs, _ := s.GetTransactions(1, 0, 100); len(txs) != 0 {
		t.Errorf("source 1 txs not deleted: %d", len(txs))
	}
	// Source 2 is untouched.
	if logs, _ := s.GetLogs(2, 0, 100); len(logs) != 1 {
		t.Errorf("source 2 logs should remain, got %d", len(logs))
	}
	if txs, _ := s.GetTransactions(2, 0, 100); len(txs) != 1 {
		t.Errorf("source 2 txs should remain, got %d", len(txs))
	}
}
