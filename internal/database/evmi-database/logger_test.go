package evmi_database

import (
	"bytes"
	"strings"
	"testing"

	"github.com/evmi-cloud/go-evm-indexer/internal/logsafe"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// A failed statement is logged with its values. One that carries a node URL
// must not show the provider key.
func TestFailedStatementIsLoggedWithoutTheNodeKey(t *testing.T) {
	const key = "s3cr3tKEY_abc-123"
	nodeURL := "https://eth-mainnet.g.alchemy.com/v2/" + key

	var raw, safe bytes.Buffer
	for _, out := range []*bytes.Buffer{&raw, &safe} {
		w := logsafe.Writer(out)
		if out == &raw {
			w = out
		}
		db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: safeLogger(w)})
		if err != nil {
			t.Fatal(err)
		}
		// No such table: the statement fails and gorm logs it.
		if err := db.Exec("UPDATE no_such_table SET rpc_url = ? WHERE id = ?", nodeURL, 1).Error; err == nil {
			t.Fatal("the statement was expected to fail")
		}
	}

	if !strings.Contains(raw.String(), key) {
		t.Fatalf("the test proves nothing: gorm did not log the value\n%s", raw.String())
	}
	if strings.Contains(safe.String(), key) {
		t.Fatalf("the key was logged:\n%s", safe.String())
	}
	if !strings.Contains(safe.String(), "https://eth-mainnet.g.alchemy.com/…") {
		t.Fatalf("the statement must stay readable, host included:\n%s", safe.String())
	}
}
