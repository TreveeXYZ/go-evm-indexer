package logsafe

import (
	"bytes"
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/rs/zerolog"
)

const key = "s3cr3tKEY_abc-123"

func TestCleanCutsWhatCarriesASecret(t *testing.T) {
	for in, want := range map[string]string{
		// The line production wrote.
		`WRN head.go:212 > head watcher: eth_blockNumber failed error="Post \"https://eth-mainnet.g.alchemy.com/v2/` + key + `\": read tcp 169.254.8.1:56404->57.129.88.148:443: read: connection reset by peer"`: `WRN head.go:212 > head watcher: eth_blockNumber failed error="Post \"https://eth-mainnet.g.alchemy.com/…\": read tcp 169.254.8.1:56404->57.129.88.148:443: read: connection reset by peer"`,
		`dial wss://eth-mainnet.g.alchemy.com/v2/` + key + `: bad handshake`:       `dial wss://eth-mainnet.g.alchemy.com/… bad handshake`,
		`GET https://rpc.example/?apikey=` + key + ` failed`:                       `GET https://rpc.example/… failed`,
		`GET https://rpc.example?apikey=` + key + ` failed`:                        `GET https://rpc.example?… failed`,
		`postgres://indexer:` + key + `@10.0.0.3:5432/evmi?sslmode=disable`:        `postgres://…@10.0.0.3:5432/…`,
		`connect failed: host=10.0.0.3 user=evmi password=` + key + ` dbname=evmi`: `connect failed: host=10.0.0.3 user=evmi password=… dbname=evmi`,
		`two: http://a.example/x/` + key + ` and ws://b.example/y/` + key:          `two: http://a.example/… and ws://b.example/…`,
		"\x1b[36merror=\x1b[0m\x1b[31mhttps://rpc.example/v2/" + key + "\x1b[0m":   "\x1b[36merror=\x1b[0m\x1b[31mhttps://rpc.example/…\x1b[0m",
	} {
		got := string(Clean([]byte(in)))
		if got != want {
			t.Errorf("Clean(%q)\n got %q\nwant %q", in, got, want)
		}
		if strings.Contains(got, key) {
			t.Errorf("Clean(%q) still holds the secret: %q", in, got)
		}
	}
}

func TestCleanLeavesTheRestAlone(t *testing.T) {
	for _, in := range []string{
		`Fetch logs fromBlock=26441689 toBlock=26441689`,
		`listening on http://127.0.0.1:8545`,
		`listening on http://127.0.0.1:8545/`,
		`../src/internal/indexer/head.go:362 > head watcher: block summary block=1 emitters=218`,
		`a ratio 3:4 and a path /v2/thing with no scheme`,
		``,
	} {
		if got := string(Clean([]byte(in))); got != in {
			t.Errorf("Clean(%q) = %q, want it unchanged", in, got)
		}
	}
}

// What the indexer really logs: an HTTP client's error, through zerolog's
// console writer, on the writer every logger is given.
func TestWriterKeepsTheNodeKeyOutOfALoggedError(t *testing.T) {
	var out bytes.Buffer
	logger := zerolog.New(zerolog.ConsoleWriter{Out: Writer(&out), NoColor: false}).With().Timestamp().Logger()
	err := &url.Error{Op: "Post", URL: "https://eth-mainnet.g.alchemy.com/v2/" + key, Err: errors.New("context deadline exceeded")}

	logger.Warn().Err(err).Msg("head watcher: reading the latest block failed")
	logger.Error().Str("source", "0xabc").Msg(err.Error())

	if strings.Contains(out.String(), key) {
		t.Fatalf("the key was logged:\n%s", out.String())
	}
	if n := strings.Count(out.String(), "https://eth-mainnet.g.alchemy.com/…"); n != 2 {
		t.Fatalf("the host must stay readable in both lines, got %d:\n%s", n, out.String())
	}
}

func TestWriterReportsTheWholeWrite(t *testing.T) {
	var out bytes.Buffer
	line := []byte("x https://rpc.example/v2/" + key + "\n")
	n, err := Writer(&out).Write(line)
	if err != nil || n != len(line) {
		t.Fatalf("Write = %d, %v; want %d, nil", n, err, len(line))
	}
}
