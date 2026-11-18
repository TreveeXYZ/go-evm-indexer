// Package logsafe keeps secrets out of what the process logs.
//
// A node URL carries the provider's key in its path, and a database URL its
// password. Go's HTTP client puts the full URL in the error of any request
// that fails on the network (`Post "https://host/v2/<key>": …`), and that
// error is logged as it comes from dozens of places. Rather than trust every
// one of them to clean it, everything written to the log goes through
// Writer, which cuts every URL down to its scheme and host.
package logsafe

import (
	"bytes"
	"io"
	"os"
	"regexp"
)

// Stderr is where the process logs. Loggers write here, never to os.Stderr
// directly.
var Stderr = Writer(os.Stderr)

// Stdout is the same for what is written to standard output: the database
// library logs its slow and failed statements there, values included.
var Stdout = Writer(os.Stdout)

// Writer returns a writer that passes everything on to w with, in each URL,
// the credentials, the path and the query removed: the places a key or a
// password sits. `https://host/v2/key` is written `https://host/…`. A URL
// that is only a scheme and a host is written as it is. A `password=…`
// setting, as in a key=value database string, loses its value too.
func Writer(w io.Writer) io.Writer {
	return &writer{w: w}
}

type writer struct {
	w io.Writer
}

var (
	// scheme://authority, then whatever follows up to the end of the URL: a
	// space, a quote, a backslash (the URL sits in a quoted string), an
	// angle bracket or the start of a colour code.
	urlPattern = regexp.MustCompile(`([A-Za-z][A-Za-z0-9+.-]*://)([^\s"'\\<>\x1b/?#]*)([/?#][^\s"'\\<>\x1b]*)?`)
	// password=value in a key=value connection string.
	passwordPattern = regexp.MustCompile(`(?i)(password\s*=\s*)('[^']*'|[^\s"'\\\x1b]+)`)

	mark = []byte("…")
)

func (s *writer) Write(p []byte) (int, error) {
	if _, err := s.w.Write(Clean(p)); err != nil {
		return 0, err
	}
	// The caller is told all of p went out: what was cut is not a short
	// write.
	return len(p), nil
}

// Clean returns p with the secrets cut out, p itself when there is nothing
// to cut.
func Clean(p []byte) []byte {
	if bytes.Contains(p, []byte("://")) {
		p = urlPattern.ReplaceAllFunc(p, cleanURL)
	}
	if bytes.Contains(bytes.ToLower(p), []byte("password")) {
		p = passwordPattern.ReplaceAll(p, []byte("${1}…"))
	}
	return p
}

func cleanURL(u []byte) []byte {
	m := urlPattern.FindSubmatch(u)
	scheme, authority, rest := m[1], m[2], m[3]
	out := make([]byte, 0, len(u))
	out = append(out, scheme...)
	// user:password@host → host
	if at := bytes.LastIndexByte(authority, '@'); at >= 0 {
		out = append(out, mark...)
		out = append(out, '@')
		authority = authority[at+1:]
	}
	out = append(out, authority...)
	// A bare "/" says nothing; anything more is cut.
	if len(rest) > 1 {
		out = append(out, rest[0])
		out = append(out, mark...)
	} else {
		out = append(out, rest...)
	}
	return out
}
