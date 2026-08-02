package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/krisiasty/k2a-token-sync/internal/legal"
)

func TestWriteThirdPartyNoticesWritesTheEmbeddedFileExactly(t *testing.T) {
	t.Parallel()

	want := legal.ThirdPartyNotices()
	if !strings.Contains(want, "k2a-token-sync — third-party notices") {
		t.Fatal("embedded notices do not contain their generated header")
	}

	var output bytes.Buffer
	if err := writeThirdPartyNotices(&output); err != nil {
		t.Fatalf("writeThirdPartyNotices returned unexpected error: %v", err)
	}
	if output.String() != want {
		t.Error("licenses output differs from the embedded notice file")
	}
}

func TestWriteThirdPartyNoticesReportsOutputFailure(t *testing.T) {
	t.Parallel()

	want := errors.New("write failed")
	err := writeThirdPartyNotices(errorWriter{err: want})
	if !errors.Is(err, want) {
		t.Fatalf("writeThirdPartyNotices error = %v, want wrapped %v", err, want)
	}
}

type errorWriter struct {
	err error
}

func (w errorWriter) Write([]byte) (int, error) {
	return 0, w.err
}
