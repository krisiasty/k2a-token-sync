package main

import (
	"fmt"
	"io"

	"github.com/krisiasty/k2a-token-sync/internal/legal"
)

func writeThirdPartyNotices(w io.Writer) error {
	if _, err := io.WriteString(w, legal.ThirdPartyNotices()); err != nil {
		return fmt.Errorf("writing third-party notices: %w", err)
	}
	return nil
}
