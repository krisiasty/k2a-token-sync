package main

import (
	"errors"
	"testing"

	"github.com/krisiasty/k2a-token-sync/internal/inventory"
)

// "Could not check" is not "current". Reporting current alongside an unverified
// message would be a claim the check explicitly declined to make, and /status is
// read by people deciding whether an upgrade landed.
func TestStatusDoesNotClaimCurrentWhenItCouldNotCheck(t *testing.T) {
	t.Parallel()

	state := newHealthState()
	state.recordSchema(inventory.SchemaCheck{Unverifiable: errors.New("forbidden")})

	got := state.report().Schema
	if got.Current {
		t.Error("Current = true for a schema that could not be read")
	}
	if got.Unverified == "" {
		t.Error("Unverified is empty, so nothing says why the answer is not known")
	}
}

// The counterpart: a checked, matching schema must say so plainly, or there is
// no way to confirm an upgrade landed.
func TestStatusReportsACurrentSchema(t *testing.T) {
	t.Parallel()

	state := newHealthState()
	state.recordSchema(inventory.SchemaCheck{})

	got := state.report().Schema
	if !got.Current || got.Unverified != "" || len(got.MissingFields) != 0 {
		t.Errorf("Schema = %+v, want a plain current verdict", got)
	}
}
