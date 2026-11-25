package main

import (
	"errors"
	"testing"
)

func TestResolveTargetHeight(t *testing.T) {
	head := func() (uint64, error) { return 8_400_000, nil }
	checkpoint := func() (uint64, error) { return 8_399_000, nil }
	for spec, want := range map[string]uint64{"123": 123, "head": 8_400_000, "checkpoint": 8_399_000} {
		if got, err := resolveTargetHeight(spec, head, checkpoint); err != nil || got != want {
			t.Fatalf("%s: %d %v", spec, got, err)
		}
	}
	if _, err := resolveTargetHeight("-1", head, checkpoint); err == nil {
		t.Fatal("negative target accepted")
	}
	missing := errors.New("no checkpoint")
	if _, err := resolveTargetHeight("checkpoint", head, func() (uint64, error) { return 0, missing }); !errors.Is(err, missing) {
		t.Fatalf("missing checkpoint: %v", err)
	}
}
