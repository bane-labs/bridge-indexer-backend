package alerter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

func TestNoopAlerter(t *testing.T) {
	noop := &NoopAlerter{}
	err := noop.Send(context.Background(), Alert{Title: "test"})
	if err != nil {
		t.Fatalf("noop should not error: %v", err)
	}
}

func TestMultiAlerter_NoChannels(t *testing.T) {
	multi := NewMultiAlerter()
	err := multi.Send(context.Background(), Alert{Title: "test"})
	if err != nil {
		t.Fatalf("empty multi should not error: %v", err)
	}
}

func TestMultiAlerter_WithNoop(t *testing.T) {
	multi := NewMultiAlerter(&NoopAlerter{}, &NoopAlerter{})
	err := multi.Send(context.Background(), Alert{
		Title:      "Stuck Operation",
		Message:    "Operation nonce 42 is stuck",
		Severity:   "warning",
		BridgeType: types.BridgeTypeNative,
		Timestamp:  time.Now(),
	})
	if err != nil {
		t.Fatalf("multi with noops should not error: %v", err)
	}
}

func TestDiscordAlerter_BuildPayload(t *testing.T) {
	d := NewDiscordAlerter("https://discord.com/api/webhooks/test")
	alert := Alert{
		Title:      "Stuck Operation",
		Message:    "Nonce 42 stuck for 10 minutes",
		Severity:   "critical",
		BridgeType: types.BridgeTypeNative,
		Timestamp:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	payload, err := d.BuildPayload(alert)
	if err != nil {
		t.Fatalf("build payload: %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(payload, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	embeds, ok := parsed["embeds"].([]interface{})
	if !ok || len(embeds) != 1 {
		t.Fatal("expected 1 embed")
	}

	embed := embeds[0].(map[string]interface{})
	if embed["title"] != "Stuck Operation" {
		t.Fatalf("expected title 'Stuck Operation', got %v", embed["title"])
	}
	if embed["color"].(float64) != 0xFF0000 {
		t.Fatalf("expected red color for critical, got %v", embed["color"])
	}
}

func TestEmailAlerter_BuildMessage(t *testing.T) {
	e := NewEmailAlerter("smtp.example.com", 587, "user", "pass", "alerts@example.com", []string{"admin@example.com"})
	alert := Alert{
		Title:      "Bridge Paused",
		Message:    "Native bridge has been paused",
		Severity:   "critical",
		BridgeType: types.BridgeTypeNative,
		Timestamp:  time.Now(),
	}

	msg := e.BuildMessage(alert)
	if msg == "" {
		t.Fatal("expected non-empty message")
	}
	if !contains(msg, "Subject:") {
		t.Fatal("expected Subject header")
	}
	if !contains(msg, "CRITICAL") {
		t.Fatal("expected CRITICAL in subject")
	}
	if !contains(msg, "alerts@example.com") {
		t.Fatal("expected from address")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
