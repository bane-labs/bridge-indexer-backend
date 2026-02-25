//go:build integration

package alerter

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

func getWebhookURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("INTEGRATION_DISCORD_WEBHOOK_URL")
	if url == "" {
		t.Skip("INTEGRATION_DISCORD_WEBHOOK_URL not set")
	}
	return url
}

func TestDiscordAlerter_SendReal(t *testing.T) {
	webhookURL := getWebhookURL(t)

	d := NewDiscordAlerter(webhookURL)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	alert := Alert{
		Title:      "[TEST] Bridge Indexer Integration Test",
		Message:    "This is an automated integration test message. If you see this, Discord alerting works correctly.",
		Severity:   "warning",
		BridgeType: types.BridgeTypeNative,
		Timestamp:  time.Now(),
	}

	if err := d.Send(ctx, alert); err != nil {
		t.Fatalf("Send() returned error: %v", err)
	}
}

func TestDiscordAlerter_SendCritical(t *testing.T) {
	webhookURL := getWebhookURL(t)

	d := NewDiscordAlerter(webhookURL)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	alert := Alert{
		Title:      "[TEST] Critical Alert",
		Message:    "Integration test: critical severity alert. Embed should be red.",
		Severity:   "critical",
		BridgeType: types.BridgeTypeToken,
		Timestamp:  time.Now(),
	}

	if err := d.Send(ctx, alert); err != nil {
		t.Fatalf("Send() returned error for critical alert: %v", err)
	}
}

func TestDiscordAlerter_SendWithOperation(t *testing.T) {
	webhookURL := getWebhookURL(t)

	d := NewDiscordAlerter(webhookURL)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	op := &types.BridgeOperation{
		Nonce:      42,
		Direction:  types.DirectionDeposit,
		BridgeType: types.BridgeTypeNative,
	}

	alert := Alert{
		Title:      "[TEST] Alert With Operation",
		Message:    "Integration test: alert with BridgeOperation attached (nonce=42, deposit).",
		Severity:   "warning",
		BridgeType: types.BridgeTypeNative,
		Operation:  op,
		Timestamp:  time.Now(),
	}

	if err := d.Send(ctx, alert); err != nil {
		t.Fatalf("Send() returned error for alert with operation: %v", err)
	}
}

func TestDiscordAlerter_InvalidWebhook(t *testing.T) {
	d := NewDiscordAlerter("https://discord.com/api/webhooks/invalid/invalid")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	alert := Alert{
		Title:      "[TEST] Should Fail",
		Message:    "This message should never appear in Discord.",
		Severity:   "warning",
		BridgeType: types.BridgeTypeNative,
		Timestamp:  time.Now(),
	}

	err := d.Send(ctx, alert)
	if err == nil {
		t.Fatal("Send() should return error for invalid webhook URL")
	}
	t.Logf("Got expected error: %v", err)
}
