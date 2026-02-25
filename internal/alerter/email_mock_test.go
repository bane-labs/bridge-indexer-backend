package alerter

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/bane-labs/bridge-indexer-backend/internal/types"
)

// startMockSMTP starts a minimal mock SMTP server on a random port.
// It accepts one connection, runs the SMTP conversation, and sends the
// captured email body (everything between DATA and the terminating dot)
// on the returned channel.
func startMockSMTP(t *testing.T) (port int, received <-chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	ch := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// SMTP greeting
		fmt.Fprintf(conn, "220 localhost SMTP\r\n")
		scanner := bufio.NewScanner(conn)
		var body strings.Builder
		inData := false
		for scanner.Scan() {
			line := scanner.Text()
			if inData {
				if line == "." {
					fmt.Fprintf(conn, "250 OK\r\n")
					ch <- body.String()
					inData = false
					continue
				}
				body.WriteString(line + "\n")
				continue
			}
			switch {
			case strings.HasPrefix(line, "EHLO"):
				fmt.Fprintf(conn, "250-localhost\r\n250 AUTH PLAIN\r\n")
			case strings.HasPrefix(line, "AUTH"):
				fmt.Fprintf(conn, "235 Authentication successful\r\n")
			case strings.HasPrefix(line, "MAIL FROM"):
				fmt.Fprintf(conn, "250 OK\r\n")
			case strings.HasPrefix(line, "RCPT TO"):
				fmt.Fprintf(conn, "250 OK\r\n")
			case line == "DATA":
				fmt.Fprintf(conn, "354 Start input\r\n")
				inData = true
			case strings.HasPrefix(line, "QUIT"):
				fmt.Fprintf(conn, "221 Bye\r\n")
				return
			}
		}
	}()

	port = ln.Addr().(*net.TCPAddr).Port
	return port, ch
}

func TestEmailAlerter_SendToMockSMTP(t *testing.T) {
	port, received := startMockSMTP(t)

	ea := NewEmailAlerter("127.0.0.1", port, "testuser", "testpass", "alerts@test.com", []string{"admin@test.com"})

	alert := Alert{
		Title:      "Bridge Paused",
		Message:    "Native bridge has been paused",
		Severity:   "critical",
		BridgeType: types.BridgeTypeNative,
		Timestamp:  time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC),
	}

	err := ea.Send(context.Background(), alert)
	if err != nil {
		t.Fatalf("Send() returned error: %v", err)
	}

	select {
	case body := <-received:
		if !strings.Contains(body, "Subject:") {
			t.Error("email body missing Subject header")
		}
		if !strings.Contains(body, "Bridge Paused") {
			t.Error("email body missing alert title")
		}
		if !strings.Contains(body, "alerts@test.com") {
			t.Error("email body missing from address")
		}
		if !strings.Contains(body, "CRITICAL") {
			t.Error("email body missing severity")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for email body from mock SMTP")
	}
}

func TestEmailAlerter_SendToMockSMTP_WithOperation(t *testing.T) {
	port, received := startMockSMTP(t)

	ea := NewEmailAlerter("127.0.0.1", port, "testuser", "testpass", "alerts@test.com", []string{"admin@test.com"})

	alert := Alert{
		Title:      "Stuck Operation",
		Message:    "Operation nonce 99 is stuck",
		Severity:   "warning",
		BridgeType: types.BridgeTypeNative,
		Operation: &types.BridgeOperation{
			BridgeType:   types.BridgeTypeNative,
			Direction:    types.DirectionDeposit,
			Nonce:        99,
			SourceChain:  "neo_n3",
			SourceTxHash: "0xabc123",
		},
		Timestamp: time.Date(2026, 2, 1, 8, 30, 0, 0, time.UTC),
	}

	err := ea.Send(context.Background(), alert)
	if err != nil {
		t.Fatalf("Send() returned error: %v", err)
	}

	select {
	case body := <-received:
		if !strings.Contains(body, "99") {
			t.Error("email body missing nonce 99")
		}
		if !strings.Contains(body, "deposit") {
			t.Error("email body missing direction")
		}
		if !strings.Contains(body, "0xabc123") {
			t.Error("email body missing source tx hash")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for email body from mock SMTP")
	}
}

func TestEmailAlerter_ConnectionRefused(t *testing.T) {
	// Get a port that is guaranteed to have nothing listening:
	// bind, capture port, then close immediately.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close() // close so nothing is listening

	ea := NewEmailAlerter("127.0.0.1", port, "user", "pass", "from@test.com", []string{"to@test.com"})

	alert := Alert{
		Title:      "Test Alert",
		Message:    "Should fail to connect",
		Severity:   "warning",
		BridgeType: types.BridgeTypeNative,
		Timestamp:  time.Now(),
	}

	err = ea.Send(context.Background(), alert)
	if err == nil {
		t.Fatal("expected error when connecting to closed port, got nil")
	}
	t.Logf("got expected error: %v", err)
}
