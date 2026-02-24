package alerter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type DiscordAlerter struct {
	webhookURL string
	client     *http.Client
}

func NewDiscordAlerter(webhookURL string) *DiscordAlerter {
	return &DiscordAlerter{
		webhookURL: webhookURL,
		client:     &http.Client{Timeout: 10 * time.Second},
	}
}

type discordPayload struct {
	Content string         `json:"content,omitempty"`
	Embeds  []discordEmbed `json:"embeds,omitempty"`
}

type discordEmbed struct {
	Title       string         `json:"title"`
	Description string         `json:"description"`
	Color       int            `json:"color"`
	Fields      []discordField `json:"fields,omitempty"`
	Timestamp   string         `json:"timestamp"`
}

type discordField struct {
	Name   string `json:"name"`
	Value  string `json:"value"`
	Inline bool   `json:"inline"`
}

func (d *DiscordAlerter) Send(ctx context.Context, alert Alert) error {
	color := 0xFFA500 // orange for warning
	if alert.Severity == "critical" {
		color = 0xFF0000 // red
	}

	embed := discordEmbed{
		Title:       alert.Title,
		Description: alert.Message,
		Color:       color,
		Timestamp:   alert.Timestamp.Format(time.RFC3339),
		Fields: []discordField{
			{Name: "Severity", Value: alert.Severity, Inline: true},
			{Name: "Bridge", Value: string(alert.BridgeType), Inline: true},
		},
	}

	if alert.Operation != nil {
		embed.Fields = append(embed.Fields,
			discordField{Name: "Nonce", Value: fmt.Sprintf("%d", alert.Operation.Nonce), Inline: true},
			discordField{Name: "Direction", Value: string(alert.Operation.Direction), Inline: true},
		)
	}

	payload := discordPayload{Embeds: []discordEmbed{embed}}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal discord payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create discord request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("send discord webhook: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("discord webhook returned %d", resp.StatusCode)
	}
	return nil
}

func (d *DiscordAlerter) BuildPayload(alert Alert) ([]byte, error) {
	color := 0xFFA500
	if alert.Severity == "critical" {
		color = 0xFF0000
	}
	embed := discordEmbed{
		Title:       alert.Title,
		Description: alert.Message,
		Color:       color,
		Timestamp:   alert.Timestamp.Format(time.RFC3339),
	}
	payload := discordPayload{Embeds: []discordEmbed{embed}}
	return json.Marshal(payload)
}
