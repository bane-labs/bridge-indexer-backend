package config

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// rawConfig is the intermediate struct for viper Unmarshal.
// mapstructure tags define the env var name mapping — single source of truth.
type rawConfig struct {
	N3RPCEndpoints        string `mapstructure:"NEO_N3_RPC_ENDPOINTS"`
	NeoXRPCEndpoints      string `mapstructure:"NEOX_RPC_ENDPOINTS"`
	StuckThresholdMinutes int    `mapstructure:"STUCK_THRESHOLD_MINUTES"`
	DiscordWebhookURL     string `mapstructure:"DISCORD_WEBHOOK_URL"`
	SMTPHost              string `mapstructure:"SMTP_HOST"`
	SMTPPort              int    `mapstructure:"SMTP_PORT"`
	SMTPUser              string `mapstructure:"SMTP_USER"`
	SMTPPass              string `mapstructure:"SMTP_PASS"`
	SMTPFrom              string `mapstructure:"SMTP_FROM"`
	AlertRecipients       string `mapstructure:"ALERT_RECIPIENTS"`
	ServerPort            string `mapstructure:"SERVER_PORT"`
	DBPath                string `mapstructure:"DB_PATH"`
	NeoXTokenBridge       string `mapstructure:"NEOX_TOKEN_BRIDGE"`
	NeoXMessageBridge     string `mapstructure:"NEOX_MESSAGE_BRIDGE"`
	N3NeoXBridge          string `mapstructure:"N3_NEOX_BRIDGE"`
	N3MessageBridge       string `mapstructure:"N3_MESSAGE_BRIDGE"`
	N3SyncConcurrency     int    `mapstructure:"N3_SYNC_CONCURRENCY"`
	N3StartBlock          int    `mapstructure:"N3_START_BLOCK"`
	NeoXStartBlock        int    `mapstructure:"NEOX_START_BLOCK"`
	NeoXSyncConcurrency   int    `mapstructure:"NEOX_SYNC_CONCURRENCY"`
}

type Config struct {
	N3RPCEndpoints      []string
	NeoXRPCEndpoints    []string
	StuckThreshold      time.Duration
	DiscordWebhookURL   string
	SMTPHost            string
	SMTPPort            int
	SMTPUser            string
	SMTPPass            string
	SMTPFrom            string
	AlertRecipients     []string
	ServerPort          string
	DBPath              string
	NeoXTokenBridge     string
	NeoXMessageBridge   string
	N3NeoXBridge        string
	N3MessageBridge     string
	N3SyncConcurrency   int
	N3StartBlock        uint64
	NeoXStartBlock      uint64
	NeoXSyncConcurrency int
}

// envKeys lists all environment variable names. Used by BindEnv so that
// Unmarshal can pick up values set via AutomaticEnv / os env.
var envKeys = []string{
	"NEO_N3_RPC_ENDPOINTS",
	"NEOX_RPC_ENDPOINTS",
	"STUCK_THRESHOLD_MINUTES",
	"DISCORD_WEBHOOK_URL",
	"SMTP_HOST",
	"SMTP_PORT",
	"SMTP_USER",
	"SMTP_PASS",
	"SMTP_FROM",
	"ALERT_RECIPIENTS",
	"SERVER_PORT",
	"DB_PATH",
	"NEOX_TOKEN_BRIDGE",
	"NEOX_MESSAGE_BRIDGE",
	"N3_NEOX_BRIDGE",
	"N3_MESSAGE_BRIDGE",
	"N3_SYNC_CONCURRENCY",
	"N3_START_BLOCK",
	"NEOX_START_BLOCK",
	"NEOX_SYNC_CONCURRENCY",
}

func SetDefaults(v *viper.Viper) {
	v.SetDefault("STUCK_THRESHOLD_MINUTES", 5)
	v.SetDefault("SMTP_PORT", 587)
	v.SetDefault("SERVER_PORT", "8080")
	v.SetDefault("DB_PATH", "./data/badger")
	v.SetDefault("NEOX_TOKEN_BRIDGE", "0x1212000000000000000000000000000000000004")
	v.SetDefault("NEOX_MESSAGE_BRIDGE", "0x1212000000000000000000000000000000000009")
	v.SetDefault("N3_NEOX_BRIDGE", "0xbb19cfc864b73159277e1fd39694b3fd5fc613d2")
	v.SetDefault("N3_MESSAGE_BRIDGE", "0xde9cc59df7484ae2f2d2c1ce7a66dccbd5c8bd11")
	v.SetDefault("N3_SYNC_CONCURRENCY", 200)
	v.SetDefault("NEOX_SYNC_CONCURRENCY", 20)
}

func Load(v *viper.Viper) (*Config, error) {
	v.SetConfigFile(".env")
	v.SetConfigType("env")
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			slog.Info("No .env file found, using environment variables only")
		} else if os.IsNotExist(err) {
			slog.Info("No .env file found, using environment variables only")
		} else {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	for _, key := range envKeys {
		_ = v.BindEnv(key)
	}

	SetDefaults(v)

	var raw rawConfig
	if err := v.Unmarshal(&raw); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	cfg := &Config{
		N3RPCEndpoints:      parseCommaSeparated(raw.N3RPCEndpoints),
		NeoXRPCEndpoints:    parseCommaSeparated(raw.NeoXRPCEndpoints),
		StuckThreshold:      time.Duration(raw.StuckThresholdMinutes) * time.Minute,
		DiscordWebhookURL:   raw.DiscordWebhookURL,
		SMTPHost:            raw.SMTPHost,
		SMTPPort:            raw.SMTPPort,
		SMTPUser:            raw.SMTPUser,
		SMTPPass:            raw.SMTPPass,
		SMTPFrom:            raw.SMTPFrom,
		AlertRecipients:     parseCommaSeparated(raw.AlertRecipients),
		ServerPort:          raw.ServerPort,
		DBPath:              raw.DBPath,
		NeoXTokenBridge:     raw.NeoXTokenBridge,
		NeoXMessageBridge:   raw.NeoXMessageBridge,
		N3NeoXBridge:        raw.N3NeoXBridge,
		N3MessageBridge:     raw.N3MessageBridge,
		N3SyncConcurrency:   raw.N3SyncConcurrency,
		N3StartBlock:        uint64(raw.N3StartBlock),
		NeoXStartBlock:      uint64(raw.NeoXStartBlock),
		NeoXSyncConcurrency: raw.NeoXSyncConcurrency,
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	var errs []string

	if len(c.N3RPCEndpoints) == 0 {
		errs = append(errs, "NEO_N3_RPC_ENDPOINTS is required but not set")
	}
	for _, ep := range c.N3RPCEndpoints {
		if err := validateEndpointURL(ep); err != nil {
			errs = append(errs, fmt.Sprintf("NEO_N3_RPC_ENDPOINTS: %v", err))
		}
	}

	if len(c.NeoXRPCEndpoints) == 0 {
		errs = append(errs, "NEOX_RPC_ENDPOINTS is required but not set")
	}
	for _, ep := range c.NeoXRPCEndpoints {
		if err := validateEndpointURL(ep); err != nil {
			errs = append(errs, fmt.Sprintf("NEOX_RPC_ENDPOINTS: %v", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("configuration validation failed: %s", strings.Join(errs, "; "))
	}

	return nil
}

func validateEndpointURL(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("URL %q is not valid: %w", endpoint, err)
	}
	if u.Host == "" {
		return fmt.Errorf("URL %q has no host", endpoint)
	}
	switch u.Scheme {
	case "http", "https", "ws", "wss":
	default:
		return fmt.Errorf("URL %q has unsupported scheme %q", endpoint, u.Scheme)
	}
	return nil
}

func (c *Config) SMTPConfigured() bool {
	return c.SMTPHost != "" && c.SMTPFrom != "" && len(c.AlertRecipients) > 0
}

func (c *Config) DiscordConfigured() bool {
	return c.DiscordWebhookURL != ""
}

func parseCommaSeparated(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}
