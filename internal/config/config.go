package config

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// rawConfig is the intermediate struct for viper Unmarshal.
// mapstructure tags define the env var name mapping — single source of truth.
type rawConfig struct {
	N3RPCEndpoints        string `mapstructure:"NEO_N3_RPC_ENDPOINTS"`
	NeoXRPCEndpoints      string `mapstructure:"NEOX_RPC_ENDPOINTS"`
	CORSAllowedOrigins    string `mapstructure:"CORS_ALLOWED_ORIGINS"`
	CORSAllowCredentials  bool   `mapstructure:"CORS_ALLOW_CREDENTIALS"`
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
	N3RPCCallIntervalMs   int    `mapstructure:"N3_RPC_CALL_INTERVAL_MS"`
	NeoXRPCCallIntervalMs int    `mapstructure:"NEOX_RPC_CALL_INTERVAL_MS"`
}

type Config struct {
	N3RPCEndpoints       []string
	NeoXRPCEndpoints     []string
	CORSAllowedOrigins   []string
	CORSAllowCredentials bool
	StuckThreshold       time.Duration
	DiscordWebhookURL    string
	SMTPHost             string
	SMTPPort             int
	SMTPUser             string
	SMTPPass             string
	SMTPFrom             string
	AlertRecipients      []string
	ServerPort           string
	DBPath               string
	NeoXTokenBridge      string
	NeoXMessageBridge    string
	N3NeoXBridge         string
	N3MessageBridge      string
	N3SyncConcurrency    int
	N3StartBlock         uint64
	NeoXStartBlock       uint64
	NeoXSyncConcurrency  int
	N3RPCCallInterval    time.Duration
	NeoXRPCCallInterval  time.Duration
}

// envKeys lists all environment variable names. Used by BindEnv so that
// Unmarshal can pick up values set via AutomaticEnv / os env.
var envKeys = []string{
	"NEO_N3_RPC_ENDPOINTS",
	"NEOX_RPC_ENDPOINTS",
	"CORS_ALLOWED_ORIGINS",
	"CORS_ALLOW_CREDENTIALS",
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
	"N3_RPC_CALL_INTERVAL_MS",
	"NEOX_RPC_CALL_INTERVAL_MS",
}

func SetDefaults(v *viper.Viper) {
	v.SetDefault("STUCK_THRESHOLD_MINUTES", 5)
	v.SetDefault("CORS_ALLOWED_ORIGINS", "")
	v.SetDefault("CORS_ALLOW_CREDENTIALS", false)
	v.SetDefault("SMTP_PORT", 587)
	v.SetDefault("SERVER_PORT", "8080")
	v.SetDefault("DB_PATH", "./data/badger")
	v.SetDefault("NEOX_TOKEN_BRIDGE", "0x1212000000000000000000000000000000000004")
	v.SetDefault("NEOX_MESSAGE_BRIDGE", "0x1212000000000000000000000000000000000009")
	v.SetDefault("N3_NEOX_BRIDGE", "0xbb19cfc864b73159277e1fd39694b3fd5fc613d2")
	v.SetDefault("N3_MESSAGE_BRIDGE", "0xde9cc59df7484ae2f2d2c1ce7a66dccbd5c8bd11")
	v.SetDefault("N3_SYNC_CONCURRENCY", 200)
	v.SetDefault("NEOX_SYNC_CONCURRENCY", 20)
	v.SetDefault("N3_RPC_CALL_INTERVAL_MS", 100)
	v.SetDefault("NEOX_RPC_CALL_INTERVAL_MS", 100)
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
	if raw.N3StartBlock < 0 {
		return nil, fmt.Errorf("N3_START_BLOCK must be >= 0, got %d", raw.N3StartBlock)
	}
	if raw.NeoXStartBlock < 0 {
		return nil, fmt.Errorf("NEOX_START_BLOCK must be >= 0, got %d", raw.NeoXStartBlock)
	}

	cfg := &Config{
		N3RPCEndpoints:       parseCommaSeparated(raw.N3RPCEndpoints),
		NeoXRPCEndpoints:     parseCommaSeparated(raw.NeoXRPCEndpoints),
		CORSAllowedOrigins:   parseCommaSeparated(raw.CORSAllowedOrigins),
		CORSAllowCredentials: raw.CORSAllowCredentials,
		StuckThreshold:       time.Duration(raw.StuckThresholdMinutes) * time.Minute,
		DiscordWebhookURL:    raw.DiscordWebhookURL,
		SMTPHost:             raw.SMTPHost,
		SMTPPort:             raw.SMTPPort,
		SMTPUser:             raw.SMTPUser,
		SMTPPass:             raw.SMTPPass,
		SMTPFrom:             raw.SMTPFrom,
		AlertRecipients:      parseCommaSeparated(raw.AlertRecipients),
		ServerPort:           raw.ServerPort,
		DBPath:               raw.DBPath,
		NeoXTokenBridge:      raw.NeoXTokenBridge,
		NeoXMessageBridge:    raw.NeoXMessageBridge,
		N3NeoXBridge:         raw.N3NeoXBridge,
		N3MessageBridge:      raw.N3MessageBridge,
		N3SyncConcurrency:    raw.N3SyncConcurrency,
		N3StartBlock:         uint64(raw.N3StartBlock),
		NeoXStartBlock:       uint64(raw.NeoXStartBlock),
		NeoXSyncConcurrency:  raw.NeoXSyncConcurrency,
		N3RPCCallInterval:    clampDuration(raw.N3RPCCallIntervalMs),
		NeoXRPCCallInterval:  clampDuration(raw.NeoXRPCCallIntervalMs),
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

	if len(c.CORSAllowedOrigins) == 0 && c.CORSAllowCredentials {
		errs = append(errs, "CORS_ALLOW_CREDENTIALS=true requires at least one CORS_ALLOWED_ORIGINS entry")
	}
	for _, origin := range c.CORSAllowedOrigins {
		if origin == "*" {
			if c.CORSAllowCredentials {
				errs = append(errs, "CORS_ALLOWED_ORIGINS cannot include * when CORS_ALLOW_CREDENTIALS=true")
			}
			continue
		}
		if err := validateOriginURL(origin); err != nil {
			errs = append(errs, fmt.Sprintf("CORS_ALLOWED_ORIGINS: %v", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("configuration validation failed: %s", strings.Join(errs, "; "))
	}

	return nil
}

func validateOriginURL(origin string) error {
	u, err := url.Parse(origin)
	if err != nil {
		return fmt.Errorf("origin %q is not valid: %w", origin, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("origin %q must include scheme and host", origin)
	}
	switch u.Scheme {
	case "http", "https":
	default:
		return fmt.Errorf("origin %q has unsupported scheme %q", origin, u.Scheme)
	}
	if u.User != nil {
		return fmt.Errorf("origin %q must not include user info", origin)
	}
	if u.Path != "" && u.Path != "/" {
		return fmt.Errorf("origin %q must not include a path", origin)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("origin %q must not include query or fragment", origin)
	}
	if strings.HasSuffix(u.Host, ":") {
		return fmt.Errorf("origin %q has invalid empty port", origin)
	}
	if port := u.Port(); port != "" {
		if _, err := strconv.Atoi(port); err != nil {
			return fmt.Errorf("origin %q has invalid port %q", origin, port)
		}
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

func clampDuration(ms int) time.Duration {
	if ms < 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}
