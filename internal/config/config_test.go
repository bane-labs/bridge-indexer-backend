package config

import (
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
)

func newViper() *viper.Viper {
	return viper.New()
}

func clearConfigEnv(t *testing.T) {
	t.Helper()
	for _, key := range envKeys {
		t.Setenv(key, "")
	}
}

func setRequiredEnv(t *testing.T) {
	t.Helper()
	clearConfigEnv(t)
	t.Setenv("NEO_N3_RPC_ENDPOINTS", "http://seed1.neo.org:10332")
	t.Setenv("NEOX_RPC_ENDPOINTS", "https://mainnet-1.rpc.banelabs.org")
}

func TestLoadConfig_RequiredOnly(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := Load(newViper())
	if err != nil {
		t.Fatalf("Load(newViper()) returned error: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() returned error: %v", err)
	}

	if len(cfg.N3RPCEndpoints) != 1 || cfg.N3RPCEndpoints[0] != "http://seed1.neo.org:10332" {
		t.Errorf("N3RPCEndpoints = %v, want [http://seed1.neo.org:10332]", cfg.N3RPCEndpoints)
	}
	if len(cfg.NeoXRPCEndpoints) != 1 || cfg.NeoXRPCEndpoints[0] != "https://mainnet-1.rpc.banelabs.org" {
		t.Errorf("NeoXRPCEndpoints = %v, want [https://mainnet-1.rpc.banelabs.org]", cfg.NeoXRPCEndpoints)
	}
	if len(cfg.CORSAllowedOrigins) != 0 {
		t.Errorf("CORSAllowedOrigins length = %d, want 0", len(cfg.CORSAllowedOrigins))
	}
	if cfg.CORSAllowCredentials {
		t.Error("CORSAllowCredentials = true, want false by default")
	}
}

func TestLoadConfig_AllFields(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("NEO_N3_RPC_ENDPOINTS", "http://seed1:10332,http://seed2:10332")
	t.Setenv("NEOX_RPC_ENDPOINTS", "https://rpc1.example.com,https://rpc2.example.com")
	t.Setenv("STUCK_THRESHOLD_MINUTES", "10")
	t.Setenv("CORS_ALLOWED_ORIGINS", "https://app.example.com,https://admin.example.com")
	t.Setenv("CORS_ALLOW_CREDENTIALS", "true")
	t.Setenv("DISCORD_WEBHOOK_URL", "https://discord.com/api/webhooks/123/abc")
	t.Setenv("SMTP_HOST", "smtp.example.com")
	t.Setenv("SMTP_PORT", "465")
	t.Setenv("SMTP_USER", "user@example.com")
	t.Setenv("SMTP_PASS", "secret")
	t.Setenv("SMTP_FROM", "alerts@example.com")
	t.Setenv("ALERT_RECIPIENTS", "admin@example.com,ops@example.com")
	t.Setenv("SERVER_PORT", "9090")
	t.Setenv("DB_PATH", "/var/data/badger")
	t.Setenv("NEOX_TOKEN_BRIDGE", "0xabc123")
	t.Setenv("NEOX_MESSAGE_BRIDGE", "0xdef456")
	t.Setenv("N3_NEOX_BRIDGE", "0x111222")
	t.Setenv("N3_MESSAGE_BRIDGE", "0x333444")
	t.Setenv("AUTO_BACKFILL_ENABLED", "true")

	cfg, err := Load(newViper())
	if err != nil {
		t.Fatalf("Load(newViper()) returned error: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() returned error: %v", err)
	}

	if len(cfg.N3RPCEndpoints) != 2 {
		t.Errorf("N3RPCEndpoints length = %d, want 2", len(cfg.N3RPCEndpoints))
	}
	if len(cfg.NeoXRPCEndpoints) != 2 {
		t.Errorf("NeoXRPCEndpoints length = %d, want 2", len(cfg.NeoXRPCEndpoints))
	}
	if cfg.StuckThreshold != 10*time.Minute {
		t.Errorf("StuckThreshold = %v, want 10m", cfg.StuckThreshold)
	}
	if len(cfg.CORSAllowedOrigins) != 2 {
		t.Errorf("CORSAllowedOrigins length = %d, want 2", len(cfg.CORSAllowedOrigins))
	}
	if !cfg.CORSAllowCredentials {
		t.Error("CORSAllowCredentials = false, want true")
	}
	if cfg.DiscordWebhookURL != "https://discord.com/api/webhooks/123/abc" {
		t.Errorf("DiscordWebhookURL = %q, want webhook URL", cfg.DiscordWebhookURL)
	}
	if cfg.SMTPHost != "smtp.example.com" {
		t.Errorf("SMTPHost = %q, want smtp.example.com", cfg.SMTPHost)
	}
	if cfg.SMTPPort != 465 {
		t.Errorf("SMTPPort = %d, want 465", cfg.SMTPPort)
	}
	if cfg.SMTPUser != "user@example.com" {
		t.Errorf("SMTPUser = %q, want user@example.com", cfg.SMTPUser)
	}
	if cfg.SMTPPass != "secret" {
		t.Errorf("SMTPPass = %q, want secret", cfg.SMTPPass)
	}
	if cfg.SMTPFrom != "alerts@example.com" {
		t.Errorf("SMTPFrom = %q, want alerts@example.com", cfg.SMTPFrom)
	}
	if len(cfg.AlertRecipients) != 2 {
		t.Errorf("AlertRecipients length = %d, want 2", len(cfg.AlertRecipients))
	}
	if cfg.ServerPort != "9090" {
		t.Errorf("ServerPort = %q, want 9090", cfg.ServerPort)
	}
	if cfg.DBPath != "/var/data/badger" {
		t.Errorf("DBPath = %q, want /var/data/badger", cfg.DBPath)
	}
	if cfg.NeoXTokenBridge != "0xabc123" {
		t.Errorf("NeoXTokenBridge = %q, want 0xabc123", cfg.NeoXTokenBridge)
	}
	if cfg.NeoXMessageBridge != "0xdef456" {
		t.Errorf("NeoXMessageBridge = %q, want 0xdef456", cfg.NeoXMessageBridge)
	}
	if cfg.N3NeoXBridge != "0x111222" {
		t.Errorf("N3NeoXBridge = %q, want 0x111222", cfg.N3NeoXBridge)
	}
	if cfg.N3MessageBridge != "0x333444" {
		t.Errorf("N3MessageBridge = %q, want 0x333444", cfg.N3MessageBridge)
	}
	if !cfg.AutoBackfillEnabled {
		t.Error("AutoBackfillEnabled = false, want true")
	}
}

func TestValidate_MissingRequiredFields(t *testing.T) {
	tests := []struct {
		name string
		key  string
	}{
		{"missing n3 endpoints", "NEO_N3_RPC_ENDPOINTS"},
		{"missing neox endpoints", "NEOX_RPC_ENDPOINTS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv(tt.key, "")

			cfg, err := Load(newViper())
			if err != nil {
				t.Fatalf("Load(newViper()) returned error: %v", err)
			}

			err = cfg.Validate()
			if err == nil {
				t.Fatalf("Validate() should return error when %s is missing", tt.key)
			}

			if got := err.Error(); !strings.Contains(got, tt.key) {
				t.Errorf("error message %q should mention %s", got, tt.key)
			}
		})
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := Load(newViper())
	if err != nil {
		t.Fatalf("Load(newViper()) returned error: %v", err)
	}

	if cfg.StuckThreshold != 5*time.Minute {
		t.Errorf("StuckThreshold = %v, want 5m0s", cfg.StuckThreshold)
	}
	if cfg.StuckThresholdMessage != 24*time.Hour {
		t.Errorf("StuckThresholdMessage = %v, want 24h (default STUCK_THRESHOLD_MESSAGE_MINUTES)", cfg.StuckThresholdMessage)
	}
	if cfg.StuckThresholdNative != 0 {
		t.Errorf("StuckThresholdNative = %v, want 0 (use default STUCK_THRESHOLD)", cfg.StuckThresholdNative)
	}
	if cfg.StuckThresholdToken != 0 {
		t.Errorf("StuckThresholdToken = %v, want 0 (use default STUCK_THRESHOLD)", cfg.StuckThresholdToken)
	}
	if len(cfg.CORSAllowedOrigins) != 0 {
		t.Errorf("CORSAllowedOrigins length = %d, want 0", len(cfg.CORSAllowedOrigins))
	}
	if cfg.CORSAllowCredentials {
		t.Error("CORSAllowCredentials = true, want false by default")
	}
	if cfg.SMTPPort != 587 {
		t.Errorf("SMTPPort = %d, want 587", cfg.SMTPPort)
	}
	if cfg.ServerPort != "8080" {
		t.Errorf("ServerPort = %q, want 8080", cfg.ServerPort)
	}
	if cfg.DBPath != "./data/badger" {
		t.Errorf("DBPath = %q, want ./data/badger", cfg.DBPath)
	}
	if cfg.NeoXTokenBridge != "0x1212000000000000000000000000000000000004" {
		t.Errorf("NeoXTokenBridge = %q, want default address", cfg.NeoXTokenBridge)
	}
	if cfg.NeoXMessageBridge != "0x1212000000000000000000000000000000000009" {
		t.Errorf("NeoXMessageBridge = %q, want default address", cfg.NeoXMessageBridge)
	}
	if cfg.N3NeoXBridge != "0xbb19cfc864b73159277e1fd39694b3fd5fc613d2" {
		t.Errorf("N3NeoXBridge = %q, want default address", cfg.N3NeoXBridge)
	}
	if cfg.N3MessageBridge != "0xde9cc59df7484ae2f2d2c1ce7a66dccbd5c8bd11" {
		t.Errorf("N3MessageBridge = %q, want default address", cfg.N3MessageBridge)
	}
	if cfg.DiscordWebhookURL != "" {
		t.Errorf("DiscordWebhookURL = %q, want empty", cfg.DiscordWebhookURL)
	}
	if cfg.AutoBackfillEnabled {
		t.Error("AutoBackfillEnabled = true, want false by default")
	}
	if cfg.SMTPHost != "" {
		t.Errorf("SMTPHost = %q, want empty", cfg.SMTPHost)
	}
	if len(cfg.AlertRecipients) != 0 {
		t.Errorf("AlertRecipients = %v, want empty", cfg.AlertRecipients)
	}
}

func TestLoadConfig_StuckThreshold(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("STUCK_THRESHOLD_MINUTES", "10")

	cfg, err := Load(newViper())
	if err != nil {
		t.Fatalf("Load(newViper()) returned error: %v", err)
	}

	if cfg.StuckThreshold != 10*time.Minute {
		t.Errorf("StuckThreshold = %v, want 10m0s", cfg.StuckThreshold)
	}
}

func TestLoadConfig_MultipleEndpoints(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("NEO_N3_RPC_ENDPOINTS", "http://seed1:10332, http://seed2:10332, http://seed3:10332")
	t.Setenv("NEOX_RPC_ENDPOINTS", "https://rpc1.example.com,https://rpc2.example.com")

	cfg, err := Load(newViper())
	if err != nil {
		t.Fatalf("Load(newViper()) returned error: %v", err)
	}

	if len(cfg.N3RPCEndpoints) != 3 {
		t.Fatalf("N3RPCEndpoints length = %d, want 3", len(cfg.N3RPCEndpoints))
	}
	if cfg.N3RPCEndpoints[0] != "http://seed1:10332" {
		t.Errorf("N3RPCEndpoints[0] = %q, want http://seed1:10332", cfg.N3RPCEndpoints[0])
	}
	if cfg.N3RPCEndpoints[1] != "http://seed2:10332" {
		t.Errorf("N3RPCEndpoints[1] = %q, want http://seed2:10332", cfg.N3RPCEndpoints[1])
	}
	if cfg.N3RPCEndpoints[2] != "http://seed3:10332" {
		t.Errorf("N3RPCEndpoints[2] = %q, want http://seed3:10332", cfg.N3RPCEndpoints[2])
	}

	if len(cfg.NeoXRPCEndpoints) != 2 {
		t.Fatalf("NeoXRPCEndpoints length = %d, want 2", len(cfg.NeoXRPCEndpoints))
	}
}

func TestValidate_DoesNotRequireOptionalSMTP(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("SMTP_HOST", "smtp.example.com")

	cfg, err := Load(newViper())
	if err != nil {
		t.Fatalf("Load(newViper()) returned error: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() returned error for partial optional SMTP config: %v", err)
	}
	if cfg.SMTPConfigured() {
		t.Fatal("SMTPConfigured() should be false for partial SMTP config")
	}
}

func TestSMTPConfigured(t *testing.T) {
	cfg := &Config{}
	if cfg.SMTPConfigured() {
		t.Error("SMTPConfigured() should be false with empty config")
	}

	cfg.SMTPHost = "smtp.example.com"
	cfg.SMTPFrom = "alerts@example.com"
	cfg.AlertRecipients = []string{"admin@example.com"}
	if !cfg.SMTPConfigured() {
		t.Error("SMTPConfigured() should be true with host, from, and recipients set")
	}
}

func TestDiscordConfigured(t *testing.T) {
	cfg := &Config{}
	if cfg.DiscordConfigured() {
		t.Error("DiscordConfigured() should be false with empty config")
	}

	cfg.DiscordWebhookURL = "https://discord.com/api/webhooks/123/abc"
	if !cfg.DiscordConfigured() {
		t.Error("DiscordConfigured() should be true with webhook URL set")
	}
}

func TestValidate_InvalidEndpointURL(t *testing.T) {
	tests := []struct {
		name    string
		n3      string
		neox    string
		wantErr string
	}{
		{"no scheme", "seed1.neo.org:10332", "https://rpc.example.com", "has no host"},
		{"bad scheme", "ftp://seed1.neo.org:10332", "https://rpc.example.com", "unsupported scheme"},
		{"neox invalid", "http://seed1.neo.org:10332", "not-a-url", "has no host"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				N3RPCEndpoints:     []string{tt.n3},
				NeoXRPCEndpoints:   []string{tt.neox},
				CORSAllowedOrigins: []string{"http://localhost:3000"},
			}
			err := cfg.Validate()
			if err == nil {
				t.Fatal("Validate() should return error for invalid endpoint URL")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestValidate_ValidEndpointSchemes(t *testing.T) {
	schemes := []string{"http", "https", "ws", "wss"}
	for _, scheme := range schemes {
		t.Run(scheme, func(t *testing.T) {
			cfg := &Config{
				N3RPCEndpoints:     []string{scheme + "://seed1.neo.org:10332"},
				NeoXRPCEndpoints:   []string{scheme + "://rpc.example.com"},
				CORSAllowedOrigins: []string{"http://localhost:3000"},
			}
			if err := cfg.Validate(); err != nil {
				t.Errorf("Validate() should accept %s scheme, got: %v", scheme, err)
			}
		})
	}
}

func TestValidate_CORSWildcardWithCredentials(t *testing.T) {
	cfg := &Config{
		N3RPCEndpoints:       []string{"http://seed1.neo.org:10332"},
		NeoXRPCEndpoints:     []string{"https://rpc.example.com"},
		CORSAllowedOrigins:   []string{"*"},
		CORSAllowCredentials: true,
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() should fail when CORS_ALLOW_CREDENTIALS=true and origin is wildcard")
	}
	if !strings.Contains(err.Error(), "CORS_ALLOWED_ORIGINS") {
		t.Errorf("error %q should mention CORS_ALLOWED_ORIGINS", err.Error())
	}
}

func TestValidate_InvalidCORSOrigin(t *testing.T) {
	cfg := &Config{
		N3RPCEndpoints:     []string{"http://seed1.neo.org:10332"},
		NeoXRPCEndpoints:   []string{"https://rpc.example.com"},
		CORSAllowedOrigins: []string{"https://app.example.com/path"},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() should fail for CORS origin with path")
	}
	if !strings.Contains(err.Error(), "CORS_ALLOWED_ORIGINS") {
		t.Errorf("error %q should mention CORS_ALLOWED_ORIGINS", err.Error())
	}
}

func TestValidate_CORSOriginFormats(t *testing.T) {
	tests := []struct {
		name    string
		origin  string
		wantErr bool
	}{
		{
			name:    "valid without trailing slash",
			origin:  "https://app.example.com",
			wantErr: false,
		},
		{
			name:    "invalid with trailing slash",
			origin:  "https://app.example.com/",
			wantErr: true,
		},
		{
			name:    "invalid with path",
			origin:  "https://app.example.com/path",
			wantErr: true,
		},
		{
			name:    "invalid with query",
			origin:  "https://app.example.com?query=1",
			wantErr: true,
		},
		{
			name:    "invalid with fragment",
			origin:  "https://app.example.com#fragment",
			wantErr: true,
		},
		{
			name:    "invalid with userinfo",
			origin:  "https://user@app.example.com",
			wantErr: true,
		},
		{
			name:    "invalid with empty port",
			origin:  "https://app.example.com:",
			wantErr: true,
		},
		{
			name:    "invalid with non-numeric port",
			origin:  "https://app.example.com:abc",
			wantErr: true,
		},
		{
			name:    "invalid with port zero",
			origin:  "https://app.example.com:0",
			wantErr: true,
		},
		{
			name:    "invalid with port out of range",
			origin:  "https://app.example.com:99999",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &Config{
				N3RPCEndpoints:     []string{"http://seed1.neo.org:10332"},
				NeoXRPCEndpoints:   []string{"https://rpc.example.com"},
				CORSAllowedOrigins: []string{tt.origin},
			}

			err := cfg.Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Validate() should fail for CORS origin %q", tt.origin)
				}
				if !strings.Contains(err.Error(), "CORS_ALLOWED_ORIGINS") {
					t.Errorf("error %q should mention CORS_ALLOWED_ORIGINS", err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("Validate() should succeed for CORS origin %q, got error: %v", tt.origin, err)
				}
			}
		})
	}
}

func TestValidate_EmptyOriginsCredentialsFalse(t *testing.T) {
	cfg := &Config{
		N3RPCEndpoints:       []string{"http://seed1.neo.org:10332"},
		NeoXRPCEndpoints:     []string{"https://rpc.example.com"},
		CORSAllowedOrigins:   nil,
		CORSAllowCredentials: false,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() should succeed with empty origins and credentials=false, got: %v", err)
	}
}

func TestValidate_EmptyOriginsCredentialsTrue(t *testing.T) {
	cfg := &Config{
		N3RPCEndpoints:       []string{"http://seed1.neo.org:10332"},
		NeoXRPCEndpoints:     []string{"https://rpc.example.com"},
		CORSAllowedOrigins:   nil,
		CORSAllowCredentials: true,
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() should fail with empty origins and credentials=true")
	}
	if !strings.Contains(err.Error(), "CORS_ALLOW_CREDENTIALS") {
		t.Errorf("error %q should mention CORS_ALLOW_CREDENTIALS", err.Error())
	}
}

func TestParseCommaSeparated(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{"empty string", "", nil},
		{"whitespace only", "   ", nil},
		{"single value", "http://example.com", []string{"http://example.com"}},
		{"two values", "a,b", []string{"a", "b"}},
		{"with spaces", " a , b , c ", []string{"a", "b", "c"}},
		{"trailing comma", "a,b,", []string{"a", "b"}},
		{"leading comma", ",a,b", []string{"a", "b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseCommaSeparated(tt.input)
			if len(got) != len(tt.want) {
				t.Fatalf("parseCommaSeparated(%q) = %v (len %d), want %v (len %d)", tt.input, got, len(got), tt.want, len(tt.want))
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("parseCommaSeparated(%q)[%d] = %q, want %q", tt.input, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestConfig_NegativeStartBlock(t *testing.T) {
	tests := []struct {
		name    string
		n3      string
		neox    string
		wantErr string
	}{
		{"negative N3_START_BLOCK", "-1", "0", "N3_START_BLOCK must be >= 0"},
		{"negative NEOX_START_BLOCK", "0", "-1", "NEOX_START_BLOCK must be >= 0"},
		{"both negative", "-1", "-1", "N3_START_BLOCK must be >= 0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnv(t)
			t.Setenv("N3_START_BLOCK", tt.n3)
			t.Setenv("NEOX_START_BLOCK", tt.neox)

			cfg, err := Load(newViper())
			if err == nil {
				t.Fatalf("Load(newViper()) should return error for negative start block, got nil")
			}
			if cfg != nil {
				t.Errorf("Load(newViper()) should return nil config on error, got %v", cfg)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestLoadConfig_RPCCallIntervalDefaults(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := Load(newViper())
	if err != nil {
		t.Fatalf("Load(newViper()) returned error: %v", err)
	}

	if cfg.N3RPCCallInterval != 250*time.Millisecond {
		t.Errorf("N3RPCCallInterval = %v, want 250ms", cfg.N3RPCCallInterval)
	}
	if cfg.NeoXRPCCallInterval != 100*time.Millisecond {
		t.Errorf("NeoXRPCCallInterval = %v, want 100ms", cfg.NeoXRPCCallInterval)
	}
}

func TestLoadConfig_RPCCallIntervalCustom(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("N3_RPC_CALL_INTERVAL_MS", "200")
	t.Setenv("NEOX_RPC_CALL_INTERVAL_MS", "0")

	cfg, err := Load(newViper())
	if err != nil {
		t.Fatalf("Load(newViper()) returned error: %v", err)
	}

	if cfg.N3RPCCallInterval != 200*time.Millisecond {
		t.Errorf("N3RPCCallInterval = %v, want 200ms", cfg.N3RPCCallInterval)
	}
	if cfg.NeoXRPCCallInterval != 0 {
		t.Errorf("NeoXRPCCallInterval = %v, want 0", cfg.NeoXRPCCallInterval)
	}
}

func TestLoadConfig_RPCCallIntervalNegativeClamped(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("N3_RPC_CALL_INTERVAL_MS", "-50")
	t.Setenv("NEOX_RPC_CALL_INTERVAL_MS", "-100")

	cfg, err := Load(newViper())
	if err != nil {
		t.Fatalf("Load(newViper()) returned error: %v", err)
	}

	if cfg.N3RPCCallInterval != 0 {
		t.Errorf("N3RPCCallInterval = %v, want 0 (clamped from -50)", cfg.N3RPCCallInterval)
	}
	if cfg.NeoXRPCCallInterval != 0 {
		t.Errorf("NeoXRPCCallInterval = %v, want 0 (clamped from -100)", cfg.NeoXRPCCallInterval)
	}
}
