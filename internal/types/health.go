package types

// HealthStatus represents the overall health of a bridge type (PD §6).
type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusDegraded  HealthStatus = "degraded"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
)

// BridgeHealth represents health metrics for a single bridge type (PD §6 + §5.2).
type BridgeHealth struct {
	BridgeType        BridgeType   `json:"bridge_type"`
	Status            HealthStatus `json:"status"`
	DepositsPaused    bool         `json:"deposits_paused"`
	WithdrawalsPaused bool         `json:"withdrawals_paused"`
	AvgCompletion24h  *float64     `json:"avg_completion_24h_seconds,omitempty"`
	AvgCompletion7d   *float64     `json:"avg_completion_7d_seconds,omitempty"`
	UptimePercentage  *float64     `json:"uptime_percentage,omitempty"`
}
