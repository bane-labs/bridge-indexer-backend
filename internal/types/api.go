package types

import "time"

type OverallHealth struct {
	Status    HealthStatus   `json:"status"`
	Bridges   []BridgeHealth `json:"bridges"`
	UpdatedAt time.Time      `json:"updated_at"`
}

type DelayMetrics struct {
	Avg24h map[BridgeType]float64 `json:"avg_24h_seconds"`
	Avg7d  map[BridgeType]float64 `json:"avg_7d_seconds"`
}
