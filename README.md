# Neo X ↔ Neo N3 Bridge Indexer & Monitoring Backend

A Go-based backend service for indexing and monitoring bridge operations between Neo X and Neo N3 blockchain networks.

## Overview

This backend service provides comprehensive monitoring and indexing of bridge operations between Neo X and Neo N3 chains. It indexes on-chain data only (no reliance on validators, relayers, or off-chain systems) and provides real-time health monitoring, alerting, and API access for transparency and operational monitoring.

### Key Features

- **Real-time Bridge Monitoring**: Track Native, Token, and Message bridges
- **On-chain Data Indexing**: Sequential block and event indexing for both chains
- **Health & Status Monitoring**: Detect stuck operations and bridge pause states
- **Alerting System**: Email (SMTP) and Discord webhook notifications
- **RESTful APIs**: Comprehensive API for bridge operations and health data
- **Embedded Database**: Single binary deployment with BadgerDB

## Supported Bridge Types

The system monitors three distinct bridge types with independent status tracking:

1. **Native Bridge** - Native asset transfers between chains
2. **Token Bridge** - Token transfers between chains
3. **Message Bridge** - Cross-chain message passing

## Core Concepts

### Bridge Operations

- **Deposit**: Transfer from Neo N3 → Neo X
- **Withdrawal**: Transfer from Neo X → Neo N3
- **Nonce**: Unique identifier for correlating source and destination chain events

### Operation Lifecycle

1. Source-chain event (deposit or withdrawal)
2. Destination-chain execution with matching nonce
3. Time between events determines health status and alerting thresholds

## Requirements

### System Requirements

- **Go**: 1.24 or higher
- **Operating System**: Linux, macOS, or Windows
- **Memory**: Minimum 1GB RAM (4GB+ recommended for production)
- **Disk Space**: At least 10GB free space for blockchain data
- **Network**: Stable internet connection for RPC endpoints

### Dependencies

- **Language**: Go (Golang)  
- **HTTP Framework**: Echo
- **Database**: BadgerDB (preferred), alternatives: BoltDB, SQLite (embedded, lightweight, NoSQL)

## Installation

### Prerequisites

1. **Go Installation**:

   ```bash
   # Verify Go 1.24+ is installed
   go version
   ```

2. **Neo RPC Endpoints**:
   - Neo N3 JSON-RPC endpoint(s)
   - Neo X JSON-RPC endpoint(s)

### Setup

1. **Clone the repository**:

   ```bash
   git clone https://github.com/bane-labs/bridge-indexer-backend.git
   cd bridge-indexer-backend
   ```

2. **Install dependencies**:

   ```bash
   go mod tidy
   ```

3. **Configure environment variables** (see Configuration section)

4. **Build the application**:

   ```bash
   go build -o bin/bridge-indexer cmd/server/main.go
   ```

## Configuration

All configuration is done via environment variables. Copy `.env.example` to `.env` for local development:

```bash
cp .env.example .env
```

### Required

| Variable | Description |
|----------|-------------|
| `NEO_N3_RPC_ENDPOINTS` | Neo N3 JSON-RPC endpoints (comma-separated) |
| `NEOX_RPC_ENDPOINTS` | Neo X JSON-RPC endpoints (comma-separated) |

### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `8080` | HTTP server port |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:3000,http://127.0.0.1:3000` | Comma-separated browser origins allowed for CORS |
| `CORS_ALLOW_CREDENTIALS` | `false` | Enables credentialed CORS requests; requires non-wildcard allowed origins |
| `DB_PATH` | `./data/badger` | BadgerDB data directory |
| `STUCK_THRESHOLD_MINUTES` | `5` | Stuck operation detection threshold (minutes) |
| `DISCORD_WEBHOOK_URL` | | Discord webhook URL for alerts |
| `SMTP_HOST` | | SMTP server host |
| `SMTP_PORT` | `587` | SMTP server port |
| `SMTP_USER` | | SMTP username |
| `SMTP_PASS` | | SMTP password |
| `SMTP_FROM` | | Alert sender email address |
| `ALERT_RECIPIENTS` | | Alert recipient emails (comma-separated) |
| `NEOX_TOKEN_BRIDGE` | `0x1212...0004` | Neo X TokenBridge contract address |
| `NEOX_MESSAGE_BRIDGE` | `0x1212...0009` | Neo X MessageBridge contract address |
| `N3_NEOX_BRIDGE` | `0xbb19...13d2` | Neo N3 NeoXBridge script hash |
| `N3_MESSAGE_BRIDGE` | `0xde9c...bd11` | Neo N3 MessageBridge script hash |

Alert channels are enabled only when full config is present. Missing optional alert config disables the channel without failing startup.

## Usage

### Development Mode

```bash
cp .env.example .env
# Edit .env with your RPC endpoints
go run cmd/server/main.go
```

### Production Mode

```bash
go build -o bin/bridge-indexer cmd/server/main.go
NEO_N3_RPC_ENDPOINTS="https://your-n3-rpc" \
NEOX_RPC_ENDPOINTS="https://your-neox-rpc" \
./bin/bridge-indexer
```

Must be able to run as a single binary. Suitable for long-running indexing workloads.

## Backend APIs (Proposed)

### Status & Health

- `GET /health` - Overall bridge health summary
- `GET /health/bridges` - Health per bridge type (native/token/message)
- `GET /health/delays` - Average completion times (24h/7d)

### Indexing State

- `GET /indexer/state` - Last indexed block height per chain
- `GET /indexer/status` - Current syncing status

### Operations API

- `GET /operations` - Filter by bridge type, direction (deposit/withdrawal), status (pending/completed/stuck)
- `GET /operations/{nonce}` - Full lifecycle view of a specific nonce

## Architecture

### Project Structure

```text
.
├── cmd/
│   └── server/          # Application entry point
├── internal/
│   ├── alert/           # Alerting boundary (email/discord)
│   ├── api/             # HTTP API handlers
│   ├── config/          # Environment configuration
│   ├── datastore/       # Existing BadgerDB adapter
│   ├── domain/          # Shared bridge domain model
│   ├── indexer/         # Indexing module boundary
│   ├── metrics/         # Metrics module boundary
│   ├── monitor/         # Monitoring module boundary
│   └── store/           # Storage module boundary
└── docs/                # Documentation
```

### Core Components

1. **Block Indexer**: Sequential processing of Neo N3 and Neo X blocks
2. **Event Processor**: Extraction and correlation of bridge events  
3. **Health Monitor**: Detection of stuck operations and bridge states
4. **Alert Manager**: Email and Discord notification system
5. **API Server**: RESTful endpoints for data access

## Indexing Responsibilities

### Block Indexing

Index blocks sequentially on Neo N3 and Neo X. Persist:

- Last indexed block height per chain
- Expose indexed block height via API for transparency

### Event Indexing (Key Events Only)

The backend should index only essential bridge-related events, not all contract events.

For each bridge type (Native/Token/Message), indexed fields per operation:

- Bridge type
- Direction (deposit/withdrawal)  
- Nonce
- Source chain / Destination chain
- Source block height / Destination block height (if completed)
- Source transaction hash / Destination transaction hash (if completed)
- Token contract (if applicable) / Amount (if applicable)
- From address / To address
- Timestamp (source event) / Completion timestamp (destination event)

## Monitoring & Alerting Logic

### Stuck Operation Detection

For each deposit or withdrawal:

- If a source-chain event with nonce does not appear on the destination chain
- Within a configurable time threshold (Default: 5 minutes, configurable via environment variable)
- Mark operation as unhealthy and trigger alert

### Bridge Pause Detection

The backend must detect and expose:

- Bridge paused/unpaused state separately for deposits and withdrawals
- Per bridge type
- This status must be included in health APIs

## Health Metrics & Aggregations

The backend must compute and expose:

- **Average bridge completion time**: Last 24 hours, Last 7 days
- **Bridge uptime percentage**: Based on successful vs failed/stuck operations  
- **Current bridge status**: Healthy/Degraded/Unhealthy per-bridge-type health summary

## Alerting System

### Supported Channels

- Email (SMTP)
- Discord (Webhook)

### Configuration Rules

- Alert channels are optional
- Each channel is enabled only if required configuration is present:
  - SMTP credentials → Email enabled
  - Discord webhook URL → Discord enabled
- Missing configuration must disable, not fail, the system

## Development

### Testing

```bash
# Run all tests
go test ./...

# Run with race detection
go test -race ./...

# Test specific package
go test ./internal/indexer
```

### Building

```bash
# Development build
go build cmd/server/main.go

# Production build
go build -ldflags="-w -s" -o bin/bridge-indexer cmd/server/main.go

# Cross-compilation for Linux
GOOS=linux GOARCH=amd64 go build -o bin/bridge-indexer-linux cmd/server/main.go
```

### Code Guidelines

- Follow standard Go conventions (`go fmt`, `go vet`)
- Use meaningful names for bridge-related concepts
- Add comprehensive tests for indexing logic
- Document all exported functions and types

## Deployment

### Production Checklist

- [ ] Configure all required RPC endpoints  
- [ ] Set up monitoring thresholds
- [ ] Configure alerting channels (Email/Discord)
- [ ] Set up log aggregation
- [ ] Configure reverse proxy with SSL/TLS
- [ ] Set up database backups
- [ ] Configure firewall rules
- [ ] Set up process monitoring (systemd/supervisor)

### Monitoring

- Health endpoints for load balancer checks
- Structured JSON logging to stdout
- Metrics exported for Prometheus (future enhancement)

## Security Considerations

- **RPC Security**: Use authenticated RPC endpoints where possible
- **API Security**: Implement rate limiting and authentication (future)
- **Data Privacy**: Bridge operations contain sensitive address information
- **Network Security**: Secure communication between components
- **Database Security**: Protect BadgerDB files with appropriate permissions

## Roadmap

### Phase 1 (Current) - Core Infrastructure

- [ ] Basic HTTP server setup
- [ ] Configuration management
- [ ] Database integration (BadgerDB)

### Phase 2 - Indexing Engine

- [ ] Neo N3 block/event indexing
- [ ] Neo X block/event indexing
- [ ] Event correlation by nonce
- [ ] Data persistence layer

### Phase 3 - Monitoring & Alerting

- [ ] Health monitoring logic
- [ ] Stuck operation detection
- [ ] Email alerting (SMTP)
- [ ] Discord webhook alerts

### Phase 4 - APIs & Production

- [ ] Complete API implementation  
- [ ] Performance optimization
- [ ] Production deployment guides
- [ ] Monitoring dashboard data

### Future Enhancements

- [ ] Frontend dashboard
- [ ] Historical analytics
- [ ] Prometheus metrics
- [ ] Kubernetes deployment
- [ ] Multi-region support

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/indexer-enhancement`)
3. Write comprehensive tests
4. Ensure all tests pass (`go test ./...`)
5. Format code (`go fmt ./...`)
6. Commit changes (`git commit -m 'Add Neo N3 event indexing'`)
7. Push to branch (`git push origin feature/indexer-enhancement`)  
8. Create Pull Request

### Development Focus Areas

- **Blockchain Integration**: Neo N3 and Neo X RPC client implementations
- **Event Processing**: Efficient parsing and correlation of bridge events
- **Monitoring Logic**: Robust health checking and alerting algorithms
- **API Design**: Clean, RESTful endpoints for operational data
- **Performance**: Optimization for continuous blockchain indexing

## Support & Resources

- **Issues**: [GitHub Issues](https://github.com/bane-labs/bridge-indexer-backend/issues)
- **Documentation**: [docs/](./docs/)
- **Neo Developer Resources**: [Neo Documentation](https://docs.neo.org/)
- **Discord**: Neo community channels for blockchain-specific questions

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.
