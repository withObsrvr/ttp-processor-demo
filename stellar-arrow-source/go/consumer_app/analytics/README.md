# Analytics Consumer for Stellar Arrow Source

A real-time analytics consumer that connects to the Stellar Arrow Source server and performs streaming analytics on the data.

## Overview

This consumer provides real-time analytics including:
- Payment volume tracking by asset
- High-value transaction monitoring
- Account activity pattern analysis

## Usage

```bash
# Basic usage
./analytics-consumer <server> [analysis-type] [window-minutes]

# Track payment volumes with 60-minute window
./analytics-consumer localhost:8815 payment-volume 60

# Monitor high-value transactions (>1000 XLM) with 30-minute window
./analytics-consumer localhost:8815 high-value 30

# Track account activity patterns
./analytics-consumer localhost:8815 account-activity 120
```

## Analysis Types

### payment-volume
Tracks total payment volumes by asset within the time window.

Output example:
```
=== payment-volume Results ===
Window: 14:30:00 to 15:30:00
Total rows analyzed: 45000

Asset volumes:
  XLM: 12500000000 stroops (1250.00 XLM)
  USDC: 85000000 stroops (8.50 USDC)
  BTC: 150000 stroops (0.02 BTC)
```

### high-value
Monitors accounts making high-value payments (>1000 XLM).

Output example:
```
=== high-value Results ===
Window: 15:00:00 to 15:30:00
Total rows analyzed: 22000

High-value payment accounts:
  GXXXXXX...: 5 payments
  GYYYYYY...: 3 payments
  GZZZZZZ...: 2 payments
```

### account-activity
Tracks operation patterns by account and operation type.

Output example:
```
=== account-activity Results ===
Window: 13:00:00 to 15:00:00
Total rows analyzed: 98000

Account activity:
  GXXXXXX.../payment: 45 operations
  GXXXXXX.../manage_offer: 12 operations
  GYYYYYY.../payment: 38 operations
```

## Building

```bash
cd consumer_app/analytics
go build -o analytics-consumer main.go
```

## Features

- **Real-time Processing**: Analyzes data as it streams
- **Time Windows**: Configurable sliding time windows
- **Filtering**: Built-in filters for different analysis types
- **Periodic Updates**: Shows results every 30 seconds
- **Graceful Shutdown**: Displays final results on interrupt

## Architecture

```
Arrow Flight Server → Stream → Analytics Engine → Results Display
                              ↓
                        Time Window Buffer
                              ↓
                        Aggregations
```

## Use Cases

1. **Payment Monitoring**: Track payment volumes and trends in real-time
2. **Risk Detection**: Identify high-value or suspicious transactions
3. **Network Analysis**: Understand account behavior patterns
4. **Compliance**: Monitor for regulatory thresholds
5. **Business Intelligence**: Real-time dashboards and alerts

## Performance

- Processes records in-memory for low latency
- Sliding window maintains only recent data
- Efficient aggregation algorithms
- Minimal memory footprint

## Extending

To add new analysis types:

1. Add a new case in the switch statement
2. Define appropriate aggregation type and filters
3. Add display logic in `displayResults()`

Example:
```go
case "asset-adoption":
    agg, err := engine.CreateAggregation(
        "asset_adoption",
        analytics.AggregationDistinct,
        time.Duration(windowMinutes)*time.Minute,
        []string{"asset_code"},
        []analytics.Filter{
            {Field: "operation_type", Operator: analytics.FilterEquals, Value: "payment"},
        },
    )
```

## Integration

This consumer can be integrated with:
- Monitoring systems (Prometheus, Grafana)
- Alert systems (PagerDuty, Slack)
- Databases for historical tracking
- Dashboard applications