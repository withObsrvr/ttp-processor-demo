# nebu for Analysts

nebu is a command-line tool for power users who need to build custom data pipelines. Extract specific blockchain events, apply custom filters, and process data locally.

## When to Use nebu vs the API

| Use Case | Use API | Use nebu |
|----------|---------|----------|
| Quick lookups | Yes | |
| Dashboard data | Yes | |
| Real-time monitoring | Yes | |
| Custom event extraction | | Yes |
| Historical batch processing | | Yes |
| Building derived datasets | | Yes |
| Local data analysis | | Yes |

## Installation

```bash
# Install nebu (macOS/Linux)
visit https://nebu.withobsrvr.com | sh

# Verify installation
nebu --version
```

## Basic Usage

nebu works as a UNIX pipeline. Data flows through processors that transform it.

```bash
# Extract token transfers from ledgers 322000-323000
nebu extract \
  --processor token-transfer \
  --start 322000 \
  --end 323000 \
  --format compact
```

Output:
```json
{"transfer":{"from":"GABC...","to":"GDEF...","amount":"1000000000","asset":"native"},"ledger":322001}
{"transfer":{"from":"GXYZ...","to":"GABC...","amount":"5000000","asset":"USDC:GA5Z..."},"ledger":322002}
```

## Processor Pipeline

Chain processors with pipes:

```bash
# Extract USDC transfers over 1000 units
nebu extract \
  --processor "token-transfer | usdc-filter | amount-filter --min 10000000000" \
  --start 322000 \
  --end 323000
```

## Available Processors

List all available processors:

```bash
nebu list-processors
```

Common processors:

| Processor | Description |
|-----------|-------------|
| `token-transfer` | All token movements (payments, path payments) |
| `contract-events` | Soroban contract events |
| `usdc-filter` | Filter for USDC transfers only |
| `amount-filter` | Filter by amount threshold |

Get detailed info about a processor:

```bash
nebu describe-processor token-transfer
```

## Export to File

```bash
# Export to JSON file
nebu extract \
  --processor token-transfer \
  --start 322000 \
  --end 323000 \
  > transfers.json

# Export to newline-delimited JSON (better for large datasets)
nebu extract \
  --processor token-transfer \
  --start 322000 \
  --end 323000 \
  --format ndjson \
  > transfers.ndjson
```

## Load into DuckDB for SQL Analysis

```bash
# 1. Extract data
nebu extract \
  --processor token-transfer \
  --start 322000 \
  --end 323000 \
  --format ndjson \
  > transfers.ndjson

# 2. Load into DuckDB
duckdb analysis.db << 'EOF'
CREATE TABLE transfers AS
SELECT * FROM read_ndjson('transfers.ndjson');

-- Now analyze with SQL
SELECT
    transfer->>'asset' as asset,
    COUNT(*) as transfer_count,
    SUM((transfer->>'amount')::BIGINT) as total_volume
FROM transfers
GROUP BY 1
ORDER BY 2 DESC;
EOF
```

## Load into Python (Pandas)

```python
import pandas as pd
import subprocess
import json

# Run nebu and capture output
result = subprocess.run([
    'nebu', 'extract',
    '--processor', 'token-transfer',
    '--start', '322000',
    '--end', '323000',
    '--format', 'ndjson'
], capture_output=True, text=True)

# Parse into DataFrame
records = [json.loads(line) for line in result.stdout.strip().split('\n')]
df = pd.json_normalize(records)

# Analyze
print(df.groupby('transfer.asset').size())
```

## Real-Time Streaming

Follow the live blockchain:

```bash
# Stream new transfers as they happen
nebu follow \
  --processor token-transfer \
  | tee -a live_transfers.ndjson \
  | jq -c '.transfer | {from, to, amount, asset}'
```

## Contract Event Extraction

```bash
# Extract all events for a specific contract
nebu extract \
  --processor contract-events \
  --start 322000 \
  --end 323000 \
  | jq 'select(.contract_id == "CAUGJT4...")'
```

## Building Custom Pipelines

For complex analysis, combine nebu with standard UNIX tools:

```bash
# Top 10 accounts by transfer volume
nebu extract \
  --processor token-transfer \
  --start 322000 \
  --end 323000 \
  --format ndjson \
  | jq -r '.transfer.from' \
  | sort \
  | uniq -c \
  | sort -rn \
  | head -10
```

## Performance Tips

1. **Use `--format ndjson`** for large datasets (streaming-friendly)
2. **Limit ledger range** when exploring (start with 1000 ledgers)
3. **Apply filters early** in the pipeline to reduce data volume
4. **Use `--parallel`** for batch processing: `nebu extract --parallel 4 ...`

## Getting Help

```bash
# General help
nebu --help

# Processor-specific help
nebu extract --help
nebu describe-processor token-transfer
```
