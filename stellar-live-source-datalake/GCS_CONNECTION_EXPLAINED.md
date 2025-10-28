# How stellar-live-source-datalake Connects to GCS

**Date**: 2025-10-26

This document explains the complete flow of how `stellar-live-source-datalake` connects to Google Cloud Storage (GCS) buckets to read Stellar ledger data.

---

## Overview

```
Environment Variables → Config → DataStore → BufferedStorageBackend → gRPC Stream
       ↓                   ↓          ↓                ↓                    ↓
GOOGLE_APPLICATION    GCS Config   GCS Client    Ledger Reader    Client receives ledgers
CREDENTIALS           Created      Connected     Fetches data
```

---

## Step 1: Environment Variables

The service uses **standard GCS authentication** via environment variables:

### Required Variables

```bash
# Storage configuration
export STORAGE_TYPE="GCS"  # or ARCHIVE_STORAGE_TYPE
export BUCKET_NAME="my-stellar-bucket"  # or ARCHIVE_BUCKET_NAME
export ARCHIVE_PATH="landing/ledgers/testnet"  # Optional subfolder

# GCS authentication (standard Google Cloud SDK)
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
```

### Optional Variables

```bash
# Schema configuration (how ledgers are organized)
export LEDGERS_PER_FILE="64"      # Default: 64 ledgers per file
export FILES_PER_PARTITION="10"   # Default: 10 files per partition

# Network configuration
export NETWORK_PASSPHRASE="Test SDF Network ; September 2015"
```

---

## Step 2: Configuration Loading

**File**: `stellar-live-source-datalake/go/server/server.go:169-237`

The server reads environment variables during initialization:

```go
// NewRawLedgerServer creates a new instance
func NewRawLedgerServer() (*RawLedgerServer, error) {
    // 1. Read environment variables
    config.ArchiveStorageType = os.Getenv("ARCHIVE_STORAGE_TYPE")
    config.ArchiveBucketName = os.Getenv("ARCHIVE_BUCKET_NAME")
    config.ArchivePath = os.Getenv("ARCHIVE_PATH")

    // 2. Fallback to legacy names for backward compatibility
    if config.ArchiveStorageType == "" {
        config.ArchiveStorageType = os.Getenv("STORAGE_TYPE")
    }
    if config.ArchiveBucketName == "" {
        config.ArchiveBucketName = os.Getenv("BUCKET_NAME")
    }

    // 3. Validate required fields
    if config.ArchiveStorageType != "GCS" && config.ArchiveStorageType != "S3" {
        return error
    }
    if config.ArchiveBucketName == "" {
        return error
    }
}
```

**Key Point**: The server supports both new (`ARCHIVE_*`) and legacy (`STORAGE_TYPE`, `BUCKET_NAME`) environment variable names for backward compatibility.

---

## Step 3: DataStore Creation

**File**: `stellar-live-source-datalake/go/server/server.go:345-394`

When a client requests ledgers, the server creates a connection to GCS:

```go
func (s *RawLedgerServer) createArchiveBackend() (ledgerbackend.LedgerBackend, error) {
    // 1. Define schema (how ledgers are organized in storage)
    schema := datastore.DataStoreSchema{
        LedgersPerFile:    64,   // 64 ledgers per Parquet file
        FilesPerPartition: 10,   // 10 files per partition folder
    }

    // 2. Build GCS configuration
    switch s.config.ArchiveStorageType {
    case "GCS":
        // Construct full GCS path
        fullPath := s.config.ArchiveBucketName
        if s.config.ArchivePath != "" {
            fullPath = s.config.ArchiveBucketName + "/" + s.config.ArchivePath
        } else {
            // Default to common location
            fullPath = s.config.ArchiveBucketName + "/landing/ledgers/testnet"
        }

        dsParams["destination_bucket_path"] = fullPath
        dsConfig = datastore.DataStoreConfig{
            Type: "GCS",
            Schema: schema,
            Params: dsParams
        }
    }

    // 3. Create the datastore (this connects to GCS)
    dataStore, err := datastore.NewDataStore(context.Background(), dsConfig)
    if err != nil {
        // Connection failed - check credentials!
        return error
    }
}
```

---

## Step 4: GCS Authentication (Automatic)

**How it Works**: The `datastore.NewDataStore()` function uses the **official Google Cloud Go SDK**, which automatically handles authentication using the **Application Default Credentials (ADC)** chain:

### Authentication Priority Order

1. **GOOGLE_APPLICATION_CREDENTIALS** environment variable (most common)
   - Points to a JSON key file for a service account
   - Example: `export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"`

2. **Workload Identity** (if running on GKE)
   - Automatically uses the Kubernetes service account
   - No credentials file needed

3. **Application Default Credentials** (if running on GCP)
   - Automatically uses the VM's service account
   - Works on Compute Engine, Cloud Run, App Engine

4. **User Credentials** (local development)
   - Uses `gcloud auth application-default login`
   - Stores credentials in `~/.config/gcloud/`

### Behind the Scenes

The Stellar Go library (`github.com/stellar/go/ingest/ledgerbackend/datastore`) uses:

```go
// From stellar/go library
import "cloud.google.com/go/storage"

// Creates GCS client using Application Default Credentials
client, err := storage.NewClient(ctx)
// This automatically finds credentials via:
// - GOOGLE_APPLICATION_CREDENTIALS env var
// - GCP metadata service
// - gcloud CLI credentials
```

**Key Point**: You don't need to explicitly pass credentials to the Go code - the Google Cloud SDK handles it automatically!

---

## Step 5: BufferedStorageBackend Creation

**File**: `stellar-live-source-datalake/go/server/server.go:401-424`

After connecting to GCS, a **buffered backend** is created to optimize reads:

```go
// Create BufferedStorageBackend configuration
bufferedConfig := ledgerbackend.BufferedStorageBackendConfig{
    BufferSize: 100,        // Process 100 ledgers in parallel
    NumWorkers: 5,          // Use 5 worker goroutines
    RetryLimit: 3,          // Retry failed operations 3 times
    RetryWait:  time.Second, // Wait 1 second between retries
}

// Wrap the datastore with buffering
backend, err := ledgerbackend.NewBufferedStorageBackend(
    bufferedConfig,
    dataStore,  // GCS datastore from Step 4
    schema      // Schema from Step 3
)
```

### What BufferedStorageBackend Does

1. **Parallel Fetching**: Downloads multiple ledger files from GCS concurrently
2. **Worker Pool**: Uses 5 goroutines to fetch data in parallel
3. **Automatic Retry**: Retries failed GCS requests up to 3 times
4. **Buffering**: Keeps 100 ledgers in memory for fast serving

---

## Step 6: Data Flow (How Ledgers Are Fetched)

```
Client Request → gRPC → StreamRawLedgers() → BufferedStorageBackend
                                                      ↓
                                        GetLedger(sequence)
                                                      ↓
                                        Calculate file path based on schema
                                                      ↓
                                        DataStore.GetFile(path)
                                                      ↓
                                        GCS Client → Download Parquet file
                                                      ↓
                                        Decompress & Parse Parquet
                                                      ↓
                                        Extract LedgerCloseMeta XDR
                                                      ↓
                                        Return to client via gRPC stream
```

### File Path Calculation

The schema determines where files are stored in GCS:

```
Example for ledger sequence 169033 with default schema:
- LedgersPerFile = 64
- FilesPerPartition = 10

Calculation:
1. File number = 169033 / 64 = 2641
2. Partition = 2641 / 10 = 264
3. File in partition = 2641 % 10 = 1

GCS Path:
gs://my-bucket/landing/ledgers/testnet/partition_264/file_1.parquet

This file contains ledgers 168896 - 168959 (64 ledgers)
```

---

## Step 7: GCS Bucket Structure

Typical organization of ledgers in GCS:

```
gs://stellar-ledgers-prod/
├── landing/
│   └── ledgers/
│       ├── testnet/
│       │   ├── partition_0/
│       │   │   ├── file_0.parquet  (ledgers 0-63)
│       │   │   ├── file_1.parquet  (ledgers 64-127)
│       │   │   └── ...
│       │   ├── partition_1/
│       │   │   ├── file_0.parquet  (ledgers 640-703)
│       │   │   └── ...
│       │   └── ...
│       └── mainnet/
│           └── ...
└── archive/
    └── ...
```

**Key Features**:
- **Parquet Format**: Columnar storage, efficient compression
- **Partitioning**: Groups files into folders for scalability
- **Predictable Paths**: Easy to calculate which file contains any ledger

---

## Step 8: Permissions Required

### Service Account Permissions

The service account JSON must have these **GCS IAM roles**:

```
roles/storage.objectViewer  (minimum)
```

Or specific permissions:
- `storage.objects.get` - Download files
- `storage.objects.list` - List bucket contents
- `storage.buckets.get` - Get bucket metadata

### Example Service Account Setup

```bash
# 1. Create service account
gcloud iam service-accounts create stellar-reader \
  --display-name="Stellar Ledger Reader"

# 2. Grant bucket access
gcloud storage buckets add-iam-policy-binding gs://stellar-ledgers-prod \
  --member="serviceAccount:stellar-reader@project.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"

# 3. Download key
gcloud iam service-accounts keys create ~/stellar-reader-key.json \
  --iam-account=stellar-reader@project.iam.gserviceaccount.com

# 4. Set environment variable
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/stellar-reader-key.json"
```

---

## Complete Example

### Local Development

```bash
# 1. Set up GCS credentials
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/stellar-service-account.json"

# 2. Configure storage
export STORAGE_TYPE="GCS"
export BUCKET_NAME="obsrvr-stellar-ledger-data-testnet-data"
export ARCHIVE_PATH="landing/ledgers/testnet"

# 3. Configure ledger schema
export LEDGERS_PER_FILE="64"
export FILES_PER_PARTITION="10"

# 4. Run the service
cd stellar-live-source-datalake
make run

# Output:
# Server listening at [::]:50053
# Health check server listening at :8088
# Creating datastore for archive backend
# Successfully created datastore connection
# Created archive backend (storage_type=GCS, bucket=obsrvr-stellar-ledger-data-testnet-data)
```

### Docker/Kubernetes

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: stellar-live-source-datalake
spec:
  containers:
  - name: server
    image: stellar-live-source-datalake:latest
    env:
    - name: STORAGE_TYPE
      value: "GCS"
    - name: BUCKET_NAME
      value: "stellar-ledgers-prod"
    - name: ARCHIVE_PATH
      value: "landing/ledgers/mainnet"
    - name: GOOGLE_APPLICATION_CREDENTIALS
      value: "/var/secrets/google/key.json"
    volumeMounts:
    - name: gcp-key
      mountPath: /var/secrets/google
      readOnly: true
  volumes:
  - name: gcp-key
    secret:
      secretName: gcp-service-account-key
```

### GKE with Workload Identity (No Credentials File!)

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: stellar-reader
  annotations:
    iam.gke.io/gcp-service-account: stellar-reader@project.iam.gserviceaccount.com

---
apiVersion: v1
kind: Pod
metadata:
  name: stellar-live-source-datalake
spec:
  serviceAccountName: stellar-reader  # Automatic authentication!
  containers:
  - name: server
    image: stellar-live-source-datalake:latest
    env:
    - name: STORAGE_TYPE
      value: "GCS"
    - name: BUCKET_NAME
      value: "stellar-ledgers-prod"
    # No GOOGLE_APPLICATION_CREDENTIALS needed - uses Workload Identity!
```

---

## Troubleshooting

### Error: "Failed to create datastore - check credentials and permissions"

**Cause**: GCS authentication failed or insufficient permissions

**Solutions**:
1. **Check credentials file exists**:
   ```bash
   ls -la $GOOGLE_APPLICATION_CREDENTIALS
   ```

2. **Verify JSON is valid**:
   ```bash
   cat $GOOGLE_APPLICATION_CREDENTIALS | jq .
   ```

3. **Test GCS access**:
   ```bash
   gsutil ls gs://your-bucket-name/
   ```

4. **Check service account permissions**:
   ```bash
   gcloud storage buckets get-iam-policy gs://your-bucket-name
   ```

### Error: "No ledger found for sequence X"

**Cause**: Ledger file doesn't exist in GCS bucket

**Solutions**:
1. **Check file exists**:
   ```bash
   # Calculate partition and file
   # For ledger 169033 with LedgersPerFile=64
   gsutil ls gs://your-bucket/landing/ledgers/testnet/partition_264/
   ```

2. **Verify bucket path**:
   ```bash
   gsutil ls gs://your-bucket/landing/ledgers/testnet/ | head
   ```

3. **Check schema configuration**:
   - Ensure `LEDGERS_PER_FILE` and `FILES_PER_PARTITION` match how data was written

### Error: "Context deadline exceeded"

**Cause**: Network issues or slow GCS response

**Solutions**:
1. **Check network connectivity**:
   ```bash
   curl -I https://storage.googleapis.com
   ```

2. **Increase timeout** (if using BufferedStorageBackend):
   - Adjust `RetryWait` configuration
   - Default is 1 second, try 5 seconds

3. **Check GCS region**:
   - Use buckets in same region as your service for lower latency

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│  stellar-live-source-datalake                               │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  gRPC Server (:50053)                                │  │
│  │  StreamRawLedgers(start_ledger) → stream RawLedger   │  │
│  └──────────────────┬───────────────────────────────────┘  │
│                     ↓                                       │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  BufferedStorageBackend                              │  │
│  │  - 5 worker goroutines                               │  │
│  │  - Buffers 100 ledgers                               │  │
│  │  - Retry logic (3 attempts)                          │  │
│  └──────────────────┬───────────────────────────────────┘  │
│                     ↓                                       │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  DataStore (github.com/stellar/go)                   │  │
│  │  - Calculates file paths                             │  │
│  │  - Parses Parquet files                              │  │
│  │  - Extracts LedgerCloseMeta XDR                      │  │
│  └──────────────────┬───────────────────────────────────┘  │
│                     ↓                                       │
└─────────────────────┼───────────────────────────────────────┘
                      ↓
         ┌────────────────────────────┐
         │  Google Cloud Storage      │
         │  (cloud.google.com/go)     │
         └────────────┬───────────────┘
                      ↓
         ┌────────────────────────────┐
         │  Authentication (ADC)      │
         │  1. GOOGLE_APPLICATION_... │
         │  2. Workload Identity      │
         │  3. Compute metadata       │
         │  4. gcloud user creds      │
         └────────────┬───────────────┘
                      ↓
         ┌────────────────────────────┐
         │  GCS API                   │
         │  storage.googleapis.com    │
         └────────────┬───────────────┘
                      ↓
         ┌────────────────────────────┐
         │  GCS Bucket                │
         │  gs://bucket/path/         │
         │  └─ partition_X/           │
         │     └─ file_Y.parquet      │
         └────────────────────────────┘
```

---

## Key Takeaways

1. **Authentication**: Uses standard Google Cloud Application Default Credentials (no custom auth code)
2. **Automatic**: Just set `GOOGLE_APPLICATION_CREDENTIALS` environment variable
3. **Schema-Based**: File paths calculated from ledger sequence number using schema
4. **Parallel**: Fetches multiple files concurrently for performance
5. **Resilient**: Automatic retry logic for transient GCS errors
6. **Production-Ready**: Supports Workload Identity, service accounts, and local development

The connection is **transparent** - once credentials are configured via environment variables, the Stellar Go SDK and Google Cloud SDK handle everything automatically!

