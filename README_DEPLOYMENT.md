# Obsrvr Lake Deployment Documentation

Complete documentation for deploying, operating, and managing the Obsrvr Lake platform.

---

## 📚 Documentation Overview

| Document | Purpose | When to Use |
|----------|---------|-------------|
| **DEPLOYMENT_GUIDE.md** | Step-by-step deployment from scratch | First-time setup, new environments |
| **OPERATIONS_GUIDE.md** | Day-to-day operations, troubleshooting, resets | Daily management, issues, maintenance |
| **ENVIRONMENT_STRATEGY.md** | Prod vs non-prod strategy | Planning environments, cost decisions |
| **SILVER_API_DOCUMENTATION.md** | Query API reference | Building integrations, understanding endpoints |
| **QUICK_REFERENCE.md** | Common API queries | Quick lookups, testing |

---

## 🚀 Quick Start

### First Time Deployment

1. **Plan your environment** → Read `ENVIRONMENT_STRATEGY.md`
2. **Deploy infrastructure** → Follow `DEPLOYMENT_GUIDE.md`
3. **Verify everything works** → Use `QUICK_REFERENCE.md` to test
4. **Learn operations** → Read `OPERATIONS_GUIDE.md`

### Already Deployed?

- **Check health**: See "Daily Operations" in `OPERATIONS_GUIDE.md`
- **Fix issues**: See "Troubleshooting" in `OPERATIONS_GUIDE.md`
- **Reset data**: See "Reset Procedures" in `OPERATIONS_GUIDE.md`
- **Query API**: Use `QUICK_REFERENCE.md`

---

## 🏗️ Platform Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Stellar Blockchain                        │
│                  (Testnet or Mainnet)                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ▼
         ┌───────────────────────────────┐
         │  stellar-postgres-ingester    │  Port 8088
         │  (continuous streaming)        │
         └───────────────┬───────────────┘
                         │
                         ▼
         ┌───────────────────────────────┐
         │   PostgreSQL stellar_hot       │  Hot Buffer
         │   (19 Hubble tables)           │  (last ~3 hours)
         │   - ledgers, accounts,         │
         │     operations, transactions   │
         └───────────────┬───────────────┘
                         │ (every 3h)
                         ▼
         ┌───────────────────────────────┐
         │  postgres-ducklake-flusher    │  Port 8090
         │  (hot → cold archival)         │
         └───────────────┬───────────────┘
                         │
                         ▼
         ┌───────────────────────────────┐
         │   DuckLake Bronze Layer        │  Cold Storage
         │   (Parquet on Backblaze B2)    │  (historical)
         │   - Columnar, compressed       │
         └───────────────┬───────────────┘
                         │
                         ▼
         ┌───────────────────────────────┐
         │  bronze-silver-transformer    │  Port 8093
         │  (raw → analytics)             │
         └───────────────┬───────────────┘
                         │
                         ▼
         ┌───────────────────────────────┐
         │   PostgreSQL silver_hot        │  Hot Buffer
         │   (18 Silver tables)           │  (last ~3 hours)
         │   - accounts_current,          │
         │     enriched_operations,       │
         │     token_transfers            │
         └───────────────┬───────────────┘
                         │ (every 3h)
                         ▼
         ┌───────────────────────────────┐
         │    silver-cold-flusher        │  Port 8095
         │    (hot → cold archival)       │
         └───────────────┬───────────────┘
                         │
                         ▼
         ┌───────────────────────────────┐
         │   DuckLake Silver Layer        │  Cold Storage
         │   (Parquet on Backblaze B2)    │  (historical)
         │   - Analytics-ready data       │
         └───────────────┬───────────────┘
                         │
                         ▼
         ┌───────────────────────────────┐
         │    stellar-query-api          │  Port 8092
         │    (unified hot+cold queries)  │
         │    ┌─────────┐  ┌──────────┐  │
         │    │   Hot   │  │   Cold   │  │
         │    │  (PG)   │  │ (Parquet)│  │
         │    └────┬────┘  └─────┬────┘  │
         │         └──────┬──────┘        │
         │                │               │
         │         Merged Results         │
         └────────────────┬──────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │   Consumer Apps       │
              │   - Web dashboards    │
              │   - Analytics tools   │
              │   - Block explorers   │
              └───────────────────────┘
```

---

## 💰 Cost Breakdown

### Current Testnet Deployment

| Component | Spec | Monthly Cost |
|-----------|------|--------------|
| Droplet | s-8vcpu-16gb-amd | $96 |
| Volume | 200GB SSD | $20 |
| PostgreSQL stellar_hot | db-s-2vcpu-4gb | $60 |
| PostgreSQL silver_hot | db-s-2vcpu-4gb | $60 |
| PostgreSQL catalog | db-s-1vcpu-1gb | $15 |
| Backblaze B2 Bronze | ~50GB | $2.50 |
| Backblaze B2 Silver | ~30GB | $1.50 |
| **Total** | | **~$255/month** |

### Projected Mainnet Deployment

| Component | Spec | Monthly Cost |
|-----------|------|--------------|
| Droplet | s-16vcpu-32gb-amd | $192 |
| Volume | 500GB SSD | $50 |
| PostgreSQL stellar_hot | db-s-4vcpu-8gb | $120 |
| PostgreSQL silver_hot | db-s-4vcpu-8gb | $120 |
| PostgreSQL catalog | db-s-2vcpu-4gb | $60 |
| Backblaze B2 Bronze | ~200GB | $10 |
| Backblaze B2 Silver | ~150GB | $7.50 |
| **Total** | | **~$560/month** |

---

## 🎯 Key Features

### Lambda Architecture (Hot + Cold)

**Hot Buffer** (PostgreSQL):
- Last ~3 hours of data
- < 1 minute lag from blockchain
- Fast queries (50-100ms)
- Real-time analytics

**Cold Storage** (DuckLake/Parquet):
- Historical data (3+ hours old)
- Compressed, columnar format
- Stored on Backblaze B2
- Cost-efficient long-term storage

**Unified Queries**:
- Query API automatically merges hot + cold
- Transparent to consumers
- Always returns latest data available

### Multi-Layer Pipeline

**Bronze Layer** (Raw Data):
- Exact copy of Stellar Hubble tables
- No transformations
- Foundation for all downstream analytics

**Silver Layer** (Analytics-Ready):
- Denormalized, enriched data
- Optimized for common queries
- Pre-computed aggregations
- Account snapshots, token transfers, enriched operations

### Services

1. **stellar-postgres-ingester**: Streams ledgers from Stellar RPC → PostgreSQL
2. **postgres-ducklake-flusher**: Archives Bronze hot → cold every 3 hours
3. **bronze-silver-transformer**: Transforms Bronze → Silver analytics
4. **silver-cold-flusher**: Archives Silver hot → cold every 3 hours
5. **stellar-query-api**: Serves unified hot+cold queries via REST API

---

## 🔧 Common Operations

### Check System Health

```bash
ssh root@<DROPLET_IP>

# Quick health check
curl http://localhost:8088/health  # Ingester
curl http://localhost:8090/health  # Bronze flusher
curl http://localhost:8093/health  # Silver transformer
curl http://localhost:8095/health  # Silver flusher
curl http://localhost:8092/health  # Query API
```

### Query the API

```bash
# From local machine (via SSH tunnel)
ssh -L 8092:localhost:8092 root@<DROPLET_IP>

# Get account current state
curl 'http://localhost:8092/api/v1/silver/accounts/current?account_id=GXXX...'

# Get top accounts
curl 'http://localhost:8092/api/v1/silver/accounts/top?limit=10'

# Get recent operations
curl 'http://localhost:8092/api/v1/silver/operations/enriched?limit=10'
```

See `QUICK_REFERENCE.md` for all endpoints and examples.

### Reset the System

```bash
# Full reset (deletes ALL data)
# See OPERATIONS_GUIDE.md "Reset Procedures" for details

# Stop services
nomad job stop -purge stellar-postgres-ingester
nomad job stop -purge postgres-ducklake-flusher
nomad job stop -purge bronze-silver-transformer
nomad job stop -purge silver-cold-flusher
nomad job stop -purge stellar-query-api

# Clear databases (see OPERATIONS_GUIDE.md for commands)

# Redeploy (see DEPLOYMENT_GUIDE.md Phase 5)
```

---

## 📊 Data Flow Timeline

### Ingestion to Query

```
T+0s     Ledger closes on Stellar blockchain
T+1s     Ingester receives ledger via RPC
T+2s     Written to PostgreSQL stellar_hot
T+3s     Transformer processes to silver_hot
T+5s     Available in Query API (hot buffer)
         ✅ User can query account state

T+3h     Bronze flusher archives to Parquet (cold)
T+3h+5m  Silver flusher archives to Parquet (cold)
         ✅ Historical data preserved long-term
```

### Query Response Time

**Account current** (hot only):
- Hot hit: 50-100ms
- Cold fallback: 200-500ms

**Account history** (hot + cold):
- Merged query: 300-700ms

**Operations** (hot + cold):
- Recent (hot): 100-200ms
- Historical (cold): 400-800ms

---

## 🚨 Troubleshooting Quick Reference

| Issue | Quick Fix |
|-------|-----------|
| Ingester stopped | Check Stellar RPC, restart: `nomad job restart stellar-postgres-ingester` |
| No new data in API | Check hot buffer has data: `psql $PG_SILVER -c "SELECT COUNT(*) FROM accounts_current;"` |
| Out of disk space | Clean Docker: `docker system prune -a`, expand volume |
| High memory usage | Increase job memory limits in Nomad job files |
| Can't connect to API | Check SSH tunnel or firewall rules |
| Flusher failing | Check B2 credentials, verify catalog schema |

See `OPERATIONS_GUIDE.md` for detailed troubleshooting.

---

## 🎓 Learning Path

### New to the Platform?

1. **Understand the architecture** → Read this README
2. **See it in action** → Run queries from `QUICK_REFERENCE.md`
3. **Learn the API** → Read `SILVER_API_DOCUMENTATION.md`
4. **Deploy your own** → Follow `DEPLOYMENT_GUIDE.md`

### Already Running?

1. **Daily checks** → `OPERATIONS_GUIDE.md` - Daily Operations
2. **Troubleshooting** → `OPERATIONS_GUIDE.md` - Troubleshooting
3. **Scaling up** → `OPERATIONS_GUIDE.md` - Scaling and Performance

### Planning Production?

1. **Environment strategy** → Read `ENVIRONMENT_STRATEGY.md`
2. **Cost planning** → Review cost breakdowns above
3. **Deployment** → Follow `DEPLOYMENT_GUIDE.md` for mainnet
4. **Operations** → Set up monitoring from `OPERATIONS_GUIDE.md`

---

## 🔐 Security Considerations

### Credentials Management

- **Never commit credentials** to git
- Use Nomad Variables for secrets (encrypted at rest)
- Rotate database passwords quarterly
- Restrict SSH access to your IP only

### Network Security

- PostgreSQL: VPC-only (no public internet)
- API: Behind firewall or reverse proxy with SSL
- Nomad: Restrict to trusted IPs
- B2: Use application keys with limited scope

### Access Control

- Droplet: SSH key authentication only (no passwords)
- Nomad: Enable ACLs for production
- PostgreSQL: Separate users for services (least privilege)
- B2: Separate buckets per environment

---

## 📈 Monitoring Recommendations

### Metrics to Track

- **Ingestion lag**: Current ledger vs Stellar network
- **Hot buffer size**: Should stay ~3 hours of data
- **Flush success rate**: Bronze and Silver flushers
- **API latency**: P50, P95, P99 response times
- **Database size**: Growth rate and capacity planning
- **B2 storage**: Cost and growth trends

### Alerts to Set

- Ingester stopped for > 5 minutes
- Hot buffer > 6 hours of data (flusher failing)
- API error rate > 1%
- Disk usage > 80%
- Database connections > 80% of max

Tools: Prometheus + Grafana (see `OPERATIONS_GUIDE.md` for setup)

---

## 🤝 Support and Resources

### Documentation

- **Stellar**: stellar.org/developers
- **DuckDB**: duckdb.org/docs
- **Nomad**: nomadproject.io/docs
- **DigitalOcean**: docs.digitalocean.com

### Community

- **Stellar Discord**: discord.gg/stellar
- **Stellar Stack Exchange**: stellar.stackexchange.com

### This Platform

- Issues/Questions: Create issues in repository
- Updates: Check git log for latest changes

---

## 📝 Document Versions

| Document | Last Updated | Version |
|----------|--------------|---------|
| README_DEPLOYMENT.md | 2026-01-01 | 1.0 |
| DEPLOYMENT_GUIDE.md | 2026-01-01 | 1.0 |
| OPERATIONS_GUIDE.md | 2026-01-01 | 1.0 |
| ENVIRONMENT_STRATEGY.md | 2026-01-01 | 1.0 |
| SILVER_API_DOCUMENTATION.md | 2026-01-01 | 1.0 |
| QUICK_REFERENCE.md | 2026-01-01 | 1.0 |

---

## ✅ Next Steps

1. **Read** `ENVIRONMENT_STRATEGY.md` to decide: testnet only or prod+non-prod
2. **Deploy** following `DEPLOYMENT_GUIDE.md` if starting fresh
3. **Test** using `QUICK_REFERENCE.md` queries
4. **Operate** using `OPERATIONS_GUIDE.md` for daily tasks
5. **Monitor** and optimize as described above

---

## 🎉 You're Ready!

This platform provides a complete Stellar data pipeline from ingestion to analytics. The hot+cold architecture balances real-time access with cost-efficient long-term storage.

For questions or issues, refer to the appropriate document above or create an issue in the repository.

Happy querying! 🚀
