# Market Data Tick Handler - Budget Analysis

## Overview

This document outlines the expected costs for the Market Data Tick Handler using Google Cloud Platform infrastructure. The system has been refactored into a package/library architecture with VM deployments for batch processing and Node.js services for live streaming. All costs are based on current GCP pricing for the asia-northeast1 region as of December 2024.

## Infrastructure Specifications

### VM Configuration (Optimized)
- **VM Type**: `e2-standard-8`
- **Specs**: 8 vCPUs, 32GB RAM, 16 Gbps network bandwidth
- **Disk**: 100GB persistent disk (pd-standard)
- **Region**: asia-northeast1-c
- **Hourly Cost**: ~$0.30 USD/hour

### Performance Configuration
- **Workers**: 8 parallel workers per VM
- **Concurrent Downloads**: 500 (Tardis API)
- **Parallel Uploads**: 200 (GCS)
- **Batch Size**: 5000 instruments
- **Memory Threshold**: 85%
- **Network Utilization**: ~1% of 16 Gbps (significant optimization potential)

### Architecture Components
- **VM Deployments**: Batch processing jobs (instruments, downloads, candles, BigQuery uploads)
- **Node.js Services**: Live streaming (not VM-deployed, runs on local/container infrastructure)
- **Package/Library**: Data access for downstream services (no additional infrastructure cost)

## Cost Breakdown

### 1. Initial Historical Data Backfill (One-Time)

**Data Range**: May 23, 2023 → October 22, 2024 (518 days)

#### Deployment Strategy
- **100 VMs** running in parallel
- **~5.18 days per VM** (518 days ÷ 100 VMs)
- **Estimated Duration**: 6-12 hours (with optimized network configuration)

#### Cost Calculation
```
VM Cost: $0.30/hour × 100 VMs × 12 hours = $360
Disk Cost: $0.04/GB/month × 100GB × 100 VMs × 0.5 days = $6.67
Network Egress: Minimal (data goes to GCS in same region)
Total Backfill Cost: ~$367 (one-time)
```

### 2. Daily Operations (Ongoing)

**Data Range**: 1 day of new data per day

#### Deployment Strategy
- **1 VM** per day for new data
- **4 hours per day** (estimated based on current performance)
- **Automated scheduling** (can be triggered daily)

#### Cost Calculation
```
Daily VM Cost: $0.30/hour × 1 VM × 4 hours = $1.20/day
Daily Disk Cost: $0.04/GB/month × 100GB × 1 VM × 0.17 days = $0.02/day
Total Daily Cost: ~$1.22/day
```

#### Annual Ongoing Costs
```
Annual VM Cost: $1.22/day × 365 days = $445/year
Peak Month (31 days): $1.22 × 31 = $38/month
```

### 3. Candle Processing (New)

**Data Range**: Process historical tick data into candles with HFT features

#### Deployment Strategy
- **1 VM** per day for candle processing
- **2 hours per day** (estimated for candle generation)
- **Runs after data download** (sequential processing)

#### Cost Calculation
```
Daily VM Cost: $0.30/hour × 1 VM × 2 hours = $0.60/day
Daily Disk Cost: $0.04/GB/month × 100GB × 1 VM × 0.08 days = $0.01/day
Total Daily Cost: ~$0.61/day
```

### 4. BigQuery Upload (New)

**Data Range**: Upload processed candles to BigQuery for analytics

#### Deployment Strategy
- **1 VM** per day for BigQuery uploads
- **1 hour per day** (estimated for upload processing)
- **Runs after candle processing** (sequential processing)

#### Cost Calculation
```
Daily VM Cost: $0.30/hour × 1 VM × 1 hour = $0.30/day
Daily Disk Cost: $0.04/GB/month × 100GB × 1 VM × 0.04 days = $0.005/day
Total Daily Cost: ~$0.31/day
```

### 5. Live Streaming (No VM Cost)

**Data Range**: Real-time streaming to BigQuery

#### Deployment Strategy
- **Node.js services** (not VM-deployed)
- **Runs on local/container infrastructure**
- **No additional GCP VM costs**

#### Cost Calculation
```
VM Cost: $0 (runs locally or on existing infrastructure)
BigQuery Cost: ~$0.01/GB for streaming data
Total Daily Cost: ~$0.01/day (BigQuery only)
```

### 6. Aggressive Batching Cost Optimization (NEW)

**Strategy**: Batch streaming data to minimize BigQuery costs

#### Batching Configuration
- **Batch Size**: 1,000 rows (BigQuery maximum limit)
- **Batch Timeout**: 1 minute (60 seconds)
- **Size Limit**: 9MB per batch (BigQuery 10MB limit with buffer)
- **Flush Triggers**: Size limit OR time limit (whichever comes first)

#### Cost Comparison

**Per-Row Streaming (Original)**
```
BTC-USDT Trades: ~4,000 trades/hour
BigQuery Cost: $0.01 per 200MB (charged per row)
Hourly Cost: ~$0.20/hour
Daily Cost: ~$4.80/day
Monthly Cost: ~$144/month
```

**Aggressive Batching (Optimized)**
```
BTC-USDT Trades: ~4,000 trades/hour
Batch Size: 1,000 rows per batch
Batch Frequency: ~4 batches/hour (every 15 minutes)
BigQuery Cost: $0.01 per 200MB (charged per batch)
Hourly Cost: ~$0.02/hour
Daily Cost: ~$0.48/day
Monthly Cost: ~$14.40/month
```

#### Cost Savings
```
Per-Row vs Batching: 90% cost reduction
Monthly Savings: $144 - $14.40 = $129.60/month
Annual Savings: $1,555/year per instrument
```

#### Multi-Instrument Scaling
```
1 Instrument (BTC-USDT): $14.40/month
10 Instruments: $144/month
50 Instruments: $720/month
100 Instruments: $1,440/month
```

#### Tardis API Costs (Unchanged)
```
WebSocket Streaming: ~$0.10/GB ingress
BTC-USDT: ~2GB/day = $0.20/day
Monthly Tardis Cost: ~$6/month per instrument
Annual Tardis Cost: ~$72/year per instrument
```

#### Total Streaming Costs (Per Instrument)
```
BigQuery (Batched): $14.40/month
Tardis API: $6.00/month
Total: $20.40/month per instrument
Annual: $244.80/year per instrument
```

## Total Cost Summary

| Component | Duration | Cost | Notes |
|-----------|----------|------|-------|
| **Historical Backfill** | One-time | $367 | 518 days of data across all venues |
| **Daily Data Download** | Ongoing | $445/year | New data processing |
| **Daily Candle Processing** | Ongoing | $223/year | Candle generation with HFT features |
| **Daily BigQuery Upload** | Ongoing | $113/year | Upload processed candles to BigQuery |
| **Live Streaming (Per-Row)** | Ongoing | $4/year | BigQuery costs only (no VM costs) |
| **Live Streaming (Batched)** | Ongoing | $245/year | BigQuery + Tardis API (1 instrument) |
| **Year 1 Total (Per-Row)** | | **$1,152** | Complete historical + 1 year daily |
| **Year 1 Total (Batched)** | | **$1,393** | Complete historical + 1 year daily + streaming |
| **Year 2+ Total (Per-Row)** | | **$785/year** | All daily operations |
| **Year 2+ Total (Batched)** | | **$1,026/year** | All daily operations + streaming (1 instrument) |

### Multi-Instrument Streaming Costs (Batched)

| Instruments | Monthly Cost | Annual Cost | Notes |
|-------------|--------------|-------------|-------|
| **1 Instrument** | $20.40 | $245 | BTC-USDT example |
| **10 Instruments** | $204 | $2,448 | Small portfolio |
| **50 Instruments** | $1,020 | $12,240 | Medium portfolio |
| **100 Instruments** | $2,040 | $24,480 | Large portfolio |

## Cost Optimization Opportunities

### 1. Aggressive Batching (90% BigQuery Cost Reduction) ⭐ **IMPLEMENTED**
- **Per-Row Streaming**: $144/month per instrument
- **Batched Streaming**: $14.40/month per instrument
- **Savings**: $129.60/month per instrument (90% reduction)
- **Annual Savings**: $1,555/year per instrument
- **Implementation**: 1-minute batches, 1,000 rows max, 9MB size limit

### 2. Preemptible VMs (70% Cost Reduction)
- **Backfill**: $367 → $110 (savings: $257)
- **Daily**: $445/year → $134/year (savings: $311/year)
- **Risk**: VMs can be terminated (acceptable for batch processing)

### 3. Spot VMs (Up to 90% Cost Reduction)
- **Backfill**: $367 → $37 (savings: $330)
- **Daily**: $445/year → $45/year (savings: $400/year)
- **Risk**: Higher termination probability

### 4. Regional Optimization
- **Current**: asia-northeast1 (premium region)
- **Alternative**: us-central1 (standard pricing, ~20% cheaper)
- **Consideration**: Network latency to Tardis API

### 5. VM Right-Sizing
- **Current**: Using <1% of network capacity
- **Opportunity**: Could process 2-3 days per VM with same cost
- **Potential**: 50 VMs instead of 100 for backfill

### 6. Storage Tier Optimization
- **Current**: Standard storage ($0.023/GB/month)
- **Nearline**: $0.013/GB/month (60% cheaper, accessed <1/month)
- **Coldline**: $0.007/GB/month (70% cheaper, accessed <1/quarter)
- **Archive**: $0.004/GB/month (83% cheaper, accessed <1/year)

## Tardis API Costs (External)

### Rate Limits & Quotas
- **Current Limit**: 1,000,000 calls per day per VM
- **100 VMs**: 100,000,000 calls per day total
- **Estimated Usage**: ~50,000-100,000 calls per VM per day

### Tardis Pricing (Estimated)
- **API Calls**: Typically $0.001-$0.01 per 1000 calls
- **Daily Cost**: $50-$1000/day for 100 VMs (depends on Tardis pricing tier)
- **Annual Cost**: $18,250-$365,000/year

**Note**: Tardis API costs are the primary expense - GCP infrastructure is minimal in comparison.

## Storage Costs (GCS)

### Historical Data Storage
- **Estimated Size**: 50-100 TB for 518 days across all venues
- **Standard Storage**: $0.023/GB/month × 75,000 GB = $1,725/month
- **Nearline Storage**: $0.013/GB/month × 75,000 GB = $975/month (if accessed <1/month)
- **Coldline Storage**: $0.007/GB/month × 75,000 GB = $525/month (if accessed <1/quarter)

### Daily Data Growth
- **New Data**: ~100-200 GB/day
- **Monthly Growth**: ~3-6 TB/month
- **Annual Storage Growth**: ~36-73 TB/year

## Budget Recommendations

### Conservative Budget (Standard Storage)
```
Year 1:
- Infrastructure: $812
- Storage (75TB): $1,725/month × 12 = $20,700
- Tardis API: $50,000-$100,000 (estimated)
Total Year 1: ~$71,512-$121,512

Ongoing Annual:
- Infrastructure: $445
- Storage (growing): $25,000-$30,000
- Tardis API: $50,000-$100,000
Total Ongoing: ~$75,445-$130,445/year
```

### Optimized Budget (Preemptible VMs + Nearline Storage)
```
Year 1:
- Infrastructure: $244 (70% savings)
- Storage (75TB Nearline): $975/month × 12 = $11,700
- Tardis API: $50,000-$100,000
Total Year 1: ~$61,944-$111,944

Ongoing Annual:
- Infrastructure: $134
- Storage: $15,000-$20,000
- Tardis API: $50,000-$100,000
Total Ongoing: ~$65,134-$120,134/year
```

## Key Takeaways

1. **Aggressive batching reduces BigQuery costs by 90%** - from $144/month to $14.40/month per instrument
2. **Infrastructure costs are minimal** compared to Tardis API and storage
3. **Network bandwidth is severely underutilized** - could process much faster
4. **Preemptible VMs offer 70% savings** with minimal risk for batch processing
5. **Storage strategy** (Standard vs Nearline vs Coldline) significantly impacts costs
6. **Tardis API costs dominate** the budget - negotiate volume pricing
7. **Batching strategy scales linearly** - 100 instruments = $2,040/month (vs $14,400/month per-row)

## Monitoring & Alerts

Set up budget alerts at:
- **Daily**: >$150 (infrastructure + API)
- **Monthly**: >$10,000 (total including storage)
- **Annual**: >$130,000 (full operational budget)

---

*Last Updated: October 24, 2025*
*VM Configuration: e2-standard-8, optimized for 16 Gbps network*
