# Developer Setup Guide

This guide helps developers set up the market data handler for local development and testing.

## Complete Data Pipeline

The system follows a comprehensive data pipeline from raw tick data to processed features:

### VM Deployment Pipeline (Scheduled Daily at 8 AM UTC)

```bash
# 1. Generate instrument definitions
python -m market_data_tick_handler.main. --mode instruments --start-date 2023-05-23 --end-date 2023-05-23

# 2. Generate missing data reports
python -m market_data_tick_handler.main. --mode missing-tick-reports --start-date 2023-05-23 --end-date 2023-05-23

# 3. Download missing tick data (default mode)
python -m market_data_tick_handler.main. --mode download --start-date 2023-05-23 --end-date 2023-05-23 --venues binance

# 4. Process candles with HFT features
python -m market_data_tick_handler.main. --mode candle-processing --start-date 2023-05-23 --end-date 2023-05-23

# 5. Upload candles to BigQuery
python -m market_data_tick_handler.main. --mode bigquery-upload --start-date 2023-05-23 --end-date 2023-05-23

# 6. Process MFT features
python -m market_data_tick_handler.main. --mode mft-processing --start-date 2023-05-23 --end-date 2023-05-23
```

### Data Storage Architecture

- **GCS Storage**:
  - Raw tick data (optimized Parquet with timestamp partitioning)
  - Processed candles (15s, 1m, 5m, 15m, 1h, 4h, 24h timeframes)
  - MFT features (1m+ timeframes)
  - Instrument definitions

- **BigQuery Storage**:
  - Candles with HFT features (one table per timeframe)
  - Real-time streaming data

- **Package Usage**:
  - Features service imports package to query BigQuery
  - Gets candle data with HFT features
  - Processes additional MFT features
  - Pushes features to GCS for backtesting

## Quick Start (No GCP Required)

For developers who don't have GCP access, you can use mock data mode:

```bash
# 1. Clone the repository
git clone <repository-url>
cd market-tick-data-handler

# 2. Set up Python environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt

# 3. Set up mock data mode
export USE_MOCK_DATA=true
export MOCK_DATA_PATH=./mock_data
export GCS_BUCKET=mock-bucket

# 4. Run tests with mock data
python examples/standalone_performance_test.py

# 5. Test download modes (with mock data)
./deploy/local/run-main.sh download --start-date 2023-05-23 --end-date 2023-05-23 --venues binance
./deploy/local/run-main.sh download --start-date 2023-05-23 --end-date 2023-05-23 --venues binance --force
```

## Full Setup (With GCP Access)

### Option 1: Use Existing Credentials (Recommended)

If you have access to the GCP project, you can use the existing credentials:

```bash
# 1. Get the credentials file from a team member
# (This file is gitignored for security)

# 2. Set environment variables
export GOOGLE_APPLICATION_CREDENTIALS=./central-element-323112-e35fb0ddafe2.json
export GCP_PROJECT_ID=central-element-323112
export GCS_BUCKET=market-data-tick

# 3. Test download modes
# Missing data mode (default) - only downloads missing data
./deploy/local/run-main.sh download --start-date 2023-05-23 --end-date 2023-05-23 --venues binance --instrument-types SPOT_PAIR --data-types trades --max-instruments 5

# Force download mode - downloads all data regardless of existing files
./deploy/local/run-main.sh download --start-date 2023-05-23 --end-date 2023-05-23 --venues binance --instrument-types SPOT_PAIR --data-types trades --max-instruments 5 --force

# 4. Run performance tests
python examples/performance_comparison_test.py
```

### Option 2: Create Your Own Service Account

If you have GCP access but want your own service account, follow the security requirements from [AUTHENTICATION_STRATEGY.md](AUTHENTICATION_STRATEGY.md):

#### Required IAM Roles

**For Data Download (Production)**:
- `roles/storage.objectAdmin` - Upload/download data to/from GCS
- `roles/secretmanager.secretAccessor` - Access Tardis API key
- `roles/bigquery.dataEditor` - Upload processed data to BigQuery

**For Data Reading Only (Development)**:
- `roles/storage.objectViewer` - Read data from GCS
- `roles/secretmanager.secretAccessor` - Access Tardis API key (optional)

#### Service Account Setup

```bash
# 1. Create service account
gcloud iam service-accounts create market-data-dev \
  --display-name="Market Data Development" \
  --description="Service account for development testing"

# 2. Assign minimal IAM roles for testing
gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:market-data-dev@your-project-id.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"

# 3. Create and download key
gcloud iam service-accounts keys create market-data-dev-key.json \
  --iam-account=market-data-dev@your-project-id.iam.gserviceaccount.com

# 4. Set environment variables
export GOOGLE_APPLICATION_CREDENTIALS=./market-data-dev-key.json
export GCP_PROJECT_ID=your-project-id
export GCS_BUCKET=market-data-tick
```

## Download Modes

The system supports two download modes:

### Missing Data Mode (Default)
Only downloads data that is marked as missing in the missing data reports:

```bash
./deploy/local/run-main.sh download --start-date 2023-05-23 --end-date 2023-05-23 --venues binance
```

### Force Download Mode
Downloads all data regardless of existing files (useful for testing or re-downloading):

```bash
./deploy/local/run-main.sh download --start-date 2023-05-23 --end-date 2023-05-23 --venues binance --force
```

## Service Account Security

For production deployments, follow these security best practices:

1. **Principle of Least Privilege**: Only assign required roles
2. **Key Rotation**: Rotate service account keys regularly
3. **Conditional Access**: Use IAM conditions for time/network restrictions
4. **Audit Logging**: Enable Cloud Audit Logs for monitoring
5. **Key Management**: Store keys securely, never commit to git

See [AUTHENTICATION_STRATEGY.md](AUTHENTICATION_STRATEGY.md) for detailed security requirements.

## Troubleshooting

### Common Issues

1. **"No download targets found"**: Check that instrument definitions exist for the date
2. **"GCS authentication failed"**: Verify credentials and bucket permissions
3. **"Memory threshold exceeded"**: Reduce `--max-instruments` or increase system memory
4. **"Tardis API key not found"**: Set `TARDIS_API_KEY` environment variable

### Getting Help

- Check logs for detailed error messages
- Verify environment variables are set correctly
- Ensure GCP permissions are properly configured
- Use mock data mode for testing without GCP access

### Option 3: Use Secret Manager (Production)

For production or when Secret Manager is available:

```bash
# 1. Enable Secret Manager API (if not already enabled)
gcloud services enable secretmanager.googleapis.com

# 2. Create Tardis API key secret
echo "TD.l6pTDHIcc9fwJZEz.Y7cp7lBSu-pkPEv.55-ZZYvZqtQL7hY.C2-pXYQ6yebRF7M.DwzJ7MFPry-C7Yp.xe1j" | gcloud secrets create tardis-api-key --data-file=-

# 3. Set environment variables
export USE_SECRET_MANAGER=true
export GCP_PROJECT_ID=central-element-323112
export GCS_BUCKET=market-data-tick

# 4. Run the system
python examples/performance_comparison_test.py
```

## Authentication Modes

The system supports multiple authentication modes:

### 1. Mock Mode (No GCP Required)
```bash
export USE_MOCK_DATA=true
export MOCK_DATA_PATH=./mock_data
export GCS_BUCKET=mock-bucket
```

### 2. Environment Variables Mode
```bash
export TARDIS_API_KEY=TD.your_api_key_here
export GOOGLE_APPLICATION_CREDENTIALS=./your-credentials.json
export GCP_PROJECT_ID=your-project-id
export GCS_BUCKET=your-bucket
```

### 3. Secret Manager Mode (Production)
```bash
export USE_SECRET_MANAGER=true
export GOOGLE_APPLICATION_CREDENTIALS=./your-credentials.json
export GCP_PROJECT_ID=your-project-id
export GCS_BUCKET=your-bucket
```

## Testing Different Scenarios

### 1. Performance Testing
```bash
# Mock data performance test
python examples/standalone_performance_test.py

# Real data performance test (requires GCP)
python examples/performance_comparison_test.py
```

### 2. Data Download
```bash
# Download BTC-USDT perps for May 23rd, 2023
./deploy/local/run-main.sh download \
    --start-date 2023-05-23 \
    --end-date 2023-05-23 \
    --venues binance \
    --instrument-types perp \
    --data-types trades \
    --max-instruments 1
```

### 3. Package Usage
```bash
# Test package functionality
python examples/package_usage_examples.py
```

## Troubleshooting

### Common Issues

#### 1. GCP Credentials Not Found
```
ValueError: GCP credentials file not found: central-element-323112-e35fb0ddafe2.json
```

**Solution**: Use mock data mode or get credentials from team member
```bash
export USE_MOCK_DATA=true
```

#### 2. Secret Manager Not Available
```
WARNING: Secret Manager utilities not available - falling back to environment variables
```

**Solution**: This is normal, the system will fall back to environment variables

#### 3. Tardis API Key Missing
```
ValueError: TARDIS_API_KEY not found
```

**Solution**: Set the API key in environment variables
```bash
export TARDIS_API_KEY=TD.your_api_key_here
```

### Getting Help

1. **Check the logs**: Look for error messages in the console output
2. **Use mock mode**: For testing without GCP access
3. **Ask team members**: For credentials or GCP access
4. **Check documentation**: See other docs in the `docs/` folder

## File Sharing

### Credentials File
The `central-element-323112-e35fb0ddafe2.json` file is gitignored for security. To share it:

1. **Share separately**: Send via secure channel (not in git)
2. **Restore from previous commit**: If it was committed before gitignore
3. **Create new credentials**: Use your own service account

### Restoring from Previous Commit
If the credentials file was committed before being gitignored:

```bash
# Find the commit where it was added
git log --oneline --follow -- central-element-323112-e35fb0ddafe2.json

# Restore from that commit
git checkout <commit-hash> -- central-element-323112-e35fb0ddafe2.json
```

## Security Notes

- **Never commit credentials** to git
- **Use environment variables** for API keys when possible
- **Use Secret Manager** for production deployments
- **Share credentials securely** via encrypted channels
- **Rotate credentials regularly** for security

## Next Steps

1. **Choose your setup method** based on your access level
2. **Run the performance test** to verify everything works
3. **Download real data** to test with actual GCS data
4. **Explore the examples** to understand the package usage
5. **Check the documentation** for detailed usage guides
