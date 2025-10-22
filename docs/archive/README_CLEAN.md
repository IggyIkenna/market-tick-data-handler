# Market Tick Data Handler

A streamlined repository for downloading, processing, and managing market tick data from Tardis.dev with proper GCS partitioning.

## 🎯 Core Functions

This repository provides exactly **5 core functions**:

### 1. **Instrument Definition Generation**
Download and process Tardis symbol data from `https://api.tardis.dev/v1/exchanges/{exchange}` into instrument definitions/keys and upload to GCS with proper partitioning.

### 2. **Download Target Definition** 
Pull GCS instrument definition data and use it to define which instruments we want to download Tardis data for using Tardis exchange and symbol.

### 3. **Data Download & Upload**
Download Tardis CSV.gz data locally then push to GCS following the `UNIVERSAL_PARTITIONING_STRATEGY.md` format.

### 4. **Missing Data Validation**
Create missing data check comparing instrument definitions vs availability of data in GCS for those instruments over a query timeframe.

### 5. **Orchestration**
Orchestrate Tardis data download in local deployment or cloud VM deployment with sharding by date.

## 📁 Repository Structure

```
market-tick-data-handler/
├── src/                           # Core modules
│   ├── instrument_processor/      # Function 1: Instrument definitions
│   │   ├── canonical_key_generator.py
│   │   └── gcs_uploader.py
│   ├── data_downloader/           # Functions 2 & 3: Data download
│   │   ├── instrument_reader.py
│   │   ├── download_orchestrator.py
│   │   └── tardis_connector.py
│   ├── data_validator/            # Function 4: Data validation
│   │   └── data_validator.py
│   └── orchestrator/              # Function 5: Orchestration
│       └── market_data_orchestrator.py
├── deploy                       # Utility scripts
├── deploy/                        # Deployment configurations
├── docs/                          # Documentation
├── archive/                       # Deprecated files
├── main.py                        # Main entry point
├── config.py                      # Configuration
└── requirements.txt               # Dependencies
```

## 🚀 Quick Start

### Installation

```bash
pip install -r requirements.txt
```

### Configuration

1. Copy `env.example` to `.env`
2. Add your Tardis API key
3. Configure GCS bucket settings

### Usage

```bash
# Run the complete pipeline
python main.py

# Or run individual functions programmatically
from src.orchestrator import MarketDataOrchestrator

orchestrator = MarketDataOrchestrator("your-gcs-bucket", "your-api-key")
results = orchestrator.run_local_deployment()
```

## 📊 Data Partitioning - BigQuery Optimized (Max 4 Levels)

All data follows the `UNIVERSAL_PARTITIONING_STRATEGY.md` with **BigQuery clustering optimization** and **maximum 4 levels of depth**:

```
gs://your-bucket/
├── instrument_availability/        # Instrument definitions (max 4 levels)
│   ├── by_date/day-2024-01-15/instruments.parquet
│   ├── by_venue/day-2024-01-15/instruments.parquet
│   ├── by_type/day-2024-01-15/instruments.parquet
│   └── instruments_20240115.parquet
└── raw_tick_data/                 # Market data (max 4 levels)
    ├── by_date/data_type-trades/venue-deribit/deribit:Perp:BTC-USDT.parquet
    ├── by_venue/data_type-trades/venue-deribit/deribit:Perp:BTC-USDT.parquet
    └── by_type/data_type-trades/type-perp/deribit:Perp:BTC-USDT.parquet
```

**🚀 BigQuery Clustering Benefits:**
- **Max 4 levels**: partition_type, data_type/day, venue/type, instrument_key
- **No over-partitioning**: Optimal depth for GCS and BigQuery
- **Fast queries**: One file per directory enables lightning-fast GCS queries
- **3-column clustering**: Respects BigQuery's clustering limit + day partition

## 🔧 Core Components

### Instrument Processor
- `CanonicalInstrumentKeyGenerator`: Generates standardized instrument keys
- `InstrumentGCSUploader`: Uploads definitions with proper partitioning

### Data Downloader  
- `InstrumentReader`: Reads instrument definitions from GCS
- `DownloadOrchestrator`: Downloads Tardis data and uploads to GCS
- `TardisConnector`: Handles Tardis API interactions

### Data Validator
- `DataValidator`: Compares expected vs actual data availability

### Orchestrator
- `MarketDataOrchestrator`: Main pipeline coordinator

## 📈 Performance

- **Instrument Generation**: ~293K Deribit symbols processed in seconds
- **Data Download**: Optimized with single API call per exchange
- **GCS Upload**: BigQuery-optimized partitioning with max 4 levels for lightning-fast queries
- **Validation**: Efficient missing data detection
- **BigQuery Ready**: Respects 3-column clustering limit + day partition for maximum performance

## 🏗️ Deployment

### Local Deployment
```bash
python main.py
```

### Cloud VM Deployment
```bash
# Use existing VM orchestration scripts
./deploy/orchestration/deploy_vms.sh
```

## 📚 Documentation

- `docs/UNIVERSAL_PARTITIONING_STRATEGY.md`: Data partitioning strategy
- `docs/INSTRUMENT_KEY.md`: Instrument key specification
- `archive/`: Deprecated scripts and documentation

## 🧹 Cleanup

Deprecated scripts have been moved to `archive/deprecated_deploy`:
- Debug scripts
- Test scripts  
- Optimization demos
- Working prototypes

The repository now focuses solely on the 5 core functions with a clean, modular architecture.
