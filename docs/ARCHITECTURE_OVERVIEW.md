# Market Data Tick Handler - New Architecture Overview

## Problem Solved

**Before**: Orchestration was scattered across multiple files at the repo root, making containerization and VM deployment difficult.

**After**: Centralized `src/main.py` entry point with environment-based configuration and clean argument parsing.

## New Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    src/main.py (Entry Point)                   │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐   │
│  │  Mode Handler   │ │  Mode Handler   │ │  Mode Handler   │   │
│  │  Instruments    │ │  Download       │ │  Full Pipeline  │   │
│  └─────────────────┘ └─────────────────┘ └─────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Configuration Layer                          │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐               │
│  │   .env      │ │  config.py  │ │  CLI Args   │               │
│  │   files     │ │  (enhanced) │ │  (parsing)  │               │
│  └─────────────┘ └─────────────┘ └─────────────┘               │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Core Components                              │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐   │
│  │  Canonical Key  │ │  Download       │ │  GCS Uploader   │   │
│  │  Generator      │ │  Orchestrator   │ │                 │   │
│  └─────────────────┘ └─────────────────┘ └─────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Deployment Options                           │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐               │
│  │   Docker    │ │     VM      │ │   Local     │               │
│  │  (updated)  │ │ (enhanced)  │ │  (direct)   │               │
│  └─────────────┘ └─────────────┘ └─────────────┘               │
└─────────────────────────────────────────────────────────────────┘
```

## Key Improvements

### 1. Centralized Entry Point
- **Single Command**: `python -m src.main --mode <mode>`
- **Consistent Interface**: Same arguments across all modes
- **Better Error Handling**: Comprehensive validation and error messages

### 2. Environment-Based Configuration
- **`.env` Files**: Easy configuration management
- **Environment Variables**: Override any setting
- **Mode-Specific Defaults**: Different defaults for different operations

### 3. Clean Mode Separation
- **Instruments Mode**: Generate and upload instrument definitions
- **Download Mode**: Download and upload tick data
- **Full Pipeline Mode**: Complete workflow

### 4. Docker-Friendly
- **Updated Dockerfiles**: Use main.py entry point
- **Environment Variables**: Pass configuration via env vars
- **Flexible Arguments**: Override any setting at runtime

## Usage Examples

### Local Development
```bash
# Generate instruments
python -m src.main --mode instruments --start-date 2023-05-23 --end-date 2023-05-25

# Download tick data
python -m src.main --mode download --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit

# Full pipeline
python -m src.main --mode full-pipeline --start-date 2023-05-23 --end-date 2023-05-25
```

### Docker Deployment
```bash
# Instrument generation
docker run --env-file .env market-tick-handler \
  python -m src.main --mode instruments --start-date 2023-05-23 --end-date 2023-05-25

# Tick data download
docker run --env-file .env market-tick-handler \
  python -m src.main --mode download --start-date 2023-05-23 --end-date 2023-05-25
```

### VM Deployment
```bash
# Use environment variables for configuration
export TARDIS_API_KEY="TD.your_key"
export GCS_BUCKET="your-bucket"
export INSTRUMENT_START_DATE="2023-05-23"
export INSTRUMENT_END_DATE="2023-05-25"

python -m src.main --mode instruments
```

## File Structure

```
src/
├── main.py                    # 🆕 Centralized entry point
├── instrument_processor/
│   ├── canonical_key_generator.py
│   └── gcs_uploader.py
├── data_downloader/
│   ├── download_orchestrator.py
│   └── tardis_connector.py
└── orchestrator/
    └── market_data_orchestrator.py

docker/
├── instrument-generation/
│   ├── Dockerfile            # 🔄 Updated to use main.py
│   └── docker-compose.yml    # 🔄 Enhanced with env vars
└── tardis-download/
    ├── Dockerfile            # 🔄 Updated to use main.py
    └── docker-compose.yml    # 🔄 Enhanced with env vars

scripts/
└── local/
    └── run-main.sh           # 🆕 Single convenience script for all operations

env.example                   # 🔄 Enhanced with comprehensive options
MAIN_USAGE.md                 # 🆕 Complete usage documentation
ARCHITECTURE_OVERVIEW.md      # 🆕 This file
```

## Migration Guide

### From Old Scripts
```bash
# OLD WAY
python run_fixed_local_instrument_generation.py
python scripts/vm_data_downloader.py

# NEW WAY
python -m src.main --mode instruments --start-date 2023-05-23 --end-date 2023-05-25
python -m src.main --mode download --start-date 2023-05-23 --end-date 2023-05-25 --venues deribit
```

### Docker Migration
```bash
# OLD WAY (hardcoded in Dockerfile)
CMD ["python", "run_fixed_local_instrument_generation.py"]

# NEW WAY (flexible arguments)
CMD ["python", "-m", "src.main", "--mode", "instruments", "--start-date", "2023-05-23", "--end-date", "2023-05-25"]
```

## Benefits

1. **Containerization**: Much easier to containerize and deploy
2. **Configuration**: Environment-based configuration is more flexible
3. **Maintainability**: Single entry point is easier to maintain
4. **Testing**: Easier to test different configurations
5. **Deployment**: Simpler VM deployment with environment variables
6. **Documentation**: Clear usage patterns and examples
7. **Flexibility**: Easy to add new modes or modify existing ones

## Next Steps

1. **Test the new main.py** with your existing data
2. **Update your deployment scripts** to use the new interface
3. **Migrate your .env files** to use the new configuration options
4. **Update your CI/CD pipelines** to use the new Docker commands
5. **Consider adding new modes** as needed (e.g., validation-only mode)
