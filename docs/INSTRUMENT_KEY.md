## Instrument Key Specification

### Purpose
- Define a canonical instrument identifier used across CeFi, DeFi, and TradFi for live and backtest.
- Enable unified routing, caching, and historical lifecycle snapshots.

### Canonical Key
- Format: VENUE:INSTRUMENT_TYPE:BASE_ASSET/QUOTE_ASSET-YYMMDD-STRIKE-OPTION_TYPE
- All components are UPPERCASE for consistency and readability
- Components:
  - VENUE: enum from existing `Venue` model (e.g., BINANCE, BINANCE-FUTURES, BYBIT, BYBIT-SPOT, OKX, OKX-FUTURES, OKX-SWAP, DERIBIT, AAVE_V3, ETHERFI, LIDO, WALLET, CME, NASDAQ)
  - INSTRUMENT_TYPE: enum: [BASETOKEN, SPOT_PAIR, SPOTASSET, PERPETUAL, FUTURE, OPTION, EQUITY, INDEX, LST, A_TOKEN, DEBT_TOKEN]
  - SYMBOL: string (venue-normalized symbol with specific formats per type)

### Symbol Formats by Instrument Type: **
- **SPOT_ASSET**: actual BASE_ASSET positions held on a specific venue (e.g., BTC, ETH, USDT)
- **SPOT_PAIR**: BASE_ASSET-QUOTE_ASSET (e.g., BTC-USDT) - routing pair for spot trading
- **PERPETUAL**: BASE_ASSET-QUOTE_ASSET (e.g., ETH-USDT)
- **FUTURE**: BASE_ASSET-QUOTE_ASSET-YYMMDD (e.g., BTC-USD-241225)
- **OPTION**: BASE_ASSET-QUOTE_ASSET-YYMMDD-STRIKE-OPTION_TYPE (e.g., BTC-USD-241225-50000-CALL)
** For DeFi Venues like AAVE_V3, ETHERFI, LIDO, there are addiitonal instrument types :LST, A_TOKEN, DEBT_TOKEN
** For TradFi Venues like CME, NASDAQ, there are addiitonal instrument types: EQUITY, INDEX

### Examples
- Balances (positions you hold):
  - BINANCE:SPOT_ASSET:BTC
  - WALLET:SPOT_ASSET:USDT
- Spot routing (optional, never stored as a position):
  - BINANCE:SPOT_PAIR:BTC-USDT
- Perpetuals:
  - BYBIT:PERPETUAL:ETH-USDT
- Futures (unified across crypto and tradfi):
  - DERIBIT:FUTURE:BTC-USD-241225
  - CME:FUTURE:ES-202412
- Options (unified):
  - DERIBIT:OPTION:BTC-USD-241225-50000-CALL
  - CME:OPTION:ES-202412-4500-CALL
- DeFi:
  - AAVE_V3:A_TOKEN:AUSDT
  - AAVE_V3:DEBT_TOKEN:DEBTWETH
  - ETHERFI:LST:WEETH

### Normalization Principles
- Keep routing/position identity stable; exchange/provider raw formats live in attributes.
- All components are UPPERCASE for consistency
- Option types use CALL/PUT instead of C/P for clarity
- Crypto spot:
  - SPOT_ASSET keys represent actual asset positions held (BTC, ETH, USDT).
- Spot Trading Flow:
  - SPOT_PAIR: Used for routing and execution (BINANCE:SPOT_PAIR:BTC-USDT)
  - When trading BTC-USDT, you execute via SPOT_PAIR but hold SPOT_ASSET positions
- Perps:
  - symbol is BASE_ASSET-QUOTE_ASSET (e.g., BTC-USDT) with attrs.inverse when settle != quote.
- Futures and Options (crypto + tradfi):
  - Use one instrument_type for each (FUTURE, OPTION).
  - attrs.expiry: precise UTC datetime. Keep raw exchange code in attrs.exchange_raw_symbol.
  - attrs.contract_size, attrs.tick_size, attrs.settle_asset, attrs.settlement_type, attrs.asset_class distinguish venues.
  - attrs.option_type: CALL/PUT (uppercase); attrs.strike as string to support formats like 5K.
- Equities:
  - NASDAQ:EQUITY:AAPL with attrs.mic, currency, lot_size.

### Attributes Schema (non-exhaustive)
- underlying: Optional[str]  // canonical reference (e.g., BTC-USDT) or another instrument_key
- base_asset: Optional[str]
- quote_asset: Optional[str]
- settle_asset: Optional[str]
- expiry: Optional[datetime]  // precise UTC
- strike: Optional[str]
- option_type: Optional["CALL","PUT"]
- contract_size: Optional[float]
- tick_size: Optional[float]
- min_size: Optional[float]
- settlement_type: Optional["cash","physical"]
- asset_class: ["crypto","traditional"]
- venue_type: ["exchange","protocol","wallet"]
- exchange_raw_symbol: Optional[str]
- ccxt_symbol: Optional[str]
- ccxt_exchange: Optional[str]
- contract_type: Optional[str]  // exchange-specific contract classification
- inverse: Optional[bool]
- tardis_symbol: Optional[str]
- tardis_exchange: Optional[str]
- data_provider: Optional["tardis","databento"]
- data_types: Optional[list[str]]  // trades, quotes, options_chain, book, etc.

### Additional Implementation Fields
- symbol_type: Raw instrument type from exchange API (spot, perpetual, future, option)
- contract_type: Exchange-specific contract classification (currently not populated, reserved for future use)

### Identity Tiers
- instrument_key: canonical routing/position identity.
- exchange_raw_symbol: provider-native identifier for IO.

### Validation Rules (summary)
- VENUE ∈ Venue enum; INSTRUMENT_TYPE ∈ InstrumentType enum.
- BASETOKEN: symbol is asset code (BTC, ETH, USDT, ...).
- SPOTASSET: symbol is asset code (BTC, ETH, USDT, ...) - represents actual holdings.
- SPOT_PAIR: symbol BASE-QUOTE; QUOTE ∈ {USD, USDT, USDC, ...}; routing only, never stored as position.
- PERPETUAL: symbol BASE-QUOTE; QUOTE ∈ {USD, USDT}; inverse in attrs when applicable.
- FUTURE: attrs.expiry required; contract_size required; symbol may include expiry code.
- OPTION: attrs.expiry, attrs.strike, attrs.option_type required.

### Worked Examples
- BTC balance vs spot pair on Binance:
  - BINANCE:SPOT_ASSET:BTC (actual BTC position)
  - BINANCE:SPOT_PAIR:BTC-USDT (routing-only)
- Spot trading workflow:
  - Execute trade: BINANCE:SPOT_PAIR:BTC/USDT (find best route)
  - Update positions: BINANCE:SPOT_ASSET:BTC, BINANCE:SPOT_ASSET:USDT
- Perpetuals:
  - BINANCE:PERPETUAL:BTC-USDT with attrs.margin="USDT", inverse=false
- Crypto futures:
  - DERIBIT:FUTURE:BTC-USD-241225 with attrs.expiry="2024-12-25T08:00:00Z"
- TradFi futures:
  - CME:FUTURE:ES-202412 with attrs.exchange_raw_symbol="ESZ4", expiry normalized
- Options:
  - DERIBIT:OPTION:BTC-USD-241225-50000-CALL with normalized expiry
  - CME:OPTION:ES-202412-4500-CALL with month-code preserved in exchange_raw_symbol
- DeFi lending/staking:
  - AAVE_V3:A_TOKEN:AUSDT, AAVE_V3:DEBT_TOKEN:DEBTWETH, ETHERFI:LST:WEETH

### Expiry Handling
- attrs.expiry holds precise UTC datetime (including half-hour if applicable).
- Daily snapshots store attrs.expiry; an auxiliary expiry_calendar lists exact intra-day expiries per day.

### FAQ
- Why SPOT_PAIR is routing-only? Trades result in SPOTASSET deltas; SPOT_PAIR simplifies execution routing without becoming a held position.
- What's the difference between BASETOKEN and SPOTASSET? BASETOKEN is a generic asset reference, SPOTASSET represents actual spot positions held on a specific venue.
- Are CME futures different from crypto futures? Same instrument_type=FUTURE; differences live in attributes (contract_size, codes, settlement, asset_class).
- Why UPPERCASE? Consistent formatting makes keys more readable and easier to parse programmatically.
- How do SPOT_PAIR and SPOTASSET work together? SPOT_PAIR is used for finding the best exchange to trade a pair, SPOTASSET tracks your actual asset holdings after the trade.


