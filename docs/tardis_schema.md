dataset,column_name,type,description
trades,exchange,string,Exchange identifier (validation column - dropped after validation)
trades,symbol,string,Symbol identifier (validation column - dropped after validation)
trades,timestamp,int64,Exchange timestamp in microseconds since epoch
trades,local_timestamp,int64,Local arrival timestamp in microseconds since epoch
trades,id,string,Trade ID (if available)
trades,side,string,Trade side ('buy' or 'sell')
trades,price,float64,Trade price
trades,amount,float64,Trade amount
book_snapshot_5,exchange,string,Exchange identifier (validation column - dropped after validation)
book_snapshot_5,symbol,string,Symbol identifier (validation column - dropped after validation)
book_snapshot_5,timestamp,int64,Exchange timestamp in microseconds since epoch
book_snapshot_5,local_timestamp,int64,Local arrival timestamp in microseconds since epoch
book_snapshot_5,ask_price_1,float64,Ask price at level 1 (BigQuery-compatible format)
book_snapshot_5,ask_volume_1,float64,Ask volume at level 1 (BigQuery-compatible format)
book_snapshot_5,bid_price_1,float64,Bid price at level 1 (BigQuery-compatible format)
book_snapshot_5,bid_volume_1,float64,Bid volume at level 1 (BigQuery-compatible format)
book_snapshot_5,ask_price_2,float64,Ask price at level 2 (BigQuery-compatible format)
book_snapshot_5,ask_volume_2,float64,Ask volume at level 2 (BigQuery-compatible format)
book_snapshot_5,bid_price_2,float64,Bid price at level 2 (BigQuery-compatible format)
book_snapshot_5,bid_volume_2,float64,Bid volume at level 2 (BigQuery-compatible format)
book_snapshot_5,ask_price_3,float64,Ask price at level 3 (BigQuery-compatible format)
book_snapshot_5,ask_volume_3,float64,Ask volume at level 3 (BigQuery-compatible format)
book_snapshot_5,bid_price_3,float64,Bid price at level 3 (BigQuery-compatible format)
book_snapshot_5,bid_volume_3,float64,Bid volume at level 3 (BigQuery-compatible format)
book_snapshot_5,ask_price_4,float64,Ask price at level 4 (BigQuery-compatible format)
book_snapshot_5,ask_volume_4,float64,Ask volume at level 4 (BigQuery-compatible format)
book_snapshot_5,bid_price_4,float64,Bid price at level 4 (BigQuery-compatible format)
book_snapshot_5,bid_volume_4,float64,Bid volume at level 4 (BigQuery-compatible format)
book_snapshot_5,ask_price_5,float64,Ask price at level 5 (BigQuery-compatible format)
book_snapshot_5,ask_volume_5,float64,Ask volume at level 5 (BigQuery-compatible format)
book_snapshot_5,bid_price_5,float64,Bid price at level 5 (BigQuery-compatible format)
book_snapshot_5,bid_volume_5,float64,Bid volume at level 5 (BigQuery-compatible format)
liquidations,exchange,string,Exchange identifier (validation column - dropped after validation)
liquidations,symbol,string,Symbol identifier (validation column - dropped after validation)
liquidations,timestamp,int64,Exchange timestamp in microseconds since epoch
liquidations,local_timestamp,int64,Local arrival timestamp in microseconds since epoch
liquidations,id,string,Liquidation ID (if available)
liquidations,side,string,"Side: 'buy' = short liquidated, 'sell' = long liquidated"
liquidations,price,float64,Liquidation price
liquidations,amount,float64,Liquidation amount
derivative_ticker,exchange,string,Exchange identifier (validation column - dropped after validation)
derivative_ticker,symbol,string,Symbol identifier (validation column - dropped after validation)
derivative_ticker,timestamp,int64,Exchange timestamp in microseconds since epoch
derivative_ticker,local_timestamp,int64,Local arrival timestamp in microseconds since epoch
derivative_ticker,funding_timestamp,int64,Funding timestamp (if provided)
derivative_ticker,funding_rate,float64,Current funding rate
derivative_ticker,predicted_funding_rate,float64,Predicted funding rate
derivative_ticker,open_interest,float64,Open interest value
derivative_ticker,last_price,float64,Last traded price
derivative_ticker,index_price,float64,Underlying index price
derivative_ticker,mark_price,float64,Mark price
options_chain,exchange,string,Exchange identifier (validation column - dropped after validation)
options_chain,symbol,string,Symbol identifier (validation column - dropped after validation)
options_chain,timestamp,int64,Exchange timestamp in microseconds since epoch
options_chain,local_timestamp,int64,Local arrival timestamp in microseconds since epoch
options_chain,type,string,Option type (put/call)
options_chain,strike_price,float64,Strike price of the option
options_chain,expiration,int64,Expiration timestamp in microseconds since epoch
options_chain,open_interest,float64,Open interest for the option
options_chain,last_price,float64,Last traded price
options_chain,bid_price,float64,Best bid price for the option
options_chain,bid_amount,float64,Best bid amount
options_chain,bid_iv,float64,Bid implied volatility
options_chain,ask_price,float64,Best ask price for the option
options_chain,ask_amount,float64,Best ask amount
options_chain,ask_iv,float64,Ask implied volatility
options_chain,mark_price,float64,Mark price of the option
options_chain,mark_iv,float64,Mark implied volatility
options_chain,underlying_index,string,Underlying instrument symbol
options_chain,underlying_price,float64,Underlying asset price
options_chain,delta,float64,Option delta
options_chain,gamma,float64,Option gamma
options_chain,vega,float64,Option vega
options_chain,theta,float64,Option theta
options_chain,rho,float64,Option rho
