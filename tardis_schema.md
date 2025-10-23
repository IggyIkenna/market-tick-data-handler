dataset,column_name,type,description
trades,exchange,tardis_exchange,Exchange identifier
trades,symbol,tardis_symbol,Instrument symbol
trades,timestamp,int64,Exchange timestamp in microseconds since epoch
trades,local_timestamp,int64,Local arrival timestamp in microseconds since epoch
trades,id,string,Trade ID (if available)
trades,side,string,Trade side ('buy' or 'sell')
trades,price,float64,Trade price
trades,amount,float64,Trade amount
liquidations,exchange,tardis_exchange,Exchange identifier
liquidations,symbol,tardis_symbol,Instrument symbol
liquidations,timestamp,int64,Exchange timestamp in microseconds since epoch
liquidations,local_timestamp,int64,Local arrival timestamp in microseconds since epoch
liquidations,id,string,Liquidation ID (if available)
liquidations,side,string,"Side: 'buy' = short liquidated, 'sell' = long liquidated"
liquidations,price,float64,Liquidation price
liquidations,amount,float64,Liquidation amount
derivative_ticker,exchange,tardis_exchange,Exchange identifier
derivative_ticker,symbol,tardis_symbol,Instrument symbol
derivative_ticker,funding_rate,float64,Current funding rate
derivative_ticker,predicted_funding_rate,float64,Predicted funding rate
derivative_ticker,open_interest,float64,Open interest value
derivative_ticker,last_price,float64,Last traded price
derivative_ticker,index_price,float64,Underlying index price
derivative_ticker,mark_price,float64,Mark price
derivative_ticker,funding_timestamp,int64,Funding timestamp (if provided)
derivative_ticker,timestamp,int64,Exchange timestamp
derivative_ticker,local_timestamp,int64,Local arrival timestamp
options_chain,exchange,tardis_exchange,Exchange identifier
options_chain,symbol,tardis_symbol,Underlying instrument symbol
options_chain,timestamp,int64,Exchange timestamp in microseconds since epoch
options_chain,local_timestamp,int64,Local arrival timestamp in microseconds since epoch
options_chain,mark_price,float64,Mark price of the option
options_chain,index_price,float64,Underlying index price
options_chain,bid_price,float64,Best bid price for the option
options_chain,bid_amount,float64,Best bid amount
options_chain,ask_price,float64,Best ask price for the option
options_chain,ask_amount,float64,Best ask amount
options_chain,delta,float64,Option delta
options_chain,gamma,float64,Option gamma
options_chain,vega,float64,Option vega
options_chain,theta,float64,Option theta
options_chain,rho,float64,Option rho
options_chain,iv,float64,Implied volatility
options_chain,open_interest,float64,Open interest for the option
options_chain,volume,float64,Traded volume over the period
options_chain,funding_timestamp,int64,Next funding or settlement timestamp if applicable
