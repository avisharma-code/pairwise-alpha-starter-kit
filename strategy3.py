import pandas as pd
import numpy as np

def generate_signals(candles_target: pd.DataFrame, candles_anchor: pd.DataFrame) -> pd.DataFrame:
    """
    Clean Momentum Strategy v9.1
    - Simplified yet effective signal logic
    - Robust column handling
    - Adaptive risk management
    - Competition date compliance
    """
    try:
        # Proper column renaming
        ltc_cols = {
            'open': 'open_LTC', 'high': 'high_LTC',
            'low': 'low_LTC', 'close': 'close_LTC',
            'volume': 'volume_LTC'
        }
        df = candles_target.rename(columns=ltc_cols)
        df = df.merge(candles_anchor, on='timestamp', how='inner')
        
        # Convert and filter dates
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df[(df['timestamp'] >= '2025-01-01') & 
               (df['timestamp'] <= '2025-05-09')].copy()

        # Core indicators
        df['atr'] = (df['high_LTC'] - df['low_LTC']).rolling(14).mean()
        df['btc_mom'] = df['close_BTC'].pct_change(4)
        df['eth_mom'] = df['close_ETH'].pct_change(4)
        df['ltc_break'] = df['close_LTC'] > df['high_LTC'].shift(1)

        # Signal logic
        df['signal'] = 'HOLD'
        in_trade = False
        entry_price = 0
        trail_stop = 0
        
        for i in range(4, len(df)):
            current_close = df.at[i, 'close_LTC']
            atr = df.at[i, 'atr']
            
            if not in_trade:
                # Clear entry condition
                if (df.at[i, 'btc_mom'] > 0.005 and 
                    df.at[i, 'eth_mom'] > 0.004 and
                    df.at[i, 'ltc_break'] and
                    atr < 0.02 * current_close):
                    
                    df.at[i, 'signal'] = 'BUY'
                    entry_price = current_close
                    trail_stop = entry_price - 2 * atr
                    in_trade = True
            else:
                # Progressive trailing stop
                trail_stop = max(trail_stop, current_close - 1.5 * atr)
                
                # Exit conditions
                if current_close < trail_stop:
                    df.at[i, 'signal'] = 'SELL'
                    in_trade = False
                elif (current_close / entry_price) >= 1.04:  # 4% profit target
                    df.at[i, 'signal'] = 'SELL'
                    in_trade = False

        return df[['timestamp', 'signal']]

    except Exception as e:
        raise RuntimeError(f"Signal error: {str(e)}")

def get_coin_metadata() -> dict:
    return {
        "target": {"symbol": "LTC", "timeframe": "1H"},
        "anchors": [
            {"symbol": "BTC", "timeframe": "1H"},
            {"symbol": "ETH", "timeframe": "1H"}
        ]
    }
