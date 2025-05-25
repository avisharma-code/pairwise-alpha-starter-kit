import pandas as pd
import numpy as np

def generate_signals(candles_target: pd.DataFrame, candles_anchor: pd.DataFrame) -> pd.DataFrame:
    """
    Multi-Timeframe Momentum Strategy v12.1
    - Triple timeframe confirmation (1H/4H/1D)
    - Volatility-scaled position sizing
    - Adaptive trailing stops with profit compounding
    - Volume-weighted momentum entries
    """
    try:
        # Column standardization
        ltc_cols = {
            'open': 'open_LTC', 'high': 'high_LTC',
            'low': 'low_LTC', 'close': 'close_LTC',
            'volume': 'volume_LTC'
        }
        df = candles_target.rename(columns=ltc_cols)
        df = df.merge(candles_anchor, on='timestamp', how='inner')
        
        # Volatility calculations (Search Result 5,7)
        df['atr'] = (df['high_LTC'] - df['low_LTC']).rolling(14).mean()
        df['volatility'] = df['close_LTC'].pct_change().rolling(24).std()
        
        # Multi-timeframe momentum (Search Result 14)
        for coin in ['BTC', 'ETH']:
            # 4H momentum (4 bars in 1H timeframe)
            df[f'{coin}_4h_mom'] = df[f'close_{coin}'].pct_change(4).rolling(8).mean()
            # 1D momentum (24 bars)
            df[f'{coin}_1d_mom'] = df[f'close_{coin}'].pct_change(24).rolling(24).mean()
        
        # Volume confirmation (Search Result 6)
        df['volume_ma'] = df['volume_LTC'].rolling(50).mean()
        df['volume_spike'] = df['volume_LTC'] > df['volume_ma'] * 1.5

        # Signal logic
        df['signal'] = 'HOLD'
        in_position = False
        entry_price = 0
        trail_stop = 0
        risk_capital = 1000  # Initial capital
        position_size = 0
        
        for i in range(4, len(df)):
            current_close = df.at[i, 'close_LTC']
            current_vol = df.at[i, 'volatility']
            
            if not in_position:
                # Entry conditions (Search Result 1,14)
                anchor_ok = (df.at[i, 'BTC_4h_mom'] > 0.008) & \
                           (df.at[i, 'ETH_4h_mom'] > 0.006) & \
                           (df.at[i, 'BTC_1d_mom'] > 0.015)
                price_break = current_close > df.at[i-1, 'high_LTC']
                vol_ok = current_vol < 0.025
                volume_ok = df.at[i, 'volume_spike']

                if anchor_ok and price_break and vol_ok and volume_ok:
                    # Volatility-based sizing (Search Result 5,16)
                    position_size = min(0.02 * risk_capital, 
                                      (0.15 * risk_capital) / (df.at[i, 'atr'] * 3))
                    
                    df.at[i, 'signal'] = 'BUY'
                    entry_price = current_close
                    trail_stop = entry_price - (df.at[i, 'atr'] * 1.8)
                    in_position = True
            else:
                # Progressive trailing stop (Search Result 7)
                unrealized_pct = (current_close / entry_price - 1) * 100
                current_atr = df.at[i, 'atr']
                
                if unrealized_pct > 3:
                    trail_stop = max(trail_stop, current_close - (current_atr * 1.5))
                if unrealized_pct > 6:
                    trail_stop = max(trail_stop, current_close - (current_atr * 1.2))
                
                # Exit conditions
                if current_close < trail_stop or unrealized_pct >= 8:
                    df.at[i, 'signal'] = 'SELL'
                    in_position = False
                    # Compounding profits (Search Result 9,17)
                    risk_capital += (current_close - entry_price) * position_size
                
                # Volatility circuit breaker (Search Result 12)
                if current_vol > 0.035:
                    df.at[i, 'signal'] = 'SELL'
                    in_position = False

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
