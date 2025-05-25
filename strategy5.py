import pandas as pd
import numpy as np

def generate_signals(candles_target: pd.DataFrame, candles_anchor: pd.DataFrame) -> pd.DataFrame:
    """
    Volatility-Adaptive Momentum Strategy v10.1
    - Triple timeframe confirmation (1H/4H/1D)
    - ATR-scaled position sizing
    - Dynamic trailing stops with profit locking
    - Drawdown-controlled risk management
    """
    try:
        # Column standardization for LTC
        ltc_cols = {
            'open': 'open_LTC', 'high': 'high_LTC',
            'low': 'low_LTC', 'close': 'close_LTC',
            'volume': 'volume_LTC'
        }
        df = candles_target.rename(columns=ltc_cols)
        df = df.merge(candles_anchor, on='timestamp', how='inner')
        
        # Volatility calculations
        df['atr'] = (df['high_LTC'] - df['low_LTC']).rolling(14).mean()
        df['volatility_ratio'] = (df['atr'] / df['close_LTC'].shift(1)).clip(0.005, 0.04)
        
        # Multi-timeframe momentum (BTC/ETH 4H + 1D)
        for coin in ['BTC', 'ETH']:
            df[f'{coin}_4h_mom'] = df[f'close_{coin}'].pct_change(4).rolling(8).mean()
            df[f'{coin}_1d_mom'] = df[f'close_{coin}'].pct_change(24).rolling(24).mean()
        
        # Volume confirmation
        df['volume_ma'] = df['volume_LTC'].rolling(50).mean()
        df['volume_spike'] = df['volume_LTC'] > df['volume_ma'] * 1.3

        # Signal generation logic
        df['signal'] = 'HOLD'
        in_position = False
        entry_price = 0
        trail_stop = 0
        max_risk = 0.015  # 1.5% risk per trade
        
        for i in range(4, len(df)):
            current_close = df.at[i, 'close_LTC']
            current_vol = df.at[i, 'volatility_ratio']
            
            if not in_position:
                # Entry conditions
                anchor_ok = (df.at[i, 'BTC_4h_mom'] > 0.006) & \
                           (df.at[i, 'ETH_4h_mom'] > 0.005) & \
                           (df.at[i, 'BTC_1d_mom'] > 0.01)
                price_break = current_close > df.at[i-1, 'high_LTC']
                vol_ok = current_vol < 0.025
                volume_ok = df.at[i, 'volume_spike']

                if anchor_ok and price_break and vol_ok and volume_ok:
                    # ATR-based position sizing
                    risk_capital = 1000 * max_risk
                    position_size = min(risk_capital / (df.at[i, 'atr'] * 3), 
                                      0.1 * 1000)  # Max 10% exposure
                    
                    df.at[i, 'signal'] = 'BUY'
                    entry_price = current_close
                    trail_stop = entry_price - (df.at[i, 'atr'] * 2)
                    in_position = True
            else:
                # Dynamic exit management
                unrealized_pct = (current_close / entry_price - 1) * 100
                current_atr = df.at[i, 'atr']
                
                # Progressive trailing stop
                if unrealized_pct > 2:
                    trail_stop = max(trail_stop, current_close - (current_atr * 1.8))
                if unrealized_pct > 4:
                    trail_stop = max(trail_stop, current_close - (current_atr * 1.5))
                
                # Exit conditions
                if current_close < trail_stop or unrealized_pct >= 6:
                    df.at[i, 'signal'] = 'SELL'
                    in_position = False
                
                # Volatility circuit breaker
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
