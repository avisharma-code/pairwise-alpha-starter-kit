import pandas as pd
import numpy as np

def generate_signals(candles_target: pd.DataFrame, candles_anchor: pd.DataFrame) -> pd.DataFrame:
    """
    Adaptive Momentum Strategy v5.1
    - Dual timeframe momentum confirmation
    - Volatility-scaled position sizing
    - Dynamic stop-loss system
    - Market regime filtering
    """
    try:
        # Standardize column names
        cols = {
            "open": "open_LTC",
            "high": "high_LTC",
            "low": "low_LTC",
            "close": "close_LTC",
            "volume": "volume_LTC"  # Critical fix
        }
        df = candles_target.rename(columns=cols).merge(
            candles_anchor, on="timestamp", how="inner")

        # Calculate volatility metrics
        df['atr'] = (df['high_LTC'] - df['low_LTC']).rolling(14).mean()
        df['volatility_ratio'] = (df['atr'] / df['close_LTC'].shift(1)).clip(0.005, 0.04)
        
        # Market regime filter (200-period MA)
        df['ma_200'] = df['close_LTC'].rolling(200).mean()
        
        # Anchor momentum system
        for coin in ['BTC', 'ETH']:
            df[f'{coin}_mom_4h'] = df[f'close_{coin}'].pct_change(4)
            df[f'{coin}_mom_24h'] = df[f'close_{coin}'].pct_change(24)
        
        # Volume confirmation
        df['volume_z'] = (df['volume_LTC'] - df['volume_LTC'].rolling(50).mean()) / df['volume_LTC'].rolling(50).std()

        # Signal generation logic
        df['signal'] = 'HOLD'
        in_position = False
        entry_price = 0
        trail_stop = 0
        position_size = 0

        for i in range(2, len(df)):
            current_close = df.at[i, 'close_LTC']
            current_vol = df.at[i, 'volatility_ratio']
            
            # Only trade in bullish regimes
            if df.at[i, 'close_LTC'] < df.at[i, 'ma_200']:
                continue
                
            if not in_position:
                # Entry conditions
                anchor_ok = (df.at[i, 'BTC_mom_4h'] > 0.005) & \
                            (df.at[i, 'ETH_mom_4h'] > 0.004)
                price_confirm = current_close > df.at[i-1, 'high_LTC']
                vol_ok = current_vol < 0.02
                volume_ok = df.at[i, 'volume_z'] > 0.5

                if anchor_ok and price_confirm and vol_ok and volume_ok:
                    # Dynamic position sizing
                    risk_capital = 1000 * 0.015  # 1.5% risk per trade
                    position_size = risk_capital / (df.at[i, 'atr'] * 2)
                    
                    df.at[i, 'signal'] = 'BUY'
                    entry_price = current_close
                    trail_stop = entry_price - (df.at[i, 'atr'] * 1.5)
                    in_position = True
            else:
                # Dynamic exit management
                unrealized_pct = (current_close / entry_price - 1) * 100
                trail_stop = max(trail_stop, current_close - (df.at[i, 'atr'] * 1.2))
                
                # Profit-taking rules
                if unrealized_pct >= 3.0 or current_close < trail_stop:
                    df.at[i, 'signal'] = 'SELL'
                    in_position = False
                
                # Volatility-based emergency exit
                if current_vol > 0.03:
                    df.at[i, 'signal'] = 'SELL'
                    in_position = False

        return df[['timestamp', 'signal']]

    except Exception as e:
        raise RuntimeError(f"Signal generation error: {str(e)}")

def get_coin_metadata() -> dict:
    return {
        "target": {"symbol": "LTC", "timeframe": "1H"},
        "anchors": [
            {"symbol": "BTC", "timeframe": "1H"},
            {"symbol": "ETH", "timeframe": "1H"}
        ]
    }
