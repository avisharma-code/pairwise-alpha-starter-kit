import pandas as pd

def generate_signals(candles_target: pd.DataFrame, candles_anchor: pd.DataFrame) -> pd.DataFrame:
    """
    Strategy: Buy LTC if BTC pumped > 2% exactly 4 hours ago.
              Sell LTC if BTC dumped < -2% exactly 4 hours ago.

    Inputs:
    - candles_target: OHLCV for LTC (4H)
    - candles_anchor: Merged OHLCV with 'close_BTC' (4H)

    Output:
    - DataFrame with ['timestamp', 'signal']
    """
    try:
        # Merge target and anchor data on timestamp
        df = pd.merge(
            candles_target[['timestamp', 'close']],
            candles_anchor[['timestamp', 'close_BTC']],
            on='timestamp',
            how='inner'
        )

        # Compute BTC return from 1 candle ago (4h lag)
        df['btc_return_4h_ago'] = df['close_BTC'].pct_change().shift(1)

        # Initialize signal column
        df['signal'] = 'HOLD'
        df.loc[df['btc_return_4h_ago'] > 0.02, 'signal'] = 'BUY'
        df.loc[df['btc_return_4h_ago'] < -0.02, 'signal'] = 'SELL'

        return df[['timestamp', 'signal']]

    except Exception as e:
        raise RuntimeError(f"Error in generate_signals: {e}")


def get_coin_metadata() -> dict:
    """
    Specifies LTC as target coin and BTC as anchor, both on 4H timeframe.

    Returns:
    {
        "target": {"symbol": "LTC", "timeframe": "4H"},
        "anchors": [
            {"symbol": "BTC", "timeframe": "4H"}
        ]
    }
    """
    return {
        "target": {
            "symbol": "LTC",
            "timeframe": "4H"
        },
        "anchors": [
            {"symbol": "BTC", "timeframe": "4H"}
        ]
    }
