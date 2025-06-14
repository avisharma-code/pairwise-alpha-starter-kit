import pandas as pd
import numpy as np


def compute_anchor_scores(df_anchor: pd.DataFrame, lookback: int = 2) -> pd.DataFrame:
    for coin in ['BTC', 'ETH']:
        close_col = f'close_{coin}'
        df_anchor[f'ret_{coin}'] = df_anchor[close_col].pct_change(lookback)
        df_anchor[f'vol_{coin}'] = df_anchor[close_col].rolling(14).std()
        df_anchor[f'score_{coin}'] = df_anchor[f'ret_{coin}'] / df_anchor[f'vol_{coin}']
    return df_anchor


def evaluate_trade_conditions(row, vol_threshold: float) -> str:
    score_btc = row['score_BTC']
    score_eth = row['score_ETH']
    ltc_ret = row['ltc_return']
    ltc_vol = row['ltc_vol']

    anchor_pump = (score_btc > 0.002 or score_eth > 0.002)
    anchor_dump = (score_btc < -0.002 or score_eth < -0.002)
    calm_market = ltc_vol <= vol_threshold
    lagging_ltc = abs(ltc_ret) < max(row['ret_BTC'], row['ret_ETH'])

    if anchor_pump and calm_market and lagging_ltc:
        return 'BUY'
    elif anchor_dump and calm_market and lagging_ltc:
        return 'SELL'
    return 'HOLD'


def generate_signals(candles_target: pd.DataFrame, candles_anchor: pd.DataFrame) -> pd.DataFrame:
    """
    Strategy v2.6A – Lagged Anchor Entry (1H) with Position Sizing
    """
    try:
        df = candles_target.copy()
        df['atr'] = (df['high'] - df['low']).rolling(14).mean()
        df['ltc_return'] = df['close'].pct_change(3)
        df['ltc_vol'] = df['close'].rolling(14).std()
        vol_threshold = df['ltc_vol'].quantile(0.85)

        df_anchor = compute_anchor_scores(candles_anchor.copy(), lookback=2)
        df = df.merge(df_anchor, on="timestamp", how="inner")
        df['signal'] = 'HOLD'
        df['position_size'] = 1.0  # Default full position

        TP_MULT = 2.5
        SL_MULT = 1.2
        holding = 0
        entry_price = None
        last_signal = "HOLD"
        max_hold = 18

        for i in range(len(df)):
            price_now = df.at[i, 'close']
            atr_now = df.at[i, 'atr']

            if holding > 0:
                holding += 1
                change = price_now / entry_price - 1
                if (change >= TP_MULT * atr_now / entry_price) or (change <= -SL_MULT * atr_now / entry_price) or holding >= max_hold:
                    df.at[i, 'signal'] = 'SELL' if last_signal == 'BUY' else 'BUY'
                    holding = 0
                    last_signal = 'HOLD'
                    continue
                else:
                    df.at[i, 'signal'] = 'HOLD'
            else:
                action = evaluate_trade_conditions(df.iloc[i], vol_threshold)
                if action != 'HOLD':
                    df.at[i, 'signal'] = action
                    entry_price = price_now
                    last_signal = action
                    holding = 1
                    # Position size scaled by average score magnitude
                    score_mag = max(abs(df.at[i, 'score_BTC']), abs(df.at[i, 'score_ETH']))
                    df.at[i, 'position_size'] = min(1.0, max(0.25, score_mag / 0.01))

        print("\n🔍 Preview – Anchor Scores & Volatility")
        print(df[['timestamp', 'score_BTC', 'score_ETH', 'ltc_return', 'ltc_vol', 'signal', 'position_size']].tail(20))

        return df[['timestamp', 'signal', 'position_size']]

    except Exception as e:
        raise RuntimeError(f"Error in generate_signals: {e}")


def get_coin_metadata() -> dict:
    return {
        "target": {"symbol": "LTC", "timeframe": "1H"},
        "anchors": [
            {"symbol": "BTC", "timeframe": "1H"},
            {"symbol": "ETH", "timeframe": "1H"}
        ]
    }
