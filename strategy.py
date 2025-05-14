import pandas as pd

def generate_signals(candles_target: pd.DataFrame, candles_anchor: pd.DataFrame) -> pd.DataFrame:
    """
    Strategy:
    - BUY LTC if BTC pumped > 1.5% and LTC dropped (lagging)
    - SELL LTC if BTC dumped < -1.5% and LTC rose (lagging)
    """
    try:
        candles_anchor = candles_anchor.rename(columns={"close": "close_BTC"})
        candles_target = candles_target.rename(columns={"close": "close_LTC"})

        df = pd.merge(
            candles_target[["timestamp", "close_LTC"]],
            candles_anchor[["timestamp", "close_BTC"]],
            on="timestamp",
            how="inner"
        )

        # Calculate lagged returns
        df["btc_return"] = df["close_BTC"].pct_change().shift(1)
        df["ltc_return"] = df["close_LTC"].pct_change().shift(1)

        df["signal"] = "HOLD"
        df.loc[(df["btc_return"] > 0.015) & (df["ltc_return"] < 0), "signal"] = "BUY"
        df.loc[(df["btc_return"] < -0.015) & (df["ltc_return"] > 0), "signal"] = "SELL"

        return df[["timestamp", "signal"]]
        print("🧠 Using divergence-catch strategy")

    except Exception as e:
        raise RuntimeError(f"Error in generate_signals: {e}")
    



def get_coin_metadata() -> dict:
    return {
        "target": {"symbol": "LTC", "timeframe": "4H"},
        "anchors": [{"symbol": "BTC", "timeframe": "4H"}]
    }
