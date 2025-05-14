import pandas as pd
import numpy as np

def backtest_signals(signals_df, price_df, fee=0.001, initial_capital=1000.0):
    """
    Simulates trading using signals and calculates performance metrics.
    
    Parameters:
    - signals_df: DataFrame with ['timestamp', 'signal']
    - price_df: DataFrame with ['timestamp', 'close']
    - fee: Transaction fee per trade side (default 0.1%)
    - initial_capital: Starting capital in USDT

    Returns:
    - DataFrame with capital over time
    - Dict with final return, Sharpe Ratio, Max Drawdown
    """
    df = pd.merge(signals_df, price_df[['timestamp', 'close']], on='timestamp', how='inner').copy()
    df['position'] = 0
    df['cash'] = initial_capital
    df['holdings'] = 0.0
    df['capital'] = initial_capital
    in_position = False

    for i in range(1, len(df)):
        signal = df.at[df.index[i], 'signal']
        price = df.at[df.index[i], 'close']

        if signal == 'BUY' and not in_position:
            shares = (df.at[df.index[i - 1], 'cash'] * (1 - fee)) / price
            df.at[df.index[i], 'holdings'] = shares
            df.at[df.index[i], 'cash'] = 0.0
            in_position = True

        elif signal == 'SELL' and in_position:
            cash = df.at[df.index[i - 1], 'holdings'] * price * (1 - fee)
            df.at[df.index[i], 'cash'] = cash
            df.at[df.index[i], 'holdings'] = 0.0
            in_position = False

        else:
            # Carry forward holdings or cash
            df.at[df.index[i], 'cash'] = df.at[df.index[i - 1], 'cash']
            df.at[df.index[i], 'holdings'] = df.at[df.index[i - 1], 'holdings']

        # Update total capital
        df.at[df.index[i], 'capital'] = (
            df.at[df.index[i], 'cash'] + df.at[df.index[i], 'holdings'] * price
        )

    # Daily returns
    df['returns'] = df['capital'].pct_change().fillna(0)

    # Metrics
    final_return_pct = (df['capital'].iloc[-1] - initial_capital) / initial_capital * 100
    sharpe_ratio = df['returns'].mean() / df['returns'].std() if df['returns'].std() > 0 else 0
    max_drawdown = ((df['capital'].cummax() - df['capital']) / df['capital'].cummax()).max() * 100

    metrics = {
        'final_capital': round(df['capital'].iloc[-1], 2),
        'final_return_pct': round(final_return_pct, 2),
        'sharpe_ratio': round(sharpe_ratio, 4),
        'max_drawdown_pct': round(max_drawdown, 2)
    }

    return df, metrics
