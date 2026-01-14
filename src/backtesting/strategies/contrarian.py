import pandas as pd
import numpy as np
from src.backtesting.data_cleaner import apply_quality_filters
from src.backtesting.transaction_costs import apply_transaction_costs
from src.backtesting.slippage import slippage_cost

def backtest_contrarian_trend_strategy(
    df: pd.DataFrame,
    initial_capital: float = 10000,
    rebalance_days: int = 7,
    low_vol_pct: float = 0.3,
    high_vol_pct: float = 0.3,
):
    """
    Contrarian Trend Strategy:
    - Buy low-volatility coins with negative momentum
    - Avoid or sell high-volatility coins with positive momentum

    Assumes:
    - df has columns ['token_address', 'timestamp', 'value', 'volatility_30d', 'momentum_30d']
    """

    dates = sorted(df["timestamp"].unique())
    capital = initial_capital
    current_positions = {}
    portfolio_history = []

    last_rebalance_idx = -rebalance_days

    for i, current_date in enumerate(dates):

        # ------------------------
        # Rebalance
        # ------------------------
        if i - last_rebalance_idx >= rebalance_days:
            eligible_tokens = apply_quality_filters(df, current_date)

            today = df[
                (df["timestamp"] == current_date)
                & (df["token_address"].isin(eligible_tokens))
            ][["token_address", "value", "volatility_30d", "momentum_30d"]].dropna()

            if today.empty:
                current_positions = {}
                last_rebalance_idx = i
                portfolio_history.append(
                    {"date": current_date, "portfolio_value": capital, "n_tokens": 0}
                )
                continue

            # Low-vol coins (bottom X%) with negative momentum → BUY
            n_low_vol = max(1, int(len(today) * low_vol_pct))
            low_vol_tokens = today.nsmallest(n_low_vol, "volatility_30d")
            buy_candidates = low_vol_tokens[low_vol_tokens["momentum_30d"] < 0]

            # High-vol coins (top X%) with positive momentum → remove/avoid
            n_high_vol = max(1, int(len(today) * high_vol_pct))
            high_vol_tokens = today.nlargest(n_high_vol, "volatility_30d")
            sell_candidates = high_vol_tokens[high_vol_tokens["momentum_30d"] > 0]

            # Update positions
            current_positions = {}
            if not buy_candidates.empty:
                allocation = capital / len(buy_candidates)
                for _, row in buy_candidates.iterrows():
                    tx_cost = apply_transaction_costs(allocation)
                    entry_price = row["value"] * (1 + tx_cost / allocation)
                    current_positions[row["token_address"]] = {
                        "entry_price": entry_price,
                        "allocation": allocation,
                    }

            last_rebalance_idx = i

        # ------------------------
        # Daily update / PnL
        # ------------------------
        daily_return = 0.0
        exits = []

        for token, pos in current_positions.items():
            today_row = df[
                (df["timestamp"] == current_date)
                & (df["token_address"] == token)
            ]
            if i == 0 or today_row.empty:
                continue

            yesterday_row = df[
                (df["timestamp"] == dates[i - 1])
                & (df["token_address"] == token)
            ]
            if yesterday_row.empty:
                continue

            today_price = today_row["value"].iloc[0]
            yesterday_price = yesterday_row["value"].iloc[0]

            pnl = (today_price - pos["entry_price"]) / pos["entry_price"]
            weight = pos["allocation"] / capital

            # Stop-loss
            if pnl < -0.08:
                slippage = slippage_cost(pos["allocation"], pool_liquidity=100_000_000)
                tx_cost = apply_transaction_costs(pos["allocation"]) / pos["allocation"]
                penalty = slippage + tx_cost

                daily_return += ((today_price - yesterday_price) / yesterday_price) * weight
                daily_return -= penalty
                exits.append(token)
            else:
                daily_return += ((today_price - yesterday_price) / yesterday_price) * weight

        for token in exits:
            current_positions.pop(token, None)

        capital *= (1 + daily_return)

        portfolio_history.append(
            {"date": current_date, "portfolio_value": capital, "n_tokens": len(current_positions)}
        )

    return pd.DataFrame(portfolio_history)
