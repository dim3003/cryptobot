import pandas as pd
from src.backtesting.data_cleaner import apply_quality_filters
from src.backtesting.transaction_costs import apply_transaction_costs
from src.backtesting.slippage import slippage_cost

def backtest_strategy(
    df: pd.DataFrame,
    initial_capital: float = 10000,
    rebalance_days: int = 7,
    bottom_pct: float = 0.10,
):
    """
    Strategy selecting the bottom X% least volatile tokens based on precomputed volatility_30d.
    
    Parameters:
    - df: DataFrame with columns ['timestamp', 'token_address', 'value', 'volatility_30d']
    - initial_capital: starting capital
    - rebalance_days: how often to rebalance
    - bottom_pct: fraction of tokens to hold (e.g., 0.1 = bottom 10% volatile)
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

            today_df = df[df["timestamp"] == current_date]
            eligible_df = today_df[today_df["token_address"].isin(eligible_tokens)]

            # Select bottom X% least volatile based on precomputed volatility_30d
            vol_df = eligible_df.dropna(subset=["volatility_30d"])
            if vol_df.empty:
                current_positions = {}
                last_rebalance_idx = i
                portfolio_history.append(
                    {
                        "date": current_date,
                        "portfolio_value": capital,
                        "n_tokens": len(current_positions),
                    }
                )
                continue

            n_select = max(1, int(len(vol_df) * bottom_pct))
            selected = vol_df.nsmallest(n_select, "volatility_30d")  # <-- Change here

            current_positions = {}
            allocation = capital / len(selected)

            for _, row in selected.iterrows():
                token = row["token_address"]
                token_price = row["value"]
                tx_cost = apply_transaction_costs(allocation)
                entry_price = token_price * (1 + tx_cost / allocation)

                current_positions[token] = {
                    "entry_price": entry_price,
                    "allocation": allocation,
                }

            last_rebalance_idx = i

        # ------------------------
        # Daily update
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
            {
                "date": current_date,
                "portfolio_value": capital,
                "n_tokens": len(current_positions),
            }
        )

    return pd.DataFrame(portfolio_history)

