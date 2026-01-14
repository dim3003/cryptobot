import pandas as pd
from src.backtesting.transaction_costs import apply_transaction_costs
from src.backtesting.slippage import slippage_cost

def backtest_strategy(df: pd.DataFrame, initial_capital: float = 10000, rebalance_days: int = 7):
    """
    Equal-weighted strategy holding all cryptos, rebalanced every `rebalance_days`.
    
    Parameters:
    - df: DataFrame with columns ['timestamp', 'token_address', 'value']
    - initial_capital: starting capital
    - rebalance_days: how often to rebalance
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
            today_df = df[df["timestamp"] == current_date]
            tokens = today_df["token_address"].unique()
            
            if len(tokens) == 0:
                current_positions = {}
                last_rebalance_idx = i
                portfolio_history.append(
                    {"date": current_date, "portfolio_value": capital, "n_tokens": 0}
                )
                continue

            # Equal allocation to all tokens
            equal_allocation = capital / len(tokens)
            current_positions = {}
            for token in tokens:
                token_price = today_df[today_df["token_address"] == token]["value"].iloc[0]
                tx_cost = apply_transaction_costs(equal_allocation)
                entry_price = token_price * (1 + tx_cost / equal_allocation)
                current_positions[token] = {"entry_price": entry_price, "allocation": equal_allocation}

            last_rebalance_idx = i

        # ------------------------
        # Daily update
        # ------------------------
        daily_return = 0.0
        exits = []

        for token, pos in current_positions.items():
            today_row = df[(df["timestamp"] == current_date) & (df["token_address"] == token)]
            if i == 0 or today_row.empty:
                continue

            yesterday_row = df[(df["timestamp"] == dates[i - 1]) & (df["token_address"] == token)]
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

