import pandas as pd
from data_loader import load_table
from categorizer import add_category_column, sample_from_chunks
from analyzer import format_amount

def optimize_budget(actual_spending: pd.DataFrame, budget: dict) -> pd.DataFrame:
    """
    Compare actual spending with an ideal budget.
    
    :param actual_spending: DataFrame with columns ['Category', 'Amount']
    :param budget: Dictionary mapping category to budget amount
    :return: DataFrame with Category, actual spending, budget, and the difference.
    """
    budget_df = pd.DataFrame(list(budget.items()), columns=["Category", "Budget"])
    merged = pd.merge(actual_spending, budget_df, on="Category", how="outer").fillna(0)
    merged["Difference"] = merged["Budget"] - merged["Amount"]
    return merged

def format_budget_report(df: pd.DataFrame) -> pd.DataFrame:
    """Format the budget comparison report with proper currency formatting."""
    formatted = df.copy()
    for col in ['Amount', 'Budget', 'Difference']:
        formatted[col] = formatted[col].apply(format_amount)
    return formatted

if __name__ == "__main__":
    # Load and sample transactions
    tx_ibm_chunks = load_table("transactions_ibm")
    df = sample_from_chunks(tx_ibm_chunks, n=100000)  # sample for performance
    df = add_category_column(df)
    
    # Calculate actual spending by category
    df['Amount'] = df['Amount'].replace('[\$,]', '', regex=True).astype(float)
    actual_spending = df.groupby('Category')['Amount'].sum().reset_index()
    
    # Define ideal monthly budget
    budget = {
        "Dining": 500,
        "Travel": 1000,
        "Groceries": 600,
        "Retail": 700,
        "Healthcare": 400,
        "Entertainment": 300,
        "Transportation": 200,
        "Utilities": 150
    }

    # Compare actual vs budget
    optimized = optimize_budget(actual_spending, budget)
    
    print("\nBudget Analysis:")
    print("-" * 80)
    print(format_budget_report(optimized))
    
    # Summary statistics
    print("\nOverall Budget Status:")
    print("-" * 80)
    total_actual = optimized['Amount'].sum()
    total_budget = optimized['Budget'].sum()
    print(f"Total Spending: {format_amount(total_actual)}")
    print(f"Total Budget:  {format_amount(total_budget)}")
    print(f"Net Position:  {format_amount(total_budget - total_actual)}")