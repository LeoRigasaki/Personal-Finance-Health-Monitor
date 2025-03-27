import pandas as pd
from .data_loader import load_table
from .categorizer import sample_from_chunks, MCC_TO_CATEGORY, add_category_column

def analyze_monthly_spending(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate total spending per month.
    Assumes transactions_df has columns: 'Year', 'Month', and 'Amount'.
    """
    # Remove any '$' symbols and convert to float.
    transactions_df["Amount"] = transactions_df["Amount"].replace("[\$,]", "", regex=True).astype(float)
    monthly = transactions_df.groupby(["Year", "Month"])["Amount"].sum().reset_index()
    monthly = monthly.sort_values(by=["Year", "Month"])
    return monthly

def top_categories(transactions_df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    """
    Identify top spending categories.
    Shows both MCC codes and their corresponding categories.
    """
    transactions_df["Amount"] = transactions_df["Amount"].replace("[\$,]", "", regex=True).astype(float)
    
    # Add categories if not already present
    if "Category" not in transactions_df.columns:
        transactions_df = add_category_column(transactions_df)
    
    # Group by both MCC and Category
    category_totals = (transactions_df.groupby(["MCC", "Category"])["Amount"]
                      .sum()
                      .reset_index()
                      .sort_values("Amount", ascending=False))
    
    return category_totals.head(top_n)

def format_amount(amount: float) -> str:
    """Format amount as currency string."""
    return f"${amount:,.2f}"

if __name__ == "__main__":
    # Example: load and sample IBM transactions from the database
    tx_ibm_chunks = load_table("transactions_ibm")
    df = sample_from_chunks(tx_ibm_chunks, n=10000)  # Sample 10k transactions for analysis
    
    monthly_spending = analyze_monthly_spending(df)
    print("\nMonthly Spending Summary:")
    print("-" * 80)
    monthly_spending["Amount"] = monthly_spending["Amount"].apply(format_amount)
    print(monthly_spending.head())
    
    top_cats = top_categories(df)
    print("\nTop Spending Categories:")
    print("-" * 80)
    top_cats["Amount"] = top_cats["Amount"].apply(format_amount)
    print(top_cats)
