import pandas as pd
from data_loader import load_table
from categorizer import sample_from_chunks

def calculate_financial_health(user_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute a financial health score for each user.
    Uses:
      - Debt-to-Income ratio (Total Debt / Yearly Income)
      - FICO Score (normalized assuming range 300-850)
    
    Returns the DataFrame with an added 'HealthScore' column scaled from 0 to 100.
    """
    df = user_df.copy()
    
    # Clean money columns by removing '$' symbols.
    money_cols = ["Yearly Income - Person", "Total Debt", "Per Capita Income - Zipcode"]
    for col in money_cols:
        if col in df.columns:
            df[col] = df[col].replace("[\$,]", "", regex=True).astype(float)
    
    # Compute Debt-to-Income ratio, guarding against division by zero.
    df["DebtToIncome"] = df.apply(lambda row: row["Total Debt"] / row["Yearly Income - Person"]
                                  if row["Yearly Income - Person"] > 0 else 1, axis=1)
    
    # Normalize the FICO score (assumed range 300-850).
    df["FICO_Norm"] = (df["FICO Score"] - 300) / (850 - 300)
    
    # Calculate HealthScore: better scores for lower DebtToIncome and higher FICO.
    df["HealthScore"] = ((1 - df["DebtToIncome"].clip(0, 1)) * 0.5 + df["FICO_Norm"] * 0.5) * 100
    df["HealthScore"] = df["HealthScore"].round(2)
    
    return df

def print_health_summary(df: pd.DataFrame) -> None:
    """Print a summary of financial health scores."""
    print("\nFinancial Health Summary:")
    print("-" * 80)
    print(f"Average Health Score: {df['HealthScore'].mean():.2f}")
    print(f"Score Distribution:")
    print("Excellent (80-100):", len(df[df['HealthScore'] >= 80]))
    print("Good (60-79)     :", len(df[(df['HealthScore'] >= 60) & (df['HealthScore'] < 80)]))
    print("Fair (40-59)     :", len(df[(df['HealthScore'] >= 40) & (df['HealthScore'] < 60)]))
    print("Poor (0-39)      :", len(df[df['HealthScore'] < 40]))

if __name__ == "__main__":
    # Load users table (it's small, so no need for chunking)
    users = load_table("users")
    scored = calculate_financial_health(users)
    
    print("\nUser Scores:")
    print(scored[["Person", "FICO Score", "Total Debt", 
                 "Yearly Income - Person", "HealthScore"]].head())
    
    print_health_summary(scored)