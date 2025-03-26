import sqlite3
import pandas as pd
import os

# Set the path to the SQLite DB in the current (src) folder.
DB_PATH = os.path.join(os.path.dirname(__file__), "finance.db")

def get_connection():
    """Return a connection object to the SQLite DB."""
    return sqlite3.connect(DB_PATH)

def load_table(table_name: str) -> pd.DataFrame:
    """Load a table from the SQLite DB into a pandas DataFrame."""
    conn = get_connection()
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df

def create_database():
    """
    Create and populate the finance.db SQLite database from CSV files.
    CSV files are located in ../credit-card-transactions relative to this file.
    """
    base_path = os.path.join(os.path.dirname(__file__), "..", "credit-card-transactions")
    
    users_path = os.path.join(base_path, "sd254_users.csv")
    cards_path = os.path.join(base_path, "sd254_cards.csv")
    user0_tx_path = os.path.join(base_path, "User0_credit_card_transactions.csv")
    ibm_tx_path = os.path.join(base_path, "credit_card_transactions-ibm_v2.csv")
    
    conn = sqlite3.connect(DB_PATH)
    
    def clean_money(df: pd.DataFrame) -> pd.DataFrame:
        # For each object column that contains '$', remove the symbol and convert to float.
        for col in df.select_dtypes(include="object").columns:
            if df[col].str.contains(r"\$").any():
                df[col] = df[col].replace("[\$,]", "", regex=True).astype(float)
        return df
    
    # Load, clean, and save users table.
    users_df = pd.read_csv(users_path)
    users_df = clean_money(users_df)
    users_df.to_sql("users", conn, if_exists="replace", index=False)
    print("Saved users table.")
    
    # Load, clean, and save cards table.
    cards_df = pd.read_csv(cards_path)
    cards_df = clean_money(cards_df)
    cards_df.to_sql("cards", conn, if_exists="replace", index=False)
    print("Saved cards table.")
    
    # Load, clean, and save User0 transactions table.
    user0_df = pd.read_csv(user0_tx_path)
    user0_df = clean_money(user0_df)
    user0_df.to_sql("transactions_user0", conn, if_exists="replace", index=False)
    print("Saved transactions_user0 table.")
    
    # Process the large IBM transactions file in chunks.
    chunk_size = 500_000
    reader = pd.read_csv(ibm_tx_path, chunksize=chunk_size)
    # Drop the table if it already exists.
    conn.execute("DROP TABLE IF EXISTS transactions_ibm")
    conn.commit()
    
    for i, chunk in enumerate(reader):
        chunk = clean_money(chunk)
        mode = "replace" if i == 0 else "append"
        chunk.to_sql("transactions_ibm", conn, if_exists=mode, index=False)
        print(f"Processed chunk {i+1} of IBM transactions.")
    
    conn.close()
    print("Database creation complete.")

if __name__ == "__main__":
    # Uncomment the next line to create/populate the database from CSV files.
    create_database()
    
    # Example: load the users table and print its shape.
    users = load_table("users")
    print("Users table loaded with shape:", users.shape)
