import pandas as pd
from data_loader import load_table

# Mapping dictionary for MCC codes.
MCC_TO_CATEGORY = {
    1711: "Contractors",
    3000: "Travel", 3001: "Travel", 3005: "Travel", 3006: "Travel",
    3007: "Travel", 3008: "Travel", 3009: "Travel", 3058: "Travel",
    3066: "Travel", 3075: "Travel", 3132: "Travel", 3144: "Travel",
    3174: "Travel", 3256: "Travel", 3260: "Travel", 3359: "Travel",
    3387: "Travel", 3389: "Travel", 3390: "Travel", 3393: "Travel",
    3395: "Travel", 3405: "Travel", 3504: "Retail", 3509: "Retail",
    3596: "Retail", 3640: "Retail", 3684: "Retail", 3722: "Retail",
    3730: "Retail", 3771: "Retail", 3775: "Retail", 3780: "Retail",
    4111: "Transportation", 4112: "Transportation", 4121: "Transportation",
    4131: "Transportation", 4214: "Transportation", 4411: "Postal/Shipping",
    4511: "Transportation", 4722: "Travel", 4784: "Transportation",
    4814: "Communication", 4829: "Financial Services", 4899: "Entertainment",
    4900: "Utilities", 5045: "Electronics", 5094: "Jewelry/Precious Metals",
    5192: "Entertainment/Education", 5193: "Florist", 5211: "Home Improvement",
    5251: "Home Improvement", 5261: "Home Improvement", 5300: "Retail",
    5310: "Retail", 5311: "Retail", 5411: "Groceries", 5499: "Groceries",
    5533: "Auto & Transport", 5541: "Auto & Transport", 5621: "Dining",
    5651: "Retail", 5655: "Retail", 5661: "Retail", 5712: "Home & Furniture",
    5719: "Home & Furniture", 5722: "Home & Furniture", 5732: "Electronics",
    5733: "Electronics", 5812: "Dining", 5813: "Dining", 5814: "Dining",
    5815: "Dining", 5816: "Dining", 5912: "Healthcare", 5921: "Alcohol",
    5932: "Retail", 5941: "Retail/Sports", 5942: "Retail/Education",
    5947: "Retail", 5970: "Retail", 5977: "Beauty", 6300: "Financial Services",
    7011: "Travel", 7210: "Services", 7230: "Beauty", 7276: "Healthcare",
    7349: "Services", 7393: "Healthcare", 7531: "Auto & Transport",
    7538: "Auto & Transport", 7542: "Auto & Transport", 7549: "Auto & Transport",
    7801: "Retail/Fashion", 7802: "Retail/Fashion", 7832: "Entertainment",
    7922: "Entertainment", 7995: "Entertainment", 7996: "Entertainment",
    8011: "Healthcare", 8021: "Healthcare", 8041: "Healthcare",
    8043: "Healthcare", 8049: "Healthcare", 8062: "Healthcare",
    8099: "Healthcare", 8111: "Legal", 8931: "Financial Services",
    9402: "Services"
}

def categorize_transaction(row: pd.Series) -> str:
    """
    Return the category for a transaction based on its MCC code.
    """
    try:
        mcc = int(row["MCC"])
    except (ValueError, KeyError):
        return "Other"
    return MCC_TO_CATEGORY.get(mcc, "Other")

def add_category_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a 'Category' column to the DataFrame based on the MCC.
    """
    df["Category"] = df.apply(categorize_transaction, axis=1)
    return df

def sample_from_chunks(chunks, n=1000):
    """
    Take a random sample from chunked data.
    
    Args:
        chunks: Iterator of DataFrames
        n: Number of samples to return
    Returns:
        DataFrame with n random samples
    """
    # Get first chunk to calculate sampling probability
    first_chunk = next(chunks)
    total_rows = first_chunk.shape[0]
    
    # Sample from first chunk
    result = first_chunk.sample(min(n, total_rows))
    rows_needed = n - result.shape[0]
    
    # If we need more samples, process remaining chunks
    if rows_needed > 0:
        for chunk in chunks:
            total_rows += chunk.shape[0]
            if rows_needed > 0:
                chunk_sample = chunk.sample(min(rows_needed, chunk.shape[0]))
                result = pd.concat([result, chunk_sample])
                rows_needed -= chunk_sample.shape[0]
    
    return result

if __name__ == "__main__":
    # Load and sample from IBM transactions
    tx_ibm_chunks = load_table("transactions_ibm")
    tx_ibm_sample = sample_from_chunks(tx_ibm_chunks, n=1000)
    
    # Categorize the sampled transactions
    categorized = add_category_column(tx_ibm_sample)
    print(categorized[["MCC", "Category"]].drop_duplicates().head(10))
