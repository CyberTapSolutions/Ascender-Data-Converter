def transform_data(df):
    # Basic transformation: clean column names
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    return df
