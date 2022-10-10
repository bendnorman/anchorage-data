import pandas as pd

# TODO: test
def read_csv_coerce_ints(path):
    df = pd.read_csv(path, na_filter=False)
    df = df.replace(r"^\s*$", pd.NA, regex=True)
    for col in df.columns:
        df[col] = df[col].astype("Int64", errors="ignore")
    return df
