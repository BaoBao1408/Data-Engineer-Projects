import pandas as pd

def load_product_ids(csv_file):
    df = pd.read_csv(csv_file, header=None)
    return df.iloc[:, 0].astype(str).tolist()
