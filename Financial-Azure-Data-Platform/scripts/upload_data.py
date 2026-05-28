import os
import io
import pandas as pd
from sqlalchemy import create_engine, text
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv
from datetime import datetime

load_dotenv() 

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('WAREHOUSE_DB_USER')}:{os.getenv('WAREHOUSE_DB_PASSWORD')}"
    f"@{os.getenv('WAREHOUSE_DB_HOST')}:{os.getenv('WAREHOUSE_DB_PORT')}/{os.getenv('WAREHOUSE_DB_NAME')}"
    f"?sslmode=disable"
)

adls = DataLakeServiceClient(
    account_url=f"https://{os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net",
    credential=os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
)
fs = adls.get_file_system_client('raw')

TABLES = [
    'financial.account_categories',
    'financial.accounts',
    'financial.balance_sheet_items',
    'financial.cash_flow_items',
    'financial.currencies',
    'financial.documents',
    'financial.entities',
    'financial.financial_ratios',
    'financial.financial_statements',
    'financial.fiscal_periods',
    'financial.gl_transactions',
    'financial.income_statement_items',
    'financial.industry_codes',
]

today = datetime.utcnow().strftime('%Y%m%d')
uploaded = []

for table in TABLES:
    schema, name = table.split('.')
    print(f'Exporting {table}...')

    with engine.connect() as conn:
        df = pd.read_sql(text(f'SELECT * FROM {table}'), conn)

    if df.empty:
        print(f'  WARNING: {table} is empty, skipping')
        continue

    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine='pyarrow')
    buf.seek(0)

    path = f'financial/{name}/date={today}/{name}.parquet'
    fs.create_directory(f'financial/{name}/date={today}')
    file_client = fs.get_file_client(path)
    file_client.upload_data(buf.read(), overwrite=True)

    uploaded.append({'table': table, 'rows': len(df), 'path': path})
    print(f'  OK {len(df)} rows -> raw/{path}')

print(f'\nDone! {len(uploaded)}/{len(TABLES)} tables uploaded')
for u in uploaded:
    print(f"  {u['table']:45s} {u['rows']:>6} rows -> {u['path']}")

# # Tạo file
# New-Item -Path scripts/upload_data.py -ItemType File

# # Paste code vào, sau đó:
# pip install pandas pyarrow sqlalchemy psycopg2-binary
# python scripts/upload_data.py