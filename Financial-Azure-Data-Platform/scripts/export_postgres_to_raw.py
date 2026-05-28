import os, json
import pandas as pd
from sqlalchemy import create_engine, text
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv
from datetime import datetime
import io

load_dotenv()

# --- Connections ---
engine = create_engine(
    f"postgresql://{os.getenv('WAREHOUSE_DB_USER')}:{os.getenv('WAREHOUSE_DB_PASSWORD')}"
    f"@{os.getenv('WAREHOUSE_DB_HOST')}:{os.getenv('WAREHOUSE_DB_PORT')}/{os.getenv('WAREHOUSE_DB_NAME')}"
)

adls = DataLakeServiceClient(
    account_url=f"https://{os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net",
    credential=os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
)
fs = adls.get_file_system_client('raw')

# --- Tables to export ---
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
    print(f'⏳ Exporting {table}...')
    
    with engine.connect() as conn:
        df = pd.read_sql(text(f'SELECT * FROM {table}'), conn)
    
    if df.empty:
        print(f'   ⚠️  {table} is empty, skipping')
        continue

    # Convert to Parquet in memory
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine='pyarrow')
    buf.seek(0)

    # Upload path: raw/financial/<table_name>/date=<today>/<table_name>.parquet
    path = f'financial/{name}/date={today}/{name}.parquet'
    fs.create_directory(f'financial/{name}/date={today}')
    file_client = fs.get_file_client(path)
    file_client.upload_data(buf.read(), overwrite=True)
    
    uploaded.append({'table': table, 'rows': len(df), 'path': path})
    print(f'   ✅ {len(df)} rows → raw/{path}')

print(f'\n🎉 Done! {len(uploaded)}/{len(TABLES)} tables uploaded to ADLS Gen2 raw zone')
print('\nSummary:')
for u in uploaded:
    print(f'  {u[\"table\"]:45s} {u[\"rows\"]:>6} rows → {u[\"path\"]}')
