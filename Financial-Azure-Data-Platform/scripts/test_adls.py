from azure.storage.filedatalake import DataLakeServiceClient
import os, json
from dotenv import load_dotenv
load_dotenv()

account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')

client = DataLakeServiceClient(
    account_url=f'https://{account_name}.dfs.core.windows.net',
    credential=account_key
)

fs = client.get_file_system_client('raw')
fs.create_directory('financial/entities')
file_client = fs.get_file_client('financial/entities/test.json')
file_client.upload_data(json.dumps({'entity': 'VCB', 'status': 'test'}), overwrite=True)
print('✅ Upload to raw/financial/entities/test.json SUCCESS')

paths = fs.get_paths('financial')
for p in paths:
    print(f'  📁 {p.name}')
