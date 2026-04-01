from google.cloud import storage
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    "C:/Users/baoqu/.ssh/glamira-data-foundation-7f36ce68ccb1.json"
)

client = storage.Client(
    credentials=credentials,
    project="glamira-data-foundation"
)