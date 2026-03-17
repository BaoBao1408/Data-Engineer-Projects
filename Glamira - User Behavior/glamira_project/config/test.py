# from pymongo import MongoClient
# from pymongo.errors import ServerSelectionTimeoutError

# try:
#     client = MongoClient(
#         "mongodb://localhost:27017",
#         serverSelectionTimeoutMS=5000
#     )

#     client.admin.command("ping")

#     print("✅ MongoDB connected")
#     print("Databases:", client.list_database_names())

# except ServerSelectionTimeoutError:
#     print("❌ Cannot connect to MongoDB. Is mongod running?")

from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")

print(client.list_database_names())