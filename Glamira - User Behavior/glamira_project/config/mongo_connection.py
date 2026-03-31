import os
from pathlib import Path
from pymongo import MongoClient
from dotenv import load_dotenv


# locate .env inside config folder
BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"

load_dotenv(ENV_PATH)


def connect_mongo():

    mongo_uri = os.getenv("MONGO_URI")
    db_name = os.getenv("DB_NAME")

    if not mongo_uri:
        raise ValueError("MONGO_URI not found in .env")

    if not db_name:
        raise ValueError("DB_NAME not found in .env")

    client = MongoClient(
        mongo_uri,
        maxPoolSize=50,
        minPoolSize=5,
        serverSelectionTimeoutMS=120000,
        socketTimeoutMS=120000,
        connectTimeoutMS=120000
    )

    db = client[db_name]

    return client, db

if __name__ == "__main__":

    try:

        db = connect_mongo()

        # test connection
        db.command("ping")

        print("✅ Connected to MongoDB")

        collections = db.list_collection_names()

        print("Collections:", collections)

    except Exception as e:

        print("❌ MongoDB connection failed")
        print(e)