import os
from pymongo import MongoClient
from dotenv import load_dotenv


# load environment variables
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")


def connect_mongo():

    if not MONGO_URI:
        raise ValueError("MONGO_URI not found in .env")

    if not DB_NAME:
        raise ValueError("DB_NAME not found in .env")

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=120000,
        socketTimeoutMS=120000,
        connectTimeoutMS=120000
    )

    db = client[DB_NAME]

    return db


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