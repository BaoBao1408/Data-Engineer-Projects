from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["glamira_raw"]

product = db.products.find_one({"product_id": "105481"})

data = product["react_data"]

print({
    "product_id": data["product_id"],
    "name": data["name"],
    "price": data["price"],
    "category": data["category_name"],
    "gender": data["gender"],
    "collection": data["collection"]
})