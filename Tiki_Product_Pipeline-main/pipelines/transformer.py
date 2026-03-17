from bs4 import BeautifulSoup

def clean_description(html):
    soup = BeautifulSoup(html or "", "lxml")
    return soup.get_text(separator=" ", strip=True)

def transform(data):
    if not data:
        return None
    return {
        "id": data.get("id"),
        "name": data.get("name"),
        "url_key": data.get("url_key"),
        "price": data.get("price"),
        "description": clean_description(data.get("description")),
        "images": [img.get("base_url") for img in (data.get("images") or [])]
    }
