from generators.category import generate_categories
from generators.brand import generate_brands
from generators.seller import generate_sellers
from generators.product import generate_products
from generators.order import generate_orders
from generators.order_item import generate_order_items
from generators.promotion import generate_promotions
from generators.promotion_product import generate_promotion_products
from utils.update_order_totals import update_order_totals

def main():
    print("Starting data generation...")

    category_id_map = generate_categories()

    brand_ids_by_category, brand_id_to_name = generate_brands(category_id_map)

    seller_ids_by_category = generate_sellers()

    products = generate_products(
        category_ids=category_id_map,
        brand_ids_by_category=brand_ids_by_category,
        brand_id_to_name=brand_id_to_name,
        seller_ids_by_category=seller_ids_by_category,
    )
    
    promotions = generate_promotions()
    generate_promotion_products(products, promotions)

    orders = generate_orders(seller_ids_by_category)
    generate_order_items(orders, products)
    update_order_totals()

    print("DONE")

if __name__ == "__main__":
    main()
