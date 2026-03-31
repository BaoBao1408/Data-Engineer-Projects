import random

def generate_discount_by_price(price: float):
    """
    Decide discount_type & discount_value based on product price
    """

    if price < 50:
        return "percentage", random.randint(5, 10)

    elif price < 200:
        return "percentage", random.randint(10, 20)

    elif price < 500:
        return "fixed_amount", random.randint(20, 50)

    elif price < 2000:
        return "fixed_amount", random.randint(50, 200)

    else:
        return "fixed_amount", random.randint(200, 500)
