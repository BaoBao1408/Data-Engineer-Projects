-- ===============================
-- SCHEMA: E-COMMERCE OLTP
-- DATABASE: ecommerce_oltp
-- ===============================

BEGIN;

-- ===============================
-- 1. BRAND
-- ===============================
CREATE TABLE IF NOT EXISTS brand (
    brand_id        SERIAL PRIMARY KEY,
    brand_name      VARCHAR(100) NOT NULL,
    country         VARCHAR(50),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DROP INDEX IF EXISTS idx_brand_name;
CREATE INDEX idx_brand_name ON brand(brand_name);

-- ===============================
-- 2. CATEGORY (SELF-REFERENCE)
-- ===============================
CREATE TABLE IF NOT EXISTS category (
    category_id         SERIAL PRIMARY KEY,
    category_name       VARCHAR(100) NOT NULL,
    parent_category_id  INT REFERENCES category(category_id),
    level               SMALLINT NOT NULL,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
DROP INDEX IF EXISTS idx_category_parent;
CREATE INDEX idx_category_parent ON category(parent_category_id);
DROP INDEX IF EXISTS idx_category_name;
CREATE INDEX idx_category_name ON category(category_name);

-- ===============================
-- 3. SELLER
-- ===============================
CREATE TABLE IF NOT EXISTS seller (
    seller_id       SERIAL PRIMARY KEY,
    seller_name     VARCHAR(150) NOT NULL,
    join_date       DATE NOT NULL,
    seller_type     VARCHAR(50) CHECK (seller_type IN ('Official', 'Marketplace')),
    rating          NUMERIC(2,1) CHECK (rating BETWEEN 0 AND 5),
    country         VARCHAR(50) DEFAULT 'Vietnam'
);
DROP INDEX IF EXISTS idx_seller_type;
CREATE INDEX idx_seller_type ON seller(seller_type);

-- ===============================
-- 4. PRODUCT
-- ===============================
CREATE TABLE IF NOT EXISTS product (
    product_id      SERIAL PRIMARY KEY,
    product_name    VARCHAR(200) NOT NULL,
    category_id     INT NOT NULL REFERENCES category(category_id),
    brand_id        INT NOT NULL REFERENCES brand(brand_id),
    seller_id       INT NOT NULL REFERENCES seller(seller_id),
    price           NUMERIC(12,2) NOT NULL CHECK (price >= 0),
    discount_price  NUMERIC(12,2) CHECK (discount_price <= price),
    stock_qty       INT CHECK (stock_qty >= 0),
    rating          NUMERIC(2,1) CHECK (rating BETWEEN 0 AND 5),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active       BOOLEAN DEFAULT TRUE
);

DROP INDEX IF EXISTS idx_product_category;
DROP INDEX IF EXISTS idx_product_seller;
DROP INDEX IF EXISTS idx_product_brand;
CREATE INDEX idx_product_category ON product(category_id);
CREATE INDEX idx_product_seller ON product(seller_id);
CREATE INDEX idx_product_brand ON product(brand_id);

-- ===============================
-- 5. ORDER
-- ===============================
CREATE TABLE IF NOT EXISTS orders (
    order_id        SERIAL PRIMARY KEY,
    order_date      TIMESTAMP NOT NULL,
    seller_id       INT NOT NULL REFERENCES seller(seller_id),
    status          VARCHAR(20) CHECK (
        status IN (
            'PLACED',
            'PAID',
            'SHIPPED',
            'DELIVERED',
            'CANCELLED',
            'RETURNED'
        )
    ),
    total_amount    NUMERIC(12,2) CHECK (total_amount >= 0),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP INDEX IF EXISTS idx_orders_seller;
DROP INDEX IF EXISTS idx_orders_date;
DROP INDEX IF EXISTS idx_orders_status;
CREATE INDEX idx_orders_seller ON orders(seller_id);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(status);

-- ===============================
-- 6. ORDER ITEM
-- ===============================
CREATE TABLE IF NOT EXISTS order_item (
    order_item_id   SERIAL PRIMARY KEY,
    order_id        INT NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    product_id      INT NOT NULL REFERENCES product(product_id),
    quantity        INT NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(12,2) NOT NULL CHECK (unit_price >= 0),
    subtotal        NUMERIC(12,2) NOT NULL CHECK (subtotal >= 0)
);

ALTER TABLE order_item
ADD COLUMN IF NOT EXISTS order_date TIMESTAMP;

ALTER TABLE order_item
ADD COLUMN IF NOT EXISTS created_at TIMESTAMP;

DROP INDEX IF EXISTS idx_order_item_order;
DROP INDEX IF EXISTS idx_order_item_product;
CREATE INDEX idx_order_item_order ON order_item(order_id);
CREATE INDEX idx_order_item_product ON order_item(product_id);

-- ===============================
-- 7. PROMOTION
-- ===============================
CREATE TABLE IF NOT EXISTS promotion (
    promotion_id       SERIAL PRIMARY KEY,
    promotion_name     VARCHAR(100) NOT NULL,
    promotion_type     VARCHAR(50),
    discount_type      VARCHAR(20) CHECK (discount_type IN ('percentage', 'fixed_amount')),
    discount_value     NUMERIC(10,2) CHECK (discount_value > 0),
    start_date         DATE NOT NULL,
    end_date           DATE NOT NULL CHECK (end_date > start_date)
);

DROP INDEX IF EXISTS idx_promotion_date;
CREATE INDEX idx_promotion_date ON promotion(start_date, end_date);

-- ===============================
-- 8. PROMOTION - PRODUCT (N:N)
-- ===============================
CREATE TABLE IF NOT EXISTS promotion_product (
    promo_product_id   SERIAL PRIMARY KEY,
    promotion_id       INT NOT NULL REFERENCES promotion(promotion_id) ON DELETE CASCADE,
    product_id         INT NOT NULL REFERENCES product(product_id),
    created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (promotion_id, product_id)
);

DROP INDEX IF EXISTS idx_promo_product_promo;
DROP INDEX IF EXISTS idx_promo_product_product;
CREATE INDEX idx_promo_product_promo ON promotion_product(promotion_id);
CREATE INDEX idx_promo_product_product ON promotion_product(product_id);

COMMIT;
