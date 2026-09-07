-- ============================================================
-- Silver Layer Transformations — Bronze → Silver
-- ============================================================
-- Aplica ELT: las transformaciones ocurren dentro del motor de
-- BigQuery (no se extraen datos hacia fuera). Cada tabla incluye
-- validación de conteos donde existe un filtro que podría
-- eliminar filas de forma inesperada.
-- ============================================================

-- VARIABLE DECLARATION
DECLARE count_bronze_orders INT64;
DECLARE count_silver_orders INT64;
DECLARE count_bronze_order_payments INT64;
DECLARE count_silver_order_payments INT64;
DECLARE count_bronze_products INT64;
DECLARE count_silver_products INT64;

-- ------------------------------------------------------------
-- ORDERS
-- Regla de negocio: solo se conservan órdenes "delivered" con
-- fecha de entrega válida. Una orden "delivered" sin fecha de
-- entrega es una contradicción de integridad de datos.
-- ------------------------------------------------------------
CREATE TEMP TABLE temp_clean_orders AS
SELECT
  order_id,
  customer_id,
  order_status,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_purchase_timestamp) AS order_purchase_timestamp,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_delivered_customer_date) AS order_delivered_customer_date,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_estimated_delivery_date) AS order_estimated_delivery_date
FROM bronze_raw.olist_orders_dataset
WHERE order_status = "delivered"
  AND SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_delivered_customer_date) IS NOT NULL;

SET count_bronze_orders = (
  SELECT COUNT(*) FROM bronze_raw.olist_orders_dataset
  WHERE order_status = "delivered"
    AND SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_delivered_customer_date) IS NOT NULL
);
SET count_silver_orders = (SELECT COUNT(*) FROM temp_clean_orders);

IF count_bronze_orders = count_silver_orders THEN
  CREATE OR REPLACE TABLE silver_staging.clean_orders AS
  SELECT * FROM temp_clean_orders;
ELSE
  RAISE USING MESSAGE = FORMAT('Validación fallida en ORDERS: Bronze (%d) != Silver (%d)', count_bronze_orders, count_silver_orders);
END IF;

-- ------------------------------------------------------------
-- ORDER_PAYMENTS
-- Semi-join: solo se conservan pagos de órdenes que sobrevivieron
-- el filtro de clean_orders.
-- ------------------------------------------------------------
CREATE TEMP TABLE temp_clean_order_payments AS
SELECT
  order_id,
  SAFE_CAST(payment_sequential AS INT64) AS payment_sequential,
  payment_type,
  SAFE_CAST(payment_installments AS INT64) AS payment_installments,
  SAFE_CAST(payment_value AS NUMERIC) AS payment_value
FROM bronze_raw.olist_order_payments_dataset
WHERE order_id IN (SELECT order_id FROM silver_staging.clean_orders);

SET count_bronze_order_payments = (
  SELECT COUNT(*) FROM bronze_raw.olist_order_payments_dataset
  WHERE order_id IN (SELECT order_id FROM silver_staging.clean_orders)
);
SET count_silver_order_payments = (SELECT COUNT(*) FROM temp_clean_order_payments);

IF count_bronze_order_payments = count_silver_order_payments THEN
  CREATE OR REPLACE TABLE silver_staging.clean_order_payments AS
  SELECT * FROM temp_clean_order_payments;
ELSE
  RAISE USING MESSAGE = FORMAT('Validación fallida en ORDER_PAYMENTS: Bronze (%d) != Silver (%d)', count_bronze_order_payments, count_silver_order_payments);
END IF;

-- ------------------------------------------------------------
-- PRODUCTS
-- Imputación de nulos numéricos con mediana (APPROX_QUANTILES),
-- calculada una sola vez vía CROSS JOIN (no soportado como
-- función de ventana en BigQuery).
-- ------------------------------------------------------------
CREATE TEMP TABLE temp_clean_products AS
WITH data_conv AS (
  SELECT * REPLACE(
    SAFE_CAST(product_weight_g AS NUMERIC) AS product_weight_g,
    SAFE_CAST(product_length_cm AS NUMERIC) AS product_length_cm,
    SAFE_CAST(product_height_cm AS NUMERIC) AS product_height_cm,
    SAFE_CAST(product_width_cm AS NUMERIC) AS product_width_cm
  )
  FROM bronze_raw.olist_products_dataset
),
medians AS (
  SELECT
    APPROX_QUANTILES(product_weight_g, 2)[OFFSET(1)] AS median_weight,
    APPROX_QUANTILES(product_length_cm, 2)[OFFSET(1)] AS median_length,
    APPROX_QUANTILES(product_height_cm, 2)[OFFSET(1)] AS median_height,
    APPROX_QUANTILES(product_width_cm, 2)[OFFSET(1)] AS median_width
  FROM data_conv
)
SELECT
  product_id,
  COALESCE(product_category_name, "unknown") AS product_category_name,
  COALESCE(product_weight_g, m.median_weight) AS product_weight_g,
  COALESCE(product_length_cm, m.median_length) AS product_length_cm,
  COALESCE(product_height_cm, m.median_height) AS product_height_cm,
  COALESCE(product_width_cm, m.median_width) AS product_width_cm
FROM data_conv
CROSS JOIN medians m;

SET count_bronze_products = (SELECT COUNT(*) FROM bronze_raw.olist_products_dataset);
SET count_silver_products = (SELECT COUNT(*) FROM temp_clean_products);

IF count_bronze_products = count_silver_products THEN
  CREATE OR REPLACE TABLE silver_staging.clean_products AS
  SELECT * FROM temp_clean_products;
ELSE
  RAISE USING MESSAGE = FORMAT('Validación fallida en PRODUCTS: Bronze (%d) != Silver (%d)', count_bronze_products, count_silver_products);
END IF;

-- ------------------------------------------------------------
-- ORDER_ITEMS
-- Sin filtros que alteren el conteo de filas -> no requiere
-- bloque de validación (COUNT sería una verdad garantizada).
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE silver_staging.clean_order_items AS
SELECT
  order_id,
  order_item_id,
  product_id,
  SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', shipping_limit_date) AS shipping_limit_date,
  SAFE_CAST(price AS NUMERIC) AS price,
  SAFE_CAST(freight_value AS NUMERIC) AS freight_value
FROM bronze_raw.olist_order_items_dataset;

-- ------------------------------------------------------------
-- CUSTOMERS
-- Sin filtros que alteren el conteo de filas -> no requiere
-- bloque de validación.
-- ------------------------------------------------------------
CREATE OR REPLACE TABLE silver_staging.clean_customers AS
SELECT
  customer_id,
  customer_unique_id,
  customer_zip_code_prefix,
  customer_city,
  customer_state
FROM bronze_raw.olist_customers_dataset;