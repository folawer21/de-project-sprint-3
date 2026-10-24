INSERT INTO mart.d_city (city_id, city_name)
SELECT city_id, MAX(city_name) FROM staging.user_order_log GROUP BY city_id
ON CONFLICT (city_id) DO UPDATE SET city_name = EXCLUDED.city_name;

INSERT INTO mart.d_customer (customer_id, first_name, last_name, city_id)
SELECT customer_id, MAX(first_name), MAX(last_name), MAX(city_id)
FROM staging.user_order_log GROUP BY customer_id
ON CONFLICT (customer_id) DO UPDATE SET
    first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, city_id = EXCLUDED.city_id;

INSERT INTO mart.d_item (item_id, item_name)
SELECT item_id, MAX(item_name) FROM staging.user_order_log GROUP BY item_id
ON CONFLICT (item_id) DO UPDATE SET item_name = EXCLUDED.item_name;
