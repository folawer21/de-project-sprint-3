DELETE FROM mart.f_sales
WHERE date_id IN (SELECT date_id FROM mart.d_calendar WHERE date_actual = %(date)s::date);

INSERT INTO mart.f_sales
    (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
SELECT dc.date_id, u.item_id, u.customer_id, u.city_id,
       CASE WHEN u.status = 'refunded' THEN -ABS(u.quantity) ELSE u.quantity END,
       CASE WHEN u.status = 'refunded' THEN -ABS(u.payment_amount) ELSE u.payment_amount END,
       u.status
FROM staging.user_order_log u
JOIN mart.d_calendar dc ON dc.date_actual = u.date_time::date
WHERE u.date_time::date = %(date)s::date;
