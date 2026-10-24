DELETE FROM mart.f_customer_retention
WHERE period_start = date_trunc('week', %(date)s::date)::date;

INSERT INTO mart.f_customer_retention
    (period_name, period_id, period_start, item_id,
     new_customers_count, returning_customers_count, refunded_customer_count,
     new_customers_revenue, returning_customers_revenue, customers_refunded)
WITH customers AS (
    SELECT date_trunc('week', dc.date_actual)::date AS period_start,
           f.item_id, f.customer_id,
           COUNT(*) FILTER (WHERE f.status = 'shipped') AS orders,
           COUNT(*) FILTER (WHERE f.status = 'refunded') AS refunds,
           COALESCE(SUM(f.payment_amount) FILTER (WHERE f.status = 'shipped'), 0) AS revenue
    FROM mart.f_sales f
    JOIN mart.d_calendar dc USING (date_id)
    WHERE dc.date_actual >= date_trunc('week', %(date)s::date)::date
      AND dc.date_actual < date_trunc('week', %(date)s::date)::date + 7
    GROUP BY 1, f.item_id, f.customer_id
)
SELECT 'weekly', EXTRACT(WEEK FROM period_start)::integer, period_start, item_id,
       COUNT(*) FILTER (WHERE orders = 1),
       COUNT(*) FILTER (WHERE orders > 1),
       COUNT(*) FILTER (WHERE refunds > 0),
       COALESCE(SUM(revenue) FILTER (WHERE orders = 1), 0),
       COALESCE(SUM(revenue) FILTER (WHERE orders > 1), 0),
       SUM(refunds)
FROM customers
GROUP BY period_start, item_id;
