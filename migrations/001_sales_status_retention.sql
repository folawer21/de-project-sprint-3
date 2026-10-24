BEGIN;
ALTER TABLE staging.user_order_log ADD COLUMN IF NOT EXISTS status text;
UPDATE staging.user_order_log SET status = 'shipped' WHERE status IS NULL;
ALTER TABLE staging.user_order_log ALTER COLUMN status SET DEFAULT 'shipped';
ALTER TABLE staging.user_order_log ALTER COLUMN status SET NOT NULL;

ALTER TABLE mart.f_sales ADD COLUMN IF NOT EXISTS status text;
-- Исторические факты до появления статуса являются отгрузками.
UPDATE mart.f_sales SET status = 'shipped' WHERE status IS NULL;
ALTER TABLE mart.f_sales ALTER COLUMN status SET DEFAULT 'shipped';
ALTER TABLE mart.f_sales ALTER COLUMN status SET NOT NULL;

CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
    period_name text NOT NULL DEFAULT 'weekly',
    period_id integer NOT NULL,
    period_start date NOT NULL,
    item_id integer NOT NULL REFERENCES mart.d_item(item_id),
    new_customers_count bigint NOT NULL,
    returning_customers_count bigint NOT NULL,
    refunded_customer_count bigint NOT NULL,
    new_customers_revenue numeric(18,2) NOT NULL,
    returning_customers_revenue numeric(18,2) NOT NULL,
    customers_refunded bigint NOT NULL,
    PRIMARY KEY (period_start, item_id)
);

CREATE TABLE IF NOT EXISTS staging.etl_reports (
    report_id text PRIMARY KEY,
    loaded_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMIT;
