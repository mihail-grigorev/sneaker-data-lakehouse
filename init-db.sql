-- Create isolated analytical database
CREATE DATABASE datamart;
GRANT ALL PRIVILEGES ON DATABASE datamart TO airflow;

-- Connect to the newly created analytical database
\c datamart;

-- Create target datamart table for sneaker sales metrics
CREATE TABLE IF NOT EXISTS public.daily_sneaker_brand_metrics (
    order_date DATE NOT NULL,
    brand VARCHAR(64) NOT NULL,
    channel VARCHAR(32) NOT NULL,
    gross_revenue NUMERIC(14, 2) NOT NULL,
    refund_amount NUMERIC(14, 2) NOT NULL,
    net_revenue NUMERIC(14, 2) NOT NULL,
    purchase_count INT NOT NULL,
    return_count INT NOT NULL,
    return_rate_pct NUMERIC(5, 2) NOT NULL,
    avg_ticket NUMERIC(10, 2) NOT NULL,
    calculated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    -- Composite primary key guarantees idempotency during task retries
    CONSTRAINT pk_sneaker_metrics PRIMARY KEY (order_date, brand, channel)
);

GRANT ALL PRIVILEGES ON TABLE public.daily_sneaker_brand_metrics TO airflow;