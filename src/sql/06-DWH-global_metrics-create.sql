--DROP TABLE IF EXISTS STV2023121134__DWH.global_metrics;
CREATE TABLE IF NOT EXISTS STV2023121134__DWH.global_metrics
(
    date_update DATE,
    currency_from INT,
    amount_total NUMERIC(18,6),
    cnt_transactions INT,
    avg_transactions_per_account NUMERIC(18,6),
    cnt_accounts_make_transactions INT
)
ORDER BY date_update, currency_from
SEGMENTED BY hash(date_update, currency_from) ALL NODES
PARTITION BY date_update::date;
