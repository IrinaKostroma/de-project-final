
--DROP TABLE IF EXISTS STV2023121134__STAGING.transactions;

CREATE TABLE IF NOT EXISTS STV2023121134__STAGING.transactions
(   operation_id VARCHAR(100) NOT NULL,
    account_number_from INT NULL,
    account_number_to INT NULL,
    currency_code INT NULL,
    country VARCHAR(100) NULL,
    status VARCHAR(100) NULL,
    transaction_type VARCHAR(100) NULL,
    amount INT NULL,
    transaction_dt TIMESTAMP(0) NULL,
    CONSTRAINT transactions_pk PRIMARY KEY (operation_id, transaction_dt,status) ENABLED
    )
ORDER BY transaction_dt, currency_code
SEGMENTED BY hash(currency_code) ALL NODES
KSAFE 1
PARTITION BY transaction_dt::DATE;

