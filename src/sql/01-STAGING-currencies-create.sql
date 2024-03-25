
--DROP TABLE IF EXISTS STV2023121134__STAGING.currencies;

CREATE TABLE if NOT EXISTS STV2023121134__STAGING.currencies
(   currency_code INTEGER,
    currency_code_with INTEGER,
    date_update TIMESTAMP,
    currency_with_div DECIMAL(3, 2),
    UNIQUE(currency_code, currency_code_with, date_update) ENABLED
)
ORDER BY currency_code, currency_code_with, date_update
SEGMENTED BY hash(currency_code,date_update) ALL NODES
PARTITION BY date_update::DATE;
