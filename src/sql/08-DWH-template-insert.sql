
INSERT INTO STV2023121134__DWH.{table_name} (
        date_update,
        currency_from,
        amount_total,
        cnt_transactions,
        avg_transactions_per_account,
        cnt_accounts_make_transactions)
WITH t_agg AS (
	SELECT
	    to_date('{date}', 'yyyy-MM-dd') AS date_update
		, currency_code AS currency_from
		, SUM(amount) AS amount_total
		, COUNT(operation_id) AS cnt_transactions
		, COUNT(operation_id)/COUNT(DISTINCT account_number_from) AS avg_transactions_per_account
		, COUNT(DISTINCT account_number_from) AS cnt_accounts_make_transactions
	FROM STV2023121134__STAGING.transactions
    WHERE transaction_dt::DATE = to_date('{date}', 'yyyy-MM-dd')
		AND account_number_from > 0
		AND amount > 0
		AND upper(status) = 'DONE'
	GROUP BY currency_code
)
SELECT
	t.date_update
	, t.currency_from
	, amount_total * COALESCE(c.currency_with_div, 1)
	, t.cnt_transactions
	, t.avg_transactions_per_account
	, t.cnt_accounts_make_transactions
FROM t_agg t
LEFT JOIN STV2023121134__STAGING.currencies c
on  t.currency_from = c.currency_code
AND c.date_update::DATE = to_date('{date}', 'yyyy-MM-dd')
AND c.currency_code_with = 420
ORDER BY t.currency_from;
