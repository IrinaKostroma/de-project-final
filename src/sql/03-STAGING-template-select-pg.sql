
SELECT *
FROM public.{table_name}
WHERE to_char({col_dt},'yyyy-mm-dd') = '{date}'
