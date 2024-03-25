
SELECT DROP_PARTITIONS(
    'STV2023121134__DWH.{table_name}',
    '{date}', /* start partition key */
    '{date}'  /* end partition key (included) */
);
