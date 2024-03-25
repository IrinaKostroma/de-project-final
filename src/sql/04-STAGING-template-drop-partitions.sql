
SELECT DROP_PARTITIONS(
    'STV2023121134__STAGING.{table_name}',
    '{date}', /* start partition key */
    '{date}'  /* end partition key (included) */
);
