
COPY STV2023121134__STAGING.{table_name} ({column_names})
FROM LOCAL '{tmp_dir_path}/stg_{table_name}-{date}.csv' DELIMITER ','
SKIP 1
REJECTED DATA '{tmp_dir_path}/rej_{table_name}-{date}.csv';
