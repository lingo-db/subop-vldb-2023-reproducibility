.header on
select sum(nCode) from t where file like '%distinct_aggregate_data%' or file like '%grouped_aggregate_data%' or file like '%physical_hash_aggregate%' or file like '%physical_ungrouped_aggregate%';
select sum(nCode) from t where file like '%physical_window%';
select sum(nCode) from t where file like '%physical_order%';
select sum(nCode) from t where file like '%physical_table_scan%';
