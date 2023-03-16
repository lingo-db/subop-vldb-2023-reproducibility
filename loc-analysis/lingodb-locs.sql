.header on
select sum(nCode) as dialect from t where file like 'include/mlir/Dialect/SubOperator/SubOp%' or file like 'lib/SubOperator/SubOp%';
select sum(nCode) as transforms from t where (file like 'lib/SubOperator/Transforms/%' and file not like '%Parallel%') or file like 'include/mlir/Dialect/SubOperator/Transforms/%';
select sum(nCode) as parallelization from t where file like '%Parallel%';
select sum(nCode) as relalgimpl from t where file like 'include/mlir/Conversion/RelAlgToSubOp/%' or file  like 'lib/Conversion/RelAlgToSubOp/%';
select sum(nCode) as subopimpl from t where file like 'lib/Conversion/SubOpToControlFlow/%';
