 SELECT
  l_orderkey,
  sum(l_quantity ) over()
FROM
  lineitem;