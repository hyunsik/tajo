SELECT * FROM (
  SELECT
    A.n_regionkey,
    B.r_regionkey,
    A.n_name,
    B.r_name
  FROM
    (
      SELECT
        *
      FROM
        nation
    ) A JOIN region B ON A.n_regionkey = B.r_regionkey
) A LEFT OUTER JOIN region B ON A.n_regionkey = B.r_regionkey AND B.r_name = 'AFRICA';