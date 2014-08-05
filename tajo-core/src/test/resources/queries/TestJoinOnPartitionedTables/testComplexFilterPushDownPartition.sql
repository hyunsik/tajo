select a.n_nationkey, a.n_name, b.c_custkey, b.c_nationkey, b.c_name
from nation a
left outer join customer_parts b on upper(a.n_nationkey::text)::int4 = b.c_custkey
where a.n_nationkey = 1;


-- to_char(add_months(to_date('201405', 'yyyymm'), -1),
-- 'yyyymm') = b.strd_ym
--
--       and s3.eqp_net_cl_cd = b.eqp_net_cl_cd
--       and b.app_grp_cd = '33'
--       and s3.app_grp_dtl_cd = b.app_grp_dtl_cd
--       and b.seg_grp_cd = '12'
--       and s3.seg_grp_dtl_cd = b.seg_grp_dtl_cd