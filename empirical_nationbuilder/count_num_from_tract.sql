select census_tract, count(*) as num
from georgia_vf_nationbuilder_full_report_rows
GROUP BY
  census_tract
ORDER BY num DESC;
