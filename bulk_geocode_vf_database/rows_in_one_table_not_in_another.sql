-- Maybe doesn't work
-- Successfully run. Total query runtime: 2 min 6 secs.
-- 300870 rows affected.

SELECT state_file_id FROM georgia_vh_nationbuilder B
WHERE NOT EXISTS (SELECT FROM georgia_vf_nationbuilder
				 WHERE state_file_id = B.state_file_id)
GROUP BY state_file_id
order by state_file_id desc;


-- seems to work, gives different results from other...

select georgia_vh_nationbuilder.state_file_id from georgia_vh_nationbuilder left outer join georgia_vf_nationbuilder on (georgia_vh_nationbuilder.state_file_id = georgia_vf_nationbuilder.state_file_id)
where georgia_vf_nationbuilder.state_file_id is null
order by georgia_vh_nationbuilder.state_file_id desc


-- same but deduplicating
-- Successfully run. Total query runtime: 2 min 28 secs.
-- 300870 rows affected.

select georgia_vh_nationbuilder.state_file_id
from georgia_vh_nationbuilder left outer join georgia_vf_nationbuilder
on (georgia_vh_nationbuilder.state_file_id = georgia_vf_nationbuilder.state_file_id)
where georgia_vf_nationbuilder.state_file_id is null
GROUP BY georgia_vh_nationbuilder.state_file_id
order by georgia_vh_nationbuilder.state_file_id desc;
