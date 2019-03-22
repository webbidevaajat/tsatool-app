DROP VIEW IF EXISTS nupuri_turkuun_c1;

CREATE OR REPLACE TEMP VIEW nupuri_turkuun_c1 AS (

WITH c1_0 AS (SELECT valid_r, istrue AS c1_0 FROM pack_ranges(p_obs_relation := 'obs_main', p_maxminutes := 30, p_statid := 1104, p_seid := 181, p_operator := '<=', p_seval := '0.40')),
c1_1 AS (SELECT valid_r, istrue AS c1_1 FROM pack_ranges(p_obs_relation := 'obs_main', p_maxminutes := 30, p_statid := 1101, p_seid := 3, p_operator := '<', p_seval := '2')),
c1_2 AS (SELECT valid_r, istrue AS c1_2 FROM pack_ranges(p_obs_relation := 'obs_main', p_maxminutes := 30, p_statid := 1104, p_seid := 181, p_operator := '<=', p_seval := '0.45')),
c1_3 AS (SELECT valid_r, istrue AS c1_3 FROM pack_ranges(p_obs_relation := 'obs_main', p_maxminutes := 30, p_statid := 1104, p_seid := 177, p_operator := '>', p_seval := '0.04')),
c1_4 AS (SELECT valid_r, istrue AS c1_4 FROM pack_ranges(p_obs_relation := 'obs_main', p_maxminutes := 30, p_statid := 1104, p_seid := 177, p_operator := '<', p_seval := '2')),

c1_3_4 AS (SELECT
			 c1_3.valid_r * c1_4.valid_r AS valid_r,
			 c1_3,
			 c1_4
			 FROM c1_3
			 JOIN c1_4
			 ON c1_3.valid_r && c1_4.valid_r),
c1_2_3_4 AS (SELECT
			 c1_2.valid_r * c1_3_4.valid_r AS valid_r,
			 c1_2,
			 c1_3,
			 c1_4
			 FROM c1_2
			 JOIN c1_3_4
			 ON c1_2.valid_r && c1_3_4.valid_r),
c1_1_2_3_4 AS (SELECT
			 c1_1.valid_r * c1_2_3_4.valid_r AS valid_r,
			 c1_1,
			 c1_2,
			 c1_3,
			 c1_4
			 FROM c1_1
			 JOIN c1_2_3_4
			 ON c1_1.valid_r && c1_2_3_4.valid_r),
c1_0_1_2_3_4 AS (SELECT
			 c1_0.valid_r * c1_1_2_3_4.valid_r AS valid_r,
			 c1_0,
			 c1_1,
			 c1_2,
			 c1_3,
			 c1_4
			 FROM c1_0
			 JOIN c1_1_2_3_4
			 ON c1_0.valid_r && c1_1_2_3_4.valid_r)

SELECT
lower(valid_r) AS vfrom,
upper(valid_r) AS vuntil,
c1_0,
c1_1,
c1_2,
c1_3,
c1_4,
((c1_0 and c1_1) or (c1_2 and c1_3 and c1_4 and c1_1)) AS c1_m
FROM c1_0_1_2_3_4);

SELECT * FROM nupuri_turkuun_c1;
