-- TSA-kannan esimerkkikyselyitä
-- Arttu K / WSP Finland 6/2019
--
-- Testattu Ubuntu-serverillä 16 Gb muistia / 2 kpl 2.6 GHz prosessoria
--
-- TAULUT
SELECT *, prop -> 'name' FROM stations;
SELECT * FROM sensors;
SELECT * FROM statobs LIMIT 1000;
SELECT * FROM seobs LIMIT 1000;

-- Analyysien yhteydessä tehtäviä operaatioita
CREATE OR REPLACE TEMP VIEW statobs_time AS
	SELECT id, tfrom, statid
	FROM statobs
	WHERE tfrom BETWEEN '2018-01-01 00:00:00' AND '2018-03-10 23:59:59';

SELECT DISTINCT statid FROM statobs_time ORDER BY statid;

CREATE OR REPLACE TEMP VIEW obs_main AS
	SELECT tfrom, statid, seid, seval
	FROM statobs_time
	INNER JOIN seobs
	ON statobs_time.id = seobs.obsid;

SELECT DISTINCT seid FROM obs_main ORDER BY seid; -- PITKÄ!

-- Funktio, jolla kysellään yksittäisen BLOKIN totuusarvot
-- PITKÄ, vajaa 1 min!
SELECT *
FROM pack_ranges(p_obs_relation := 'obs_main', p_maxminutes := 30, p_statid := 1122, p_seid := 176, p_operator := '<=', p_seval := '0.15');

-- Yhden ehtolauseen tuloksia vastaava näkymä:
-- s1122#kitka3 <= 0.15 AND s1115#tie_1 < 2
CREATE OR REPLACE TEMP VIEW vt7_itasalmi_lanteen_d1_vaarallinen AS (
WITH d1_vaarallinen_0 AS (SELECT valid_r, istrue AS d1_vaarallinen_0
						  FROM pack_ranges(p_obs_relation := 'obs_main', p_maxminutes := 30, p_statid := 1122, p_seid := 176, p_operator := '<=', p_seval := '0.15')),
d1_vaarallinen_1 AS (SELECT valid_r, istrue AS d1_vaarallinen_1
					 FROM pack_ranges(p_obs_relation := 'obs_main', p_maxminutes := 30, p_statid := 1115, p_seid := 3, p_operator := '<', p_seval := '2')),
d1_vaarallinen_0_1 AS (SELECT
d1_vaarallinen_0.valid_r * d1_vaarallinen_1.valid_r AS valid_r,
d1_vaarallinen_0,
d1_vaarallinen_1
FROM d1_vaarallinen_0
JOIN d1_vaarallinen_1
ON d1_vaarallinen_0.valid_r && d1_vaarallinen_1.valid_r)
SELECT
lower(valid_r) AS vfrom,
upper(valid_r) AS vuntil,
upper(valid_r)-lower(valid_r) AS vdiff,
d1_vaarallinen_0,
d1_vaarallinen_1,
(d1_vaarallinen_0 and d1_vaarallinen_1) AS master
FROM d1_vaarallinen_0_1);

-- ... ja sen sisältö
-- PITKÄ, vajaa 1 min!
SELECT * FROM vt7_itasalmi_lanteen_d1_vaarallinen;

-- Monimutkaisempi ehtolauseen näkymä:
-- ((s1115#sade in (6) AND s1115#sade_intensiteetti > 4) OR s1115#sade_intensiteetti > 8) AND (s1122#veden_maara3 > 0.4 AND s1122#jaan_maara3 < 0.02 AND s1122#lumen_maara3 < 0.02)
CREATE OR REPLACE TEMP VIEW vt7_itasalmi_lanteen_c4_vesiliirto AS (
WITH c4_vesiliirto_0 AS (SELECT valid_r, istrue AS c4_vesiliirto_0 FROM pack_ranges(p_obs_relation := 'obs_main', p_maxminutes := 30, p_statid := 1115, p_seid := 22, p_operator := 'in', p_seval := '(6)')),
c4_vesiliirto_1 AS (SELECT valid_r, istrue AS c4_vesiliirto_1 FROM pack_ranges(p_obs_relation := 'obs_main', p_maxminutes := 30, p_statid := 1115, p_seid := 23, p_operator := '>', p_seval := '4')),
c4_vesiliirto_2 AS (SELECT valid_r, istrue AS c4_vesiliirto_2 FROM pack_ranges(p_obs_relation := 'obs_main', p_maxminutes := 30, p_statid := 1115, p_seid := 23, p_operator := '>', p_seval := '8')),
c4_vesiliirto_3 AS (SELECT valid_r, istrue AS c4_vesiliirto_3 FROM pack_ranges(p_obs_relation := 'obs_main', p_maxminutes := 30, p_statid := 1122, p_seid := 177, p_operator := '>', p_seval := '0.4')),
c4_vesiliirto_4 AS (SELECT valid_r, istrue AS c4_vesiliirto_4 FROM pack_ranges(p_obs_relation := 'obs_main', p_maxminutes := 30, p_statid := 1122, p_seid := 179, p_operator := '<', p_seval := '0.02')),
c4_vesiliirto_5 AS (SELECT valid_r, istrue AS c4_vesiliirto_5 FROM pack_ranges(p_obs_relation := 'obs_main', p_maxminutes := 30, p_statid := 1122, p_seid := 178, p_operator := '<', p_seval := '0.02')),
c4_vesiliirto_4_5 AS (SELECT
c4_vesiliirto_4.valid_r * c4_vesiliirto_5.valid_r AS valid_r,
c4_vesiliirto_4,
c4_vesiliirto_5
FROM c4_vesiliirto_4
JOIN c4_vesiliirto_5
ON c4_vesiliirto_4.valid_r && c4_vesiliirto_5.valid_r),
c4_vesiliirto_3_4_5 AS (SELECT
c4_vesiliirto_3.valid_r * c4_vesiliirto_4_5.valid_r AS valid_r,
c4_vesiliirto_3,
c4_vesiliirto_4,
c4_vesiliirto_5
FROM c4_vesiliirto_3
JOIN c4_vesiliirto_4_5
ON c4_vesiliirto_3.valid_r && c4_vesiliirto_4_5.valid_r),
c4_vesiliirto_2_3_4_5 AS (SELECT
c4_vesiliirto_2.valid_r * c4_vesiliirto_3_4_5.valid_r AS valid_r,
c4_vesiliirto_2,
c4_vesiliirto_3,
c4_vesiliirto_4,
c4_vesiliirto_5
FROM c4_vesiliirto_2
JOIN c4_vesiliirto_3_4_5
ON c4_vesiliirto_2.valid_r && c4_vesiliirto_3_4_5.valid_r),
c4_vesiliirto_1_2_3_4_5 AS (SELECT
c4_vesiliirto_1.valid_r * c4_vesiliirto_2_3_4_5.valid_r AS valid_r,
c4_vesiliirto_1,
c4_vesiliirto_2,
c4_vesiliirto_3,
c4_vesiliirto_4,
c4_vesiliirto_5
FROM c4_vesiliirto_1
JOIN c4_vesiliirto_2_3_4_5
ON c4_vesiliirto_1.valid_r && c4_vesiliirto_2_3_4_5.valid_r),
c4_vesiliirto_0_1_2_3_4_5 AS (SELECT
c4_vesiliirto_0.valid_r * c4_vesiliirto_1_2_3_4_5.valid_r AS valid_r,
c4_vesiliirto_0,
c4_vesiliirto_1,
c4_vesiliirto_2,
c4_vesiliirto_3,
c4_vesiliirto_4,
c4_vesiliirto_5
FROM c4_vesiliirto_0
JOIN c4_vesiliirto_1_2_3_4_5
ON c4_vesiliirto_0.valid_r && c4_vesiliirto_1_2_3_4_5.valid_r)
SELECT
lower(valid_r) AS vfrom,
upper(valid_r) AS vuntil,
upper(valid_r)-lower(valid_r) AS vdiff,
c4_vesiliirto_0,
c4_vesiliirto_1,
c4_vesiliirto_2,
c4_vesiliirto_3,
c4_vesiliirto_4,
c4_vesiliirto_5,
(((c4_vesiliirto_0 and c4_vesiliirto_1) or c4_vesiliirto_2) and (c4_vesiliirto_3 and c4_vesiliirto_4 and c4_vesiliirto_5)) AS master
FROM c4_vesiliirto_0_1_2_3_4_5);

-- ... ja sen sisältö
-- PITKÄ, n. 1 min
SELECT * FROM vt7_itasalmi_lanteen_c4_vesiliirto;
