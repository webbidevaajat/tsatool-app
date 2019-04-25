-- Get old-new id pairs of stations from db;
-- lotju dumps include "id" ids, but
-- the prod database uses Digitraffic id, here "vanha_id".
-- Either use the subset
-- or select all and do the subsetting inside the python script.

SELECT id, vanha_id FROM tiesaa_asema WHERE vanha_id IN
(1006, 1011, 1019, 1022, 1083, 1101, 1103, 1104, 1105, 1106, 1107,
1108, 1109, 1110, 1111, 1115, 1116, 1118, 1119, 1120, 1121, 1122,
1123, 1124, 1125, 1126, 1131, 1132, 3001, 3002, 3006, 3023, 3024,
3029, 3030, 3045, 3047, 3048, 3051, 3052, 3053, 3054, 3056, 3057,
3058, 3059, 3062, 3063, 3064, 3065, 3066, 3067, 3074, 3075, 3077,
3078, 3079, 3080, 3081, 3082)
ORDER BY id;

-- OR:
SELECT id, vanha_id FROM tiesaa_asema ORDER BY id;
