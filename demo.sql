-- Apakah kereta 1 pernah bertemu dengan kereta 5?
-- lifted_pred: text x (point[], point[], tsrange[]) x (point[], point[], tsrange[]) -> (boolean[], tsrange[])
SELECT
	unnest(lifted_bool_values(lifted_pred('ST_Intersects', t1.p_s, t1.p_e, t1.i_t, t2.p_s, t2.p_e, t2.i_t))),
	unnest(lifted_periods(lifted_pred('ST_Intersects', t1.p_s, t1.p_e, t1.i_t, t2.p_s, t2.p_e, t2.i_t)))									  
FROM (
	SELECT
		train_id,
		array_agg(p_start ORDER BY time_period) p_s,
		array_agg(p_end ORDER BY time_period) p_e,
		array_agg(time_period ORDER BY time_period) i_t
	FROM trains WHERE train_id = 1 GROUP BY train_id
) t1, (
	SELECT
		train_id,
		array_agg(p_start ORDER BY time_period) p_s,
		array_agg(p_end ORDER BY time_period) p_e,
		array_agg(time_period ORDER BY time_period) i_t
	FROM trains WHERE train_id = 5 GROUP BY train_id
) t2

-- Apakah jarak antara kereta 2 dan rute 3 lebih dekat dari 1 km?
-- lifted_opt: text x record x real -> (boolean[], tsrange[])
SELECT
	unnest(lifted_bool_values(lifted_opt('<', lifted_num('ST_Distance', p_s, p_e, i_t, geodata), 1000.0))),
	unnest(lifted_periods(lifted_opt('<', lifted_num('ST_Distance', p_s, p_e, i_t, geodata), 1000.0)))									  
FROM (
	SELECT
		train_id,
		array_agg(p_start ORDER BY time_period) p_s,
		array_agg(p_end ORDER BY time_period) p_e,
		array_agg(time_period ORDER BY time_period) i_t
	FROM trains WHERE train_id = 2 GROUP BY train_id
) trains, trainroutes
WHERE trainroutes.id = 3

-- Berapa jarak antara badai salju 1 dengan stasiun 15?
-- lifted_num: text x (region[], tsrange[]) x geometry -> (real[], real[], tsrange[]) 
SELECT
	unnest(lifted_num_start(lifted_num('ST_Distance', ar, i_s, geodata))),
	unnest(lifted_num_end(lifted_num('ST_Distance', ar, i_s, geodata))),
	unnest(lifted_periods(lifted_num('ST_Distance', ar, i_s, geodata)))
FROM (
	SELECT
		array_agg(area ORDER BY time_period) ar,
		array_agg(time_period ORDER BY time_period) i_s
	FROM snow WHERE snow_id = 1 GROUP BY snow_id
) snow, stations
WHERE stations.id = 15

-- Apakah kereta 4 melewati rute 3 sebelum melewati rute 4?
-- pattern: (boolean[], tsrange[]) x (boolean[], tsrange[]) x varchar[] -> boolean
SELECT pattern(
	lifted_pred('ST_Within', p_s, p_e, i_t, t1.geodata),
	lifted_pred('ST_Within', p_s, p_e, i_t, t2.geodata),
	vec('aabb')
)
FROM (
	SELECT
		train_id,
		array_agg(p_start ORDER BY time_period) p_s,
		array_agg(p_end ORDER BY time_period) p_e,
		array_agg(time_period ORDER BY time_period) i_t
	FROM trains WHERE train_id = 4 GROUP BY train_id
) trains, trainroutes t1, trainroutes t2
WHERE t1.id = 3 AND t2.id = 4

-- Apakah kereta 4 melewati stasiun 10 tepat 24 menit sebelum melewati stasiun 28?
-- pattern: (boolean[], tsrange[]) x (boolean[], tsrange[]) x varchar[] -> boolean
-- ["2003-11-20 06:32:00","2003-11-20 06:34:00") 10
-- ["2003-11-20 06:58:00","2003-11-20 07:00:00") 28
SELECT pattern(
	lifted_pred('ST_Within', p_s, p_e, i_t, s1.geodata),
	lifted_pred('ST_Within', p_s, p_e, i_t, s2.geodata),
	vec('aa(24m.)bb')
)
FROM (
	SELECT
		train_id,
		array_agg(p_start ORDER BY time_period) p_s,
		array_agg(p_end ORDER BY time_period) p_e,
		array_agg(time_period ORDER BY time_period) i_t
	FROM trains WHERE train_id = 4 GROUP BY train_id
) trains, stations s1, stations s2
WHERE s1.id = 10 AND s2.id = 28;

-- Apakah kereta 10 melewati badai salju 1 saat melewati rute 5?
-- pattern: (boolean[], tsrange[]) x (boolean[], tsrange[]) x varchar[] -> boolean
SELECT pattern(
	lifted_pred('ST_Within', p_s, p_e, i_t, geodata),
	lifted_pred('ST_Intersects', p_s, p_e, i_t, ar, i_s),
	vec('abab', 'abba', 'baab', 'baba', 'a(.)bab', 'ab(.)ab', 'aba(.)b', 'a(.)bba', 'ab(.)ba', 'abb(.)a',
	    'b(.)aab', 'ba(.)ab', 'baa(.)b', 'b(.)aba', 'ba(.)ba', 'bab(.)a')
)
FROM (
	SELECT
		array_agg(area ORDER BY time_period) ar,
		array_agg(time_period ORDER BY time_period) i_s
	FROM snow WHERE snow_id = 1 GROUP BY snow_id
) snow, (
	SELECT
		train_id,
		array_agg(p_start ORDER BY time_period) p_s,
		array_agg(p_end ORDER BY time_period) p_e,
		array_agg(time_period ORDER BY time_period) i_t
	FROM trains WHERE train_id = 10 GROUP BY train_id
) trains, trainroutes
WHERE trainroutes.id = 5