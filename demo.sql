-- Apakah kereta 1 pernah bertemu dengan kereta 5?
-- lifted_pred: text x (point[], point[], tsrange[]) x (point[], point[], tsrange[]) -> (boolean[], tsrange[])
SELECT
	unnest(lifted_bool_values(lifted_pred('ST_Intersects', t1.p_s, t1.p_e, t1.i_t, t2.p_s, t2.p_e, t2.i_t))),
	unnest(lifted_periods(lifted_pred('ST_Intersects', t1.p_s, t1.p_e, t1.i_t, t2.p_s, t2.p_e, t2.i_t)))									  
FROM (
	SELECT
		array_agg(p_start ORDER BY time_period) p_s,
		array_agg(p_end ORDER BY time_period) p_e,
		array_agg(time_period ORDER BY time_period) i_t
	FROM trains WHERE train_id = 1 GROUP BY train_id
) t1, (
	SELECT
		array_agg(p_start ORDER BY time_period) p_s,
		array_agg(p_end ORDER BY time_period) p_e,
		array_agg(time_period ORDER BY time_period) i_t
	FROM trains WHERE train_id = 5 GROUP BY train_id
) t2;

-- Apakah jarak antara kereta 2 dan rute 3 lebih dekat dari 1 km?
-- lifted_opt: text x record x real -> (boolean[], tsrange[])
SELECT
	unnest(lifted_bool_values(lifted_opt('<', lifted_num('ST_Distance', p_s, p_e, i_t, geodata), 1000.0))),
	unnest(lifted_periods(lifted_opt('<', lifted_num('ST_Distance', p_s, p_e, i_t, geodata), 1000.0)))									  
FROM (
	SELECT
		array_agg(p_start ORDER BY time_period) p_s,
		array_agg(p_end ORDER BY time_period) p_e,
		array_agg(time_period ORDER BY time_period) i_t
	FROM trains WHERE train_id = 2 GROUP BY train_id
) trains, trainroutes
WHERE trainroutes.id = 3;

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
WHERE stations.id = 15;

-- Apakah kereta 4 melewati rute 3 sebelum melewati rute 4?
-- pattern: (boolean[], tsrange[]) x (boolean[], tsrange[]) x varchar[] -> boolean
SELECT pattern(
	lifted_pred('ST_Within', p_s, p_e, i_t, t1.geodata),
	lifted_pred('ST_Within', p_s, p_e, i_t, t2.geodata),
	vec('aabb')
)
FROM (
	SELECT
		array_agg(p_start ORDER BY time_period) p_s,
		array_agg(p_end ORDER BY time_period) p_e,
		array_agg(time_period ORDER BY time_period) i_t
	FROM trains WHERE train_id = 4 GROUP BY train_id
) trains, trainroutes t1, trainroutes t2
WHERE t1.id = 3 AND t2.id = 4;

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
FROM (SELECT
		array_agg(area ORDER BY time_period) ar,
		array_agg(time_period ORDER BY time_period) i_s
	FROM snow WHERE snow_id = 1 GROUP BY snow_id
) snow, (SELECT
		array_agg(p_start ORDER BY time_period) p_s,
		array_agg(p_end ORDER BY time_period) p_e,
		array_agg(time_period ORDER BY time_period) i_t
	FROM trains WHERE train_id = 10 GROUP BY train_id
) trains, trainroutes
WHERE trainroutes.id = 5;

-- Kereta apa saja yang melewati rute 2, rute 3, dan rute 4 secara berurutan?
-- STP Query dengan 3 operasi predikat lifted dan 2 batasan temporal
SELECT DISTINCT train_id
FROM (SELECT
	  	train_id,
		array_agg(p_start ORDER BY time_period) p_s,
		array_agg(p_end ORDER BY time_period) p_e,
		array_agg(time_period ORDER BY time_period) i_t
	FROM trains GROUP BY train_id
) trains, trainroutes t1, trainroutes t2, trainroutes t3
WHERE t1.id = 2 AND t2.id = 3 AND t3.id = 4 AND pattern(
	lifted_pred('ST_Within', p_s, p_e, i_t, t1.geodata),
	lifted_pred('ST_Within', p_s, p_e, i_t, t2.geodata),
	vec('aabb')
) AND pattern(
	lifted_pred('ST_Within', p_s, p_e, i_t, t2.geodata),
	lifted_pred('ST_Within', p_s, p_e, i_t, t3.geodata),
	vec('aabb')
)
ORDER BY train_id;

-- Pada rute apa saja jarak kereta 5 dan 6 kecil dari 5 km?
-- STP Query dengan operasi lifted_pred, lifted_opt, dan lifted_num
SELECT DISTINCT trainroutes.id, trainroutes.name, trainroutes.type
FROM (SELECT
		array_agg(p_start ORDER BY time_period) p_s,
		array_agg(p_end ORDER BY time_period) p_e,
		array_agg(time_period ORDER BY time_period) i_t
	FROM trains WHERE train_id = 5 GROUP BY train_id
) t1, (SELECT
		array_agg(p_start ORDER BY time_period) p_s,
		array_agg(p_end ORDER BY time_period) p_e,
		array_agg(time_period ORDER BY time_period) i_t
	FROM trains WHERE train_id = 6 GROUP BY train_id
) t2, trainroutes
WHERE pattern(
	lifted_opt('<', lifted_num('ST_Distance', t1.p_s, t1.p_e, t1.i_t, t2.p_s, t2.p_e, t2.i_t), 5000.0),
	lifted_pred('ST_Within', t1.p_s, t1.p_e, t1.i_t, geodata),
	vec('abab', 'abba', 'baab', 'baba', 'a(.)bab', 'ab(.)ab', 'aba(.)b', 'a(.)bba', 'ab(.)ba', 'abb(.)a',
		'b(.)aab', 'ba(.)ab', 'baa(.)b', 'b(.)aba', 'ba(.)ba', 'bab(.)a')
) AND pattern(
	lifted_pred('ST_Within', t1.p_s, t1.p_e, t1.i_t, geodata),
	lifted_pred('ST_Within', t2.p_s, t2.p_e, t2.i_t, geodata),
	vec('abab', 'abba', 'baab', 'baba', 'a(.)bab', 'ab(.)ab', 'aba(.)b', 'a(.)bba', 'ab(.)ba', 'abb(.)a',
		'b(.)aab', 'ba(.)ab', 'baa(.)b', 'b(.)aba', 'ba(.)ba', 'bab(.)a')
)
ORDER BY trainroutes.id;

-- Kereta apa saja yang melewati badai salju dan juga melewati rute 5?
-- STP Query dengan tipe data region yang bersifat moving
SELECT DISTINCT train_id
FROM (SELECT
		train_id,
		array_agg(p_start ORDER BY time_period) p_s,
		array_agg(p_end ORDER BY time_period) p_e,
		array_agg(time_period ORDER BY time_period) i_t
	FROM trains GROUP BY train_id
) trains, (SELECT
		array_agg(area ORDER BY time_period) ar,
		array_agg(time_period ORDER BY time_period) i_s
	FROM snow WHERE snow_id = 5 GROUP BY snow_id
) snow, trainroutes
WHERE trainroutes.id = 1 AND pattern(
	lifted_pred('ST_Within', p_s, p_e, i_t, ar, i_s),
	lifted_pred('ST_Within', p_s, p_e, i_t, geodata),
	vec('abab', 'abba', 'baab', 'baba', 'a(.)bab', 'ab(.)ab', 'aba(.)b', 'a(.)bba', 'ab(.)ba', 'abb(.)a',
		'b(.)aab', 'ba(.)ab', 'baa(.)b', 'b(.)aba', 'ba(.)ba', 'bab(.)a')
)
ORDER BY train_id;

-- Perbandingan kinerja kakas
-- 1NF
SELECT DISTINCT taxi_id
FROM (SELECT
	  	taxi_id,
		array_agg(p_start ORDER BY time_period) p_s,
		array_agg(p_end ORDER BY time_period) p_e,
		array_agg(time_period ORDER BY time_period) i_t
	FROM taxi_1nf GROUP BY taxi_id
) taxi
WHERE pattern(
	lifted_pred('ST_Intersects', p_s, p_e, i_t, geometry(POINT(116.35, 39.30))),
	lifted_opt('>=', speed(p_s, p_e, i_t), 10.0),
	vec('a(10s)a(.)b(1h25m)b', 'a(1y2M)ba(.)b', 'b(2d)aa(5s)b', 'b(5M10d)ab(238m)a')
);

-- N1NF
SELECT DISTINCT id
FROM taxi_n1nf
WHERE pattern(
	ARRAY[
		lifted_pred('ST_Intersects', trip, geometry(POINT(116.35, 39.30))),
		lifted_opt('>=', speed(trip), 2.0)
	], ARRAY [
		stconstraint(1, 2, vec('aa.bb', 'aba.b', 'b.aab', 'b.ab.a'))
	]			  
);