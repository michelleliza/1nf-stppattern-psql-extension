-- Apakah kereta 1 pernah bertemu dengan kereta 5?
-- lifted_pred: text x (point[], point[], tsrange[]) x (point[], point[], tsrange[]) -> (boolean[], tsrange[])
SELECT
	unnest(lifted_bool_values(lifted_pred('ST_Intersects', t1.p_s, t1.p_e, t1.t_p, t2.p_s, t2.p_e, t2.t_p))),
	unnest(lifted_periods(lifted_pred('ST_Intersects', t1.p_s, t1.p_e, t1.t_p, t2.p_s, t2.p_e, t2.t_p)))									  
FROM 
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) t1,
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) t2
WHERE t1.temporal_pk = 1 AND t2.temporal_pk = 5;

-- Apakah jarak antara kereta 2 dan rute 3 lebih dekat dari 1 km?
-- lifted_opt: text x record x real -> (boolean[], tsrange[])
SELECT
	unnest(lifted_bool_values(lifted_opt('<', lifted_num('ST_Distance', p_s, p_e, t_p, geodata), 1000.0))),
	unnest(lifted_periods(lifted_opt('<', lifted_num('ST_Distance', p_s, p_e, t_p, geodata), 1000.0)))									  
FROM
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) trains, trainroutes
WHERE temporal_pk = 2 AND trainroutes.id = 3;

-- Berapa jarak antara badai salju 1 dengan stasiun 15?
-- lifted_num: text x (region[], tsrange[]) x geometry -> (real[], real[], tsrange[]) 
SELECT
	unnest(lifted_num_start(lifted_num('ST_Distance', r_v, t_p, geodata))),
	unnest(lifted_num_end(lifted_num('ST_Distance', r_v, t_p, geodata))),
	unnest(lifted_periods(lifted_num('ST_Distance', r_v, t_p, geodata)))
FROM
	form_mregion('snow', array['area', 'time_period', 'snow_id']) snow, stations
WHERE temporal_pk = 1 AND stations.id = 15;

-- Berapa jarak antara badai salju 1 dengan titik awalnya?
-- lifted_num: text x geometry[] x (region[], tsrange[]) -> (real[], real[], tsrange[]) 
SELECT
	unnest(lifted_num_start(lifted_num('ST_Distance', s_p, area, t_p))),
	unnest(lifted_num_end(lifted_num('ST_Distance', s_p, area, t_p))),
	unnest(lifted_num_end(lifted_num('ST_Distance', s_p, area, t_p)))								 
FROM (SELECT
	array_agg(area ORDER BY time_period) area,
	array_agg(time_period ORDER BY time_period) t_p,
	array_agg(DISTINCT starting_point) s_p
	FROM snow GROUP BY snow_id
) snow;

-- Apakah kereta 4 melewati rute 3 sebelum melewati rute 4?
-- pattern: (boolean[], tsrange[]) x (boolean[], tsrange[]) x varchar[] -> boolean
SELECT pattern(
	lifted_pred('ST_Within', p_s, p_e, t_p, t1.geodata),
	lifted_pred('ST_Within', p_s, p_e, t_p, t2.geodata),
	vec('aabb')
)
FROM 
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) trains,
	trainroutes t1, trainroutes t2
WHERE temporal_pk = 4 AND t1.id = 3 AND t2.id = 4;

-- Apakah kereta 4 meninggalkan stasiun 10 tepat 24 menit sebelum memasuki stasiun 28?
-- pattern: (boolean[], tsrange[]) x (boolean[], tsrange[]) x varchar[] -> boolean
-- ["2003-11-20 06:32:00","2003-11-20 06:34:00") 10
-- ["2003-11-20 06:58:00","2003-11-20 07:00:00") 28
SELECT pattern(
	lifted_pred('ST_Within', p_s, p_e, t_p, s1.geodata),
	lifted_pred('ST_Within', p_s, p_e, t_p, s2.geodata),
	vec('aa(24m.)bb')
)
FROM
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) trains,
	stations s1, stations s2
WHERE temporal_pk = 4 AND s1.id = 10 AND s2.id = 28;

-- Apakah kereta 10 melewati badai salju 1 saat melewati rute 5?
-- pattern: (boolean[], tsrange[]) x (boolean[], tsrange[]) x varchar[] -> boolean
SELECT pattern(
	lifted_pred('ST_Within', p_s, p_e, trains.t_p, geodata),
	lifted_pred('ST_Intersects', p_s, p_e, trains.t_p, r_v, snow.t_p),
	vec('abab', 'abba', 'baab', 'baba', 'a(.)bab', 'ab(.)ab', 'aba(.)b', 'a(.)bba', 'ab(.)ba', 'abb(.)a',
	    'b(.)aab', 'ba(.)ab', 'baa(.)b', 'b(.)aba', 'ba(.)ba', 'bab(.)a')
)
FROM
	form_mregion('snow', array['area', 'time_period', 'snow_id']) snow,
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) trains,
	trainroutes
WHERE trains.temporal_pk = 10 AND snow.temporal_pk = 1 AND trainroutes.id = 5;

-- Kereta apa saja yang melewati rute 2, rute 3, dan rute 4 secara berurutan?
-- STP Query dengan 3 operasi predikat lifted dan 2 batasan temporal
SELECT DISTINCT temporal_pk
FROM
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) trains,
	trainroutes t1, trainroutes t2, trainroutes t3
WHERE t1.id = 2 AND t2.id = 3 AND t3.id = 4 AND pattern(
	lifted_pred('ST_Within', p_s, p_e, t_p, t1.geodata),
	lifted_pred('ST_Within', p_s, p_e, t_p, t2.geodata),
	vec('aabb')
) AND pattern(
	lifted_pred('ST_Within', p_s, p_e, t_p, t2.geodata),
	lifted_pred('ST_Within', p_s, p_e, t_p, t3.geodata),
	vec('aabb')
)
ORDER BY temporal_pk;

-- Pada rute apa saja jarak kereta 5 dan 6 kecil dari 5 km?
-- STP Query dengan operasi lifted_pred, lifted_opt, dan lifted_num
SELECT DISTINCT trainroutes.id, trainroutes.name, trainroutes.type
FROM 
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) t1,
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) t2,
	trainroutes
WHERE t1.temporal_pk = 5 AND t2.temporal_pk = 6 AND pattern(
	lifted_opt('<', lifted_num('ST_Distance', t1.p_s, t1.p_e, t1.t_p, t2.p_s, t2.p_e, t2.t_p), 5000.0),
	lifted_pred('ST_Within', t1.p_s, t1.p_e, t1.t_p, geodata),
	vec('abab', 'abba', 'baab', 'baba', 'a(.)bab', 'ab(.)ab', 'aba(.)b', 'a(.)bba', 'ab(.)ba', 'abb(.)a',
		'b(.)aab', 'ba(.)ab', 'baa(.)b', 'b(.)aba', 'ba(.)ba', 'bab(.)a')
) AND pattern(
	lifted_pred('ST_Within', t1.p_s, t1.p_e, t1.t_p, geodata),
	lifted_pred('ST_Within', t2.p_s, t2.p_e, t2.t_p, geodata),
	vec('abab', 'abba', 'baab', 'baba', 'a(.)bab', 'ab(.)ab', 'aba(.)b', 'a(.)bba', 'ab(.)ba', 'abb(.)a',
		'b(.)aab', 'ba(.)ab', 'baa(.)b', 'b(.)aba', 'ba(.)ba', 'bab(.)a')
)
ORDER BY trainroutes.id;

-- Kereta apa saja yang melewati badai salju dengan kecepatan kurang dari 20 m/s?
-- STP Query dengan tipe data region yang bersifat moving
SELECT DISTINCT trains.temporal_pk
FROM 
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) trains,
	form_mregion('snow', array['area', 'time_period', 'snow_id']) snow,
	trainroutes
WHERE trainroutes.id = 5 AND snow.temporal_pk = 1 AND pattern(
	lifted_pred('ST_Within', p_s, p_e, trains.t_p, r_v, snow.t_p),
	lifted_opt('<', speed(p_s, p_e, trains.t_p), 20.0),
	vec('abab', 'abba', 'baab', 'baba', 'a(.)bab', 'ab(.)ab', 'aba(.)b', 'a(.)bba', 'ab(.)ba', 'abb(.)a',
		'b(.)aab', 'ba(.)ab', 'baa(.)b', 'b(.)aba', 'ba(.)ba', 'bab(.)a')
)
ORDER BY trains.temporal_pk;

-- Perbandingan kinerja kakas
-- 1NF
SELECT DISTINCT temporal_pk
FROM 
	form_mpoint('taxi_1nf', array['p_start', 'p_end', 'time_period', 'taxi_id'])
WHERE pattern(
	lifted_pred('ST_Intersects', p_s, p_e, t_p, geometry(POINT(116.35, 39.30))),
	lifted_opt('>=', lifted_num('ST_Distance', p_s, p_e, t_p, geometry(POINT(150.54, 37.40))), 2.0),
	vec('a(10s)a(.)b(1h25m)b', 'a(1y2M)ba(.)b', 'b(2d)aa(5s)b', 'b(5M10d)ab(238m)a')
);

SELECT DISTINCT taxi_id FROM (SELECT taxi_id,array_agg(p_start ORDER BY time_period) p_s,array_agg(p_end ORDER BY time_period) p_e,array_agg(time_period ORDER BY time_period) t_p FROM taxi_1nf WHERE taxi_id < 25 GROUP BY taxi_id) taxi WHERE pattern(lifted_pred('ST_Intersects', p_s, p_e, t_p, geometry(POINT(116.35, 39.30))), lifted_opt('>=', lifted_num('ST_Distance', p_s, p_e, t_p, geometry(POINT(150.54, 37.40))), 2.0), vec('a(10s)a(.)b(1h25m)b', 'a(1y2M)ba(.)b', 'b(2d)aa(5s)b', 'b(5M10d)ab(238m)a'));

SELECT DISTINCT taxi_id FROM (SELECT taxi_id,array_agg(p_start ORDER BY time_period) p_s,array_agg(p_end ORDER BY time_period) p_e,array_agg(time_period ORDER BY time_period) t_p FROM taxi_1nf WHERE taxi_id < 38 GROUP BY taxi_id) taxi WHERE pattern(lifted_pred('ST_Intersects', p_s, p_e, t_p, geometry(POINT(116.35, 39.30))), lifted_opt('>=', lifted_num('ST_Distance', p_s, p_e, t_p, geometry(POINT(150.54, 37.40))), 2.0), vec('a(10s)a(.)b(1h25m)b', 'a(1y2M)ba(.)b', 'b(2d)aa(5s)b', 'b(5M10d)ab(238m)a'));

SELECT DISTINCT taxi_id FROM (SELECT taxi_id,array_agg(p_start ORDER BY time_period) p_s,array_agg(p_end ORDER BY time_period) p_e,array_agg(time_period ORDER BY time_period) t_p FROM taxi_1nf WHERE taxi_id < 64 GROUP BY taxi_id) taxi WHERE pattern(lifted_pred('ST_Intersects', p_s, p_e, t_p, geometry(POINT(116.35, 39.30))), lifted_opt('>=', lifted_num('ST_Distance', p_s, p_e, t_p, geometry(POINT(150.54, 37.40))), 2.0), vec('a(10s)a(.)b(1h25m)b', 'a(1y2M)ba(.)b', 'b(2d)aa(5s)b', 'b(5M10d)ab(238m)a'));

SELECT DISTINCT taxi_id FROM (SELECT taxi_id,array_agg(p_start ORDER BY time_period) p_s,array_agg(p_end ORDER BY time_period) p_e,array_agg(time_period ORDER BY time_period) t_p FROM taxi_1nf WHERE taxi_id < 85 GROUP BY taxi_id) taxi WHERE pattern(lifted_pred('ST_Intersects', p_s, p_e, t_p, geometry(POINT(116.35, 39.30))), lifted_opt('>=', lifted_num('ST_Distance', p_s, p_e, t_p, geometry(POINT(150.54, 37.40))), 2.0), vec('a(10s)a(.)b(1h25m)b', 'a(1y2M)ba(.)b', 'b(2d)aa(5s)b', 'b(5M10d)ab(238m)a'));

SELECT DISTINCT taxi_id FROM (SELECT taxi_id,array_agg(p_start ORDER BY time_period) p_s,array_agg(p_end ORDER BY time_period) p_e,array_agg(time_period ORDER BY time_period) t_p FROM taxi_1nf GROUP BY taxi_id) taxi WHERE pattern(lifted_pred('ST_Intersects', p_s, p_e, t_p, geometry(POINT(116.35, 39.30))), lifted_opt('>=', lifted_num('ST_Distance', p_s, p_e, t_p, geometry(POINT(150.54, 37.40))), 2.0), vec('a(10s)a(.)b(1h25m)b', 'a(1y2M)ba(.)b', 'b(2d)aa(5s)b', 'b(5M10d)ab(238m)a'));

SELECT DISTINCT taxi_id FROM (SELECT taxi_id,array_agg(p_start ORDER BY time_period) p_s,array_agg(p_end ORDER BY time_period) p_e,array_agg(time_period ORDER BY time_period) t_p FROM taxi_1nf WHERE lower(time_period) >= '2008-02-02 00:00:00'::timestamp AND upper(time_period) <= '2008-02-03 16:00:00'::timestamp GROUP BY taxi_id) taxi WHERE pattern(lifted_pred('ST_Intersects', p_s, p_e, t_p, geometry(POINT(116.35, 39.30))), lifted_opt('>=', lifted_num('ST_Distance', p_s, p_e, t_p, geometry(POINT(150.54, 37.40))), 2.0), vec('a(10s)a(.)b(1h25m)b', 'a(1y2M)ba(.)b', 'b(2d)aa(5s)b', 'b(5M10d)ab(238m)a'));

SELECT DISTINCT taxi_id FROM (SELECT taxi_id,array_agg(p_start ORDER BY time_period) p_s,array_agg(p_end ORDER BY time_period) p_e,array_agg(time_period ORDER BY time_period) t_p FROM taxi_1nf WHERE lower(time_period) >= '2008-02-02 00:00:00'::timestamp AND upper(time_period) <= '2008-02-04 16:30:00'::timestamp GROUP BY taxi_id) taxi WHERE pattern(lifted_pred('ST_Intersects', p_s, p_e, t_p, geometry(POINT(116.35, 39.30))), lifted_opt('>=', lifted_num('ST_Distance', p_s, p_e, t_p, geometry(POINT(150.54, 37.40))), 2.0), vec('a(10s)a(.)b(1h25m)b', 'a(1y2M)ba(.)b', 'b(2d)aa(5s)b', 'b(5M10d)ab(238m)a'));

SELECT DISTINCT taxi_id FROM (SELECT taxi_id,array_agg(p_start ORDER BY time_period) p_s,array_agg(p_end ORDER BY time_period) p_e,array_agg(time_period ORDER BY time_period) t_p FROM taxi_1nf WHERE lower(time_period) >= '2008-02-02 00:00:00'::timestamp AND upper(time_period) <= '2008-02-05 19:30:00'::timestamp GROUP BY taxi_id) taxi WHERE pattern(lifted_pred('ST_Intersects', p_s, p_e, t_p, geometry(POINT(116.35, 39.30))), lifted_opt('>=', lifted_num('ST_Distance', p_s, p_e, t_p, geometry(POINT(150.54, 37.40))), 2.0), vec('a(10s)a(.)b(1h25m)b', 'a(1y2M)ba(.)b', 'b(2d)aa(5s)b', 'b(5M10d)ab(238m)a'));

SELECT DISTINCT taxi_id FROM (SELECT taxi_id,array_agg(p_start ORDER BY time_period) p_s,array_agg(p_end ORDER BY time_period) p_e,array_agg(time_period ORDER BY time_period) t_p FROM taxi_1nf WHERE lower(time_period) >= '2008-02-02 00:00:00'::timestamp AND upper(time_period) <= '2008-02-07 07:30:00'::timestamp GROUP BY taxi_id) taxi WHERE pattern(lifted_pred('ST_Intersects', p_s, p_e, t_p, geometry(POINT(116.35, 39.30))), lifted_opt('>=', lifted_num('ST_Distance', p_s, p_e, t_p, geometry(POINT(150.54, 37.40))), 2.0), vec('a(10s)a(.)b(1h25m)b', 'a(1y2M)ba(.)b', 'b(2d)aa(5s)b', 'b(5M10d)ab(238m)a'));

SELECT DISTINCT taxi_id FROM (SELECT taxi_id,array_agg(p_start ORDER BY time_period) p_s,array_agg(p_end ORDER BY time_period) p_e,array_agg(time_period ORDER BY time_period) t_p FROM taxi_1nf GROUP BY taxi_id) taxi WHERE pattern(lifted_pred('ST_Intersects', p_s, p_e, t_p, geometry(POINT(116.35, 39.30))), lifted_opt('>=', lifted_num('ST_Distance', p_s, p_e, t_p, geometry(POINT(150.54, 37.40))), 2.0), vec('a(10s)a(.)b(1h25m)b', 'a(1y2M)ba(.)b', 'b(2d)aa(5s)b', 'b(5M10d)ab(238m)a'));

-- N1NF
SELECT DISTINCT id
FROM taxi_n1nf
WHERE pattern(
	ARRAY[
		lifted_pred('ST_Intersects', trip, geometry(POINT(116.35, 39.30))),
		lifted_opt('>=', lifted_num('ST_Distance', trip, geometry(POINT(150.54, 37.40))), 2.0)
	], ARRAY [
		stconstraint(1, 2, vec('aa.bb', 'aba.b', 'b.aab', 'b.ab.a'))
	]			  
);

SELECT DISTINCT id FROM taxi_n1nf WHERE pattern(ARRAY[lifted_pred('ST_Intersects', trip, geometry(POINT(116.35, 39.30))),lifted_opt('>=', lifted_num('ST_Distance', trip, geometry(POINT(150.54, 37.40))), 2.0)], ARRAY[stconstraint(1, 2, vec('aa.bb', 'aba.b', 'b.aab', 'b.ab.a'))]) AND id < 25;

SELECT DISTINCT id FROM taxi_n1nf WHERE pattern(ARRAY[lifted_pred('ST_Intersects', trip, geometry(POINT(116.35, 39.30))),lifted_opt('>=', lifted_num('ST_Distance', trip, geometry(POINT(150.54, 37.40))), 2.0)], ARRAY[stconstraint(1, 2, vec('aa.bb', 'aba.b', 'b.aab', 'b.ab.a'))]) AND id < 38;

SELECT DISTINCT id FROM taxi_n1nf WHERE pattern(ARRAY[lifted_pred('ST_Intersects', trip, geometry(POINT(116.35, 39.30))),lifted_opt('>=', lifted_num('ST_Distance', trip, geometry(POINT(150.54, 37.40))), 2.0)], ARRAY[stconstraint(1, 2, vec('aa.bb', 'aba.b', 'b.aab', 'b.ab.a'))]) AND id < 64;

SELECT DISTINCT id FROM taxi_n1nf WHERE pattern(ARRAY[lifted_pred('ST_Intersects', trip, geometry(POINT(116.35, 39.30))),lifted_opt('>=', lifted_num('ST_Distance', trip, geometry(POINT(150.54, 37.40))), 2.0)], ARRAY[stconstraint(1, 2, vec('aa.bb', 'aba.b', 'b.aab', 'b.ab.a'))]) AND id < 85;

SELECT DISTINCT id FROM (SELECT id, slice(trip, '2008-02-02 00:00:00'::timestamp, '2008-02-03 16:00:00') trip FROM taxi_n1nf) taxi WHERE pattern(ARRAY[lifted_pred('ST_Intersects', trip, geometry(POINT(116.35, 39.30))),lifted_opt('>=', lifted_num('ST_Distance', trip, geometry(POINT(150.54, 37.40))), 2.0)], ARRAY[stconstraint(1, 2, vec('aa.bb', 'aba.b', 'b.aab', 'b.ab.a'))]);

SELECT DISTINCT id FROM (SELECT id, slice(trip, '2008-02-02 00:00:00'::timestamp, '2008-02-04 16:30:00') trip FROM taxi_n1nf) taxi WHERE pattern(ARRAY[lifted_pred('ST_Intersects', trip, geometry(POINT(116.35, 39.30))),lifted_opt('>=', lifted_num('ST_Distance', trip, geometry(POINT(150.54, 37.40))), 2.0)], ARRAY[stconstraint(1, 2, vec('aa.bb', 'aba.b', 'b.aab', 'b.ab.a'))]);

SELECT DISTINCT id FROM (SELECT id, slice(trip, '2008-02-02 00:00:00'::timestamp, '2008-02-05 19:30:00') trip FROM taxi_n1nf) taxi WHERE pattern(ARRAY[lifted_pred('ST_Intersects', trip, geometry(POINT(116.35, 39.30))),lifted_opt('>=', lifted_num('ST_Distance', trip, geometry(POINT(150.54, 37.40))), 2.0)], ARRAY[stconstraint(1, 2, vec('aa.bb', 'aba.b', 'b.aab', 'b.ab.a'))]);

SELECT DISTINCT id FROM (SELECT id, slice(trip, '2008-02-02 00:00:00'::timestamp, '2008-02-07 07:30:00') trip FROM taxi_n1nf) taxi WHERE pattern(ARRAY[lifted_pred('ST_Intersects', trip, geometry(POINT(116.35, 39.30))),lifted_opt('>=', lifted_num('ST_Distance', trip, geometry(POINT(150.54, 37.40))), 2.0)], ARRAY[stconstraint(1, 2, vec('aa.bb', 'aba.b', 'b.aab', 'b.ab.a'))]);

SELECT DISTINCT id FROM taxi_n1nf WHERE pattern(ARRAY[lifted_pred('ST_Intersects', trip, geometry(POINT(116.35, 39.30))),lifted_opt('>=', lifted_num('ST_Distance', trip, geometry(POINT(150.54, 37.40))), 2.0)], ARRAY[stconstraint(1, 2, vec('aa.bb', 'aba.b', 'b.aab', 'b.ab.a'))]);