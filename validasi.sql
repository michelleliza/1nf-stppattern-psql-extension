-- TC 1
SELECT
	ST_Intersects(
		(atinstant(t1.p_s, t1.p_e, t1.i_t, '2003-11-20 06:24:59.998'::timestamp)).out_v, 
		(atinstant(t2.p_s, t2.p_e, t2.i_t, '2003-11-20 06:24:59.998'::timestamp)).out_v
	),
	ST_Intersects(
		(atinstant(t1.p_s, t1.p_e, t1.i_t, '2003-11-20 06:30:59.996'::timestamp)).out_v, 
		(atinstant(t2.p_s, t2.p_e, t2.i_t, '2003-11-20 06:30:59.996'::timestamp)).out_v
	)
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

-- TC 2
SELECT
	ST_Distance(
		(atinstant(p_s, p_e, i_t, '2003-11-20 06:28:49.79'::timestamp)).out_v, geodata
	)
FROM (
	SELECT
		array_agg(p_start ORDER BY time_period) p_s,
		array_agg(p_end ORDER BY time_period) p_e,
		array_agg(time_period ORDER BY time_period) i_t
	FROM trains WHERE train_id = 2 GROUP BY train_id
) trains, trainroutes
WHERE trainroutes.id = 3;

-- TC 3
SELECT
	ST_Distance(
		(atinstant(ar, i_s, '2003-11-20 06:00:00'::timestamp)).out_v, geodata
	),
	ST_Distance(
		(atinstant(ar, i_s, '2003-11-20 06:15:00'::timestamp)).out_v, geodata
	),
	ST_Distance(
		(atinstant(ar, i_s, '2003-11-20 06:55:00'::timestamp)).out_v, geodata
	),
	ST_Distance(
		(atinstant(ar, i_s, '2003-11-20 07:10:00'::timestamp)).out_v, geodata
	),
	ST_Distance(
		(atinstant(ar, i_s, '2003-11-20 07:50:00'::timestamp)).out_v, geodata
	)
FROM (
	SELECT
		array_agg(area ORDER BY time_period) ar,
		array_agg(time_period ORDER BY time_period) i_s
	FROM snow WHERE snow_id = 1 GROUP BY snow_id
) snow, stations
WHERE stations.id = 15;

-- TC 4
SELECT
	unnest(lifted_bool_values(lifted_pred('ST_Within', p_s, p_e, i_t, t1.geodata))),
	unnest(lifted_periods(lifted_pred('ST_Within', p_s, p_e, i_t, t1.geodata))),
	unnest(lifted_bool_values(lifted_pred('ST_Within', p_s, p_e, i_t, t2.geodata))),
	unnest(lifted_periods(lifted_pred('ST_Within', p_s, p_e, i_t, t2.geodata)))
FROM (
	SELECT
		array_agg(p_start ORDER BY time_period) p_s,
		array_agg(p_end ORDER BY time_period) p_e,
		array_agg(time_period ORDER BY time_period) i_t
	FROM trains WHERE train_id = 4 GROUP BY train_id
) trains, trainroutes t1, trainroutes t2
WHERE t1.id = 3 AND t2.id = 4;

-- TC 5
SELECT
	unnest(lifted_bool_values(lifted_pred('ST_Within', p_s, p_e, i_t, s1.geodata))),
	unnest(lifted_periods(lifted_pred('ST_Within', p_s, p_e, i_t, s1.geodata))),
	unnest(lifted_bool_values(lifted_pred('ST_Within', p_s, p_e, i_t, s2.geodata))),
	unnest(lifted_periods(lifted_pred('ST_Within', p_s, p_e, i_t, s2.geodata)))
FROM(
	SELECT
		array_agg(p_start ORDER BY time_period) p_s,
		array_agg(p_end ORDER BY time_period) p_e,
		array_agg(time_period ORDER BY time_period) i_t
	FROM trains WHERE train_id = 4 GROUP BY train_id
) trains, stations s1, stations s2
WHERE s1.id = 10 AND s2.id = 28;

-- TC 6
SELECT
	unnest(lifted_bool_values(lifted_pred('ST_Within', p_s, p_e, i_t, geodata))),
	unnest(lifted_periods(lifted_pred('ST_Within', p_s, p_e, i_t, geodata))),
	unnest(lifted_bool_values(lifted_pred('ST_Intersects', p_s, p_e, i_t, ar, i_s))),
	unnest(lifted_periods(lifted_pred('ST_Intersects', p_s, p_e, i_t, ar, i_s)))
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

-- TC 7, 8, 9
-- just execute it