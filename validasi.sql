-- TC 1
SELECT
	ST_Intersects(
		(atinstant(t1.p_s, t1.p_e, t1.t_p, '2003-11-20 06:08:00'::timestamp)).out_v, 
		(atinstant(t2.p_s, t2.p_e, t2.t_p, '2003-11-20 06:08:00'::timestamp)).out_v
	),
	ST_Intersects(
		(atinstant(t1.p_s, t1.p_e, t1.t_p, '2003-11-20 06:24:59.998'::timestamp)).out_v, 
		(atinstant(t2.p_s, t2.p_e, t2.t_p, '2003-11-20 06:24:59.998'::timestamp)).out_v
	),
	ST_Intersects(
		(atinstant(t1.p_s, t1.p_e, t1.t_p, '2003-11-20 06:28:09.445'::timestamp)).out_v, 
		(atinstant(t2.p_s, t2.p_e, t2.t_p, '2003-11-20 06:28:09.445'::timestamp)).out_v
	),
	ST_Intersects(
		(atinstant(t1.p_s, t1.p_e, t1.t_p, '2003-11-20 06:30:59.996'::timestamp)).out_v, 
		(atinstant(t2.p_s, t2.p_e, t2.t_p, '2003-11-20 06:30:59.996'::timestamp)).out_v
	),
	ST_Intersects(
		(atinstant(t1.p_s, t1.p_e, t1.t_p, '2003-11-20 06:31:15.805'::timestamp)).out_v, 
		(atinstant(t2.p_s, t2.p_e, t2.t_p, '2003-11-20 06:31:15.805'::timestamp)).out_v
	)
FROM
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) t1,
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) t2
WHERE t1.temporal_pk[1] = '1' AND t2.temporal_pk[1] = '5';

-- TC 2
SELECT
	ST_Distance(
		(atinstant(p_s, p_e, t_p, '2003-11-20 06:10:00'::timestamp)).out_v, geodata
	),
	ST_Distance(
		(atinstant(p_s, p_e, t_p, '2003-11-20 06:28:49.79'::timestamp)).out_v, geodata
	),
	ST_Distance(
		(atinstant(p_s, p_e, t_p, '2003-11-20 06:37:26.068'::timestamp)).out_v, geodata
	)
FROM
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) trains, trainroutes
WHERE temporal_pk[1] = '2' AND trainroutes.id = 3;

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
		(atinstant(ar, i_s, '2003-11-20 07:30:00'::timestamp)).out_v, geodata
	)
FROM
	form_mregion('snow', array['area', 'time_period', 'snow_id']) snow, stations
WHERE temporal_pk[1] = '1' AND stations.id = 15;

-- TC 4
SELECT
	ST_Distance(
		(atinstant(r_v, t_p, '2003-11-20 06:00:00'::timestamp)).out_v, temporal_pk[2]::geometry
	),
	ST_Distance(
		(atinstant(r_v, t_p, '2003-11-20 06:15:00'::timestamp)).out_v, temporal_pk[2]::geometry
	),
	ST_Distance(
		(atinstant(r_v, t_p, '2003-11-20 06:55:00'::timestamp)).out_v, temporal_pk[2]::geometry
	),
	ST_Distance(
		(atinstant(r_v, t_p, '2003-11-20 07:10:00'::timestamp)).out_v, temporal_pk[2]::geometry
	),
	ST_Distance(
		(atinstant(r_v, t_p, '2003-11-20 07:30:00'::timestamp)).out_v, temporal_pk[2]::geometry
	)
FROM form_mregion('snow', array['area', 'time_period', 'snow_id', 'ST_AsText(starting_point)']) snow;

-- TC 5
SELECT
	unnest(lifted_bool_values(lifted_pred('ST_Within', p_s, p_e, t_p, t1.geodata))),
	unnest(lifted_periods(lifted_pred('ST_Within', p_s, p_e, t_p, t1.geodata))),
	unnest(lifted_bool_values(lifted_pred('ST_Within', p_s, p_e, t_p, t2.geodata))),
	unnest(lifted_periods(lifted_pred('ST_Within', p_s, p_e, t_p, t2.geodata)))
FROM 
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) trains,
	trainroutes t1, trainroutes t2
WHERE temporal_pk[1] = '4' AND t1.id = 3 AND t2.id = 4;

-- TC 6
SELECT
	unnest(lifted_bool_values(lifted_pred('ST_Within', p_s, p_e, t_p, s1.geodata))),
	unnest(lifted_periods(lifted_pred('ST_Within', p_s, p_e, t_p, s1.geodata))),
	unnest(lifted_bool_values(lifted_pred('ST_Within', p_s, p_e, t_p, s2.geodata))),
	unnest(lifted_periods(lifted_pred('ST_Within', p_s, p_e, t_p, s2.geodata)))
FROM
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) trains,
	stations s1, stations s2
WHERE temporal_pk[1] = '4' AND s1.id = 10 AND s2.id = 28;

-- TC 7
SELECT
	unnest(lifted_bool_values(lifted_pred('ST_Within', p_s, p_e, trains.t_p, geodata))),
	unnest(lifted_periods(lifted_pred('ST_Within', p_s, p_e, trains.t_p, geodata))),
	unnest(lifted_bool_values(lifted_pred('ST_Intersects', p_s, p_e, trains.t_p, ar, snow.t_p))),
	unnest(lifted_periods(lifted_pred('ST_Intersects', p_s, p_e, trains.t_p, ar, snow.t_p)))
FROM
	form_mregion('snow', array['area', 'time_period', 'snow_id']) snow,
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) trains,
	trainroutes
WHERE trainroutes.id = 5;

-- TC 8, 9, 10
-- just execute it