-- Apakah kereta 1 pernah bertemu dengan kereta 5?
-- lifted_pred: text x (point[], point[], tsrange[]) x (point[], point[], tsrange[]) -> (boolean[], tsrange[])
SELECT
	unnest(lifted_bool_values(lifted_pred('ST_Intersects', t1.p_s, t1.p_e, t1.t_p, t2.p_s, t2.p_e, t2.t_p))),
	unnest(lifted_periods(lifted_pred('ST_Intersects', t1.p_s, t1.p_e, t1.t_p, t2.p_s, t2.p_e, t2.t_p)))									  
FROM 
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) t1,
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) t2
WHERE t1.temporal_pk = 1 AND t2.temporal_pk = 5;

-- TC 1
SELECT
	ST_Intersects(
		(atinstant(t1.p_s, t1.p_e, t1.t_p, '2003-11-20 06:24:59.998'::timestamp)).out_v, 
		(atinstant(t2.p_s, t2.p_e, t2.t_p, '2003-11-20 06:24:59.998'::timestamp)).out_v
	),
	ST_Intersects(
		(atinstant(t1.p_s, t1.p_e, t1.t_p, '2003-11-20 06:30:59.996'::timestamp)).out_v, 
		(atinstant(t2.p_s, t2.p_e, t2.t_p, '2003-11-20 06:30:59.996'::timestamp)).out_v
	)
FROM
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) t1,
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) t2
WHERE t1.temporal_pk = 1 AND t2.temporal_pk = 5;

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

-- TC 5
SELECT
	unnest(lifted_bool_values(lifted_pred('ST_Within', p_s, p_e, t_p, s1.geodata))),
	unnest(lifted_periods(lifted_pred('ST_Within', p_s, p_e, t_p, s1.geodata))),
	unnest(lifted_bool_values(lifted_pred('ST_Within', p_s, p_e, t_p, s2.geodata))),
	unnest(lifted_periods(lifted_pred('ST_Within', p_s, p_e, t_p, s2.geodata)))
FROM
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) trains,
	stations s1, stations s2
WHERE temporal_pk = 4 AND s1.id = 10 AND s2.id = 28;

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

-- Validasi
SELECT ST_Distance(
		(atinstant(t1.p_s, t1.p_e, t1.t_p, '2003-11-20 06:13:00'::timestamp)).out_v,
		(atinstant(t2.p_s, t2.p_e, t2.t_p, '2003-11-20 06:13:00'::timestamp)).out_v
	),
	ST_Within(
		(atinstant(t1.p_s, t1.p_e, t1.t_p, '2003-11-20 06:08:28.656'::timestamp)).out_v,
		geodata
	),
	ST_Within(
		(atinstant(t2.p_s, t2.p_e, t2.t_p, '2003-11-20 06:13:28.656'::timestamp)).out_v,
		geodata
	)
FROM
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) t1,
	form_mpoint('trains', array['p_start', 'p_end', 'time_period', 'train_id']) t2,
	trainroutes
WHERE t1.temporal_pk = 5 AND t2.temporal_pk = 6 AND trainroutes.id = 5