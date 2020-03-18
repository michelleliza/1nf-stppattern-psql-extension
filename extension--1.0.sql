-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION stpqueries" to load this file. \quit

CREATE TYPE ubool AS (
	i tsrange,
	val boolean
);

CREATE TYPE mbool AS (
	units ubool[]
);

CREATE OR REPLACE FUNCTION partitioning (
	interval_1 tsrange[],
	interval_2 tsrange[],
	OUT o tsrange[]
) AS $$
DECLARE
	tsr_1 tsrange;
	tsr_2 tsrange;
	ts_1 timestamp[];
	ts_2 timestamp[];
	i_1 integer := 1;
	i_2 integer := 1;
	i_out integer := 1;
	out_start timestamp;
  	out_end timestamp;
BEGIN
	FOREACH tsr_1 IN ARRAY interval_1
	LOOP
		ts_1 := array_append(ts_1, lower(tsr_1));
		ts_1 := array_append(ts_1, upper(tsr_1));
	END LOOP;
	FOREACH tsr_2 IN ARRAY interval_2
	LOOP
		ts_2 := array_append(ts_2, lower(tsr_2));
		ts_2 := array_append(ts_2, upper(tsr_2));
	END LOOP;
	IF ts_1[i_1] = ts_2[i_2] THEN
		out_start := ts_1[i_1];
		i_1 = i_1 + 1;
		i_2 = i_2 + 1;
	ELSE
		IF (ts_1[i_1] < ts_2[i_2]) THEN
      		out_start := ts_1[i_1];
      		i_1 := i_1 + 1;
    	ELSE
      		out_start := ts_2[i_2];
      		i_2 := i_2 + 1;
    	END IF;
  	END IF;
	WHILE i_1 <= array_length(ts_1, 1) AND i_2 <= array_length(ts_2, 1) LOOP
		IF (ts_1[i_1] = ts_2[i_2]) THEN
      		out_end := ts_1[i_1];
      		IF (out_start != out_end) THEN
				o[i_out] := tsrange(out_start, out_end, '[)');
				i_out := i_out + 1;
			END IF;
			out_start := out_end;
			i_1 := i_1 + 1;
			i_2 := i_2 + 1;
    	ELSE
      		IF (ts_1[i_1] < ts_2[i_2]) THEN
      			out_end := ts_1[i_1];
        		IF (out_start != out_end) THEN
        			o[i_out] := tsrange(out_start, out_end, '[)');
          			i_out := i_out + 1;
        		END IF;
        		out_start := out_end;
        		i_1 := i_1 + 1;
      		ELSE
        		out_end := ts_2[i_2];
        		IF (out_start != out_end) THEN
          			o[i_out] := tsrange(out_start, out_end, '[)');
          			i_out := i_out + 1;
        		END IF;
        		out_start := out_end;
        		i_2 := i_2 + 1;
      		END IF;
    	END IF;							 
	END LOOP;
    WHILE (i_1 <= array_length(ts_1, 1)) LOOP
		out_end := ts_1[i_1];
		IF (out_start != out_end) THEN
			o[i_out] := tsrange(out_start, out_end, '[)');
          	i_out := i_out + 1;
        END IF;
		out_start := out_end;
		i_1 := i_1 + 1;
	END LOOP;
	WHILE (i_2 <= array_length(ts_2, 1)) LOOP
		out_end := ts_2[i_2];
		IF (out_start != out_end) THEN
			o[i_out] := tsrange(out_start, out_end, '[)');
          	i_out := i_out + 1;
        END IF;
		out_start := out_end;
		i_2 := i_2 + 1;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION atinstant (
	in_obj integer[],
	obj_periods tsrange[],
	inst timestamp without time zone,
	OUT out_v integer,
	OUT out_ts timestamp without time zone
) AS $$
DECLARE
	i integer := 1;
BEGIN
	out_ts := inst;
	WHILE (i <= array_length(obj_periods, 1) AND lower(obj_periods[i]) <= inst) LOOP
		IF (inst >= lower(obj_periods[i]) AND inst <= upper(obj_periods[i]) AND NOT ST_IsEmpty(in_obj[i])) THEN
			out_v := in_obj[i];
		END IF;
		i := i + 1;
	END LOOP;
	END IF;
	RETURN NEXT;
END;
$$ LANGUAGE 'plpgsql' STRICT;

CREATE OR REPLACE FUNCTION atinstant (
	in_obj text[],
	obj_periods tsrange[],
	inst timestamp without time zone,
	OUT out_v text,
	OUT out_ts timestamp without time zone
) 
DECLARE
	i integer := 1;
BEGIN
	out_ts := inst;
	WHILE (i <= array_length(obj_periods, 1) AND lower(obj_periods[i]) <= inst) LOOP
		IF (inst >= lower(obj_periods[i]) AND inst <= upper(obj_periods[i]) AND NOT ST_IsEmpty(in_obj[i])) THEN
			out_v := in_obj[i];
		END IF;
		i := i + 1;
	END LOOP;
	END IF;
	RETURN NEXT;
END;
$$ LANGUAGE 'plpgsql' STRICT;

CREATE OR REPLACE FUNCTION atinstant (
	in_obj boolean[],
	obj_periods tsrange[],
	inst timestamp without time zone,
	OUT out_v boolean,
	OUT out_ts timestamp without time zone
) AS $$
DECLARE
	i integer := 1;
BEGIN
	out_ts := inst;
	WHILE (i <= array_length(obj_periods, 1) AND lower(obj_periods[i]) <= inst) LOOP
		IF (inst >= lower(obj_periods[i]) AND inst <= upper(obj_periods[i]) AND NOT ST_IsEmpty(in_obj[i])) THEN
			out_v := in_obj[i];
		END IF;
		i := i + 1;
	END LOOP;
	END IF;
	RETURN NEXT;
END;
$$ LANGUAGE 'plpgsql' STRICT;

CREATE OR REPLACE FUNCTION atinstant (
	in_obj_start real[],
	in_obj_end real[],
	obj_periods tsrange[],
	inst timestamp without time zone,
	OUT out_v real,
	OUT out_ts timestamp without time zone
) AS $$
DECLARE
	i integer := 1;
	k real;
BEGIN
	out_ts := inst;
	WHILE (i <= array_length(obj_periods, 1) AND lower(obj_periods[i]) <= inst) LOOP
		IF (inst = lower(obj_periods[i])) THEN
			out_v := in_obj_start[i];				 
		ELSE
			IF (inst = upper(obj_periods[i])) THEN
				out_v := in_obj_end[i];
			ELSE
				IF (inst > lower(obj_periods[i]) AND inst < upper(obj_periods[i])) THEN
					k := EXTRACT(EPOCH FROM (inst - lower(obj_periods[i]))) / EXTRACT(EPOCH FROM (upper(obj_periods[i]) - lower(obj_periods[i])));
					out_v := in_obj_start[i] + k * (in_obj_end[i] - in_obj_start[i]);																														
				END IF;
			END IF;
		END IF;
		i := i + 1;
	END LOOP;
	RETURN NEXT;
END;
$$ LANGUAGE 'plpgsql' STRICT;

CREATE OR REPLACE FUNCTION atinstant (
	in_obj_start geometry(POINT)[],
	in_obj_end geometry(POINT)[],
	obj_periods tsrange[],
	inst timestamp without time zone
	OUT out_v geometry(POINT),
	OUT out_ts timestamp without time zone
) AS $$
DECLARE
	i integer := 1;
	j real;
	x real;
	y real;
BEGIN
	out_ts := inst;
	WHILE (i <= array_length(obj_periods, 1) AND lower(obj_periods[i]) <= inst) LOOP
		IF (inst = lower(obj_periods[i])) THEN
			out_v := in_obj_start[i];				 
		ELSE
			IF (inst = upper(obj_periods[i])) THEN
				out_v := in_obj_end[i];
			ELSE
				IF (inst > lower(obj_periods[i]) AND inst < upper(obj_periods[i])) THEN
					j := EXTRACT(EPOCH FROM (inst - lower(obj_periods[i]))) / EXTRACT(EPOCH FROM (upper(obj_periods[i]) - lower(obj_periods[i])));
					x := ST_X(in_obj_start[i]) + j * (ST_X(in_obj_end[i]) - ST_X(in_obj_start[i]));
					y := ST_Y(in_obj_start[i]) + j * (ST_Y(in_obj_end[i]) - ST_Y(in_obj_start[i]));
					out_v := ST_Point(x, y);
				END IF;
			END IF;
		END IF;
		i := i + 1;
	END LOOP;
	IF out_v IS NULL THEN
		out_v := 'POINT EMPTY';
	END IF;
	RETURN NEXT;
END;
$$ LANGUAGE 'plpgsql' STRICT;

CREATE OR REPLACE FUNCTION atinstant (
	in_obj geometry(POLYGON)[],
	obj_periods tsrange[],
	inst timestamp without time zone,
	OUT out_v geometry(POLYGON),
	OUT out_ts timestamp without time zone
) AS $$
DECLARE
	i integer := 1;
BEGIN
	out_ts := inst;
	WHILE (i <= array_length(obj_periods, 1) AND lower(obj_periods[i]) <= inst) LOOP
		IF (inst >= lower(obj_periods[i]) AND inst <= upper(obj_periods[i]) AND NOT ST_IsEmpty(in_obj[i])) THEN
			out_v := in_obj[i];
		END IF;
		i := i + 1;
	END LOOP;
	IF out_v IS NULL THEN
		out_v := 'POLYGON EMPTY';
	END IF;
	RETURN NEXT;
END;
$$ LANGUAGE 'plpgsql' STRICT;

CREATE OR REPLACE FUNCTION atperiods (
	in_obj integer[],
	in_obj_periods tsrange[],
	periods tsrange[],
	OUT out_obj integer[],
	OUT out_obj_periods tsrange[]
) AS $$
DECLARE
	instant record;
	ts tsrange;
BEGIN
  FOREACH ts IN ARRAY periods LOOP
  	instant := atinstant(in_obj_start, in_obj_start, in_obj_periods, lower(ts));
	out_obj := array_append(out_obj, instant.out_v);
	out_obj_periods := array_append(out_obj_periods, ts);
  END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION atperiods (
	in_obj text[],
	in_obj_periods tsrange[],
	periods tsrange[],
	OUT out_obj text[],
	OUT out_obj_periods tsrange[]
) AS $$
DECLARE
	instant record;
	ts tsrange;
BEGIN
  FOREACH ts IN ARRAY periods LOOP
  	instant := atinstant(in_obj_start, in_obj_start, in_obj_periods, lower(ts));
	out_obj := array_append(out_obj, instant.out_v);
	out_obj_periods := array_append(out_obj_periods, ts);
  END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION atperiods (
	in_obj boolean[],
	in_obj_periods tsrange[],
	periods tsrange[],
	OUT out_obj boolean[],
	OUT out_obj_periods tsrange[]
) AS $$
DECLARE
	instant record;
	ts tsrange;
BEGIN
  FOREACH ts IN ARRAY periods LOOP
  	instant := atinstant(in_obj_start, in_obj_start, in_obj_periods, lower(ts));
	out_obj := array_append(out_obj, instant.out_v);
	out_obj_periods := array_append(out_obj_periods, ts);
  END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION atperiods (
	in_obj_start real[],
	in_obj_end real[],
	in_obj_periods tsrange[],
	periods tsrange[],
	OUT out_obj_start real[],
	OUT out_obj_end real[],
	OUT out_obj_periods tsrange[]
) AS $$
DECLARE
	instant_lower record;
	instant_upper record;
	ts tsrange;
BEGIN
  FOREACH ts IN ARRAY periods LOOP
  	instant_lower := atinstant(in_obj_start, in_obj_end, in_obj_periods, lower(ts));
	instant_upper := atinstant(in_obj_start, in_obj_end, in_obj_periods, upper(ts));
	out_obj_start := array_append(out_obj_start, instant_lower.out_v);
	out_obj_end := array_append(out_obj_end, instant_upper.out_v);
	out_obj_periods := array_append(out_obj_periods, ts);
  END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION atperiods (
	in_obj_start geometry(POINT)[],
	in_obj_end geometry(POINT)[],
	in_obj_periods tsrange[],
	periods tsrange[],
	OUT out_obj_start geometry(POINT)[],
	OUT out_obj_end geometry(POINT)[],
	OUT out_obj_periods tsrange[]
) AS $$
DECLARE
	instant_lower record;
	instant_upper record;
	ts tsrange;
BEGIN
  FOREACH ts IN ARRAY periods LOOP
  	instant_lower := atinstant(in_obj_start, in_obj_end, in_obj_periods, lower(ts));
	instant_upper := atinstant(in_obj_start, in_obj_end, in_obj_periods, upper(ts));
	out_obj_start := array_append(out_obj_start, instant_lower.out_v);
	out_obj_end := array_append(out_obj_end, instant_upper.out_v);
	out_obj_periods := array_append(out_obj_periods, ts);
  END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION atperiods (
	in_obj geometry(POLYGON)[],
	in_obj_periods tsrange[],
	periods tsrange[],
	OUT out_obj geometry(POLYGON)[],
	OUT out_obj_periods tsrange[]
) AS $$
DECLARE
	instant record;
	ts tsrange;
BEGIN
  FOREACH ts IN ARRAY periods LOOP
  	instant := atinstant(in_obj_start, in_obj_start, in_obj_periods, lower(ts));
	out_obj := array_append(out_obj, instant.out_v);
	out_obj_periods := array_append(out_obj_periods, ts);
  END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION merging (
	in_obj boolean[],
  	in_periods tsrange[],
	OUT out_obj boolean[],
	OUT out_periods tsrange[]
) AS $$
DECLARE
	i integer;
	current_val boolean;
	current_start timestamp;
	current_end timestamp;
BEGIN
	current_val := in_obj[i];
	current_start := lower(in_periods[i]);
	FOR i IN 1..array_length(in_obj, 1) LOOP
		IF (in_obj[i] != current_val) THEN
			current_end := upper(in_periods[i-1]);
			out_obj := array_append(out_obj, current_val);
			out_periods := array_append(out_periods, tsrange(current_start, current_end, '[)'));
			current_val := in_obj[i];
			current_start := lower(in_periods[i]);
		END IF;
	END LOOP;
	current_end := upper(in_periods[i-1]);
	out_obj := array_append(out_obj, current_val);
	out_periods := array_append(out_periods, tsrange(current_start, current_end, '[)'));
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION merging (
	in_obj_start real[],
	in_obj_end real[],
  	in_periods tsrange[],
	OUT out_obj_start real[],
	OUT out_obj_end real[],
	OUT out_periods tsrange[]
) AS $$
DECLARE
	i integer;
	current_val_start real;
	current_val_end real;
	current_start timestamp;
	current_end timestamp;
BEGIN
	current_val_start := in_obj_start[i];
	current_val_end := in_obj_end[i];
	current_start := lower(in_periods[i]);
	FOR i IN 1..array_length(in_obj, 1) LOOP
		IF (in_obj_start[i] != current_val_start OR in_obj_end != current_val_end) THEN
			current_end := upper(in_periods[i-1]);
			out_obj_start := array_append(out_obj_start, current_val_start);
			out_obj_end := array_append(out_obj_end, current_val_end);
			out_periods := array_append(out_periods, tsrange(current_start, current_end, '[)'));
			current_val_start := in_obj_start[i];
			current_val_end := in_obj_end[i];
			current_start := lower(in_periods[i]);
		END IF;
	END LOOP;
	current_end := upper(in_periods[i-1]);
	out_obj_start := array_append(out_obj_start, current_val_start);
	out_obj_end := array_append(out_obj_end, current_val_end);
	out_periods := out_periods := array_append(out_periods, tsrange(current_start, current_end, '[)'));
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE lifted_pred (
	command text,
	ARGS
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	new_periods tsrange[];
	command_result boolean;
	merge_result record;
BEGIN
	--Preprocessing
	new_periods := partitioning(periods1, periods2);
	new_object_1 := (atperiods()).out_obj;
	new_object_2 := (atperiods()).out_obj;

	--Main
	FOR i IN 1..array_length(new_periods, 1) LOOP
		EXECUTE 'SELECT '
		|| command
		|| '()'
		INTO command_result
		USING
		bool_values := array_append(bool_values, command_result);
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, new_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE lifted_opt (
	opt text,
	ARGS
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	new_periods tsrange[];
	opt_result boolean;
	merge_result record;
BEGIN
	--Preprocessing
	new_periods := partitioning(periods1, periods2);
	new_object_1 := (atperiods()).out_obj;
	new_object_2 := (atperiods()).out_obj;

	--Main
	FOR i IN 1..array_length(new_periods, 1) LOOP
		EXECUTE 'SELECT '
		|| opt
		|| ''
		INTO opt_result
		USING
		bool_values := array_append(bool_values, opt_result);
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, new_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE lifted_num (
	command text,
	ARGS
	OUT values_start real[],
	OUT values_end real[],
	OUT periods tsrange[]
) AS $$
DECLARE
	new_periods tsrange[];
	command_result_start real;
	command_result_end real;
	merge_result record;
BEGIN
	--Preprocessing
	new_periods := partitioning(periods1, periods2);
	new_object_1 := (atperiods()).out_obj;
	new_object_2 := (atperiods()).out_obj;

	--Main
	FOR i IN 1..array_length(new_periods, 1) LOOP
		EXECUTE 'SELECT '
		|| command
		|| ''
		INTO command_result_start
		USING
		EXECUTE 'SELECT '
		|| command
		|| ''
		INTO command_result_end
		USING
		values_start := array_append(bool_values, command_result_start);
		values_end := array_append(bool_values, command_result_end);
	END LOOP;

	--Postprocessing
	merge_result := merging(values_start, values_end, new_periods);
	values_start := merge_result.out_obj_start;
	values_end := merge_result.out_obj_end;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE lifted_bool_values (
	r record,
	OUT val boolean[]
) AS $$
BEGIN
	val := r.values;
END;
$$ LANGUAGE plpgsql STRICT

CREATE OR REPLACE lifted_num_start (
	r record,
	OUT val anyarray
) AS $$
BEGIN
	val := r.value_start;
END;
$$ LANGUAGE plpgsql STRICT

CREATE OR REPLACE lifted_num_end (
	r record,
	OUT val anyarray
) AS $$
BEGIN
	val := r.value_end;
END;
$$ LANGUAGE plpgsql STRICT

CREATE OR REPLACE lifted_periods (
	r record,
	OUT val tsrange[]
) AS $$
BEGIN
	val := r.periods;
END;
$$ LANGUAGE plpgsql STRICT

-- CREATE OR REPLACE FUNCTION parse (
-- 	const varchar[],
-- 	OUT o boolean
-- ) AS $$
-- DECLARE
-- 	i integer := 1;
-- 	quan integer;
-- 	quan_length varchar[];
-- 	temp_quan varchar[];
-- 	is_quan boolean := FALSE;
-- 	op varchar;
-- BEGIN
-- 	WHILE i <= array_length(const, 1) LOOP
-- 		IF const[i] = 'a' OR const[i] = 'b' OR const[i] = 's' OR const[i] = 'm' OR const[i] = 'h' OR const[i] = 'd' OR const[i] = 'M' OR const[i] = 'y' THEN
-- 			RAISE NOTICE '%', const[i];
-- 			i := i + 1;
-- 		ELSE
-- 			quan_length = '{}';
-- 			temp_quan = '{}';
-- 			is_quan := TRUE;
-- 			WHILE is_quan LOOP
-- 				quan_length := array_append(quan_length, '9');
-- 				temp_quan := array_append(temp_quan, const[i]);
-- 				i := i + 1;
-- 				IF const[i] = 's' OR const[i] = 'm' OR const[i] = 'h' OR const[i] = 'd' OR const[i] = 'M' OR const[i] = 'y' THEN
-- 					is_quan := FALSE;
-- 					op := const[i];
-- 				END IF;
-- 			END LOOP;
-- 			quan = to_number(text(temp_quan), text(quan_length));
-- 			RAISE NOTICE '%', quan;
-- 			RAISE NOTICE '%', op;
-- 			i := i + 1;
-- 		END IF;	
-- 	END LOOP;
-- END;
-- $$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION vec (
	VARIADIC elmts varchar[],
	OUT vector varchar[]
) AS $$
BEGIN
	FOR i IN 1..array_length(elmts, 1) LOOP
		vector[i] := elmts[i];
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

-- CREATE OR REPLACE FUNCTION form_constraint (
--   idx_1 integer,
--   idx_2 integer,
--   ir varchar[],
--   OUT o varchar[]
-- ) AS $$
-- BEGIN
--   o[1] := idx_1::varchar;
--   o[2] := idx_2::varchar;
--   o[3] := ir::varchar;
-- END;
-- $$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION filtering (
	lifted_pred mbool,
	OUT o tsrange[]
) AS $$
DECLARE
	i_out integer := 1;
BEGIN
	FOR i_in IN 1..array_length(lifted_pred, 1) LOOP
		IF lifted_pred.units[i_in].val THEN
			o[i_out] := lifted_pred.units[i_in].i;
			i_out := i_out + 1;
		END IF;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION filtering (
	pred_values boolean[],
	pred_periods tsrange[],
	OUT o tsrange[]
) AS $$
DECLARE
	i_out integer := 1;
BEGIN
	FOR i_in IN 1..array_length(pred_values, 1) LOOP
		IF pred_values[i_in] THEN
			o[i_out] := pred_periods[i_in];
			i_out := i_out + 1;
		END IF;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION split_sort_op (
	op text,
	OUT ops_sorted text[],
	OUT nums_sorted integer[]
) AS $$
DECLARE
	ops text[];
	nums integer[];
	i integer := 1;
	num_start integer;
	num_length integer;
	step integer := 1;
	op_found boolean;
BEGIN
	WHILE i <= char_length(op) LOOP
		num_start := i;
		num_length := 0;
		WHILE substring(op, i, 1) != 's' AND substring(op, i, 1) != 'm' AND substring(op, i, 1) != 'h' AND substring(op, i, 1) != 'd' AND substring(op, i, 1) != 'M' AND substring(op, i, 1) != 'y' AND substring(op, i, 1) != '.' LOOP
			num_length := num_length + 1;
			i := i + 1;
		END LOOP;
		ops := array_append(ops, substring(op, i, 1));
		IF substring(op, i, 1) = '.' THEN
			nums := array_append(nums, 0);
		ELSE
			nums := array_append(nums, substring(op, num_start, num_length)::integer); 
		END IF;
		i := i + 1;
	END LOOP;
	WHILE step <= 7 LOOP
		IF step = 1 THEN
			i := 1;
			op_found := FALSE;
			WHILE NOT op_found AND i <= array_length(ops, 1) LOOP
				IF ops[i] = 's' THEN
					op_found := TRUE;
					ops_sorted := array_append(ops_sorted, ops[i]);
					nums_sorted := array_append(nums_sorted, nums[i]);
				END IF;
				i := i + 1;
			END LOOP;
			step := step + 1;
		ELSIF step = 2 THEN
			i := 1;
			op_found := FALSE;
			RAISE NOTICE '%', i;
			WHILE NOT op_found AND i <= array_length(ops, 1) LOOP
				IF ops[i] = 'm' THEN
					op_found := TRUE;
					ops_sorted := array_append(ops_sorted, ops[i]);
					nums_sorted := array_append(nums_sorted, nums[i]);
				END IF;
				i := i + 1;
			END LOOP;
			step := step + 1;
		ELSIF step = 3 THEN
			i := 1;
			op_found := FALSE;
			WHILE NOT op_found AND i <= array_length(ops, 1) LOOP
				IF ops[i] = 'h' THEN
					op_found := TRUE;
					ops_sorted := array_append(ops_sorted, ops[i]);
					nums_sorted := array_append(nums_sorted, nums[i]);
				END IF;
				i := i + 1;
			END LOOP;
			step := step + 1;
		ELSIF step = 4 THEN
			i := 1;
			op_found := FALSE;
			WHILE NOT op_found AND i <= array_length(ops, 1) LOOP
				IF ops[i] = 'd' THEN
					op_found := TRUE;
					ops_sorted := array_append(ops_sorted, ops[i]);
					nums_sorted := array_append(nums_sorted, nums[i]);
				END IF;
				i := i + 1;
			END LOOP;
			step := step + 1;
		ELSIF step = 5 THEN
			i := 1;
			op_found := FALSE;
			WHILE NOT op_found AND i <= array_length(ops, 1) LOOP
				IF ops[i] = 'M' THEN
					op_found := TRUE;
					ops_sorted := array_append(ops_sorted, ops[i]);
					nums_sorted := array_append(nums_sorted, nums[i]);
				END IF;
				i := i + 1;
			END LOOP;
			step := step + 1;
		ELSIF step = 6 THEN
			i := 1;
			op_found := FALSE;
			WHILE NOT op_found AND i <= array_length(ops, 1) LOOP
				IF ops[i] = 'y' THEN
					op_found := TRUE;
					ops_sorted := array_append(ops_sorted, ops[i]);
					nums_sorted := array_append(nums_sorted, nums[i]);
				END IF;
				i := i + 1;
			END LOOP;
			step := step + 1;
		ELSE
			i := 1;
			op_found := FALSE;
			WHILE NOT op_found AND i <= array_length(ops, 1) LOOP
				IF ops[i] = '.' THEN
					op_found := TRUE;
					ops_sorted := array_append(ops_sorted, ops[i]);
					nums_sorted := array_append(nums_sorted, nums[i]);
				END IF;
				i := i + 1;
			END LOOP;
			step := step + 1;
		END IF;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION checkoperation (
	ts1 timestamp,
	ts2 timestamp,
	op text,
	OUT o boolean
) AS $$
DECLARE
	record r;
BEGIN
	r := split_sort_op(op);
	IF op LIKE '%.%' THEN
		IF array_length(r.ops_sorted) = 1 THEN
			--.
				o := (ts1 = ts2);
		ELSIF array_length(r.ops_sorted) = 2 THEN
			--s, .
			IF op LIKE '%s%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = r.nums_sorted[1]);
			--m, .
			ELSIF op LIKE '%m%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) / 60 = r.nums_sorted[1]);
			--h, .
			ELSIF op LIKE '%h%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) / 3600 = r.nums_sorted[1]);
			--d, .
			ELSIF op LIKE '%d%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) / 86400 = r.nums_sorted[1]);
			--M, .
			ELSIF op LIKE '%M%' THEN
				IF EXTRACT(YEAR FROM ts1) = EXTRACT(YEAR FROM ts2) THEN
					o := ((EXTRACT(MONTH FROM ts2) - EXTRACT(MONTH FROM ts1) = r.nums_sorted[1]) AND
						  (EXTRACT(DAY FROM ts2) = EXTRACT(DAY FROM ts2)) AND
						  (EXTRACT(HOUR FROM ts2) = EXTRACT(HOUR FROM ts2)) AND
						  (EXTRACT(MINUTE FROM ts2) = EXTRACT(MINUTE FROM ts2)) AND
						  (EXTRACT(SECOND FROM ts2) = EXTRACT(SECOND FROM ts2)));
				ELSE
					o := ((EXTRACT(DAY FROM ts2) = EXTRACT(DAY FROM ts2)) AND
						  (EXTRACT(HOUR FROM ts2) = EXTRACT(HOUR FROM ts2)) AND
						  (EXTRACT(MINUTE FROM ts2) = EXTRACT(MINUTE FROM ts2)) AND
						  (EXTRACT(SECOND FROM ts2) = EXTRACT(SECOND FROM ts2)) AND
						  (EXTRACT(MONTH FROM ts2) + (12 - EXTRACT(MONTH FROM ts1)) = r.nums_sorted[1]));
				END IF;
			--y, .
			ELSE
				o := (EXTRACT(epoch FROM ts2 - ts1) / 31536000 = r.nums_sorted[1]);
			END IF;
		ELSIF array_length(r.ops_sorted) = 3 THEN
			--s, m, .
			IF op LIKE '%s%' AND op LIKE '%m%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] + r.nums_sorted[2] * 60));
			--s, h, .
			ELSIF op LIKE '%s%' AND op LIKE '%h%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] + r.nums_sorted[2] * 3600));
			--s, d, .
			ELSIF op LIKE '%s%' AND op LIKE '%d%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] + r.nums_sorted[2] * 86400));
			--s, M, .
			ELSIF op LIKE '%s%' AND op LIKE '%M%' THEN
			
			--s, y, .
			ELSIF op LIKE '%s%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] + r.nums_sorted[2] * 31536000));
			--m, h, .
			ELSIF op LIKE '%m%' AND op LIKE '%h%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600));
			--m, d, .
			ELSIF op LIKE '%m%' AND op LIKE '%d%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 86400));
			--m, M, .
			ELSIF op LIKE '%m%' AND op LIKE '%M%' THEN

			--m, y, .
			ELSIF op LIKE '%m%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600));
			--h, d, .
			ELSIF op LIKE '%h%' AND op LIKE '%d%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 86400));
			--h, M, .
			ELSIF op LIKE '%h%' AND op LIKE '%M%' THEN

			--h, y, .
			ELSIF op LIKE '%h%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 31536000));
			--d, M, .
			ELSIF op LIKE '%d%' AND op LIKE '%M%' THEN

			--d, y, .
			ELSIF op LIKE '%d%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] * 86400 + r.nums_sorted[2] * 31536000));
			--M, y, .
			ELSE

			END IF;
		ELSIF array_length(r.ops_sorted) = 4 THEN
			--s, m, h, .
			IF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%h%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600));
			--s, m, d, .
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%d%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 86400));
			--s, m, M, .
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%M%' THEN

			--s, m, y, .
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 31536000));
			--s, h, d, .
			ELSIF op LIKE '%s%' AND op LIKE '%h%' AND op LIKE '%d%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400));
			--s, h, M, .
			ELSIF op LIKE '%s%' AND op LIKE '%h%' AND op LIKE '%M%' THEN

			--s, h, y, .
			ELSIF op LIKE '%s%' AND op LIKE '%h%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 31536000));
			--s, d, M, .
			ELSIF op LIKE '%s%' AND op LIKE '%d%' AND op LIKE '%M%' THEN

			--s, d, y, .
			ELSIF op LIKE '%s%' AND op LIKE '%d%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] + r.nums_sorted[2] * 86400 + r.nums_sorted[3] * 31536000));
			--s, M, y, .
			ELSIF op LIKE '%s%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--m, h, d, .
			ELSIF op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%d%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400));
			--m, h, M, .
			ELSIF op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%h%' THEN

			--m, h, y, .
			ELSIF op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 31536000));
			--m, d, M, .
			ELSIF op LIKE '%m%' AND op LIKE '%d%' AND op LIKE '%M%' THEN

			--m, d, y, .
			ELSIF op LIKE '%m%' AND op LIKE '%d%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 86400 + r.nums_sorted[3] * 31536000));
			--m, M, y, .
			ELSIF op LIKE '%m%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--h, d, M, .
			ELSIF op LIKE '%h%' AND op LIKE '%d%' AND op LIKE '%M%' THEN

			--h, d, y, .
			ELSIF op LIKE '%h%' AND op LIKE '%d%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 86400 + r.nums_sorted[3] * 31536000));
			--h, M, y, .
			ELSIF op LIKE '%h%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--d, M, y, .
			ELSE

			END IF;
		ELSIF array_length(r.ops_sorted) = 5 THEN
			--s, m, h, d, .
			IF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%d%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 86400));
			--s, m, h, M, .
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%M%' THEN

			--s, m, h, y, .
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 31536000));
			--s, m, d, M, .
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%d%' AND op LIKE '%M%' THEN
			
			--s, m, d, y, .
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%d%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 86400 + r.nums_sorted[4] * 31536000));
			--s, m, M, y, .
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--s, h, d, M, .
			ELSIF op LIKE '%s%' AND op LIKE '%h%' AND op LIKE '%d%' AND op LIKE '%M%' THEN

			--s, h, d, y, .
			ELSIF op LIKE '%s%' AND op LIKE '%h%' AND op LIKE '%d%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400 + r.nums_sorted[4] * 31536000));
			--s, h, M, y, .
			ELSIF op LIKE '%s%' AND op LIKE '%h%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--s, d, M, y, .
			ELSIF op LIKE '%s%' AND op LIKE '%d%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--m, h, d, M, .
			ELSIF op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%d%' AND op LIKE '%M%' THEN

			--m, h, d, y, .
			ELSIF op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%d%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400 + r.nums_sorted[4] * 31536000));
			--m, h, M, y, .
			ELSIF op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--m, d, M, y, .
			ELSIF op LIKE '%m%' AND op LIKE '%d%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--h, d, M, y, .
			ELSE

			END IF;
		ELSIF array_length(r.ops_sorted) = 6 THEN
			--s, m, h, d, M, .
			IF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%d%' AND op LIKE '%M%' THEN

			--s, m, h, d, y, .
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%d%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 86400 + r.nums_sorted[5] * 31536000));
			--s, m, h, M, y, .
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--s, m, d, M, y, .
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%d%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--s, h, d, M, y, .
			ELSIF op LIKE '%s%' AND op LIKE '%h%' AND op LIKE '%d%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--m, h, d, M, y, .
			ELSE

			END IF;
		ELSE
			--s, m, h, d, M, y, .
			
		END IF;
	ELSE
		IF array_length(r.ops_sorted) = 1 THEN
			--s
			IF op LIKE '%s%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > r.nums_sorted[1]);
			--m
			ELSIF op LIKE '%m%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) / 60 > r.nums_sorted[1]);
			--h
			ELSIF op LIKE '%h%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) / 3600 > r.nums_sorted[1]);
			--d
			ELSIF op LIKE '%d%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) / 86400 > r.nums_sorted[1]);
			--M
			ELSIF op LIKE '%M%' THEN
				IF EXTRACT(YEAR FROM ts1) = EXTRACT(YEAR FROM ts2) THEN
					o := ((EXTRACT(MONTH FROM ts2) - EXTRACT(MONTH FROM ts1) > r.nums_sorted[1]) OR 
						  ((EXTRACT(MONTH FROM ts2) - EXTRACT(MONTH FROM ts1) = r.nums_sorted[1]) AND
						   ((EXTRACT(DAY FROM ts2) > EXTRACT(DAY FROM ts2)) OR
						    ((EXTRACT(DAY FROM ts2) = EXTRACT(DAY FROM ts2)) AND
							 (EXTRACT(HOUR FROM ts2) > EXTRACT(HOUR FROM ts2))) OR
							((EXTRACT(DAY FROM ts2) = EXTRACT(DAY FROM ts2)) AND
							 (EXTRACT(HOUR FROM ts2) = EXTRACT(HOUR FROM ts2)) AND
							 (EXTRACT(MINUTE FROM ts2) > EXTRACT(MINUTE FROM ts2))) OR
							((EXTRACT(DAY FROM ts2) = EXTRACT(DAY FROM ts2)) AND
							 (EXTRACT(HOUR FROM ts2) = EXTRACT(HOUR FROM ts2)) AND
							 (EXTRACT(MINUTE FROM ts2) = EXTRACT(MINUTE FROM ts2)) AND
							 (EXTRACT(SECOND FROM ts2) > EXTRACT(SECOND FROM ts2))))));
				ELSE
					o := ((EXTRACT(MONTH FROM ts2) + (12 - EXTRACT(MONTH FROM ts1)) > r.nums_sorted[1]) OR 
						  ((EXTRACT(MONTH FROM ts2) + (12 - EXTRACT(MONTH FROM ts1)) = r.nums_sorted[1]) AND
						   ((EXTRACT(DAY FROM ts2) > EXTRACT(DAY FROM ts2)) OR
						    ((EXTRACT(DAY FROM ts2) = EXTRACT(DAY FROM ts2)) AND
							 (EXTRACT(HOUR FROM ts2) > EXTRACT(HOUR FROM ts2))) OR
							((EXTRACT(DAY FROM ts2) = EXTRACT(DAY FROM ts2)) AND
							 (EXTRACT(HOUR FROM ts2) = EXTRACT(HOUR FROM ts2)) AND
							 (EXTRACT(MINUTE FROM ts2) > EXTRACT(MINUTE FROM ts2))) OR
							((EXTRACT(DAY FROM ts2) = EXTRACT(DAY FROM ts2)) AND
							 (EXTRACT(HOUR FROM ts2) = EXTRACT(HOUR FROM ts2)) AND
							 (EXTRACT(MINUTE FROM ts2) = EXTRACT(MINUTE FROM ts2)) AND
							 (EXTRACT(SECOND FROM ts2) > EXTRACT(SECOND FROM ts2))))));
				END IF;
			--y
			ELSE
				o := (EXTRACT(epoch FROM ts2 - ts1) / 31536000 > r.nums_sorted[1]);
			END IF;
		ELSIF array_length(r.ops_sorted) = 2 THEN
			--s, m
			IF op LIKE '%s%' AND op LIKE '%m%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] + r.nums_sorted[2] * 60));
			--s, h
			ELSIF op LIKE '%s%' AND op LIKE '%h%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] + r.nums_sorted[2] * 3600));
			--s, d
			ELSIF op LIKE '%s%' AND op LIKE '%d%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] + r.nums_sorted[2] * 86400));
			--s, M
			ELSIF op LIKE '%s%' AND op LIKE '%M%' THEN
			
			--s, y
			ELSIF op LIKE '%s%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] + r.nums_sorted[2] * 31536000));
			--m, h
			ELSIF op LIKE '%m%' AND op LIKE '%h%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600));
			--m, d
			ELSIF op LIKE '%m%' AND op LIKE '%d%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 86400));
			--m, M
			ELSIF op LIKE '%m%' AND op LIKE '%M%' THEN

			--m, y
			ELSIF op LIKE '%m%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600));
			--h, d
			ELSIF op LIKE '%h%' AND op LIKE '%d%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 86400));
			--h, M
			ELSIF op LIKE '%h%' AND op LIKE '%M%' THEN

			--h, y
			ELSIF op LIKE '%h%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 31536000));
			--d, M
			ELSIF op LIKE '%d%' AND op LIKE '%M%' THEN

			--d, y
			ELSIF op LIKE '%d%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] * 86400 + r.nums_sorted[2] * 31536000));
			--M, y
			ELSE

			END IF;
		ELSIF array_length(r.ops_sorted) = 3 THEN
			--s, m, h
			IF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%h%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600));
			--s, m, d
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%d%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 86400));
			--s, m, M
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%M%' THEN

			--s, m, y
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 31536000));
			--s, h, d
			ELSIF op LIKE '%s%' AND op LIKE '%h%' AND op LIKE '%d%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400));
			--s, h, M
			ELSIF op LIKE '%s%' AND op LIKE '%h%' AND op LIKE '%M%' THEN

			--s, h, y
			ELSIF op LIKE '%s%' AND op LIKE '%h%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 31536000));
			--s, d, M
			ELSIF op LIKE '%s%' AND op LIKE '%d%' AND op LIKE '%M%' THEN

			--s, d, y
			ELSIF op LIKE '%s%' AND op LIKE '%d%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] + r.nums_sorted[2] * 86400 + r.nums_sorted[3] * 31536000));
			--s, M, y
			ELSIF op LIKE '%s%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--m, h, d
			ELSIF op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%d%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400));
			--m, h, M
			ELSIF op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%h%' THEN

			--m, h, y
			ELSIF op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 31536000));
			--m, d, M
			ELSIF op LIKE '%m%' AND op LIKE '%d%' AND op LIKE '%M%' THEN

			--m, d, y
			ELSIF op LIKE '%m%' AND op LIKE '%d%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 86400 + r.nums_sorted[3] * 31536000));
			--m, M, y
			ELSIF op LIKE '%m%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--h, d, M
			ELSIF op LIKE '%h%' AND op LIKE '%d%' AND op LIKE '%M%' THEN

			--h, d, y
			ELSIF op LIKE '%h%' AND op LIKE '%d%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 86400 + r.nums_sorted[3] * 31536000));
			--h, M, y
			ELSIF op LIKE '%h%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--d, M, y
			ELSE

			END IF;
		ELSIF array_length(r.ops_sorted) = 4 THEN
			--s, m, h, d
			IF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%d%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 86400));
			--s, m, h, M
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%M%' THEN

			--s, m, h, y
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 31536000));
			--s, m, d, M
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%d%' AND op LIKE '%M%' THEN
			
			--s, m, d, y
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%d%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 86400 + r.nums_sorted[4] * 31536000));
			--s, m, M, y
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--s, h, d, M
			ELSIF op LIKE '%s%' AND op LIKE '%h%' AND op LIKE '%d%' AND op LIKE '%M%' THEN

			--s, h, d, y
			ELSIF op LIKE '%s%' AND op LIKE '%h%' AND op LIKE '%d%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400 + r.nums_sorted[4] * 31536000));
			--s, h, M, y
			ELSIF op LIKE '%s%' AND op LIKE '%h%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--s, d, M, y
			ELSIF op LIKE '%s%' AND op LIKE '%d%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--m, h, d, M
			ELSIF op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%d%' AND op LIKE '%M%' THEN

			--m, h, d, y
			ELSIF op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%d%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400 + r.nums_sorted[4] * 31536000));
			--m, h, M, y
			ELSIF op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--m, d, M, y
			ELSIF op LIKE '%m%' AND op LIKE '%d%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--h, d, M, y
			ELSE

			END IF;
		ELSIF array_length(r.ops_sorted) = 5 THEN
			--s, m, h, d, M
			IF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%d%' AND op LIKE '%M%' THEN

			--s, m, h, d, y
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%d%' AND op LIKE '%y%' THEN
				o := (EXTRACT(epoch FROM ts2 - ts1) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 86400 + r.nums_sorted[5] * 31536000));
			--s, m, h, M, y
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%h%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--s, m, d, M, y
			ELSIF op LIKE '%s%' AND op LIKE '%m%' AND op LIKE '%d%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--s, h, d, M, y
			ELSIF op LIKE '%s%' AND op LIKE '%h%' AND op LIKE '%d%' AND op LIKE '%M%' AND op LIKE '%y%' THEN

			--m, h, d, M, y
			ELSE

			END IF;
		ELSE
			--s, m, h, d, M, y
		END IF;
	END IF;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION checkconstraint (
	interval_1 tsrange,
	interval_2 tsrange,
	ir varchar,
	OUT o boolean
) AS $$
DECLARE
	rep text[];
	rep_component text[];
	is_first_a boolean := TRUE;
	is_first_b boolean := TRUE;
	op_start integer;
	op_length integer;
	rep_i integer := 1;
BEGIN
	o := TRUE;
	WHILE i <= char_length(ir) LOOP
		IF substring(ir, i, 1) = 'a' THEN
			IF (is_first_a) THEN
				rep := array_append(rep, lower(interval_1)::text);
				rep_component := array_append(rep_component, 'a');
				is_first_a := FALSE;
			ELSE
				rep := array_append(rep, upper(interval_1)::text);
				rep_component := array_append(rep_component, 'a');
			END IF;
		ELSE
			IF substring(ir, i, 1) = 'b' THEN
				IF (is_first_b) THEN
					rep := array_append(rep, lower(interval_2)::text);
					rep_component := array_append(rep_component, 'b');
					is_first_b := FALSE;
				ELSE
					rep := array_append(rep, upper(interval_2)::text);
					rep_component := array_append(rep_component, 'b');
				END IF;
			ELSE
				i := i + 1;
				op_start := i;
				op_length := 0;
				WHILE substring(ir, i, 1) != ')' LOOP
					op_length := op_length + 1;
					i := i + 1;
				END LOOP;
				rep := array_append(rep, substring(ir, op_start, op_length));
				rep_component := array_append(rep_component, 'o');
			END IF;
		END IF;
		i := i + 1;
	END LOOP;
	WHILE o AND rep_i < array_length(rep, 1) LOOP
		IF (rep_component[i] != 'o') THEN
			IF (rep_component[i+1] != 'o') THEN
				IF (rep[i]::timestamp >= rep[i+1]::timestamp) THEN
					o := FALSE;
				END IF;
			ELSE
				IF (((i + 2) <= array_length(rep, 1)) AND (rep_component[i+2] != 'o')) THEN
					o := checkoperation(rep[i]::timestamp, rep[i+2]::timestamp, rep[i+1]);
				END IF;
			END IF;
		END IF;
		rep_i := rep_i + 1;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION pattern (
	pred_1_vals boolean[],
	pred_1_intervals tsrange[],
	pred_2_vals boolean[],
	pred_2_intervals tsrange[],
	ir varchar[],
	OUT o boolean
) AS $$
DECLARE
	true_intervals_1 tsrange[];
	true_intervals_2 tsrange[];
	ir_i integer := 1;
	i_1 integer;
	i_2 integer;
BEGIN
	--Preprocessing
	true_intervals_1 := filtering(pred_1_vals, pred_1_intervals);
  	true_intervals_2 := filtering(pred_2_vals, pred_2_intervals);

	--Main
	o := FALSE;
	WHILE NOT o AND ir_i <= array_length(ir, 1) LOOP
		i_1 := 1;
		WHILE NOT o AND i_1 <= array_length(true_intervals_1, 1) LOOP
			i_2 := 1;
			WHILE NOT o AND i_2 <= array_length(true_intervals_2, 1) LOOP
				i_2 := i_2 + 1;
				o := checkcontstraint(true_intervals_1[i_1], true_intervals_2[i_2], ir[ir_i]);
			END LOOP;
			i_1 := i_1 + 1;
		END LOOP;
		ir_i := ir_i + 1;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;