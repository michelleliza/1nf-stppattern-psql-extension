-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION stpqueries" to load this file. \quit

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
	inst timestamp without time zone
) RETURNS TABLE (
	out_i integer,
	ts timestamp without time zone
) AS $$
DECLARE
	i integer := 1;
BEGIN
	ts := inst;
	WHILE (i <= array_length(obj_periods, 1) AND lower(obj_periods[i]) <= inst) LOOP
		IF (inst >= lower(obj_periods[i]) AND inst <= upper(obj_periods[i]) AND NOT ST_IsEmpty(in_obj[i])) THEN
			out_i := in_obj[i];
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
	inst timestamp without time zone
) RETURNS TABLE (
	out_t text,
	ts timestamp without time zone
) AS $$
DECLARE
	i integer := 1;
BEGIN
	ts := inst;
	WHILE (i <= array_length(obj_periods, 1) AND lower(obj_periods[i]) <= inst) LOOP
		IF (inst >= lower(obj_periods[i]) AND inst <= upper(obj_periods[i]) AND NOT ST_IsEmpty(in_obj[i])) THEN
			out_t := in_obj[i];
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
	inst timestamp without time zone
) RETURNS TABLE (
	out_b boolean,
	ts timestamp without time zone
) AS $$
DECLARE
	i integer := 1;
BEGIN
	ts := inst;
	WHILE (i <= array_length(obj_periods, 1) AND lower(obj_periods[i]) <= inst) LOOP
		IF (inst >= lower(obj_periods[i]) AND inst <= upper(obj_periods[i]) AND NOT ST_IsEmpty(in_obj[i])) THEN
			out_b := in_obj[i];
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
	inst timestamp without time zone
) RETURNS TABLE (
	out_r real,
	ts timestamp without time zone
) AS $$
DECLARE
	i integer := 1;
	k real;
BEGIN
	ts := inst;
	WHILE (i <= array_length(obj_periods, 1) AND lower(obj_periods[i]) <= inst) LOOP
		IF (inst = lower(obj_periods[i])) THEN
			out_i := in_obj_start[i];				 
		ELSE
			IF (inst = upper(obj_periods[i])) THEN
				out_i := in_obj_end[i];
			ELSE
				IF (inst > lower(obj_periods[i]) AND inst < upper(obj_periods[i])) THEN
					k := EXTRACT(EPOCH FROM (inst - lower(obj_periods[i]))) / EXTRACT(EPOCH FROM (upper(obj_periods[i]) - lower(obj_periods[i])));
					out_i := in_obj_start[i] + k * (in_obj_end[i] - in_obj_start[i]);																														
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
) RETURNS TABLE (
	out_p geometry(POINT),
	ts timestamp without time zone
) AS $$
DECLARE
	i integer := 1;
	j real;
	x real;
	y real;
BEGIN
	ts := inst;
	WHILE (i <= array_length(obj_periods, 1) AND lower(obj_periods[i]) <= inst) LOOP
		IF (inst = lower(obj_periods[i])) THEN
			out_p := in_obj_start[i];				 
		ELSE
			IF (inst = upper(obj_periods[i])) THEN
				out_p := in_obj_end[i];
			ELSE
				IF (inst > lower(obj_periods[i]) AND inst < upper(obj_periods[i])) THEN
					j := EXTRACT(EPOCH FROM (inst - lower(obj_periods[i]))) / EXTRACT(EPOCH FROM (upper(obj_periods[i]) - lower(obj_periods[i])));
					x := ST_X(in_obj_start[i]) + j * (ST_X(in_obj_end[i]) - ST_X(in_obj_start[i]));
					y := ST_Y(in_obj_start[i]) + j * (ST_Y(in_obj_end[i]) - ST_Y(in_obj_start[i]));
					out_p := ST_Point(x, y);
				END IF;
			END IF;
		END IF;
		i := i + 1;
	END LOOP;
	IF out_p IS NULL THEN
		out_p := 'POINT EMPTY';
	END IF;
	RETURN NEXT;
END;
$$ LANGUAGE 'plpgsql' STRICT;

CREATE OR REPLACE FUNCTION atinstant (
	in_obj geometry(POLYGON)[],
	obj_periods tsrange[],
	inst timestamp without time zone
) RETURNS TABLE (
	out_r geometry(POLYGON),
	ts timestamp without time zone
) AS $$
DECLARE
	i integer := 1;
BEGIN
	ts := inst;
	WHILE (i <= array_length(obj_periods, 1) AND lower(obj_periods[i]) <= inst) LOOP
		IF (inst >= lower(obj_periods[i]) AND inst <= upper(obj_periods[i]) AND NOT ST_IsEmpty(in_obj[i])) THEN
			out_r := in_obj[i];
		END IF;
		i := i + 1;
	END LOOP;
	IF out_r IS NULL THEN
		out_r := 'POLYGON EMPTY';
	END IF;
	RETURN NEXT;
END;
$$ LANGUAGE 'plpgsql' STRICT;

CREATE OR REPLACE FUNCTION atperiods (
	in_obj integer[],
	in_obj_periods tsrange[],
	periods tsrange[]
) RETURNS TABLE (
	out_obj integer,
	out_obj_periods tsrange
)
AS $$
DECLARE
	instant record;
	ts tsrange;
BEGIN
  FOREACH ts IN ARRAY periods LOOP
  	instant := atinstant(in_obj_start, in_obj_start, in_obj_periods, lower(ts));
	out_obj := instant.out_i;
	out_obj_periods := ts;
	RETURN NEXT;
  END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION atperiods (
	in_obj text[],
	in_obj_periods tsrange[],
	periods tsrange[]
) RETURNS TABLE (
	out_obj text,
	out_obj_periods tsrange
)
AS $$
DECLARE
	instant record;
	ts tsrange;
BEGIN
  FOREACH ts IN ARRAY periods LOOP
  	instant := atinstant(in_obj_start, in_obj_start, in_obj_periods, lower(ts));
	out_obj := instant.out_t;
	out_obj_periods := ts;
	RETURN NEXT;
  END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION atperiods (
	in_obj boolean[],
	in_obj_periods tsrange[],
	periods tsrange[]
) RETURNS TABLE (
	out_obj boolean,
	out_obj_periods tsrange
)
AS $$
DECLARE
	instant record;
	ts tsrange;
BEGIN
  FOREACH ts IN ARRAY periods LOOP
  	instant := atinstant(in_obj_start, in_obj_start, in_obj_periods, lower(ts));
	out_obj := instant.out_b;
	out_obj_periods := ts;
	RETURN NEXT;
  END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION atperiods (
	in_obj_start real[],
	in_obj_end real[],
	in_obj_periods tsrange[],
	periods tsrange[]
) RETURNS TABLE (
	out_obj_start real,
	out_obj_end real,
	out_obj_periods tsrange
)
AS $$
DECLARE
	instant_lower record;
	instant_upper record;
	ts tsrange;
BEGIN
  FOREACH ts IN ARRAY periods LOOP
  	instant_lower := atinstant(in_obj_start, in_obj_end, in_obj_periods, lower(ts));
	instant_upper := atinstant(in_obj_start, in_obj_end, in_obj_periods, upper(ts));
	out_obj_start := instant_lower.out_r;
	out_obj_end := instant_upper.out_r;
	out_obj_periods := ts;
	RETURN NEXT;
  END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION atperiods (
	in_obj_start geometry(POINT)[],
	in_obj_end geometry(POINT)[],
	in_obj_periods tsrange[],
	periods tsrange[]
) RETURNS TABLE (
	out_obj_start geometry(POINT),
	out_obj_end geometry(POINT),
	out_obj_periods tsrange
)
AS $$
DECLARE
	instant_lower record;
	instant_upper record;
	ts tsrange;
BEGIN
  FOREACH ts IN ARRAY periods LOOP
  	instant_lower := atinstant(in_obj_start, in_obj_end, in_obj_periods, lower(ts));
	instant_upper := atinstant(in_obj_start, in_obj_end, in_obj_periods, upper(ts));
	out_obj_start := instant_lower.out_p;
	out_obj_end := instant_upper.out_p;
	out_obj_periods := ts;
	RETURN NEXT;
  END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION atperiods (
	in_obj geometry(POLYGON)[],
	in_obj_periods tsrange[],
	periods tsrange[]
) RETURNS TABLE (
	out_obj geometry(POLYGON),
	out_obj_periods tsrange
)
AS $$
DECLARE
	instant record;
	ts tsrange;
BEGIN
  FOREACH ts IN ARRAY periods LOOP
  	instant := atinstant(in_obj_start, in_obj_start, in_obj_periods, lower(ts));
	out_obj := instant.out_r;
	out_obj_periods := ts;
	RETURN NEXT;
  END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION merging (
	in_obj boolean[],
  	in_periods tsrange[]
) RETURNS TABLE (
	out_obj boolean,
	out_period tsrange
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
			out_obj := current_val;
			out_period := tsrange(current_start, current_end, '[)');
			RETURN NEXT;
			current_val := in_obj[i];
			current_start := lower(in_periods[i]);
		END IF;
	END LOOP;
	current_end := upper(in_periods[i-1]);
	out_obj := current_val;
	out_period := tsrange(current_start, current_end, '[)');
	RETURN NEXT;
END;
$$ LANGUAGE plpgsql STRICT;