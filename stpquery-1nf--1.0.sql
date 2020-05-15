-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION stpqueries" to load this file. \quit

CREATE OR REPLACE FUNCTION form_mint (
	tablename text,
	column_names text[]	
) RETURNS TABLE (
	temporal_pk text[],
	i_v integer[],
	t_p tsrange[]
)
AS $$
DECLARE
	ids text[];
	_row record;
	query text;
BEGIN
	query = 'SELECT DISTINCT array[';
	FOR i IN 3..array_length(column_names, 1) LOOP
		query = query || column_names[i] || '::text';
		IF (i != array_length(column_names, 1)) THEN
			query = query || ', ';			 
		END IF;
	END LOOP;
	query = query || '] tmp_pk FROM ' || tablename;
	RAISE NOTICE '%', query;
	FOR _row IN EXECUTE query LOOP
		RAISE NOTICE '%', _row.tmp_pk;
		temporal_pk = _row.tmp_pk;
		query = 'SELECT array_agg('
		|| column_names[1]
		|| ' ORDER BY '
		|| column_names[2]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 3..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-2] || '''';
			IF (i != array_length(column_names, 1)) THEN
				query = query || ' AND ';			 
			END IF;
		END LOOP;
		EXECUTE query INTO i_v;
		query = 'SELECT array_agg('
		|| column_names[2]
		|| ' ORDER BY '
		|| column_names[2]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 3..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-2] || '''';
			IF (i != array_length(column_names, 1)) THEN
				query = query || ' AND ';			 
			END IF;
		END LOOP;
		EXECUTE query INTO t_p;
		RETURN NEXT;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION form_mint (
	tablename text,
	column_names text[],
	where_clause text
) RETURNS TABLE (
	temporal_pk text[],
	i_v integer[],
	t_p tsrange[]
)
AS $$
DECLARE
	ids text[];
	_row record;
	query text;
BEGIN
	query = 'SELECT DISTINCT array[';
	FOR i IN 3..array_length(column_names, 1) LOOP
		query = query || column_names[i] || '::text';
		IF (i != array_length(column_names, 1)) THEN
			query = query || ', ';			 
		END IF;
	END LOOP;
	query = query || '] tmp_pk FROM ' || tablename || ' WHERE ' || where_clause;
	RAISE NOTICE '%', query;
	FOR _row IN EXECUTE query LOOP
		RAISE NOTICE '%', _row.tmp_pk;
		temporal_pk = _row.tmp_pk;
		query = 'SELECT array_agg('
		|| column_names[1]
		|| ' ORDER BY '
		|| column_names[2]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 3..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-2] || ''' AND ';
		END LOOP;
		query = query || where_clause;
		EXECUTE query INTO i_v;
		query = 'SELECT array_agg('
		|| column_names[2]
		|| ' ORDER BY '
		|| column_names[2]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 3..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-2] || ''' AND ';
		END LOOP;
		query = query || where_clause;
		EXECUTE query INTO t_p;
		RETURN NEXT;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION form_mtext (
	tablename text,
	column_names text[]	
) RETURNS TABLE (
	temporal_pk text[],
	t_v text[],
	t_p tsrange[]
)
AS $$
DECLARE
	ids text[];
	_row record;
	query text;
BEGIN
	query = 'SELECT DISTINCT array[';
	FOR i IN 3..array_length(column_names, 1) LOOP
		query = query || column_names[i] || '::text';
		IF (i != array_length(column_names, 1)) THEN
			query = query || ', ';			 
		END IF;
	END LOOP;
	query = query || '] tmp_pk FROM ' || tablename;
	RAISE NOTICE '%', query;
	FOR _row IN EXECUTE query LOOP
		RAISE NOTICE '%', _row.tmp_pk;
		temporal_pk = _row.tmp_pk;
		query = 'SELECT array_agg('
		|| column_names[1]
		|| ' ORDER BY '
		|| column_names[2]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 3..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-2] || '''';
			IF (i != array_length(column_names, 1)) THEN
				query = query || ' AND ';			 
			END IF;
		END LOOP;
		EXECUTE query INTO t_v;
		query = 'SELECT array_agg('
		|| column_names[2]
		|| ' ORDER BY '
		|| column_names[2]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 3..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-2] || '''';
			IF (i != array_length(column_names, 1)) THEN
				query = query || ' AND ';			 
			END IF;
		END LOOP;
		EXECUTE query INTO t_p;
		RETURN NEXT;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION form_mtext (
	tablename text,
	column_names text[],
	where_clause text
) RETURNS TABLE (
	temporal_pk text[],
	t_v text[],
	t_p tsrange[]
)
AS $$
DECLARE
	ids text[];
	_row record;
	query text;
BEGIN
	query = 'SELECT DISTINCT array[';
	FOR i IN 3..array_length(column_names, 1) LOOP
		query = query || column_names[i] || '::text';
		IF (i != array_length(column_names, 1)) THEN
			query = query || ', ';			 
		END IF;
	END LOOP;
	query = query || '] tmp_pk FROM ' || tablename || ' WHERE ' || where_clause;
	RAISE NOTICE '%', query;
	FOR _row IN EXECUTE query LOOP
		RAISE NOTICE '%', _row.tmp_pk;
		temporal_pk = _row.tmp_pk;
		query = 'SELECT array_agg('
		|| column_names[1]
		|| ' ORDER BY '
		|| column_names[2]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 3..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-2] || ''' AND ';
		END LOOP;
		query = query || where_clause;
		EXECUTE query INTO t_v;
		query = 'SELECT array_agg('
		|| column_names[2]
		|| ' ORDER BY '
		|| column_names[2]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 3..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-2] || ''' AND ';
		END LOOP;
		query = query || where_clause;
		EXECUTE query INTO t_p;
		RETURN NEXT;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION form_mbool (
	tablename text,
	column_names text[]	
) RETURNS TABLE (
	temporal_pk text[],
	b_v boolean[],
	t_p tsrange[]
)
AS $$
DECLARE
	ids text[];
	_row record;
	query text;
BEGIN
	query = 'SELECT DISTINCT array[';
	FOR i IN 3..array_length(column_names, 1) LOOP
		query = query || column_names[i] || '::text';
		IF (i != array_length(column_names, 1)) THEN
			query = query || ', ';			 
		END IF;
	END LOOP;
	query = query || '] tmp_pk FROM ' || tablename;
	RAISE NOTICE '%', query;
	FOR _row IN EXECUTE query LOOP
		RAISE NOTICE '%', _row.tmp_pk;
		temporal_pk = _row.tmp_pk;
		query = 'SELECT array_agg('
		|| column_names[1]
		|| ' ORDER BY '
		|| column_names[2]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 3..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-2] || '''';
			IF (i != array_length(column_names, 1)) THEN
				query = query || ' AND ';			 
			END IF;
		END LOOP;
		EXECUTE query INTO b_v;
		query = 'SELECT array_agg('
		|| column_names[2]
		|| ' ORDER BY '
		|| column_names[2]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 3..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-2] || '''';
			IF (i != array_length(column_names, 1)) THEN
				query = query || ' AND ';			 
			END IF;
		END LOOP;
		EXECUTE query INTO t_p;
		RETURN NEXT;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION form_mbool (
	tablename text,
	column_names text[],
	where_clause text
) RETURNS TABLE (
	temporal_pk text[],
	b_v boolean[],
	t_p tsrange[]
)
AS $$
DECLARE
	ids text[];
	_row record;
	query text;
BEGIN
	query = 'SELECT DISTINCT array[';
	FOR i IN 3..array_length(column_names, 1) LOOP
		query = query || column_names[i] || '::text';
		IF (i != array_length(column_names, 1)) THEN
			query = query || ', ';			 
		END IF;
	END LOOP;
	query = query || '] tmp_pk FROM ' || tablename || ' WHERE ' || where_clause;
	RAISE NOTICE '%', query;
	FOR _row IN EXECUTE query LOOP
		RAISE NOTICE '%', _row.tmp_pk;
		temporal_pk = _row.tmp_pk;
		query = 'SELECT array_agg('
		|| column_names[1]
		|| ' ORDER BY '
		|| column_names[2]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 3..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-2] || ''' AND ';
		END LOOP;
		query = query || where_clause;
		EXECUTE query INTO b_v;
		query = 'SELECT array_agg('
		|| column_names[2]
		|| ' ORDER BY '
		|| column_names[2]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 3..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-2] || ''' AND ';
		END LOOP;
		query = query || where_clause;
		EXECUTE query INTO t_p;
		RETURN NEXT;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION form_mreal (
	tablename text,
	column_names text[]	
) RETURNS TABLE (
	temporal_pk text[],
	r_s real[],
	r_e real[],
	t_p tsrange[]
)
AS $$
DECLARE
	ids text[];
	_row record;
	query text;
BEGIN
	query = 'SELECT DISTINCT array[';
	FOR i IN 4..array_length(column_names, 1) LOOP
		query = query || column_names[i] || '::text';
		IF (i != array_length(column_names, 1)) THEN
			query = query || ', ';			 
		END IF;
	END LOOP;
	query = query || '] tmp_pk FROM ' || tablename;
	RAISE NOTICE '%', query;
	FOR _row IN EXECUTE query LOOP
		RAISE NOTICE '%', _row.tmp_pk;
		temporal_pk = _row.tmp_pk;
		query = 'SELECT array_agg('
		|| column_names[1]
		|| ' ORDER BY '
		|| column_names[3]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 4..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-3] || '''';
			IF (i != array_length(column_names, 1)) THEN
				query = query || ' AND ';			 
			END IF;
		END LOOP;
		EXECUTE query INTO r_s;
		query = 'SELECT array_agg('
		|| column_names[2]
		|| ' ORDER BY '
		|| column_names[3]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 4..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-3] || '''';
			IF (i != array_length(column_names, 1)) THEN
				query = query || ' AND ';			 
			END IF;
		END LOOP;
		EXECUTE query INTO r_e;
		query = 'SELECT array_agg('
		|| column_names[3]
		|| ' ORDER BY '
		|| column_names[3]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 4..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-3] || '''';
			IF (i != array_length(column_names, 1)) THEN
				query = query || ' AND ';			 
			END IF;
		END LOOP;
		EXECUTE query INTO t_p;
		RETURN NEXT;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION form_mreal (
	tablename text,
	column_names text[],
	where_clause text
) RETURNS TABLE (
	temporal_pk text[],
	r_s real[],
	r_e real[],
	t_p tsrange[]
)
AS $$
DECLARE
	ids text[];
	_row record;
	query text;
BEGIN
	query = 'SELECT DISTINCT array[';
	FOR i IN 4..array_length(column_names, 1) LOOP
		query = query || column_names[i] || '::text';
		IF (i != array_length(column_names, 1)) THEN
			query = query || ', ';			 
		END IF;
	END LOOP;
	query = query || '] tmp_pk FROM ' || tablename || ' WHERE ' || where_clause;
	FOR _row IN EXECUTE query LOOP
		temporal_pk = _row.tmp_pk;
		query = 'SELECT array_agg('
		|| column_names[1]
		|| ' ORDER BY '
		|| column_names[3]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 4..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-3] || ''' AND ';
		END LOOP;
		query = query || where_clause;
		EXECUTE query INTO r_s;
		query = 'SELECT array_agg('
		|| column_names[2]
		|| ' ORDER BY '
		|| column_names[3]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 4..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-3] || ''' AND ';
		END LOOP;
		query = query || where_clause;
		EXECUTE query INTO r_e;
		query = 'SELECT array_agg('
		|| column_names[3]
		|| ' ORDER BY '
		|| column_names[3]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 4..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-3] || ''' AND ';
		END LOOP;
		query = query || where_clause;
		EXECUTE query INTO t_p;
		RETURN NEXT;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION form_mpoint (
	tablename text,
	column_names text[]	
) RETURNS TABLE (
	temporal_pk text[],
	p_s geometry(POINT)[],
	p_e geometry(POINT)[],
	t_p tsrange[]
)
AS $$
DECLARE
	ids text[];
	_row record;
	query text;
BEGIN
	query = 'SELECT DISTINCT array[';
	FOR i IN 4..array_length(column_names, 1) LOOP
		query = query || column_names[i] || '::text';
		IF (i != array_length(column_names, 1)) THEN
			query = query || ', ';			 
		END IF;
	END LOOP;
	query = query || '] tmp_pk FROM ' || tablename;
	RAISE NOTICE '%', query;
	FOR _row IN EXECUTE query LOOP
		RAISE NOTICE '%', _row.tmp_pk;
		temporal_pk = _row.tmp_pk;
		query = 'SELECT array_agg('
		|| column_names[1]
		|| ' ORDER BY '
		|| column_names[3]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 4..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-3] || '''';
			IF (i != array_length(column_names, 1)) THEN
				query = query || ' AND ';			 
			END IF;
		END LOOP;
		EXECUTE query INTO p_s;
		query = 'SELECT array_agg('
		|| column_names[2]
		|| ' ORDER BY '
		|| column_names[3]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 4..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-3] || '''';
			IF (i != array_length(column_names, 1)) THEN
				query = query || ' AND ';			 
			END IF;
		END LOOP;
		EXECUTE query INTO p_e;
		query = 'SELECT array_agg('
		|| column_names[3]
		|| ' ORDER BY '
		|| column_names[3]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 4..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-3] || '''';
			IF (i != array_length(column_names, 1)) THEN
				query = query || ' AND ';			 
			END IF;
		END LOOP;
		EXECUTE query INTO t_p;
		RETURN NEXT;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION form_mpoint (
	tablename text,
	column_names text[],
	where_clause text
) RETURNS TABLE (
	temporal_pk text[],
	p_s geometry(POINT)[],
	p_e geometry(POINT)[],
	t_p tsrange[]
)
AS $$
DECLARE
	ids text[];
	_row record;
	query text;
BEGIN
	query = 'SELECT DISTINCT array[';
	FOR i IN 4..array_length(column_names, 1) LOOP
		query = query || column_names[i] || '::text';
		IF (i != array_length(column_names, 1)) THEN
			query = query || ', ';			 
		END IF;
	END LOOP;
	query = query || '] tmp_pk FROM ' || tablename || ' WHERE ' || where_clause;
	FOR _row IN EXECUTE query LOOP
		temporal_pk = _row.tmp_pk;
		query = 'SELECT array_agg('
		|| column_names[1]
		|| ' ORDER BY '
		|| column_names[3]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 4..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-3] || ''' AND ';
		END LOOP;
		query = query || where_clause;
		EXECUTE query INTO p_s;
		query = 'SELECT array_agg('
		|| column_names[2]
		|| ' ORDER BY '
		|| column_names[3]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 4..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-3] || ''' AND ';
		END LOOP;
		query = query || where_clause;
		EXECUTE query INTO p_e;
		query = 'SELECT array_agg('
		|| column_names[3]
		|| ' ORDER BY '
		|| column_names[3]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 4..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-3] || ''' AND ';
		END LOOP;
		query = query || where_clause;
		EXECUTE query INTO t_p;
		RETURN NEXT;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION form_mregion (
	tablename text,
	column_names text[]	
) RETURNS TABLE (
	temporal_pk text[],
	r_v geometry(POLYGON)[],
	t_p tsrange[]
)
AS $$
DECLARE
	ids text[];
	_row record;
	query text;
BEGIN
	query = 'SELECT DISTINCT array[';
	FOR i IN 3..array_length(column_names, 1) LOOP
		query = query || column_names[i] || '::text';
		IF (i != array_length(column_names, 1)) THEN
			query = query || ', ';			 
		END IF;
	END LOOP;
	query = query || '] tmp_pk FROM ' || tablename;
	RAISE NOTICE '%', query;
	FOR _row IN EXECUTE query LOOP
		RAISE NOTICE '%', _row.tmp_pk;
		temporal_pk = _row.tmp_pk;
		query = 'SELECT array_agg('
		|| column_names[1]
		|| ' ORDER BY '
		|| column_names[2]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 3..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-2] || '''';
			IF (i != array_length(column_names, 1)) THEN
				query = query || ' AND ';			 
			END IF;
		END LOOP;
		EXECUTE query INTO r_v;
		query = 'SELECT array_agg('
		|| column_names[2]
		|| ' ORDER BY '
		|| column_names[2]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 3..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-2] || '''';
			IF (i != array_length(column_names, 1)) THEN
				query = query || ' AND ';			 
			END IF;
		END LOOP;
		EXECUTE query INTO t_p;
		RETURN NEXT;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION form_mregion (
	tablename text,
	column_names text[],
	where_clause text
) RETURNS TABLE (
	temporal_pk text[],
	r_v geometry(POLYGON)[],
	t_p tsrange[]
)
AS $$
DECLARE
	ids text[];
	_row record;
	query text;
BEGIN
	query = 'SELECT DISTINCT array[';
	FOR i IN 3..array_length(column_names, 1) LOOP
		query = query || column_names[i] || '::text';
		IF (i != array_length(column_names, 1)) THEN
			query = query || ', ';			 
		END IF;
	END LOOP;
	query = query || '] tmp_pk FROM ' || tablename || ' WHERE ' || where_clause;
	RAISE NOTICE '%', query;
	FOR _row IN EXECUTE query LOOP
		RAISE NOTICE '%', _row.tmp_pk;
		temporal_pk = _row.tmp_pk;
		query = 'SELECT array_agg('
		|| column_names[1]
		|| ' ORDER BY '
		|| column_names[2]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 3..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-2] || ''' AND ';
		END LOOP;
		query = query || where_clause;
		EXECUTE query INTO r_v;
		query = 'SELECT array_agg('
		|| column_names[2]
		|| ' ORDER BY '
		|| column_names[2]
		|| ') FROM '
		|| tablename
		|| ' WHERE ';
		FOR i IN 3..array_length(column_names, 1) LOOP
			query = query || column_names[i] || '::text = ''' || _row.tmp_pk[i-2] || ''' AND ';
		END LOOP;
		query = query || where_clause;
		EXECUTE query INTO t_p;
		RETURN NEXT;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION partitioning (
	intervals_1 tsrange[],
	intervals_2 tsrange[],
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
	FOREACH tsr_1 IN ARRAY intervals_1
	LOOP
		ts_1 := array_append(ts_1, lower(tsr_1));
		ts_1 := array_append(ts_1, upper(tsr_1));
	END LOOP;
	FOREACH tsr_2 IN ARRAY intervals_2
	LOOP
		ts_2 := array_append(ts_2, lower(tsr_2));
		ts_2 := array_append(ts_2, upper(tsr_2));
	END LOOP;
	IF (ts_1[i_1] = ts_2[i_2]) THEN
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
	WHILE ((i_1 <= array_length(ts_1, 1)) AND (i_2 <= array_length(ts_2, 1))) LOOP
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
	WHILE ((i <= array_length(obj_periods, 1)) AND (lower(obj_periods[i]) <= inst)) LOOP
		IF ((inst >= lower(obj_periods[i])) AND (inst <= upper(obj_periods[i])) AND (in_obj[i] IS NOT NULL)) THEN
			out_v := in_obj[i];
		END IF;
		i := i + 1;
	END LOOP;
END;
$$ LANGUAGE 'plpgsql' STRICT;

CREATE OR REPLACE FUNCTION atinstant (
	in_obj text[],
	obj_periods tsrange[],
	inst timestamp without time zone,
	OUT out_v text,
	OUT out_ts timestamp without time zone
) AS $$
DECLARE
	i integer := 1;
BEGIN
	out_ts := inst;
	WHILE ((i <= array_length(obj_periods, 1)) AND (lower(obj_periods[i]) <= inst)) LOOP
		IF ((inst >= lower(obj_periods[i])) AND (inst <= upper(obj_periods[i])) AND (in_obj[i] IS NOT NULL)) THEN
			out_v := in_obj[i];
		END IF;
		i := i + 1;
	END LOOP;
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
	WHILE ((i <= array_length(obj_periods, 1)) AND (lower(obj_periods[i]) <= inst)) LOOP
		IF ((inst >= lower(obj_periods[i])) AND (inst <= upper(obj_periods[i])) AND (in_obj[i] IS NOT NULL)) THEN
			out_v := in_obj[i];
		END IF;
		i := i + 1;
	END LOOP;
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
				IF ((inst > lower(obj_periods[i])) AND (inst < upper(obj_periods[i])) AND (in_obj_start[i] IS NOT NULL) AND (in_obj_end[i] IS NOT NULL)) THEN
					k := EXTRACT(EPOCH FROM (inst - lower(obj_periods[i]))) / EXTRACT(EPOCH FROM (upper(obj_periods[i]) - lower(obj_periods[i])));
					out_v := in_obj_start[i] + k * (in_obj_end[i] - in_obj_start[i]);																														
				END IF;
			END IF;
		END IF;
		i := i + 1;
	END LOOP;
END;
$$ LANGUAGE 'plpgsql' STRICT;

CREATE OR REPLACE FUNCTION atinstant (
	in_obj_start geometry(POINT)[],
	in_obj_end geometry(POINT)[],
	obj_periods tsrange[],
	inst timestamp without time zone,
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
	WHILE ((i <= array_length(obj_periods, 1)) AND (lower(obj_periods[i]) <= inst)) LOOP
		IF (inst = lower(obj_periods[i])) THEN
			out_v := in_obj_start[i];				 
		ELSE
			IF (inst = upper(obj_periods[i])) THEN
				out_v := in_obj_end[i];
			ELSE
				IF ((inst > lower(obj_periods[i])) AND (inst < upper(obj_periods[i]))) THEN
					j := EXTRACT(EPOCH FROM (inst - lower(obj_periods[i]))) / EXTRACT(EPOCH FROM (upper(obj_periods[i]) - lower(obj_periods[i])));
					x := ST_X(in_obj_start[i]) + j * (ST_X(in_obj_end[i]) - ST_X(in_obj_start[i]));
					y := ST_Y(in_obj_start[i]) + j * (ST_Y(in_obj_end[i]) - ST_Y(in_obj_start[i]));
					out_v := ST_Point(x, y);
				END IF;
			END IF;
		END IF;
		i := i + 1;
	END LOOP;
	IF (out_v IS NULL) THEN
		out_v := 'POINT EMPTY';
	END IF;
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
	WHILE ((i <= array_length(obj_periods, 1)) AND (lower(obj_periods[i]) <= inst)) LOOP
		IF ((inst >= lower(obj_periods[i])) AND (inst <= upper(obj_periods[i])) AND (NOT (ST_IsEmpty(in_obj[i])))) THEN
			out_v := in_obj[i];
		END IF;
		i := i + 1;
	END LOOP;
	IF (out_v IS NULL) THEN
		out_v := 'POLYGON EMPTY';
	END IF;
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
		instant := atinstant(in_obj, in_obj_periods, lower(ts));
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
	current_val boolean;
	current_start timestamp;
	current_end timestamp;
BEGIN
	current_val := in_obj[1];
	current_start := lower(in_periods[1]);
	FOR i IN 1..array_length(in_obj, 1) LOOP
		IF (in_obj[i] != current_val) THEN
			current_end := upper(in_periods[i-1]);
			out_obj := array_append(out_obj, current_val);
			out_periods := array_append(out_periods, tsrange(current_start, current_end, '[)'));
			current_val := in_obj[i];
			current_start := lower(in_periods[i]);
		END IF;
	END LOOP;
	out_obj := array_append(out_obj, current_val);
	out_periods := array_append(out_periods, tsrange(current_start, upper(in_periods[array_upper(in_periods, 1)]), '[)'));
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
	current_val_start real;
	current_val_end real;
	current_start timestamp;
	current_end timestamp;
BEGIN
	current_val_start := in_obj_start[1];
	current_val_end := in_obj_end[1];
	current_start := lower(in_periods[1]);
	FOR i IN 1..array_length(in_obj_start, 1) LOOP
		IF ((in_obj_start[i] != current_val_start) OR (in_obj_end[i] != current_val_end)) THEN
			current_end := upper(in_periods[i-1]);
			out_obj_start := array_append(out_obj_start, current_val_start);
			out_obj_end := array_append(out_obj_end, current_val_end);
			out_periods := array_append(out_periods, tsrange(current_start, current_end, '[)'));
			current_val_start := in_obj_start[i];
			current_val_end := in_obj_end[i];
			current_start := lower(in_periods[i]);
		END IF;
	END LOOP;
	out_obj_start := array_append(out_obj_start, current_val_start);
	out_obj_end := array_append(out_obj_end, current_val_end);
	out_periods := array_append(out_periods, tsrange(current_start, upper(in_periods[array_upper(in_periods, 1)]), '[)'));
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_pred (
	command text,
	p_start geometry(POINT)[],
	p_end geometry(POINT)[],
	p_periods tsrange[],
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	command_result boolean;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Main
	IF (array_length(p_start, 1) = 1) THEN
		FOR i IN 1..array_length(p_periods, 1) LOOP
			IF ((NOT ST_IsEmpty(p_start[1])) AND (NOT ST_IsEmpty(p_end[i]))) THEN
				EXECUTE 'SELECT '
				|| command
				|| '($1, $2)'
				INTO command_result
				USING p_start[1], p_end[i];
				bool_values := array_append(bool_values, command_result);
				not_empty_periods := array_append(not_empty_periods, p_periods[i]);
			END IF;
		END LOOP;
	ELSE
		FOR i IN 1..array_length(p_periods, 1) LOOP
			IF (NOT ST_IsEmpty(p_start[i])) THEN
				EXECUTE 'SELECT '
				|| command
				|| '($1)'
				INTO command_result
				USING p_start[i];
				bool_values := array_append(bool_values, command_result);
				not_empty_periods := array_append(not_empty_periods, p_periods[i]);
			END IF;
		END LOOP;
	END IF;

	--Postprocessing
	merge_result := merging(bool_values, not_empty_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_pred (
	command text,
	p_start geometry(POINT)[],
	p_end geometry(POINT)[],
	p_periods tsrange[],
	geom geometry,
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	command_result boolean;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(p_periods, 1) LOOP
		IF (NOT ST_IsEmpty(p_start[i])) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result
			USING p_start[i], geom;
			bool_values := array_append(bool_values, command_result);
			not_empty_periods := array_append(not_empty_periods, p_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, not_empty_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_pred (
	command text,
	p_start geometry(POINT)[],
	p_end geometry(POINT)[],
	p_periods tsrange[],
	geom geometry[],
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	command_result boolean;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(p_periods, 1) LOOP
		IF (NOT ST_IsEmpty(p_start[i])) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result
			USING p_start[i], geom[1];
			bool_values := array_append(bool_values, command_result);
			not_empty_periods := array_append(not_empty_periods, p_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, not_empty_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_pred (
	command text,
	geom geometry,
	p_start geometry(POINT)[],
	p_end geometry(POINT)[],
	p_periods tsrange[],
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	command_result boolean;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(p_periods, 1) LOOP
		IF (NOT ST_IsEmpty(p_start[i])) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result
			USING geom, p_start[i];
			bool_values := array_append(bool_values, command_result);
			not_empty_periods := array_append(not_empty_periods, p_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, not_empty_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_pred (
	command text,
	geom geometry[],
	p_start geometry(POINT)[],
	p_end geometry(POINT)[],
	p_periods tsrange[],
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	command_result boolean;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(p_periods, 1) LOOP
		IF (NOT ST_IsEmpty(p_start[i])) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result
			USING geom[1], p_start[i];
			bool_values := array_append(bool_values, command_result);
			not_empty_periods := array_append(not_empty_periods, p_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, not_empty_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_pred (
	command text,
	p1_start geometry(POINT)[],
	p1_end geometry(POINT)[],
	p1_periods tsrange[],
	p2_start geometry(POINT)[],
	p2_end geometry(POINT)[],
	p2_periods tsrange[],
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	new_periods tsrange[];
	new_object_1 record;
	new_object_2 record;
	command_result boolean;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Preprocessing
	new_periods := partitioning(p1_periods, p2_periods);
	new_object_1 := atperiods(p1_start, p1_end, p1_periods, new_periods);
	new_object_2 := atperiods(p2_start, p2_end, p2_periods, new_periods);

	--Main
	FOR i IN 1..array_length(new_periods, 1) LOOP
		IF ((NOT ST_IsEmpty(new_object_1.out_obj_start[i])) AND (NOT ST_IsEmpty(new_object_2.out_obj_start[i]))) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result
			USING new_object_1.out_obj_start[i], new_object_2.out_obj_start[i];
			bool_values := array_append(bool_values, command_result);
			not_empty_periods := array_append(not_empty_periods, new_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, not_empty_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_pred (
	command text,
	region geometry(POLYGON)[],
	r_periods tsrange[],
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	command_result boolean;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(r_periods, 1) LOOP
		IF (NOT ST_IsEmpty(region[i])) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1)'
			INTO command_result
			USING region[i];
			bool_values := array_append(bool_values, command_result);
			not_empty_periods := array_append(not_empty_periods, r_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, not_empty_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_pred (
	command text,
	region geometry(POLYGON)[],
	r_periods tsrange[],
	geom geometry,
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	command_result boolean;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(r_periods, 1) LOOP
		IF (NOT ST_IsEmpty(region[i])) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result
			USING region[i], geom;
			bool_values := array_append(bool_values, command_result);
			not_empty_periods := array_append(not_empty_periods, r_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, not_empty_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_pred (
	command text,
	region geometry(POLYGON)[],
	r_periods tsrange[],
	geom geometry[],
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	command_result boolean;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(r_periods, 1) LOOP
		IF (NOT ST_IsEmpty(region[i])) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result
			USING region[i], geom[1];
			bool_values := array_append(bool_values, command_result);
			not_empty_periods := array_append(not_empty_periods, r_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, not_empty_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_pred (
	command text,
	geom geometry,
	region geometry(POLYGON)[],
	r_periods tsrange[],
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	command_result boolean;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(r_periods, 1) LOOP
		IF (NOT ST_IsEmpty(region[i])) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result
			USING geom, region[i];
			bool_values := array_append(bool_values, command_result);
			not_empty_periods := array_append(not_empty_periods, r_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, not_empty_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_pred (
	command text,
	r1 geometry(POLYGON)[],
	r1_periods tsrange[],
	r2 geometry(POLYGON)[],
	r2_periods tsrange[],
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	new_periods tsrange[];
	new_object_1 record;
	new_object_2 record;
	command_result boolean;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Preprocessing
	new_periods := partitioning(r1_periods, r2_periods);
	new_object_1 := atperiods(r2, r2_periods, new_periods);
	new_object_2 := atperiods(r1, r1_periods, new_periods);

	--Main
	FOR i IN 1..array_length(new_periods, 1) LOOP
		IF ((NOT ST_IsEmpty(new_object_1.out_obj[i])) AND (NOT ST_IsEmpty(new_object_2.out_obj[i]))) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result
			USING new_object_1.out_obj[i], new_object_2.out_obj[i];
			bool_values := array_append(bool_values, command_result);
			not_empty_periods := array_append(not_empty_periods, new_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, not_empty_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_pred (
	command text,
	p_start geometry(POINT)[],
	p_end geometry(POINT)[],
	p_periods tsrange[],
	region geometry(POLYGON)[],
	r_periods tsrange[],
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	new_periods tsrange[];
	new_object_1 record;
	new_object_2 record;
	command_result boolean;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Preprocessing
	new_periods := partitioning(p_periods, r_periods);
	new_object_1 := atperiods(p_start, p_end, p_periods, new_periods);
	new_object_2 := atperiods(region, r_periods, new_periods);

	--Main
	FOR i IN 1..array_length(new_periods, 1) LOOP
		IF ((NOT ST_IsEmpty(new_object_1.out_obj_start[i])) AND (NOT ST_IsEmpty(new_object_2.out_obj[i]))) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result
			USING new_object_1.out_obj_start[i], new_object_2.out_obj[i];
			bool_values := array_append(bool_values, command_result);
			not_empty_periods := array_append(not_empty_periods, new_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, not_empty_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_pred (
	command text,
	region geometry(POLYGON)[],
	r_periods tsrange[],
	p_start geometry(POINT)[],
	p_end geometry(POINT)[],
	p_periods tsrange[],
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	new_periods tsrange[];
	new_object_1 record;
	new_object_2 record;
	command_result boolean;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Preprocessing
	new_periods := partitioning(p_periods, r_periods);
	new_object_1 := atperiods(p_start, p_end, p_periods, new_periods);
	new_object_2 := atperiods(region, r_periods, new_periods);

	--Main
	FOR i IN 1..array_length(new_periods, 1) LOOP
		IF ((NOT ST_IsEmpty(new_object_1.out_obj_start[i])) AND (NOT ST_IsEmpty(new_object_2.out_obj[i]))) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result
			USING new_object_2.out_obj[i], new_object_1.out_obj_start[i];
			bool_values := array_append(bool_values, command_result);
			not_empty_periods := array_append(not_empty_periods, new_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, not_empty_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_opt (
	opt text,
	r1 real,
	r2_vals_start real[],
	r2_vals_end real[],
	r2_periods tsrange[],
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	opt_result boolean;
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(r2_periods, 1) LOOP
		EXECUTE 'SELECT $1'
		|| opt
		|| '$2'
		INTO opt_result
		USING r1, r2_vals_start[i];
		bool_values := array_append(bool_values, opt_result);
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, r2_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_opt (
	opt text,
	r1 real[],
	r2_vals_start real[],
	r2_vals_end real[],
	r2_periods tsrange[],
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	opt_result boolean;
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(r2_periods, 1) LOOP
		EXECUTE 'SELECT $1'
		|| opt
		|| '$2'
		INTO opt_result
		USING r1[1], r2_vals_start[i];
		bool_values := array_append(bool_values, opt_result);
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, r2_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_opt (
	opt text,
	r1 real,
	r2 record,
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	opt_result boolean;
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(r2.periods, 1) LOOP
		EXECUTE 'SELECT $1'
		|| opt
		|| '$2'
		INTO opt_result
		USING r1, r2.values_start[i];
		bool_values := array_append(bool_values, opt_result);
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, r2.periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_opt (
	opt text,
	r1 real[],
	r2 record,
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	opt_result boolean;
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(r2.periods, 1) LOOP
		EXECUTE 'SELECT $1'
		|| opt
		|| '$2'
		INTO opt_result
		USING r1[1], r2.values_start[i];
		bool_values := array_append(bool_values, opt_result);
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, r2.periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_opt (
	opt text,
	r1_vals_start real[],
	r1_vals_end real[],
	r1_periods tsrange[],
	r2 real,
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	opt_result boolean;
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(r1_periods, 1) LOOP
		EXECUTE 'SELECT $1'
		|| opt
		|| '$2'
		INTO opt_result
		USING r1_vals_start[i], r2;
		bool_values := array_append(bool_values, opt_result);
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, r1_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_opt (
	opt text,
	r1_vals_start real[],
	r1_vals_end real[],
	r1_periods tsrange[],
	r2 real[],
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	opt_result boolean;
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(r1_periods, 1) LOOP
		EXECUTE 'SELECT $1'
		|| opt
		|| '$2'
		INTO opt_result
		USING r1_vals_start[i], r2[1];
		bool_values := array_append(bool_values, opt_result);
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, r1_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_opt (
	opt text,
	r1 record,
	r2 real,
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	opt_result boolean;
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(r1.periods, 1) LOOP
		EXECUTE 'SELECT $1'
		|| opt
		|| '$2'
		INTO opt_result
		USING r1.values_start[i], r2;
		bool_values := array_append(bool_values, opt_result);
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, r1.periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_opt (
	opt text,
	r1 record,
	r2 real[1],
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	opt_result boolean;
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(r1.periods, 1) LOOP
		EXECUTE 'SELECT $1'
		|| opt
		|| '$2'
		INTO opt_result
		USING r1.values_start[i], r2[1];
		bool_values := array_append(bool_values, opt_result);
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, r1.periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_opt (
	opt text,
	r1_vals_start real[],
	r1_vals_end real[],
	r1_periods tsrange[],
	r2_vals_start real[],
	r2_vals_end real[],
	r2_periods tsrange[],
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	new_periods tsrange[];
	new_object_1 record;
	new_object_2 record;
	opt_result boolean;
	merge_result record;
BEGIN
	--Preprocessing
	new_periods := partitioning(r1_periods, r2_periods);
	new_object_1 := atperiods(r1_vals_start, r1_vals_end, r1_periods, new_periods);
	new_object_2 := atperiods(r2_vals_start, r2_vals_end, r2_periods, new_periods);

	--Main
	FOR i IN 1..array_length(new_periods, 1) LOOP
		EXECUTE 'SELECT $1'
		|| opt
		|| '$2'
		INTO opt_result
		USING r1_vals_start[i], r2_vals_start[i];
		bool_values := array_append(bool_values, opt_result);
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, new_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_opt (
	opt text,
	r1 record,
	r2 record,
	OUT bool_values boolean[],
	OUT periods tsrange[]
) AS $$
DECLARE
	new_periods tsrange[];
	new_object_1 record;
	new_object_2 record;
	opt_result boolean;
	merge_result record;
BEGIN
	--Preprocessing
	new_periods := partitioning(r1_periods, r2_periods);
	new_object_1 := atperiods(r1_vals_start, r1_vals_end, r1_periods, new_periods);
	new_object_2 := atperiods(r2_vals_start, r2_vals_end, r2_periods, new_periods);

	--Main
	FOR i IN 1..array_length(new_periods, 1) LOOP
		EXECUTE 'SELECT $1'
		|| opt
		|| '$2'
		INTO opt_result
		USING r1_vals_start[i], r2_vals_start[i];
		bool_values := array_append(bool_values, opt_result);
	END LOOP;

	--Postprocessing
	merge_result := merging(bool_values, new_periods);
	bool_values := merge_result.out_obj;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_num (
	command text,
	p_start geometry(POINT)[],
	p_end geometry(POINT)[],
	p_periods tsrange[],
	OUT values_start real[],
	OUT values_end real[],
	OUT periods tsrange[]
) AS $$
DECLARE
	command_result_start real;
	command_result_end real;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Main
	IF (array_length(p_start, 1) = 1) THEN
		FOR i IN 1..(array_length(p_periods, 1) - 1) LOOP
			IF ((NOT ST_IsEmpty(p_end[i])) AND (NOT ST_IsEmpty(p_end[i+1]))) THEN
				EXECUTE 'SELECT '
				|| command
				|| '($1, $2)'
				INTO command_result_start
				USING p_end[i], p_start[1];
				EXECUTE 'SELECT '
				|| command
				|| '($1, $2)'
				INTO command_result_end
				USING p_end[i+1], p_start[1];
				values_start := array_append(values_start, command_result_start);
				values_end := array_append(values_end, command_result_end);
				not_empty_periods := array_append(not_empty_periods, p_periods[i]);
			END IF;
		END LOOP;
	ELSE
		FOR i IN 1..array_length(p_periods, 1) LOOP
			IF ((NOT ST_IsEmpty(p_start[i])) AND (NOT ST_IsEmpty(p_end[i]))) THEN
				EXECUTE 'SELECT '
				|| command
				|| '($1)'
				INTO command_result_start
				USING p_start[i];
				EXECUTE 'SELECT '
				|| command
				|| '($1)'
				INTO command_result_end
				USING p_end[i];
				values_start := array_append(values_start, command_result_start);
				values_end := array_append(values_end, command_result_end);
				not_empty_periods := array_append(not_empty_periods, p_periods[i]);
			END IF;
		END LOOP;
	END IF;

	--Postprocessing
	merge_result := merging(values_start, values_end, not_empty_periods);
	values_start := merge_result.out_obj_start;
	values_end := merge_result.out_obj_end;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_num (
	command text,
	p_start geometry(POINT)[],
	p_end geometry(POINT)[],
	p_periods tsrange[],
	geom geometry,
	OUT values_start real[],
	OUT values_end real[],
	OUT periods tsrange[]
) AS $$
DECLARE
	command_result_start real;
	command_result_end real;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(p_periods, 1) LOOP
		IF ((NOT ST_IsEmpty(p_start[i])) AND (NOT ST_IsEmpty(p_end[i]))) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_start
			USING p_start[i], geom;
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_end
			USING p_end[i], geom;
			values_start := array_append(values_start, command_result_start);
			values_end := array_append(values_end, command_result_end);
			not_empty_periods := array_append(not_empty_periods, p_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(values_start, values_end, not_empty_periods);
	values_start := merge_result.out_obj_start;
	values_end := merge_result.out_obj_end;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_num (
	command text,
	p_start geometry(POINT)[],
	p_end geometry(POINT)[],
	p_periods tsrange[],
	geom geometry[],
	OUT values_start real[],
	OUT values_end real[],
	OUT periods tsrange[]
) AS $$
DECLARE
	command_result_start real;
	command_result_end real;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(p_periods, 1) LOOP
		IF ((NOT ST_IsEmpty(p_start[i])) AND (NOT ST_IsEmpty(p_end[i]))) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_start
			USING p_start[i], geom[1];
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_end
			USING p_end[i], geom[1];
			values_start := array_append(values_start, command_result_start);
			values_end := array_append(values_end, command_result_end);
			not_empty_periods := array_append(not_empty_periods, p_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(values_start, values_end, not_empty_periods);
	values_start := merge_result.out_obj_start;
	values_end := merge_result.out_obj_end;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_num (
	command text,
	geom geometry,
	p_start geometry(POINT)[],
	p_end geometry(POINT)[],
	p_periods tsrange[],
	OUT values_start real[],
	OUT values_end real[],
	OUT periods tsrange[]
) AS $$
DECLARE
	command_result_start real;
	command_result_end real;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(p_periods, 1) LOOP
		IF ((NOT ST_IsEmpty(p_start[i])) AND (NOT ST_IsEmpty(p_end[i]))) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_start
			USING geom, p_start[i];
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_end
			USING geom, p_end[i];
			values_start := array_append(values_start, command_result_start);
			values_end := array_append(values_end, command_result_end);
			not_empty_periods := array_append(not_empty_periods, p_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(values_start, values_end, not_empty_periods);
	values_start := merge_result.out_obj_start;
	values_end := merge_result.out_obj_end;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_num (
	command text,
	geom geometry[],
	p_start geometry(POINT)[],
	p_end geometry(POINT)[],
	p_periods tsrange[],
	OUT values_start real[],
	OUT values_end real[],
	OUT periods tsrange[]
) AS $$
DECLARE
	command_result_start real;
	command_result_end real;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..array_length(p_periods, 1) LOOP
		IF ((NOT ST_IsEmpty(p_start[i])) AND (NOT ST_IsEmpty(p_end[i]))) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_start
			USING geom[1], p_start[i];
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_end
			USING geom[1], p_end[i];
			values_start := array_append(values_start, command_result_start);
			values_end := array_append(values_end, command_result_end);
			not_empty_periods := array_append(not_empty_periods, p_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(values_start, values_end, not_empty_periods);
	values_start := merge_result.out_obj_start;
	values_end := merge_result.out_obj_end;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_num (
	command text,
	p1_start geometry(POINT)[],
	p1_end geometry(POINT)[],
	p1_periods tsrange[],
	p2_start geometry(POINT)[],
	p2_end geometry(POINT)[],
	p2_periods tsrange[],
	OUT values_start real[],
	OUT values_end real[],
	OUT periods tsrange[]
) AS $$
DECLARE
	new_periods tsrange[];
	new_object_1 record;
	new_object_2 record;
	command_result_start real;
	command_result_end real;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Preprocessing
	new_periods := partitioning(p1_periods, p2_periods);
	new_object_1 := atperiods(p1_start, p1_end, p1_periods, new_periods);
	new_object_2 := atperiods(p2_start, p2_end, p2_periods, new_periods);

	--Main
	FOR i IN 1..array_length(new_periods, 1) LOOP
		IF ((NOT ST_IsEmpty(new_object_1.out_obj_start[i])) AND (NOT ST_IsEmpty(new_object_1.out_obj_end[i])) AND
			(NOT ST_IsEmpty(new_object_2.out_obj_start[i])) AND (NOT ST_IsEmpty(new_object_2.out_obj_end[i]))) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_start
			USING new_object_1.out_obj_start[i], new_object_2.out_obj_start[i];
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_end
			USING new_object_1.out_obj_end[i], new_object_2.out_obj_end[i];
			values_start := array_append(values_start, command_result_start);
			values_end := array_append(values_end, command_result_end);
			not_empty_periods := array_append(not_empty_periods, new_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(values_start, values_end, not_empty_periods);
	values_start := merge_result.out_obj_start;
	values_end := merge_result.out_obj_end;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_num (
	command text,
	region geometry(POLYGON)[],
	r_periods tsrange[],
	OUT values_start real[],
	OUT values_end real[],
	OUT periods tsrange[]
) AS $$
DECLARE
	command_result_start real;
	command_result_end real;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..(array_length(r_periods, 1) - 1) LOOP
		IF ((NOT ST_IsEmpty(region[i])) AND (NOT ST_IsEmpty(region[i+1]))) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1)'
			INTO command_result_start
			USING region[i];
			EXECUTE 'SELECT '
			|| command
			|| '($1)'
			INTO command_result_end
			USING region[i+1];
			values_start := array_append(values_start, command_result_start);
			values_end := array_append(values_end, command_result_end);
			not_empty_periods := array_append(not_empty_periods, r_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(values_start, values_end, not_empty_periods);
	values_start := merge_result.out_obj_start;
	values_end := merge_result.out_obj_end;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_num (
	command text,
	region geometry(POLYGON)[],
	r_periods tsrange[],
	geom geometry,
	OUT values_start real[],
	OUT values_end real[],
	OUT periods tsrange[]
) AS $$
DECLARE
	command_result_start real;
	command_result_end real;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..(array_length(r_periods, 1) - 1) LOOP
		IF ((NOT ST_IsEmpty(region[i])) AND (NOT ST_IsEmpty(region[i+1]))) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_start
			USING region[i], geom;
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_end
			USING region[i+1], geom;
			values_start := array_append(values_start, command_result_start);
			values_end := array_append(values_end, command_result_end);
			not_empty_periods := array_append(not_empty_periods, r_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(values_start, values_end, not_empty_periods);
	values_start := merge_result.out_obj_start;
	values_end := merge_result.out_obj_end;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_num (
	command text,
	region geometry(POLYGON)[],
	r_periods tsrange[],
	geom geometry[],
	OUT values_start real[],
	OUT values_end real[],
	OUT periods tsrange[]
) AS $$
DECLARE
	command_result_start real;
	command_result_end real;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..(array_length(r_periods, 1) - 1) LOOP
		IF ((NOT ST_IsEmpty(region[i])) AND (NOT ST_IsEmpty(region[i+1]))) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_start
			USING region[i], geom[1];
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_end
			USING region[i+1], geom[1];
			values_start := array_append(values_start, command_result_start);
			values_end := array_append(values_end, command_result_end);
			not_empty_periods := array_append(not_empty_periods, r_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(values_start, values_end, not_empty_periods);
	values_start := merge_result.out_obj_start;
	values_end := merge_result.out_obj_end;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_num (
	command text,
	geom geometry,
	region geometry(POLYGON)[],
	r_periods tsrange[],
	OUT values_start real[],
	OUT values_end real[],
	OUT periods tsrange[]
) AS $$
DECLARE
	command_result_start real;
	command_result_end real;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Main
	FOR i IN 1..(array_length(r_periods, 1) - 1) LOOP
		IF ((NOT ST_IsEmpty(region[i])) AND (NOT ST_IsEmpty(region[i+1]))) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_start
			USING geom, region[i];
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_end
			USING geom, region[i+1];
			values_start := array_append(values_start, command_result_start);
			values_end := array_append(values_end, command_result_end);
			not_empty_periods := array_append(not_empty_periods, r_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(values_start, values_end, not_empty_periods);
	values_start := merge_result.out_obj_start;
	values_end := merge_result.out_obj_end;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_num (
	command text,
	r1 geometry(POLYGON)[],
	r1_periods tsrange[],
	r2 geometry(POLYGON)[],
	r2_periods tsrange[],
	OUT values_start real[],
	OUT values_end real[],
	OUT periods tsrange[]
) AS $$
DECLARE
	new_periods tsrange[];
	new_object_1 record;
	new_object_2 record;
	command_result_start real;
	command_result_end real;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Preprocessing
	new_periods := partitioning(r1_periods, r2_periods);
	new_object_1 := atperiods(r1, r1_periods, new_periods);
	new_object_2 := atperiods(r2, r2_periods, new_periods);

	--Main
	FOR i IN 1..(array_length(new_periods, 1) - 1) LOOP
		IF ((NOT ST_IsEmpty(new_object_1.out_obj[i])) AND (NOT ST_IsEmpty(new_object_1.out_obj[i+1])) AND
			(NOT ST_IsEmpty(new_object_2.out_obj[i])) AND (NOT ST_IsEmpty(new_object_2.out_obj[i+1]))) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_start
			USING new_object_1.out_obj[i], new_object_2.out_obj[i];
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_end
			USING new_object_1.out_obj[i+1], new_object_2.out_obj[i+1];
			values_start := array_append(values_start, command_result_start);
			values_end := array_append(values_end, command_result_end);
			not_empty_periods := array_append(not_empty_periods, new_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(values_start, values_end, not_empty_periods);
	values_start := merge_result.out_obj_start;
	values_end := merge_result.out_obj_end;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_num (
	command text,
	p_start geometry(POINT)[],
	p_end geometry(POINT)[],
	p_periods tsrange[],
	region geometry(POLYGON)[],
	r_periods tsrange[],
	OUT values_start real[],
	OUT values_end real[],
	OUT periods tsrange[]
) AS $$
DECLARE
	new_periods tsrange[];
	new_object_1 record;
	new_object_2 record;
	command_result_start real;
	command_result_end real;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Preprocessing
	new_periods := partitioning(p_periods, r_periods);
	new_object_1 := atperiods(p_start, p_end, p_periods, new_periods);
	new_object_2 := atperiods(region, r_periods, new_periods);

	--Main
	FOR i IN 1..array_length(new_periods, 1) LOOP
		IF ((NOT ST_IsEmpty(new_object_1.out_obj_start[i])) AND (NOT ST_IsEmpty(new_object_1.out_obj_end[i])) AND
			(NOT ST_IsEmpty(new_object_2.out_obj[i]))) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_start
			USING new_object_1.out_obj_start[i], new_object_2.out_obj[i];
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_end
			USING new_object_1.out_obj_end[i], new_object_2.out_obj[i];
			values_start := array_append(values_start, command_result_start);
			values_end := array_append(values_end, command_result_end);
			not_empty_periods := array_append(not_empty_periods, new_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(values_start, values_end, not_empty_periods);
	values_start := merge_result.out_obj_start;
	values_end := merge_result.out_obj_end;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_num (
	command text,
	region geometry(POLYGON)[],
	r_periods tsrange[],
	p_start geometry(POINT)[],
	p_end geometry(POINT)[],
	p_periods tsrange[],
	OUT values_start real[],
	OUT values_end real[],
	OUT periods tsrange[]
) AS $$
DECLARE
	new_periods tsrange[];
	new_object_1 record;
	new_object_2 record;
	command_result_start real;
	command_result_end real;
	not_empty_periods tsrange[];
	merge_result record;
BEGIN
	--Preprocessing
	new_periods := partitioning(p_periods, not_empty_periods);
	new_object_1 := atperiods(p_start, p_end, p_periods, new_periods);
	new_object_2 := atperiods(region, r_periods, new_periods);

	--Main
	FOR i IN 1..array_length(new_periods, 1) LOOP
		IF ((NOT ST_IsEmpty(new_object_1.out_obj_start[i])) AND (NOT ST_IsEmpty(new_object_1.out_obj_end[i])) AND
			(NOT ST_IsEmpty(new_object_2.out_obj[i]))) THEN
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_start
			USING new_object_2.out_obj[i], new_object_1.out_obj_start[i];
			EXECUTE 'SELECT '
			|| command
			|| '($1, $2)'
			INTO command_result_end
			USING new_object_2.out_obj[i], new_object_1.out_obj_end[i];
			values_start := array_append(values_start, command_result_start);
			values_end := array_append(values_end, command_result_end);
			not_empty_periods := array_append(not_empty_periods, new_periods[i]);
		END IF;
	END LOOP;

	--Postprocessing
	merge_result := merging(values_start, values_end, not_empty_periods);
	values_start := merge_result.out_obj_start;
	values_end := merge_result.out_obj_end;
	periods := merge_result.out_periods;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_bool_values (
	r record,
	OUT val boolean[]
) AS $$
BEGIN
	val := r.bool_values;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_num_start (
	r record,
	OUT val real[]
) AS $$
BEGIN
	val := r.values_start;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_num_end (
	r record,
	OUT val real[]
) AS $$
BEGIN
	val := r.values_end;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION lifted_periods (
	r record,
	OUT val tsrange[]
) AS $$
BEGIN
	val := r.periods;
END;
$$ LANGUAGE plpgsql STRICT;

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
	WHILE (i <= char_length(op)) LOOP
		num_start := i;
		num_length := 0;
		WHILE ((substring(op, i, 1) != 's') AND (substring(op, i, 1) != 'm') AND (substring(op, i, 1) != 'h') AND (substring(op, i, 1) != 'd') AND (substring(op, i, 1) != 'M') AND (substring(op, i, 1) != 'y') AND (substring(op, i, 1) != '.')) LOOP
			num_length := num_length + 1;
			i := i + 1;
		END LOOP;
		ops := array_append(ops, substring(op, i, 1));
		IF (substring(op, i, 1) = '.') THEN
			nums := array_append(nums, 0);
		ELSE
			nums := array_append(nums, substring(op, num_start, num_length)::integer); 
		END IF;
		i := i + 1;
	END LOOP;
	WHILE (step <= 7) LOOP
		IF (step = 1) THEN
			i := 1;
			op_found := FALSE;
			WHILE ((NOT (op_found)) AND (i <= array_length(ops, 1))) LOOP
				IF (ops[i] = 's') THEN
					op_found := TRUE;
					ops_sorted := array_append(ops_sorted, ops[i]);
					nums_sorted := array_append(nums_sorted, nums[i]);
				END IF;
				i := i + 1;
			END LOOP;
			step := step + 1;
		ELSIF (step = 2) THEN
			i := 1;
			op_found := FALSE;
			WHILE ((NOT (op_found)) AND (i <= array_length(ops, 1))) LOOP
				IF (ops[i] = 'm') THEN
					op_found := TRUE;
					ops_sorted := array_append(ops_sorted, ops[i]);
					nums_sorted := array_append(nums_sorted, nums[i]);
				END IF;
				i := i + 1;
			END LOOP;
			step := step + 1;
		ELSIF (step = 3) THEN
			i := 1;
			op_found := FALSE;
			WHILE ((NOT (op_found)) AND (i <= array_length(ops, 1))) LOOP
				IF (ops[i] = 'h') THEN
					op_found := TRUE;
					ops_sorted := array_append(ops_sorted, ops[i]);
					nums_sorted := array_append(nums_sorted, nums[i]);
				END IF;
				i := i + 1;
			END LOOP;
			step := step + 1;
		ELSIF (step = 4) THEN
			i := 1;
			op_found := FALSE;
			WHILE ((NOT (op_found)) AND (i <= array_length(ops, 1))) LOOP
				IF (ops[i] = 'd') THEN
					op_found := TRUE;
					ops_sorted := array_append(ops_sorted, ops[i]);
					nums_sorted := array_append(nums_sorted, nums[i]);
				END IF;
				i := i + 1;
			END LOOP;
			step := step + 1;
		ELSIF (step = 5) THEN
			i := 1;
			op_found := FALSE;
			WHILE ((NOT (op_found)) AND (i <= array_length(ops, 1))) LOOP
				IF (ops[i] = 'M') THEN
					op_found := TRUE;
					ops_sorted := array_append(ops_sorted, ops[i]);
					nums_sorted := array_append(nums_sorted, nums[i]);
				END IF;
				i := i + 1;
			END LOOP;
			step := step + 1;
		ELSIF (step = 6) THEN
			i := 1;
			op_found := FALSE;
			WHILE ((NOT (op_found)) AND (i <= array_length(ops, 1))) LOOP
				IF (ops[i] = 'y') THEN
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
			WHILE ((NOT (op_found)) AND (i <= array_length(ops, 1))) LOOP
				IF (ops[i] = '.') THEN
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

CREATE OR REPLACE FUNCTION convert_day_time_int (
	ts_1 timestamp,
	ts_2 timestamp,
	OUT diff integer
) AS $$
BEGIN
	diff := ((EXTRACT(DAY FROM ts_2) - EXTRACT(DAY FROM ts_1)) * 86400 +
			 (EXTRACT(HOUR FROM ts_2) - EXTRACT(HOUR FROM ts_1)) * 3600 +
			 (EXTRACT(MINUTE FROM ts_2) - EXTRACT(MINUTE FROM ts_1)) * 60 +
			 (EXTRACT(SECOND FROM ts_2) - EXTRACT(SECOND FROM ts_1)));
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION checkoperation (
	ts_1 timestamp,
	ts_2 timestamp,
	op text,
	OUT o boolean
) AS $$
DECLARE
	r record;
BEGIN
	r := split_sort_op(op);
	IF (op LIKE '%.%') THEN
		IF (array_length(r.ops_sorted, 1) = 1) THEN
			--.
				o := (ts_1 = ts_2);
		ELSIF (array_length(r.ops_sorted, 1) = 2) THEN
			--s, .
			IF (op LIKE '%s%') THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = r.nums_sorted[1]);
			--m, .
			ELSIF (op LIKE '%m%') THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] * 60));
			--h, .
			ELSIF (op LIKE '%h%') THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] * 3600));
			--d, .
			ELSIF (op LIKE '%d%') THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] * 86400));
			--M, .
			ELSIF (op LIKE '%M%') THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[1]) AND
						  (convert_day_time_int(ts_1, ts_2) = 0));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[1]) AND
						  (convert_day_time_int(ts_1, ts_2) = 0));
				ELSE
					o := FALSE;
				END IF;
			--y, .
			ELSE
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] * 31536000));
			END IF;
		ELSIF (array_length(r.ops_sorted, 1) = 3) THEN
			--s, m, .
			IF ((op LIKE '%s%') AND (op LIKE '%m%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] + r.nums_sorted[2] * 60));
			--s, h, .
			ELSIF ((op LIKE '%s%') AND (op LIKE '%h%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] + r.nums_sorted[2] * 3600));
			--s, d, .
			ELSIF ((op LIKE '%s%') AND (op LIKE '%d%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] + r.nums_sorted[2] * 86400));
			--s, M, .
			ELSIF ((op LIKE '%s%') AND (op LIKE '%M%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[2]) AND
						  (convert_day_time_int(ts_1, ts_2) = r.nums_sorted[1]));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[2]) AND
						  (convert_day_time_int(ts_1, ts_2) = r.nums_sorted[1]));
				ELSE
					o := FALSE;
				END IF;
			--s, y, .
			ELSIF ((op LIKE '%s%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] + r.nums_sorted[2] * 31536000));
			--m, h, .
			ELSIF ((op LIKE '%m%') AND (op LIKE '%h%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600));
			--m, d, .
			ELSIF ((op LIKE '%m%') AND (op LIKE '%d%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 86400));
			--m, M, .
			ELSIF ((op LIKE '%m%') AND (op LIKE '%M%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[2]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 60)));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[2]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 60)));
				ELSE
					o := FALSE;
				END IF;
			--m, y, .
			ELSIF ((op LIKE '%m%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600));
			--h, d, .
			ELSIF ((op LIKE '%h%') AND (op LIKE '%d%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 86400));
			--h, M, .
			ELSIF ((op LIKE '%h%') AND (op LIKE '%M%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[2]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 3600)));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[2]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 3600)));
				ELSE
					o := FALSE;
				END IF;
			--h, y, .
			ELSIF ((op LIKE '%h%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 31536000));
			--d, M, .
			ELSIF ((op LIKE '%d%') AND (op LIKE '%M%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[2]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 86400)));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[2]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 86400)));
				ELSE
					o := FALSE;
				END IF;
			--d, y, .
			ELSIF ((op LIKE '%d%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] * 86400 + r.nums_sorted[2] * 31536000));
			--M, y, .
			ELSE
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[2]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[1]) AND
						  (convert_day_time_int(ts_1, ts_2) = 0));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[2]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[1]) AND
						  (convert_day_time_int(ts_1, ts_2) = 0));
				ELSE
					o := FALSE;
				END IF;
			END IF;
		ELSIF (array_length(r.ops_sorted, 1) = 4) THEN
			--s, m, h, .
			IF ((op LIKE '%s%') AND (op LIKE '%m%') AND (op LIKE '%h%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600));
			--s, m, d, .
			ELSIF ((op LIKE '%s%') AND (op LIKE '%m%') AND (op LIKE '%d%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 86400));
			--s, m, M, .
			ELSIF ((op LIKE '%s%') AND (op LIKE '%m%') AND (op LIKE '%M%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 60)));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 60)));
				ELSE
					o := FALSE;
				END IF;
			--s, m, y, .
			ELSIF ((op LIKE '%s%') AND (op LIKE '%m%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 31536000));
			--s, h, d, .
			ELSIF ((op LIKE '%s%') AND (op LIKE '%h%') AND (op LIKE '%d%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400));
			--s, h, M, .
			ELSIF ((op LIKE '%s%') AND (op LIKE '%h%') AND (op LIKE '%M%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 3600)));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 3600)));
				ELSE
					o := FALSE;
				END IF;
			--s, h, y, .
			ELSIF ((op LIKE '%s%') AND (op LIKE '%h%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 31536000));
			--s, d, M, .
			ELSIF ((op LIKE '%s%') AND (op LIKE '%d%') AND (op LIKE '%M%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 86400)));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 86400)));
				ELSE
					o := FALSE;
				END IF;
			--s, d, y, .
			ELSIF ((op LIKE '%s%') AND (op LIKE '%d%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] + r.nums_sorted[2] * 86400 + r.nums_sorted[3] * 31536000));
			--s, M, y, .
			ELSIF ((op LIKE '%s%') AND (op LIKE '%M%') AND (op LIKE '%y%')) THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[3]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[2]) AND
						  (convert_day_time_int(ts_1, ts_2) = r.nums_sorted[1]));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[2]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[2]) AND
						  (convert_day_time_int(ts_1, ts_2) = r.nums_sorted[1]));
				ELSE
					o := FALSE;
				END IF;
			--m, h, d, .
			ELSIF ((op LIKE '%m%') AND (op LIKE '%h%') AND (op LIKE '%d%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400));
			--m, h, M, .
			ELSIF ((op LIKE '%m%') AND (op LIKE '%h%') AND (op LIKE '%h%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600)));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600)));
				ELSE
					o := FALSE;
				END IF;
			--m, h, y, .
			ELSIF ((op LIKE '%m%') AND (op LIKE '%h%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 31536000));
			--m, d, M, .
			ELSIF ((op LIKE '%m%') AND (op LIKE '%d%') AND (op LIKE '%M%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 86400)));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 86400)));
				ELSE
					o := FALSE;
				END IF;
			--m, d, y, .
			ELSIF ((op LIKE '%m%') AND (op LIKE '%d%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 86400 + r.nums_sorted[3] * 31536000));
			--m, M, y, .
			ELSIF ((op LIKE '%m%') AND (op LIKE '%M%') AND (op LIKE '%y%')) THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[3]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[2]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 60)));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[2]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[2]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 60)));
				ELSE
					o := FALSE;
				END IF;
			--h, d, M, .
			ELSIF ((op LIKE '%h%') AND (op LIKE '%d%') AND (op LIKE '%M%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 86400)));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 86400)));
				ELSE
					o := FALSE;
				END IF;
			--h, d, y, .
			ELSIF ((op LIKE '%h%') AND (op LIKE '%d%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 86400 + r.nums_sorted[3] * 31536000));
			--h, M, y, .
			ELSIF ((op LIKE '%h%') AND (op LIKE '%M%') AND (op LIKE '%y%')) THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[3]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[2]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 3600)));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[2]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[2]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 3600)));
				ELSE
					o := FALSE;
				END IF;
			--d, M, y, .
			ELSE
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[3]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[2]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 86400)));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[2]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[2]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 86400)));
				ELSE
					o := FALSE;
				END IF;
			END IF;
		ELSIF array_length(r.ops_sorted, 1) = 5 THEN
			--s, m, h, d, .
			IF ((op NOT LIKE '%M%') AND (op NOT LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 86400));
			--s, m, h, M, .
			ELSIF ((op NOT LIKE '%d%') AND (op LIKE '%y%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[4]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600)));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[4]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600)));
				ELSE
					o := FALSE;
				END IF;
			--s, m, h, y, .
			ELSIF ((op NOT LIKE '%d%') AND (op LIKE '%M%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 31536000));
			--s, m, d, M, .
			ELSIF ((op NOT LIKE '%h%') AND (op NOT LIKE '%y%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[4]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 86400)));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[4]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 86400)));
				ELSE
					o := FALSE;
				END IF;
			--s, m, d, y, .
			ELSIF ((op NOT LIKE '%h%') AND (op NOT LIKE '%M%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 86400 + r.nums_sorted[4] * 31536000));
			--s, m, M, y, .
			ELSIF ((op NOT LIKE '%h%') AND (op NOT LIKE '%d%')) THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 60)));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 60)));
				ELSE
					o := FALSE;
				END IF;
			--s, h, d, M, .
			ELSIF ((op NOT LIKE '%m%') AND (op NOT LIKE '%y%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[4]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400)));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[4]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400)));
				ELSE
					o := FALSE;
				END IF;
			--s, h, d, y, .
			ELSIF ((op NOT LIKE '%m%') AND (op NOT LIKE '%M%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400 + r.nums_sorted[4] * 31536000));
			--s, h, M, y, .
			ELSIF ((op NOT LIKE '%m%') AND (op NOT LIKE '%d%')) THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 3600)));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 3600)));
				ELSE
					o := FALSE;
				END IF;
			--s, d, M, y, .
			ELSIF ((op NOT LIKE '%m%') AND (op NOT LIKE '%h%')) THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 86400)));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 86400)));
				ELSE
					o := FALSE;
				END IF;
			--m, h, d, M, .
			ELSIF ((op NOT LIKE '%s%') AND (op NOT LIKE '%y%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[4]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400)));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[4]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400)));
				ELSE
					o := FALSE;
				END IF;
			--m, h, d, y, .
			ELSIF ((op NOT LIKE '%s%') AND (op NOT LIKE '%M%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400 + r.nums_sorted[4] * 31536000));
			--m, h, M, y, .
			ELSIF ((op NOT LIKE '%s%') AND (op NOT LIKE '%d%')) THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600)));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600)));
				ELSE
					o := FALSE;
				END IF;
			--m, d, M, y, .
			ELSIF ((op NOT LIKE '%s%') AND (op NOT LIKE '%h%')) THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 86400)));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 86400)));
				ELSE
					o := FALSE;
				END IF;
			--h, d, M, y, .
			ELSE
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 86400)));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 86400)));
				ELSE
					o := FALSE;
				END IF;
			END IF;
		ELSIF array_length(r.ops_sorted, 1) = 6 THEN
			--s, m, h, d, M, .
			IF (op NOT LIKE '%y%') THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[5]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 86400)));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[5]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 86400)));
				ELSE
					o := FALSE;
				END IF;
			--s, m, h, d, y, .
			ELSIF (op NOT LIKE '%M%') THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 86400 + r.nums_sorted[5] * 31536000));
			--s, m, h, M, y, .
			ELSIF (op NOT LIKE '%d%') THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[5]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[4]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600)));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[5]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[4]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600)));
				ELSE
					o := FALSE;
				END IF;
			--s, m, d, M, y, .
			ELSIF (op NOT LIKE '%h%') THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[5]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[4]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 86400)));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[5]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[4]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 86400)));
				ELSE
					o := FALSE;
				END IF;
			--s, h, d, M, y, .
			ELSIF (op NOT LIKE '%m%') THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[5]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[4]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400)));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[5]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[4]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400)));
				ELSE
					o := FALSE;
				END IF;
			--m, h, d, M, y, .
			ELSE
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[5]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[4]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400)));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[5]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[4]) AND
						  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400)));
				ELSE
					o := FALSE;
				END IF;
			END IF;
		ELSE
			--s, m, h, d, M, y, .
			IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[6]) THEN
				o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[5]) AND
					  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 86400)));
			ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[6]) THEN
				o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[5]) AND
					  (convert_day_time_int(ts_1, ts_2) = (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 86400)));
			ELSE
				o := FALSE;
			END IF;
		END IF;
	ELSE
		IF (array_length(r.ops_sorted, 1) = 1) THEN
			--s
			IF (op LIKE '%s%') THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > r.nums_sorted[1]);
			--m
			ELSIF (op LIKE '%m%') THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] * 60));
			--h
			ELSIF (op LIKE '%h%') THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] * 3600));
			--d
			ELSIF (op LIKE '%d%') THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] * 86400));
			--M
			ELSIF (op LIKE '%M%') THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[1]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[1]) AND
						   (convert_day_time_int(ts_1, ts_2) > 0)));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[1]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[1]) AND
						   (convert_day_time_int(ts_1, ts_2) > 0)));
				ELSE
					o := FALSE;
				END IF;
			--y
			ELSE
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] * 31536000));
			END IF;
		ELSIF (array_length(r.ops_sorted, 1) = 2) THEN
			--s, m
			IF ((op LIKE '%s%') AND (op LIKE '%m%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] + r.nums_sorted[2] * 60));
			--s, h
			ELSIF ((op LIKE '%s%') AND (op LIKE '%h%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] + r.nums_sorted[2] * 3600));
			--s, d
			ELSIF ((op LIKE '%s%') AND (op LIKE '%d%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] + r.nums_sorted[2] * 86400));
			--s, M
			ELSIF ((op LIKE '%s%') AND (op LIKE '%M%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[2]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[2]) AND
						   (convert_day_time_int(ts_1, ts_2) > r.nums_sorted[1])));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[2]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[2]) AND
						   (convert_day_time_int(ts_1, ts_2) > r.nums_sorted[1])));
				ELSE
					o := FALSE;
				END IF;
			--s, y
			ELSIF ((op LIKE '%s%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] + r.nums_sorted[2] * 31536000));
			--m, h
			ELSIF ((op LIKE '%m%') AND (op LIKE '%h%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600));
			--m, d
			ELSIF ((op LIKE '%m%') AND (op LIKE '%d%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 86400));
			--m, M
			ELSIF ((op LIKE '%m%') AND (op LIKE '%M%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[2]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[2]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 60))));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[2]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[2]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 60))));
				ELSE
					o := FALSE;
				END IF;
			--m, y
			ELSIF ((op LIKE '%m%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600));
			--h, d
			ELSIF ((op LIKE '%h%') AND (op LIKE '%d%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 86400));
			--h, M
			ELSIF ((op LIKE '%h%') AND (op LIKE '%M%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[2]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[2]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 3600))));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[2]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[2]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 3600))));
				ELSE
					o := FALSE;
				END IF;
			--h, y
			ELSIF ((op LIKE '%h%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 31536000));
			--d, M
			ELSIF ((op LIKE '%d%') AND (op LIKE '%M%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[2]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[2]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 86400))));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[2]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[2]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 86400))));
				ELSE
					o := FALSE;
				END IF;
			--d, y
			ELSIF ((op LIKE '%d%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] * 86400 + r.nums_sorted[2] * 31536000));
			--M, y
			ELSE
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[2]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[1]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[1]) AND
						   (convert_day_time_int(ts_1, ts_2) > 0)));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[2]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[1]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[1]) AND
						   (convert_day_time_int(ts_1, ts_2) > 0)));
				ELSE
					o := FALSE;
				END IF;
			END IF;
		ELSIF array_length(r.ops_sorted, 1) = 3 THEN
			--s, m, h
			IF ((op LIKE '%s%') AND (op LIKE '%m%') AND (op LIKE '%h%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600));
			--s, m, d
			ELSIF ((op LIKE '%s%') AND (op LIKE '%m%') AND (op LIKE '%d%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 86400));
			--s, m, M
			ELSIF ((op LIKE '%s%') AND (op LIKE '%m%') AND (op LIKE '%M%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 60))));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 60))));
				ELSE
					o := FALSE;
				END IF;
			--s, m, y
			ELSIF ((op LIKE '%s%') AND (op LIKE '%m%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 31536000));
			--s, h, d
			ELSIF ((op LIKE '%s%') AND (op LIKE '%h%') AND (op LIKE '%d%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400));
			--s, h, M
			ELSIF ((op LIKE '%s%') AND (op LIKE '%h%') AND (op LIKE '%M%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 3600))));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 3600))));
				ELSE
					o := FALSE;
				END IF;
			--s, h, y
			ELSIF ((op LIKE '%s%') AND (op LIKE '%h%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 31536000));
			--s, d, M
			ELSIF ((op LIKE '%s%') AND (op LIKE '%d%') AND (op LIKE '%M%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 86400))));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 86400))));
				ELSE
					o := FALSE;
				END IF;
			--s, d, y
			ELSIF ((op LIKE '%s%') AND (op LIKE '%d%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] + r.nums_sorted[2] * 86400 + r.nums_sorted[3] * 31536000));
			--s, M, y
			ELSIF ((op LIKE '%s%') AND (op LIKE '%M%') AND (op LIKE '%y%')) THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[3]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[2]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[2]) AND
						   (convert_day_time_int(ts_1, ts_2) > r.nums_sorted[1])));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[3]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[2]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[2]) AND
						   (convert_day_time_int(ts_1, ts_2) > r.nums_sorted[1])));
				ELSE
					o := FALSE;
				END IF;
			--m, h, d
			ELSIF ((op LIKE '%m%') AND (op LIKE '%h%') AND (op LIKE '%d%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400));
			--m, h, M
			ELSIF ((op LIKE '%m%') AND (op LIKE '%h%') AND (op LIKE '%h%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600))));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600))));
				ELSE
					o := FALSE;
				END IF;
			--m, h, y
			ELSIF ((op LIKE '%m%') AND (op LIKE '%h%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 31536000));
			--m, d, M
			ELSIF ((op LIKE '%m%') AND (op LIKE '%d%') AND (op LIKE '%M%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 86400))));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 86400))));
				ELSE
					o := FALSE;
				END IF;
			--m, d, y
			ELSIF ((op LIKE '%m%') AND (op LIKE '%d%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 86400 + r.nums_sorted[3] * 31536000));
			--m, M, y
			ELSIF ((op LIKE '%m%') AND (op LIKE '%M%') AND (op LIKE '%y%')) THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[3]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[2]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[2]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 60))));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[3]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[2]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[2]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 60))));
				ELSE
					o := FALSE;
				END IF;
			--h, d, M
			ELSIF ((op LIKE '%h%') AND (op LIKE '%d%') AND (op LIKE '%M%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 86400))));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 86400))));
				ELSE
					o := FALSE;
				END IF;
			--h, d, y
			ELSIF ((op LIKE '%h%') AND (op LIKE '%d%') AND (op LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 86400 + r.nums_sorted[3] * 31536000));
			--h, M, y
			ELSIF ((op LIKE '%h%') AND (op LIKE '%M%') AND (op LIKE '%y%')) THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[3]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[2]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[2]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 3600))));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[3]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[2]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[2]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 3600))));
				ELSE
					o := FALSE;
				END IF;
			--d, M, y
			ELSE
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[3]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[2]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[2]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 86400))));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[3]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[2]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[2]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 86400))));
				ELSE
					o := FALSE;
				END IF;
			END IF;
		ELSIF (array_length(r.ops_sorted, 1) = 4) THEN
			--s, m, h, d
			IF ((op NOT LIKE '%M%') AND (op NOT LIKE '%y%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 86400));
			--s, m, h, M
			ELSIF ((op NOT LIKE '%d%') AND (op LIKE '%y%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[4]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[4]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600))));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[4]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[4]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600))));
				ELSE
					o := FALSE;
				END IF;
			--s, m, h, y
			ELSIF ((op NOT LIKE '%d%') AND (op LIKE '%M%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 31536000));
			--s, m, d, M
			ELSIF ((op NOT LIKE '%h%') AND (op NOT LIKE '%y%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[4]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[4]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 86400))));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[4]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[4]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 86400))));
				ELSE
					o := FALSE;
				END IF;
			--s, m, d, y
			ELSIF ((op NOT LIKE '%h%') AND (op NOT LIKE '%M%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 86400 + r.nums_sorted[4] * 31536000));
			--s, m, M, y
			ELSIF ((op NOT LIKE '%h%') AND (op NOT LIKE '%d%')) THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 60))));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 60))));
				ELSE
					o := FALSE;
				END IF;
			--s, h, d, M
			ELSIF ((op NOT LIKE '%m%') AND (op NOT LIKE '%y%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[4]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[4]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400))));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[4]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[4]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400))));
				ELSE
					o := FALSE;
				END IF;
			--s, h, d, y
			ELSIF ((op NOT LIKE '%m%') AND (op NOT LIKE '%M%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400 + r.nums_sorted[4] * 31536000));
			--s, h, M, y
			ELSIF ((op NOT LIKE '%m%') AND (op NOT LIKE '%d%')) THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 3600))));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 3600))));
				ELSE
					o := FALSE;
				END IF;
			--s, d, M, y
			ELSIF ((op NOT LIKE '%m%') AND (op NOT LIKE '%h%')) THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 86400))));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 86400))));
				ELSE
					o := FALSE;
				END IF;
			--m, h, d, M
			ELSIF ((op NOT LIKE '%s%') AND (op NOT LIKE '%y%')) THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[4]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[4]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400))));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[4]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[4]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400))));
				ELSE
					o := FALSE;
				END IF;
			--m, h, d, y
			ELSIF ((op NOT LIKE '%s%') AND (op NOT LIKE '%M%')) THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400 + r.nums_sorted[4] * 31536000));
			--m, h, M, y
			ELSIF ((op NOT LIKE '%s%') AND (op NOT LIKE '%d%')) THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600))));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600))));
				ELSE
					o := FALSE;
				END IF;
			--m, d, M, y
			ELSIF ((op NOT LIKE '%s%') AND (op NOT LIKE '%h%')) THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 86400))));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 86400))));
				ELSE
					o := FALSE;
				END IF;
			--h, d, M, y
			ELSE
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 86400))));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[4]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[3]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[3]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 3600 + r.nums_sorted[2] * 86400))));
				ELSE
					o := FALSE;
				END IF;
			END IF;
		ELSIF (array_length(r.ops_sorted, 1) = 5) THEN
			--s, m, h, d, M
			IF (op NOT LIKE '%y%') THEN
				IF (EXTRACT(YEAR FROM ts_1) = EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[5]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[5]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 86400))));
				ELSIF (EXTRACT(YEAR FROM ts_1) < EXTRACT(YEAR FROM ts_2)) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[5]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[5]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 86400))));
				ELSE
					o := FALSE;
				END IF;
			--s, m, h, d, y
			ELSIF (op NOT LIKE '%M%') THEN
				o := (EXTRACT(epoch FROM ts_2 - ts_1) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 86400 + r.nums_sorted[5] * 31536000));
			--s, m, h, M, y
			ELSIF (op NOT LIKE '%d%') THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[5]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[4]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[4]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600))));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[5]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[4]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[4]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600))));
				ELSE
					o := FALSE;
				END IF;
			--s, m, d, M, y
			ELSIF (op NOT LIKE '%h%') THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[5]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[4]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[4]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 86400))));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[5]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[4]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[4]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 86400))));
				ELSE
					o := FALSE;
				END IF;
			--s, h, d, M, y
			ELSIF (op NOT LIKE '%m%') THEN
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[5]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[4]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[4]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400))));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[5]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[4]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[4]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400))));
				ELSE
					o := FALSE;
				END IF;
			--m, h, d, M, y
			ELSE
				IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[5]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[4]) OR 
						  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[4]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400))));
				ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[5]) THEN
					o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[4]) OR 
						  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[4]) AND
						   (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] * 60 + r.nums_sorted[2] * 3600 + r.nums_sorted[3] * 86400))));
				ELSE
					o := FALSE;
				END IF;
			END IF;
		ELSE
			--s, m, h, d, M, y
			IF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) = r.nums_sorted[6]) THEN
				o := ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) > r.nums_sorted[5]) OR 
					  ((EXTRACT(MONTH FROM ts_2) - EXTRACT(MONTH FROM ts_1) = r.nums_sorted[5]) AND
					  (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 86400))));
			ELSIF ((EXTRACT(YEAR FROM ts_2) - EXTRACT(YEAR FROM ts_1)) > r.nums_sorted[6]) THEN
				o := ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) > r.nums_sorted[5]) OR 
					  ((EXTRACT(MONTH FROM ts_2) + (12 - EXTRACT(MONTH FROM ts_1)) = r.nums_sorted[5]) AND
					  (convert_day_time_int(ts_1, ts_2) > (r.nums_sorted[1] + r.nums_sorted[2] * 60 + r.nums_sorted[3] * 3600 + r.nums_sorted[4] * 86400))));
			ELSE
				o := FALSE;
			END IF;
		END IF;
	END IF;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION stconstraint (
	interval_1 tsrange,
	interval_2 tsrange,
	ir varchar,
	OUT o boolean
) AS $$
DECLARE
	i integer := 1;
	rep text[];
	rep_component text[];
	is_first_a boolean := TRUE;
	is_first_b boolean := TRUE;
	op_start integer;
	op_length integer;
	rep_i integer := 1;
BEGIN
	o := TRUE;
	WHILE (i <= char_length(ir)) LOOP
		IF (substring(ir, i, 1) = 'a') THEN
			IF (is_first_a) THEN
				rep := array_append(rep, lower(interval_1)::text);
				rep_component := array_append(rep_component, 'a');
				is_first_a := FALSE;
			ELSE
				rep := array_append(rep, upper(interval_1)::text);
				rep_component := array_append(rep_component, 'a');
			END IF;
		ELSE
			IF (substring(ir, i, 1) = 'b') THEN
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
				WHILE (substring(ir, i, 1) != ')') LOOP
					op_length := op_length + 1;
					i := i + 1;
				END LOOP;
				rep := array_append(rep, substring(ir, op_start, op_length));
				rep_component := array_append(rep_component, 'o');
			END IF;
		END IF;
		i := i + 1;
	END LOOP;
	WHILE (o AND (rep_i < array_length(rep, 1))) LOOP 
		IF (rep_component[rep_i] != 'o') THEN
			IF (rep_component[rep_i+1] != 'o') THEN
				o := (rep[rep_i]::timestamp < rep[rep_i+1]::timestamp);
			ELSE
				IF (((rep_i + 2) <= array_length(rep, 1)) AND (rep_component[rep_i+2] != 'o')) THEN
					o := checkoperation(rep[rep_i]::timestamp, rep[rep_i+2]::timestamp, rep[rep_i+1]);
				END IF;
			END IF;
		END IF;
		rep_i := rep_i + 1;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;

CREATE OR REPLACE FUNCTION pattern (
	pred1 record,
	pred2 record,
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
	true_intervals_1 := filtering(pred1.bool_values, pred1.periods);
  	true_intervals_2 := filtering(pred2.bool_values, pred2.periods);

	--Main
	o := FALSE;
	WHILE ((NOT o) AND (ir_i <= array_length(ir, 1))) LOOP
		i_1 := 1;
		WHILE ((NOT o) AND (i_1 <= array_length(true_intervals_1, 1))) LOOP
			i_2 := 1;
			WHILE ((NOT o) AND (i_2 <= array_length(true_intervals_2, 1))) LOOP
				o := stconstraint(true_intervals_1[i_1], true_intervals_2[i_2], ir[ir_i]);
				i_2 := i_2 + 1;
			END LOOP;
			i_1 := i_1 + 1;
		END LOOP;
		ir_i := ir_i + 1;
	END LOOP;
END;
$$ LANGUAGE plpgsql STRICT;