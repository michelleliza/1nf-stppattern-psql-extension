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
