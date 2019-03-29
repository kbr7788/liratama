
CREATE OR REPLACE FUNCTION lrtm_count_distinct_append(internal, anyelement)
    RETURNS internal
    AS 'lrtm_count_distinct', 'lrtm_count_distinct_append'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION lrtm_count_distinct_elements_append(internal, anyarray)
    RETURNS internal
    AS 'lrtm_count_distinct', 'lrtm_count_distinct_elements_append'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION lrtm_count_distinct(internal)
    RETURNS bigint
    AS 'lrtm_count_distinct', 'lrtm_count_distinct'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION lrtm_array_agg_distinct(internal, anynonarray)
    RETURNS anyarray
    AS 'lrtm_count_distinct', 'array_agg_distinct_type_by_element'
    LANGUAGE C IMMUTABLE;

CREATE OR REPLACE FUNCTION lrtm_array_agg_distinct(internal, anyarray)
    RETURNS anyarray
    AS 'lrtm_count_distinct', 'lrtm_array_agg_distinct_type_by_array'
    LANGUAGE C IMMUTABLE;

/* Server supports parallel aggregation (9.6+) */

/* serialize data */
CREATE OR REPLACE FUNCTION lrtm_count_distinct_serial(p_pointer internal)
    RETURNS bytea
    AS 'lrtm_count_distinct', 'lrtm_count_distinct_serial'
    LANGUAGE C IMMUTABLE STRICT;

/* deserialize data */
CREATE OR REPLACE FUNCTION lrtm_count_distinct_deserial(p_value bytea, p_dummy internal)
    RETURNS internal
    AS 'lrtm_count_distinct', 'lrtm_count_distinct_deserial'
    LANGUAGE C IMMUTABLE STRICT;

/* combine data */
CREATE OR REPLACE FUNCTION lrtm_count_distinct_combine(p_state_1 internal, p_state_2 internal)
    RETURNS internal
    AS 'lrtm_count_distinct', 'lrtm_count_distinct_combine'
    LANGUAGE C IMMUTABLE;

/* Create the aggregate functions */
CREATE AGGREGATE lrtm_count_distinct(anyelement) (
       SFUNC = lrtm_count_distinct_append,
       STYPE = internal,
       FINALFUNC = lrtm_count_distinct,
       COMBINEFUNC = lrtm_count_distinct_combine,
       SERIALFUNC = lrtm_count_distinct_serial,
       DESERIALFUNC = lrtm_count_distinct_deserial,
       PARALLEL = SAFE
);

CREATE AGGREGATE lrtm_array_agg_distinct(anynonarray) (
       SFUNC = lrtm_count_distinct_append,
       STYPE = internal,
       FINALFUNC = lrtm_array_agg_distinct,
       FINALFUNC_EXTRA,
       COMBINEFUNC = lrtm_count_distinct_combine,
       SERIALFUNC = lrtm_count_distinct_serial,
       DESERIALFUNC = lrtm_count_distinct_deserial,
       PARALLEL = SAFE
);

CREATE AGGREGATE lrtm_count_distinct_elements(anyarray) (
       SFUNC = lrtm_count_distinct_elements_append,
       STYPE = internal,
       FINALFUNC = lrtm_count_distinct,
       COMBINEFUNC = lrtm_count_distinct_combine,
       SERIALFUNC = lrtm_count_distinct_serial,
       DESERIALFUNC = lrtm_count_distinct_deserial,
       PARALLEL = SAFE
);

CREATE AGGREGATE lrtm_array_agg_distinct_elements(anyarray) (
       SFUNC = lrtm_count_distinct_elements_append,
       STYPE = internal,
       FINALFUNC = lrtm_array_agg_distinct,
       FINALFUNC_EXTRA,
       COMBINEFUNC = lrtm_count_distinct_combine,
       SERIALFUNC = lrtm_count_distinct_serial,
       DESERIALFUNC = lrtm_count_distinct_deserial,
       PARALLEL = SAFE
);
x`
