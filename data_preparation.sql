-- data_preparation.sql
--
-- This file is an SQL script that has to be executed on the
-- aiace.db database for the preparation of the datasets for the
-- analyses of the AIACE project.

-- Create conversion table between Facebook IDs and province IDs.
DROP TABLE IF EXISTS fbid2id;
CREATE TABLE fbid2id AS
SELECT unnest(string_to_array(fb_province_id, '|')) fb_province_id, province_id
FROM province_lookup ;

-- Create conversion table between Facebook Names and Province IDs.
DROP TABLE IF EXISTS fbname2id;
CREATE TABLE fbname2id AS
SELECT unnest(string_to_array(fb_province_name, '|')) fb_province_name, province_id
FROM province_lookup ;

-- Create conversion table between ISS Province names and Province IDs.
DROP TABLE IF EXISTS issname2id;
CREATE TABLE issname2id AS
SELECT unnest(string_to_array(iss_province_name, '|')) iss_province_name, province_id
FROM province_lookup ;

-- Create movement between provinces table.
DROP TABLE IF EXISTS movement_prov;
CREATE TABLE movement_prov AS
SELECT date_time,
    start_id,
    end_id,
    SUM(n_crisis)::integer n_crisis,
    SUM(n_baseline)::integer n_baseline
FROM (
    SELECT date_time,
        pl1.province_id start_id,
        pl2.province_id end_id,
        n_crisis,
        n_baseline
    FROM movement_adm ma
    JOIN fbid2id pl1 on pl1.fb_province_id = ma.start_polygon_id::text 
    JOIN fbid2id pl2 on pl2.fb_province_id = ma.end_polygon_id::text 
) xx
GROUP BY date_time, start_id, end_id ;

-- Create Facebook population of provinces table.
DROP TABLE IF EXISTS popluation_prov;
CREATE TABLE population_prov AS
SELECT date_time,
    province_id,
    sum(n_baseline)::integer n_baseline,
    sum(n_crisis)::integer n_crisis,
    sum(density_baseline)::integer density_baseline,
    sum(density_crisis)::integer density_crisis
FROM (
    SELECT date_time,
        p1.province_id,
        n_baseline,
        n_crisis,
        density_baseline,
        density_crisis
    FROM population_adm pa
    JOIN fbname2id p1 ON p1.fb_province_name = pa.polygon_name
) xx
GROUP BY date_time, province_id ;

-- Create COVID19 cases table.
DROP TABLE IF EXISTS covid_prov;
CREATE TABLE covid_prov AS
SELECT province_id, date_time, SUM(cases)::integer cases
FROM (
    SELECT p1.province_id,
        date_time,
        cases
    FROM iss_positivi ip
    JOIN issname2id p1 ON p1.iss_province_name = ip.province
) xx
GROUP BY province_id, date_time ;

-- Create view for total of people moving from one place.
DROP VIEW IF EXISTS movement_prov_totalfrom;
CREATE VIEW movement_prov_totalfrom AS
SELECT date_time,
    start_id,
    SUM(n_crisis)::integer n_crisis,
    SUM(n_baseline)::integer n_baseline
FROM movement_prov
GROUP BY date_time, start_id ;

-- Create view for people moving but every 24 hours.
DROP VIEW IF EXISTS movement_prov_totalday;
CREATE VIEW movement_prov_totalday AS
SELECT datetrunc('day', date_time) date,
    start_id,
    end_id,
    SUM(n_crisis)::integer n_crisis,
    SUM(n_baseline)::integer n_baseline
FROM movement_prov
GROUP BY 1, 2, 3 ;

-- Create view for daily probability
DROP VIEW IF EXISTS movement_prov_totalday_totalfrom;
CREATE VIEW movement_prov_totalday_totalfrom AS
SELECT date,
    start_id,
    SUM(n_crisis)::integer n_crisis,
    SUM(n_baseline)::integer n_baseline
FROM movement_prov_totalday
GROUP BY 1, 2 ;

-- Compute view with daily probabilities of movement.
DROP VIEW IF EXISTS movement_daily_probabilities;
CREATE VIEW movement_daily_probabilities AS
SELECT date,
    start_id,
    end_id,
    n_crisis*1. / SUM("n_crisis") OVER (PARTITION BY date, start_id) AS "n_crisis_prob",
    n_baseline*1. / SUM("n_baseline") OVER (PARTITION BY date, start_id) AS "n_baseline_prob"
FROM movement_prov_totalday ;

-- Compute probabilities along all times to move from point A to point B.
DROP VIEW IF EXISTS movement_total_probabilities;
CREATE VIEW movement_total_probabilities AS
SELECT start_id,
    end_id,
    n_crisis*1. / SUM("n_crisis") OVER (PARTITION BY start_id) AS "n_crisis_prob",
    n_baseline*1. / SUM("n_baseline") OVER (PARTITION BY start_id) AS "n_baseline_prob"
FROM (
    SELECT start_id,
        end_id,
        SUM(n_crisis) n_crisis,
        SUM(n_baseline) n_baseline
    FROM movement_prov
    GROUP BY 1, 2
) xx ;

-- Compute week long rolling average transfer matrix.
DROP VIEW IF EXISTS movement_weekra_probabilities;
CREATE VIEW movement_weekra_probabilities AS
SELECT date,
    start_id,
    end_id,
    n_crisis*1. / SUM("n_crisis") OVER (PARTITION BY "start_id", "end_id"
                         ORDER BY "date" ASC
                         RANGE BETWEEN INTERVAL 3 DAYS PRECEDING
                         AND INTERVAL 3 DAYS FOLLOWING) AS "n_crisis_7ddprob",
    n_baseline*1. / SUM("n_baseline") OVER (PARTITION BY "start_id", "end_id"
                         ORDER BY "date" ASC
                         RANGE BETWEEN INTERVAL 3 DAYS PRECEDING
                         AND INTERVAL 3 DAYS FOLLOWING) AS "n_baseline_7ddprob"
FROM movement_prov_totalday ;
