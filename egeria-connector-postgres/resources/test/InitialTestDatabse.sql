-- SPDX-License-Identifier: Apache-2.0 */
-- Copyright Contributors to the ODPi Egeria project. */
--
--Test Database script used for testing the egeria postgres database connector
--Provides the initial state for the test automation
--
--creates the following entities
--Two Tables with six cols each
--Two primary keys
--A view with two cols
--A trigger function
--

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;


-- CREATE DATABASE EgeriaConnectorTestOneDB OWNER postgres TABLESPACE egeriaTableSpace;--

-- \connect EgeriaConnectorTestOneDB;

DROP PROCEDURE IF EXISTS proc1;
DROP FUNCTION IF EXISTS trigger1;
DROP VIEW IF EXISTS view1;
DROP TABLE IF EXISTS table2;
DROP TABLE IF EXISTS table1;
DROP SCHEMA IF EXISTS schema1 CASCADE;
DROP DATABASE IF EXISTS  egeriadb;

CREATE DATABASE egeriadb OWNER postgres;

CREATE SCHEMA schema1;

grant usage on schema schema1 to postgres;
grant create on schema schema1 to postgres;

CREATE TABLE schema1.table2(
    tb2_pk      INT GENERATED ALWAYS AS IDENTITY,
    tb2_col_one character varying(45) NOT NULL,
    tb2_col_two character varying(45) NOT NULL,
    tb2_col_three int NOT NULL,
    PRIMARY KEY(tb2_pk),
    create_date date DEFAULT ('now'::text)::date NOT NULL,
    last_update timestamp without time zone DEFAULT now()
);

CREATE TABLE schema1.table1 (
    tb1_pk SERIAL PRIMARY KEY,
    tb2_pk int,
    tb1_col_one character varying(45) NOT NULL,
    tb1_col_two character varying(45) NOT NULL,
    tb1_col_three int NOT NULL,
    create_date date DEFAULT ('now'::text)::date NOT NULL,
    last_update timestamp without time zone DEFAULT now(),

    CONSTRAINT fk1
      FOREIGN KEY(tb2_pk)
	  REFERENCES schema1.table2(tb2_pk)
);



CREATE VIEW schema1.view1  AS
  SELECT t1.tb1_pk, t2.tb2_pk
  FROM schema1.table1 t1, schema1.table2 t2;


CREATE OR REPLACE FUNCTION schema1.trigger1()
  RETURNS TRIGGER
  LANGUAGE PLPGSQL
  AS
$$
BEGIN
	IF NEW.tb1_col_one <> OLD.tb1_col_one THEN
		 INSERT INTO schema1.table2 (tb2_col_one,last_name,tb2_col_two)
		 VALUES(OLD.tb1_col_one,OLD.tb1_col_two);
	END IF;

	RETURN NEW;
END;
$$
;

CREATE OR REPLACE PROCEDURE schema1.proc1(
   param1 INT,
   param2 INT
)
language plpgsql
as $$
begin
end;
$$
