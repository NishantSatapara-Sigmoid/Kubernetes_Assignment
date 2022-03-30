DROP TABLE IF EXISTS log_table;
CREATE TABLE log_table AS SELECT * FROM dag_run;
