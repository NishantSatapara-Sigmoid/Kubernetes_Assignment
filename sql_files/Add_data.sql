
DROP table If EXISTS docker_assignment_table;
create table docker_assignment_table as select dag_id, execution_date from dag_run WHERE dag_id = 'Docker_assignment' order by execution_date;