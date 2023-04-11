CREATE TABLE jobs
(
id int PRIMARY KEY,
job varchar(255)
)


CREATE TABLE departments
(
id int PRIMARY KEY,
department varchar(255) 
)


CREATE TABLE hired_employees
(
id int PRIMARY KEY,
name varchar(255),
datetime varchar(50),
department_id int references departments(id),
job_id int references jobs(id)
)



drop table departments
drop table jobs
drop table hired_employees

INSERT INTO jobs values(0,'NO PROPORCIONADO')
INSERT INTO departments values(0,'NO PROPORCIONADO')




select count(*) from jobs
select * from jobs
select count(*) from departments
select * from departments
select count(*) from hired_employees
select * from hired_employees


delete hired_employees
delete departments
delete jobs



exec sp_columns jobs --ver descripcion de columnas

EXEC sp_helptext 'dbo.jobs';   --ver ddl