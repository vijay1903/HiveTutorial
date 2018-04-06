CREATE TABLE temporary_driver (col_1 STRING);

LOAD DATA INPATH '/user/hive/DriverData/drivers.csv' OVERWRITE INTO TABLE temporary_driver;

SELECT * FROM temporary_driver;







CREATE TABLE drivers (
    driverID INT,
    name STRING,
    ssn BIGINT,
    loc STRING,
    certified STRING,
    wage_plan STRING
    );
    
insert overwrite table drivers
SELECT
  regexp_extract(col_1, '^(?:([^,]*),?){1}', 1) driverId,
  regexp_extract(col_1, '^(?:([^,]*),?){2}', 1) name,
  regexp_extract(col_1, '^(?:([^,]*),?){3}', 1) ssn,
  regexp_extract(col_1, '^(?:([^,]*),?){4}', 1) loc,
  regexp_extract(col_1, '^(?:([^,]*),?){5}', 1) certified,
  regexp_extract(col_1, '^(?:([^,]*),?){6}', 1) wage_plan

from temporary_driver;

select * from drivers;





CREATE TABLE temporary_timesheet (col_1 string);


LOAD DATA INPATH '/user/hive/DriverData/timesheet.csv' OVERWRITE INTO TABLE temporary_timesheet;


select * from temporary_timesheet;





CREATE TABLE timesheet (driverId INT, week INT, hours_logged INT , miles_logged INT);


insert overwrite table timesheet
SELECT
  regexp_extract(col_1, '^(?:([^,]*),?){1}', 1) driverId,
  regexp_extract(col_1, '^(?:([^,]*),?){2}', 1) week,
  regexp_extract(col_1, '^(?:([^,]*),?){3}', 1) hours_logged,
  regexp_extract(col_1, '^(?:([^,]*),?){4}', 1) miles_logged
from temporary_timesheet;


select * from timesheet;








SELECT driverId, sum(hours_logged), sum(miles_logged) FROM timesheet GROUP BY driverId;


--If you get an error saying "Error while processing statement: FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.mr.MapredLocalTask"
--then use this line
--set hive.auto.convert.join.noconditionaltask=false;

SELECT d.driverId, d.name, t.total_hours, t.total_miles from drivers d
JOIN (SELECT driverId, sum(hours_logged)total_hours, sum(miles_logged)total_miles FROM timesheet GROUP BY driverId ) t
ON (d.driverId = t.driverId);



drop table drivers;
drop table timesheet;
drop table temporary_driver;
drop table temporary_timesheet;
