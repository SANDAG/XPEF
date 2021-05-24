%let xpef=xpef20; /* version of xpef */
%let estver=2018_03; /* versino of estimates */
%let by1=2018; /* base year */
%let usver=438; /* urbansim output version*/

libname e "T:\socioec\Current_Projects\estimates\&estver\input_data";

/*
proc sql;
create table du_1 as select int((lckey-10000000000)/100) as parcel_id,sum(du) as du
from e.lmb_2 where yr=2017
group by parcel_id;

create table du_1a as select sum(du) as du
from du_1;
quit;
*/



proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table j_0 as select *
from connection to odbc
(
SELECT parcel_id,2016 as yr,count(job_id) as jobs
FROM [isam].[&xpef].[dw_jobs_base]
group by parcel_id
)
order by parcel_id
;

create table j_1 as select *
from connection to odbc
(
SELECT parcel_id,yr,count(*) as jobs
FROM [isam].[&xpef].[dw_jobs_4]
group by parcel_id,yr
)
order by parcel_id,yr
;

create table j_1a as select yr,sum(jobs) as jobs
from j_1 group by yr;

create table du_1 as select *
from connection to odbc
(
SELECT parcel_id,sum(du_&by1) as du
FROM [urbansim].[urbansim].[parcel] where du_&by1 > 0
group by parcel_id
)
order by parcel_id
;

create table du_2 as select year_simulation + 1 as yr,parcel_id,du
from connection to odbc
(
SELECT year_simulation,parcel_id,sum(unit_change) as du
FROM [urbansim].[urbansim].[urbansim_lite_output] where run_id=&usver
group by year_simulation,parcel_id
)
order by yr,parcel_id
;

disconnect from odbc;

create table du_2a as select yr,sum(du) as du
from du_2 group by yr;
quit;

data j_0_(drop=yr rename=(yrr=yr));set j_0;
do yrr=yr to 2050;
	output;
end;
run;

data j_1_(drop=yr rename=(yrr=yr));set j_1;
do yrr=yr to 2050;
	output;
end;
run;

data j_2;set j_0_ j_1_;run;


proc sql;
create table j_3 as select parcel_id,yr,sum(jobs) as jobs
from j_2 group by parcel_id,yr;

create table j_3a as select yr,sum(jobs) as jobs
from j_3 group by yr;
quit;


data du_1_;set du_1;
do yr=&by1 to 2051;
	output;
end;
run;

data du_2_(drop=yr rename=(yrr=yr));set du_2;
do yrr=yr to 2051;
	output;
end;
run;

data du_3; set du_1_ du_2_;run;


proc sql;
create table du_4 as select parcel_id,yr,sum(du) as du
from du_3 group by parcel_id,yr;

create table du_4a as select yr,sum(du) as du
from du_4 group by yr;
quit;


/* this is for December 31 of each year */
proc sql;
create table du_5 as select parcel_id,yr-1 as yr,du
from du_4 order by parcel_id,yr;

create table j_5 as select parcel_id,yr,jobs
from j_3 where yr >= (&by1 - 1)
order by parcel_id,yr;
quit;
