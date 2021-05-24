%let xver=xpef06;

/* setting version id for urbansim*/
%let ver=111;

libname urb odbc noprompt="driver=SQL Server; server=sql2014a8; database=urbansim;
Trusted_Connection=yes" schema=urbansim;

libname e1 "T:\socioec\Current_Projects\&xver\input_data";


proc import out=hu_2
datafile="T:\socioec\Current_Projects\&xver\input_data\HU Construction and PPH projections 7.xlsx"
replace dbms=excelcs; RANGE='Data2$k1:l36'n;
run;


proc sql;
create table hu_urb_1 as select &ver as version_id,yr_built_during as yr /* this is BUILD DURING YR */
,round(hu_g,1) as housing_units_add format=6.
from hu_2 where yr_built_during in (2017:2050) order by yr;
quit;


proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table dof_hu_0 as select
case when area_type="City" then area_name else summary_type end as Name
,est_yr,total_hu as hu
from connection to odbc
(
select * FROM [socioec_data].[ca_dof].[population_housing_estimates]
where county_name= 'San Diego' and vintage_yr = 2018 and est_yr >= 2017 and (area_type = 'City' or summary_type = 'Unincorporated')
);

DISCONNECT FROM odbc;
quit;

proc sql;
create table dof_hu_1 as select x.name as jurisdiction, x.est_yr as yr
,y.hu - x.hu as hu_construction
from dof_hu_0 as x
inner join dof_hu_0 as y on x.name=y.name
where x.est_yr = 2017 and y.est_yr = 2018
order by jurisdiction;

create table dof_hu_1a as select yr
,sum(hu_construction) as hu_construction
from dof_hu_1 group by yr;
quit;


proc sql;
create table dof_hu_2 as select &ver as version_id
,x.yr
,x.jurisdiction as jurisdiction_name
,y.jur as jurisdiction_id
,x.hu_construction as housing_units_add format=6.
from dof_hu_1 as x
left join e1.sf1_place as y on x.jurisdiction=y.name
order by jurisdiction_id;
quit;


/*
proc sql;
CONNECT TO ODBC(noprompt="driver=SQL Server; server=sql2014a8;DBCOMMIT=10000;Trusted_Connection=yes;") ;
EXECUTE ( DROP TABLE IF EXISTS [urbansim].[urbansim].[urbansim_target_housing_units] ) BY ODBC ;
DISCONNECT FROM ODBC ;
quit;
*/

/*
proc sql;
create table urb.urbansim_target_hu_jur  as select * from dof_hu_2;
quit;
*/


proc sql;
delete from urb.urbansim_target_housing_units where version_id=&ver;
delete from urb.urbansim_target_hu_jur where version_id=&ver;

insert into urb.urbansim_target_housing_units select * from hu_urb_1;
insert into urb.urbansim_target_hu_jur select * from dof_hu_2;
quit;
