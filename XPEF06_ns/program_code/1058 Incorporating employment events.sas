/*
%let xver=xpef06;

libname e1 "T:\socioec\Current_Projects\&xver\input_data";
*/

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table jb_0 as select *
from connection to odbc
(
select x.job_id,x.sector_id,x.building_id,y.parcel_id
FROM [urbansim].[urbansim]./*[job_by_sector_id]*/[job_2016] as x
inner join [urbansim].[urbansim].[building_by_sector_id] as y on x.building_id=y.building_id
/*inner join [urbansim].[urbansim].[parcel] as z on y.parcel_id=z.parcel_id*/);


/* selecting buildings (points) that have job spaces
then finding which jur-cpa they fall into */
create table bld_location_0 as select *
from connection to odbc
(
select
x.building_id,x.parcel_id,y.js,coalesce(u.j,0) as j,z.mgra,z.jur_2017
FROM [urbansim].[urbansim].[building_by_sector_id] as x
inner join (select building_id,sum(job_spaces) as js from [urbansim].[urbansim].[job_space_2016] group by building_id) as y
	on x.building_id=y.building_id
left join (select building_id,count(job_id) as j from [urbansim].[urbansim].[job_2016] group by building_id) as u
	on x.building_id=u.building_id
LEFT JOIN [ws].[dbo].[BLK2010_JUR2017] as z on x.centroid.STIntersects(z.shape) = 1
)
order by building_id;

disconnect from odbc;

update bld_location_0 set mgra = 14542 where parcel_id = 831901;
/* pier in Oceanside */

quit;

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;
/* selecting parcels (polygons) that have buildings with job spaces
then overlaying such parcels with jur-cpa boundaries */
create table prc_location_0 as select *
from connection to odbc
(
select
x.parcel_id,u.mgra,u.jur_2017, x.shape.STIntersection(u.shape).STArea() as area
from [urbansim].[urbansim].[parcel] as x

inner join
(
select distinct a.parcel_id
from (select parcel_id,building_id from [urbansim].[urbansim].[building_by_sector_id]) as a
inner join (select building_id,sum(job_spaces) as js from [urbansim].[urbansim].[job_space_2016] group by building_id) as b
	on a.building_id=b.building_id
) as y
on x.parcel_id = y.parcel_id

LEFT JOIN [ws].[dbo].[BLK2010_JUR2017] as u on x.shape.STIntersects(u.shape) = 1
)
order by parcel_id,area desc;

disconnect from odbc;

update prc_location_0 set mgra = 14542 where parcel_id = 831901;
quit;

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table dev_j_0 as select *
from connection to odbc
(
SELECT x.[siteid],year(coalesce(x.[compdate_imputed],x.[compdate])) as yr
,y.[parcel_id],y.[civemp_imputed] as j,y.[sector_id]
,z.mgra,z.jur_2017
,y.shape.STIntersection(z.shape).STArea() as area
FROM [urbansim].[ref].[scheduled_development_site] /*[spacecore].[GIS].[scheduled_development_sites]*/ as x
inner join [urbansim].[urbansim].[scheduled_development_parcel] as y on x.siteid=y.site_id
LEFT JOIN [ws].[dbo].[BLK2010_JUR2017] as z on y.shape.STIntersects(z.shape) = 1
where y.civemp_imputed >0 and x.civemp_imputed >0
)
order by siteid,parcel_id,yr,sector_id,area desc;

disconnect from odbc;

update dev_j_0 set sector_id = 17 where sector_id = . and siteid in (19000,19001);

update dev_j_0 set yr = 2022 where yr = . and siteid in (4014);

quit;


data dev_j_1(drop=jur_2017 area);set dev_j_0;by siteid parcel_id yr sector_id;
if first.sector_id;
jur_id=int(jur_2017/100);
if jur_id in (14,19) then cpa_id=jur_2017; else cpa_id=0;
run;

proc sql;
create table test_01 as select * from dev_j_1 where yr=.;
quit;

proc sql;
create table dev_j_1a as select distinct yr from dev_j_1;
create table dev_j_1b as select distinct sector_id from dev_j_1;
create table dev_j_1c as select distinct siteid,sector_id from dev_j_1 where sector_id in (21,24,26);
quit;

proc sql;
create table dev_j_1d as select yr,siteid,parcel_id,mgra,jur_id,cpa_id
,case
when siteid = 12057 then 16 /* VA clinic */
when siteid = 14037 then 28 /* reclassified from 26; local gov */
/* when siteid in (19000,19001) then */
else sector_id end as sandag_industry_id
,case
when siteid = 12057 then 2 /* federal gov */
when siteid = 14037 then 4 /* local gov */
when siteid in (19000,19001) then 4 /* Tribal Casinos; local gov */
else 1 /* private */ end as type
,j
from dev_j_1;
quit;


/*proc sort data=dev_j_1;by parcel_id sector_id yr;run;*/





data bld_location_1(drop=jur_2017);set bld_location_0;
jur_id=int(jur_2017/100);
if jur_id in (14,19) then cpa_id=jur_2017; else cpa_id=0;
run;

/* parcel and other geo identifiers for existing jobs spaces */
data prc_location_1(drop=jur_2017);set prc_location_0;where mgra>0;by parcel_id;
if first.parcel_id;
jur_id=int(jur_2017/100);
if jur_id in (14,19) then cpa_id=jur_2017; else cpa_id=0;
run;


data e1.parcel_xref_base_jobs;set prc_location_1(drop=area);run;



proc sql;
create table bld_location_2 as select x.building_id,x.parcel_id,x.js,x.j,y.mgra,y.jur_id,y.cpa_id
from bld_location_1 as x
left join prc_location_1 as y on x.parcel_id=y.parcel_id
order by building_id,parcel_id;
quit;

proc sql;
create table jb_0_ as select x.job_id,x.building_id,x.parcel_id
,case
when job_id > 200000000 then 5 /* self-employed */
when job_id > 50000000 then 2 /* fed government owned */
when job_id > 40000000 then 3 /* state government owned */
when job_id > 30000000 then 4 /* local government owned */
else 1 /* private */
end as type
,case
when sector_id in (23,25) then 15 /* state and local education */
when sector_id in (21,24,26) then 28 /* federal, state, and local public administration */
else sector_id
end as sandag_industry_id
/* ,case when 200000000 <= x.job_id < 300000000 then "SE" else "WS" end as j_type */
,y.mgra,y.jur_id,y.cpa_id
from jb_0 as x
left join bld_location_2 as y on x.building_id=y.building_id;
quit;


/* base jobs */

proc sql;
create table jb_1 as select mgra,jur_id,cpa_id,sandag_industry_id,type,count(job_id) as j
from jb_0_ group by mgra,jur_id,cpa_id,sandag_industry_id,type;
quit;


/* urbansim jobs from capacities */


proc sql;
create table usj_1 as
select 2018 as yr,mgra,jur_id,cpa_id,sandag_industry_id,type, count(*) as j
from e1.jobs_from_capacities where yr <= 2018 group by mgra,jur_id,cpa_id,sandag_industry_id,type
	union all
select 2020 as yr,mgra,jur_id,cpa_id,sandag_industry_id,type, count(*) as j
from e1.jobs_from_capacities where yr <= 2020 group by mgra,jur_id,cpa_id,sandag_industry_id,type
	union all
select 2025 as yr,mgra,jur_id,cpa_id,sandag_industry_id,type, count(*) as j
from e1.jobs_from_capacities where yr <= 2025 group by mgra,jur_id,cpa_id,sandag_industry_id,type
	union all
select 2030 as yr,mgra,jur_id,cpa_id,sandag_industry_id,type, count(*) as j
from e1.jobs_from_capacities where yr <= 2030 group by mgra,jur_id,cpa_id,sandag_industry_id,type
	union all
select 2035 as yr,mgra,jur_id,cpa_id,sandag_industry_id,type, count(*) as j
from e1.jobs_from_capacities where yr <= 2035 group by mgra,jur_id,cpa_id,sandag_industry_id,type
	union all
select 2040 as yr,mgra,jur_id,cpa_id,sandag_industry_id,type, count(*) as j
from e1.jobs_from_capacities where yr <= 2040 group by mgra,jur_id,cpa_id,sandag_industry_id,type
	union all
select 2045 as yr,mgra,jur_id,cpa_id,sandag_industry_id,type, count(*) as j
from e1.jobs_from_capacities where yr <= 2045 group by mgra,jur_id,cpa_id,sandag_industry_id,type
	union all
select 2050 as yr,mgra,jur_id,cpa_id,sandag_industry_id,type, count(*) as j
from e1.jobs_from_capacities where yr <= 2050 group by mgra,jur_id,cpa_id,sandag_industry_id,type;

create table usj_1a as select yr,sum(j) as j
from usj_1 group by yr;
quit;

/* additional college and military jobs */

proc sql;
create table emp_colmil as select yr,mgra,jur_id,cpa_id,ct
,case when sector_id = 23 then 15 else sector_id end as sandag_industry_id
,case
when sector_id = 15 then 1 /* private */
when sector_id = 23 then 3 /* state gov */
when sector_id = 27 then 2 /* federal gov */
end as type
,j
from e1.emp_colmil;
quit;

proc sql;
create table j_colmil_1 as
select 2018 as yr,mgra,jur_id,cpa_id,sandag_industry_id, type,sum(j) as j
from Emp_colmil where yr <= 2018 group by mgra,jur_id,cpa_id,sandag_industry_id, type
	union all
select 2020 as yr,mgra,jur_id,cpa_id,sandag_industry_id, type,sum(j) as j
from Emp_colmil where yr <= 2020 group by mgra,jur_id,cpa_id,sandag_industry_id, type
	union all
select 2025 as yr,mgra,jur_id,cpa_id,sandag_industry_id, type,sum(j) as j
from Emp_colmil where yr <= 2025 group by mgra,jur_id,cpa_id,sandag_industry_id, type
	union all
select 2030 as yr,mgra,jur_id,cpa_id,sandag_industry_id, type,sum(j) as j
from Emp_colmil where yr <= 2030 group by mgra,jur_id,cpa_id,sandag_industry_id, type
	union all
select 2035 as yr,mgra,jur_id,cpa_id,sandag_industry_id, type,sum(j) as j
from Emp_colmil where yr <= 2035 group by mgra,jur_id,cpa_id,sandag_industry_id, type
	union all
select 2040 as yr,mgra,jur_id,cpa_id,sandag_industry_id, type,sum(j) as j
from Emp_colmil where yr <= 2040 group by mgra,jur_id,cpa_id,sandag_industry_id, type
	union all
select 2045 as yr,mgra,jur_id,cpa_id,sandag_industry_id, type,sum(j) as j
from Emp_colmil where yr <= 2045 group by mgra,jur_id,cpa_id,sandag_industry_id, type
	union all
select 2050 as yr,mgra,jur_id,cpa_id,sandag_industry_id, type,sum(j) as j
from Emp_colmil where yr <= 2050 group by mgra,jur_id,cpa_id,sandag_industry_id, type;
quit;


proc sql;
create table dev_j_1e as select yr,mgra,jur_id,cpa_id,sandag_industry_id,type,sum(j) as j
from dev_j_1d group by yr,mgra,jur_id,cpa_id,sandag_industry_id,type;
quit;

data dev_j_1e;set dev_j_1e;
id + 1;
run;

%let s = 6; /*spreading over s-1 years */
data dev_j_2(rename=(j=jt));set dev_j_1e(rename=(yr=yr1));
if yr1<2045 then yr2 = yr1 + (&s - 1);else yr2 = yr1;
do yr=yr1 to yr2;
	j0 = ceil(j / &s);
/*
	ws = ceil(jt_ws / &s);
	se = ceil(jt_se / &s);
*/
	output;
end;
run;

data dev_j_3;set dev_j_2;by id;retain jc;
if first.id then
do;
j1 = min(j0, jt);
jc = j1;
end;
else do;
j1 = min(j0, (jt - jc));
jc = jc + j1;
end;
run;

proc sql;
create table dev_j_4 as select yr,mgra,jur_id,cpa_id,sandag_industry_id,type
,sum(j1) as j
from dev_j_3
group by yr,mgra,jur_id,cpa_id,sandag_industry_id,type;
quit;

/* jobs from development events */
proc sql;
create table j_dev_1 as
select 2018 as yr,mgra,jur_id,cpa_id,sandag_industry_id, type, sum(j) as j
from dev_j_4 where yr <= 2018 group by mgra,jur_id,cpa_id,sandag_industry_id, type
	union all
select 2020 as yr,mgra,jur_id,cpa_id,sandag_industry_id, type, sum(j) as j
from dev_j_4 where yr <= 2020 group by mgra,jur_id,cpa_id,sandag_industry_id, type
	union all
select 2025 as yr,mgra,jur_id,cpa_id,sandag_industry_id, type, sum(j) as j
from dev_j_4 where yr <= 2025 group by mgra,jur_id,cpa_id,sandag_industry_id, type
	union all
select 2030 as yr,mgra,jur_id,cpa_id,sandag_industry_id, type, sum(j) as j
from dev_j_4 where yr <= 2030 group by mgra,jur_id,cpa_id,sandag_industry_id, type
	union all
select 2035 as yr,mgra,jur_id,cpa_id,sandag_industry_id, type, sum(j) as j
from dev_j_4 where yr <= 2035 group by mgra,jur_id,cpa_id,sandag_industry_id, type
	union all
select 2040 as yr,mgra,jur_id,cpa_id,sandag_industry_id, type, sum(j) as j
from dev_j_4 where yr <= 2040 group by mgra,jur_id,cpa_id,sandag_industry_id, type
	union all
select 2045 as yr,mgra,jur_id,cpa_id,sandag_industry_id, type, sum(j) as j
from dev_j_4 where yr <= 2045 group by mgra,jur_id,cpa_id,sandag_industry_id, type
	union all
select 2050 as yr,mgra,jur_id,cpa_id,sandag_industry_id, type, sum(j) as j
from dev_j_4 where yr <= 2050 group by mgra,jur_id,cpa_id,sandag_industry_id, type;
quit;


proc sql;
create table jb_2 as select x.yr,y.*
from (select distinct yr from usj_1) as x
cross join jb_1 as y;
quit;


proc sql;
create table usj_3 as select
coalesce(x.yr,y.yr) as yr
,coalesce(x.mgra,y.mgra) as mgra
,coalesce(x.jur_id,y.jur_id) as jur_id
,coalesce(x.cpa_id,y.cpa_id) as cpa_id
,coalesce(x.sandag_industry_id,y.sandag_industry_id) as sandag_industry_id
,coalesce(x.type,y.type) as type
,coalesce(x.j,0) as jb
,coalesce(y.j,0) as jc
from jb_2 as x
full join usj_1 as y on x.yr=y.yr and x.mgra=y.mgra and x.jur_id=y.jur_id and x.cpa_id=y.cpa_id
and x.sandag_industry_id = y.sandag_industry_id and x.type = y.type;

create table usj_4 as select
coalesce(x.yr,y.yr) as yr
,coalesce(x.mgra,y.mgra) as mgra
,coalesce(x.jur_id,y.jur_id) as jur_id
,coalesce(x.cpa_id,y.cpa_id) as cpa_id
,coalesce(x.sandag_industry_id,y.sandag_industry_id) as sandag_industry_id
,coalesce(x.type,y.type) as type
,coalesce(x.jb,0) as jb
,coalesce(x.jc,0) as jc
,coalesce(y.j,0) as j1
from usj_3 as x
full join j_colmil_1 as y on x.yr=y.yr and x.mgra=y.mgra and x.jur_id=y.jur_id and x.cpa_id=y.cpa_id
and x.sandag_industry_id = y.sandag_industry_id and x.type = y.type;

/*
jb: base jobs
jc: jobs from capacities
j1: college and military
j2: jobs from development events
*/
create table usj_5 as select
coalesce(x.yr,y.yr) as yr
,coalesce(x.mgra,y.mgra) as mgra
,coalesce(x.jur_id,y.jur_id) as jur_id
,coalesce(x.cpa_id,y.cpa_id) as cpa_id
,coalesce(x.sandag_industry_id,y.sandag_industry_id) as sandag_industry_id
,coalesce(x.type,y.type) as type
,coalesce(x.jb,0) as jb
,coalesce(x.jc,0) as jc
,coalesce(x.j1,0) as j1
,coalesce(y.j,0) as j2
from usj_4 as x
full join j_dev_1 as y on x.yr=y.yr and x.mgra=y.mgra and x.jur_id=y.jur_id and x.cpa_id=y.cpa_id
and x.sandag_industry_id = y.sandag_industry_id and x.type = y.type;
quit;


proc sql;
create table usj_5a as select
yr, sandag_industry_id, type
,sum(jb) as jb
,sum(jc) as jc
,sum(j1) as j1
,sum(j2) as j2
,sum(jb+jc) as target
from usj_5 group by yr, sandag_industry_id, type;

quit;

proc sql;
create table usj_5d as select yr,sandag_industry_id,type
,sum(jb) as jb
,sum(jc) as jc
,sum(j1) as j1
,sum(j2) as j2
,sum(jc) - sum(j1) - sum(j2) as jc_target
from usj_5 group by yr,sandag_industry_id,type;

create table usj_5e as select * from usj_5d where jc_target < 0;

update usj_5d set jc_target = 0 where jc_target < 0;
quit;

proc sql;
create table usj_6 as select yr,mgra,jur_id,cpa_id,sandag_industry_id,type
,jb
,j1
,j2
,jc as jc_old
,case when j2 > jc then 0 else jc - j2 end as jc_new
from usj_5;

create table usj_6a as select yr,sandag_industry_id,type
,sum(jc_old) as jc_old
,sum(jc_new) as jc_new
,sum(j2) as j2
,case
when calculated j2 > calculated jc_new then calculated jc_new
when calculated j2 = 0 then 0
when calculated jc_new < calculated jc_old and calculated j2 < calculated jc_new
	then calculated j2 - (calculated jc_old - calculated jc_new)
else calculated j2 end as jc_drop
from usj_6 group by yr,sandag_industry_id,type
order by sandag_industry_id,type,yr;
quit;

/*
proc sql;
create table usj_7 as
select yr,sandag_industry_id,type,jc_drop from usj_6a where jc_drop > 0;
quit;
*/

proc sql;
create table jc_1 as
select sandag_industry_id,type,mgra,jur_id,cpa_id,jc_new as j,yr
from usj_6 where jc_new>0
order by sandag_industry_id,type,mgra,jur_id,cpa_id,yr;
quit;

/* j1 is annual net change (additions) */
data jc_2; set jc_1; by sandag_industry_id type mgra jur_id cpa_id; retain c;
if first.cpa_id then do; j1 = j; c = j1; end;
else do; j1 = j - c; c = j; end;
run;

/* creating micro data */
data jc_3(drop = j1 j i); set jc_2(drop = c); where j1 > 0;
do i = 1 to j1;
	r = ranuni(2018);
	output;
end;
run;

proc sort data=jc_3;by sandag_industry_id type yr r;run;

data jc_4(drop=r);set jc_3;by sandag_industry_id type;retain id;
if first.type then id=1;else id=id+1;
run;

proc sql;
create table jc_5 as select y.yr,x.mgra,x.jur_id,x.cpa_id,x.sandag_industry_id,x.type,x.id
from jc_4 as x
cross join usj_1a as y where x.yr<=y.yr
order by sandag_industry_id,type,id,yr;

create table jc_5a as select sandag_industry_id,type,yr,count(*) as jc
from jc_5 group by sandag_industry_id,type,yr;

/*
create table jc_5b as select sector_id,yr,count(*) as jc
from jc_5 group by sector_id,yr;
*/

create table jc_5c as select yr,count(*) as jc
from jc_5 group by yr;
quit;


proc sql;
create table jc_catch_1 as select x.*
from jc_5 as x
inner join usj_5d as y on x.yr = y.yr and x.sandag_industry_id = y.sandag_industry_id and x.type = y.type
where x.id <= y.jc_target;

create table jc_catch_2 as select yr,mgra,jur_id,cpa_id,sandag_industry_id,type,count(id) as jc
from jc_catch_1 group by yr,mgra,jur_id,cpa_id,sandag_industry_id,type;
quit;

proc sql;
create table usj_8 as select x.yr,x.mgra,x.jur_id,x.cpa_id,x.sandag_industry_id,x.type
,x.jb,x.j1,x.j2, coalesce(y.jc,0) as jc
,x.jb + x.j1 + x.j2 + coalesce(y.jc,0) as jt
from usj_6 as x
left join jc_catch_2 as y on x.yr = y.yr and x.mgra = y.mgra and x.jur_id = y.jur_id and x.cpa_id = y.cpa_id
and x.sandag_industry_id = y.sandag_industry_id and x.type = y.type;

create table usj_8a as select yr,sandag_industry_id,type
,sum(jb) as jb, sum(j1) as j1, sum(j2) as j2, sum(jc) as jc, sum(jt) as jt
from usj_8 group by yr,sandag_industry_id,type;
quit;

proc sql;
create table test_10 as select x.*, y.jt, y.j1, y.j2, x.j - y.jt as d
from e1.employment_controls as x
inner join (select distinct yr from jb_2) as z on x.yr = z.yr
left join usj_8a as y
on x.yr = y.yr and x.sandag_industry_id = y.sandag_industry_id and x.type = y.type
order by yr,sandag_industry_id,type;

/* this table shows which industries/years have more jobs than the target */
create table test_11 as select * from test_10 where d ^= 0
order by sandag_industry_id,type,yr;
quit;


proc sql;

/* jobs from capacities and events are included in jobs_all */

create table e1.jobs_all as
select yr,mgra,jur_id,cpa_id,sandag_industry_id,type
,jt as j /* all jobs */
,jb as j_base /* jobs from base year */
,jc as j_capacity /* jobs FROM capacities (not capacity for jobs !!!) */
,j1 as j_col_mil /* jobs from college and military expansion */
,j2 as j_dev_events /* jobs from development events */
from usj_8
	union all
select 2016 as yr,mgra,jur_id,cpa_id,sandag_industry_id,type
,jb as j /* all jobs */
,jb as j_base /* jobs from base year */
,0 as j_capacity /* jobs FROM capacities (not capacity for jobs !!!) */
,0 as j_col_mil /* jobs from college and military expansion */
,0 as j_dev_events /* jobs from developmnet events */
from usj_8 where yr = 2018;
quit;

data e1.job_events_parcels;set dev_j_1d;run;

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

/* parcel-level jobs from 2016 */
create table e1.jobs_base as select parcel_id,job_id,sector_id
,case
when job_id > 200000000 then 5 /* self-employed */
when job_id > 50000000 then 2 /* fed government owned */
when job_id > 40000000 then 3 /* state government owned */
when job_id > 30000000 then 4 /* local government owned */
else 1 /* private */
end as type
,case
when sector_id in (21,24,26) then 28 /* public administration */
when sector_id in (23,25) then 15 /* education */
else sector_id 
end as sandag_industry_id
from connection to odbc
(select x.job_id,x.sector_id,y.parcel_id FROM [urbansim].[urbansim].[job_2016] as x
inner join [urbansim].[urbansim].[building_by_sector_id] as y on x.building_id=y.building_id)
;

disconnect from odbc;
quit;
