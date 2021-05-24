%let xver=xpef05;

libname e1 "T:\socioec\Current_Projects\&xver\input_data";

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
SELECT x.[siteid],year(x.[compdate_imputed]) as yr
,y.[parcel_id],y.[civemp_imputed] as j,y.[sector_id]
,z.mgra,z.jur_2017
,y.shape.STIntersection(z.shape).STArea() as area
FROM [spacecore].[GIS].[scheduled_development_sites] as x
inner join [urbansim].[urbansim].[scheduled_development_parcel] as y on x.siteid=y.site_id
LEFT JOIN [ws].[dbo].[BLK2010_JUR2017] as z on y.shape.STIntersects(z.shape) = 1
where y.civemp_imputed >0 and x.civemp_imputed >0
)
order by siteid,parcel_id,yr,sector_id,area desc;

disconnect from odbc;

quit;


data dev_j_1(drop=jur_2017 area);set dev_j_0;by siteid parcel_id yr sector_id;
if first.sector_id;
jur_id=int(jur_2017/100);
if jur_id in (14,19) then cpa_id=jur_2017; else cpa_id=0;
run;

/* site 12057 is a VA clinic */

proc sql;
create table dev_j_1a as select distinct yr from dev_j_1;
quit;

/*proc sort data=dev_j_1;by parcel_id sector_id yr;run;*/





data bld_location_1(drop=jur_2017);set bld_location_0;
jur_id=int(jur_2017/100);
if jur_id in (14,19) then cpa_id=jur_2017; else cpa_id=0;
run;

data prc_location_1(drop=jur_2017);set prc_location_0;where mgra>0;by parcel_id;
if first.parcel_id;
jur_id=int(jur_2017/100);
if jur_id in (14,19) then cpa_id=jur_2017; else cpa_id=0;
run;

proc sql;
create table bld_location_2 as select x.building_id,x.parcel_id,x.js,x.j,y.mgra,y.jur_id,y.cpa_id
from bld_location_1 as x
left join prc_location_1 as y on x.parcel_id=y.parcel_id
order by building_id,parcel_id;
quit;

proc sql;
create table jb_0_ as select x.*
,case
when 200000000 <= x.job_id < 300000000 then "SE" else "WS" end as j_type
,y.mgra,y.jur_id,y.cpa_id
from jb_0 as x
left join bld_location_2 as y on x.building_id=y.building_id;
quit;


/* base jobs */
proc sql;
create table jb_1 as select x.*, x.j - coalesce(y.j,0) as j_ws, coalesce(y.j,0) as j_se
from (select mgra,jur_id,cpa_id,sector_id,count(job_id) as j from jb_0_ group by mgra,jur_id,cpa_id,sector_id) as x
left join (select mgra,jur_id,cpa_id,sector_id,count(job_id) as j from jb_0_ where j_type="SE" group by mgra,jur_id,cpa_id,sector_id) as y
on x.mgra=y.mgra and x.jur_id=y.jur_id and x.cpa_id=y.cpa_id and x.sector_id=y.sector_id;
quit;



/* urbansim jobs from capacities */
/*
proc sql;
create table usj_1 as select yr,mgra,sector_id,sum(j) as j
from dw.jobs_from_capacities where yr in (2018,2020,2025,2030,2035,2040,2045,2050)
group by yr,mgra,sector_id;
quit;
*/

proc sql;
create table usj_1 as
select 2018 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(wsj) as wsj,sum(sej) as sej
from e1.jobs_from_capacities where yr <= 2018 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2020 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(wsj) as wsj,sum(sej) as sej
from e1.jobs_from_capacities where yr <= 2020 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2025 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(wsj) as wsj,sum(sej) as sej
from e1.jobs_from_capacities where yr <= 2025 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2030 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(wsj) as wsj,sum(sej) as sej
from e1.jobs_from_capacities where yr <= 2030 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2035 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(wsj) as wsj,sum(sej) as sej
from e1.jobs_from_capacities where yr <= 2035 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2040 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(wsj) as wsj,sum(sej) as sej
from e1.jobs_from_capacities where yr <= 2040 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2045 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(wsj) as wsj,sum(sej) as sej
from e1.jobs_from_capacities where yr <= 2045 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2050 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(wsj) as wsj,sum(sej) as sej
from e1.jobs_from_capacities where yr <= 2050 group by mgra,jur_id,cpa_id,sector_id;
quit;

/* additional college and military jobs */
proc sql;
create table j_colmil_1 as
select 2018 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(j) as wsj,0 as sej
from e1.Emp_colmil where yr <= 2018 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2020 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(j) as wsj,0 as sej
from e1.Emp_colmil where yr <= 2020 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2025 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(j) as wsj,0 as sej
from e1.Emp_colmil where yr <= 2025 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2030 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(j) as wsj,0 as sej
from e1.Emp_colmil where yr <= 2030 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2035 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(j) as wsj,0 as sej
from e1.Emp_colmil where yr <= 2035 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2040 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(j) as wsj,0 as sej
from e1.Emp_colmil where yr <= 2040 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2045 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(j) as wsj,0 as sej
from e1.Emp_colmil where yr <= 2045 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2050 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(j) as wsj,0 as sej
from e1.Emp_colmil where yr <= 2050 group by mgra,jur_id,cpa_id,sector_id;
quit;

proc sql;
create table usj_1a as select yr,sum(j) as j,sum(wsj) as wsj,sum(sej) as sej
from usj_1 group by yr;

create table usj_1b as select sector_id,sum(wsj)/sum(j) as wsj_s
from usj_1 group by sector_id;
quit;


proc sql;
create table dev_j_1b as select yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j
from dev_j_1 group by yr,mgra,jur_id,cpa_id,sector_id;

create table dev_j_1c as select x.*, round(x.j * coalesce(y.wsj_s,0),1) as j_ws, x.j - calculated j_ws as j_se
from dev_j_1b as x
left join usj_1b as y on x.sector_id=y.sector_id
order by yr,mgra,jur_id,cpa_id,sector_id;
quit;

data dev_j_1d;set dev_j_1c;
id + 1;
run;


%let s = 6; /*spreading over s-1 years */
data dev_j_2(drop=j);set dev_j_1d(rename=(j_ws=jt_ws j_se=jt_se yr=yr1));
if yr1<2045 then yr2 = yr1 + (&s - 1);else yr2 = yr1;
do yr=yr1 to yr2;
	ws = ceil(jt_ws / &s);
	se = ceil(jt_se / &s);
	output;
end;
run;

data dev_j_3;set dev_j_2;by id;retain jc_ws jc_se;
if first.id then
do;
j1_ws = min(ws, jt_ws);
jc_ws = j1_ws;
j1_se = min(se, jt_se);
jc_se = j1_se;
end;

else do;
j1_ws = min(ws, (jt_ws - jc_ws));
jc_ws = jc_ws + j1_ws;
j1_se = min(se, (jt_se - jc_se));
jc_se = jc_se + j1_se;
end;
run;


proc sql;
create table dev_j_4 as select yr,mgra,jur_id,cpa_id,sector_id
,sum(j1_ws + j1_se) as j
,sum(j1_ws) as wsj
,sum(j1_se) as sej
from dev_j_3
group by yr,mgra,jur_id,cpa_id,sector_id;
quit;



/* jobs from development events */
proc sql;
create table j_dev_1 as
select 2018 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(wsj) as wsj,sum(sej) as sej
from dev_j_4 where yr <= 2018 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2020 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(wsj) as wsj,sum(sej) as sej
from dev_j_4 where yr <= 2020 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2025 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(wsj) as wsj,sum(sej) as sej
from dev_j_4 where yr <= 2025 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2030 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(wsj) as wsj,sum(sej) as sej
from dev_j_4 where yr <= 2030 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2035 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(wsj) as wsj,sum(sej) as sej
from dev_j_4 where yr <= 2035 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2040 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(wsj) as wsj,sum(sej) as sej
from dev_j_4 where yr <= 2040 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2045 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(wsj) as wsj,sum(sej) as sej
from dev_j_4 where yr <= 2045 group by mgra,jur_id,cpa_id,sector_id
	union all
select 2050 as yr,mgra,jur_id,cpa_id,sector_id,sum(j) as j,sum(wsj) as wsj,sum(sej) as sej
from dev_j_4 where yr <= 2050 group by mgra,jur_id,cpa_id,sector_id;
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
,coalesce(x.sector_id,y.sector_id) as sector_id
,coalesce(x.j,0) as jb
,coalesce(y.j,0) as jc
,coalesce(x.j_ws,0) as jb_ws
,coalesce(y.wsj,0) as jc_ws
,coalesce(x.j_se,0)  as jb_se
,coalesce(y.sej,0) as jc_se
from jb_2 as x
full join usj_1 as y on x.yr=y.yr and x.sector_id=y.sector_id and x.mgra=y.mgra and x.jur_id=y.jur_id and x.cpa_id=y.cpa_id;

create table usj_4 as select
coalesce(x.yr,y.yr) as yr
,coalesce(x.mgra,y.mgra) as mgra
,coalesce(x.jur_id,y.jur_id) as jur_id
,coalesce(x.cpa_id,y.cpa_id) as cpa_id
,coalesce(x.sector_id,y.sector_id) as sector_id
,coalesce(x.jb,0) as jb
,coalesce(x.jc,0) as jc
,coalesce(y.j,0) as j1
,coalesce(x.jb_ws,0) as jb_ws
,coalesce(x.jc_ws,0) as jc_ws
,coalesce(y.wsj,0) as j1_ws
,coalesce(x.jb_se,0)  as jb_se
,coalesce(x.jc_se,0)  as jc_se
,coalesce(y.sej,0)  as j1_se
from usj_3 as x
full join j_colmil_1 as y on x.yr=y.yr and x.sector_id=y.sector_id and x.mgra=y.mgra and x.jur_id=y.jur_id and x.cpa_id=y.cpa_id;

/*
jb: base jobs
jc: jobs from capacities
j1: collehe and military
j2: jobs from development events
*/
create table usj_5 as select
coalesce(x.yr,y.yr) as yr
,coalesce(x.mgra,y.mgra) as mgra
,coalesce(x.jur_id,y.jur_id) as jur_id
,coalesce(x.cpa_id,y.cpa_id) as cpa_id
,coalesce(x.sector_id,y.sector_id) as sector_id
,coalesce(x.jb,0) as jb
,coalesce(x.jc,0) as jc
,coalesce(x.j1,0) as j1
,coalesce(y.j,0) as j2
,coalesce(x.jb_ws,0) as jb_ws
,coalesce(x.jc_ws,0) as jc_ws
,coalesce(x.j1_ws,0) as j1_ws
,coalesce(y.wsj,0) as j2_ws
,coalesce(x.jb_se,0)  as jb_se
,coalesce(x.jc_se,0)  as jc_se
,coalesce(x.j1_se,0)  as j1_se
,coalesce(y.sej,0)  as j2_se
from usj_4 as x
full join j_dev_1 as y on x.yr=y.yr and x.sector_id=y.sector_id and x.mgra=y.mgra and x.jur_id=y.jur_id and x.cpa_id=y.cpa_id;
quit;


proc sql;
create table usj_5a as select
yr,sector_id
,sum(jb) as jb,sum(jc) as jc
,sum(j1) as j1,sum(j2) as j2
,sum(jc_ws) as jc_ws,sum(jc_se) as jc_se
,sum(j2_ws) as j2_ws,sum(j2_se) as j2_se
,sum(jb+jc) as target
,sum(j1+j2) as j_new
from usj_5 group by yr,sector_id;

create table usj_5b as select *,j_new - jc as d
from usj_5a where sector_id not in (27,23) and jc<j_new;

create table usj_5c as select yr,sum(d) as d from usj_5b group by yr;
quit;

proc sql;
create table usj_5d as select yr,sector_id
,sum(jc_ws) as jc_ws,sum(jc_se) as jc_se
,sum(j2_ws) as j2_ws,sum(j2_se) as j2_se
,sum(jc_ws) - sum(j2_ws) as jc_ws_target
,sum(jc_se) - sum(j2_se) as jc_se_target
from usj_5 group by yr,sector_id;

update usj_5d set jc_ws_target = 0 where jc_ws_target < 0;
update usj_5d set jc_se_target = 0 where jc_se_target < 0;
quit;

proc sql;
create table usj_6 as select yr,mgra,jur_id,cpa_id,sector_id
,jb_ws, jb_se, j1_ws, j1_se
,j2_ws, j2_se
,jc_ws as jc_ws_old, jc_se as jc_se_old
,case when j2_ws > jc_ws then 0 else jc_ws - j2_ws end as jc_ws_new
,case when j2_se > jc_se then 0 else jc_se - j2_se end as jc_se_new
from usj_5;

create table usj_6a as select yr,sector_id
,sum(jc_ws_old) as jc_ws_old,sum(jc_se_old) as jc_se_old
,sum(jc_ws_new) as jc_ws_new,sum(jc_se_new) as jc_se_new
,sum(j2_ws) as j2_ws,sum(j2_se) as j2_se
,case
when calculated j2_ws > calculated jc_ws_new then calculated jc_ws_new
when calculated j2_ws = 0 then 0
else calculated j2_ws end as jc_ws_drop
,case
when calculated j2_se > calculated jc_se_new then calculated jc_se_new
when calculated j2_se = 0 then 0
else calculated j2_se end as jc_se_drop
from usj_6 group by yr,sector_id
order by sector_id,yr;
quit;

proc sql;
create table usj_7 as
select "ws" as type,yr,sector_id,jc_ws_drop as jc_drop from usj_6a where jc_ws_drop > 0
	union all
select "se" as type,yr,sector_id,jc_se_drop as jc_drop from usj_6a where jc_se_drop > 0;
quit;

proc sql;
create table jc_1 as
select "ws" as type,sector_id,mgra,jur_id,cpa_id,jc_ws_new as j,yr from usj_6 where jc_ws_new>0
	union all
select "se" as type,sector_id,mgra,jur_id,cpa_id,jc_se_new as j,yr from usj_6 where jc_se_new>0
order by type,sector_id,mgra,jur_id,cpa_id,yr;
quit;

data jc_2; set jc_1;by type sector_id mgra jur_id cpa_id;retain c;
if first.cpa_id then do; j1=j; c=j1;end;
else do;j1=j-c; c=j;end;
run;

data jc_3(drop=j1 j i);set jc_2(drop=c);where j1>0;
do i=1 to j1;
	r=ranuni(2018);
	output;
end;
run;

proc sort data=jc_3;by type sector_id yr r;run;

data jc_4(drop=r);set jc_3;by type sector_id;retain id;
if first.sector_id then id=1;else id=id+1;
run;

proc sql;
create table jc_5 as select y.yr,x.type,x.mgra,x.jur_id,x.cpa_id,x.sector_id,x.id
from jc_4 as x
cross join usj_1a as y where x.yr<=y.yr
order by type,sector_id,id,yr;

create table jc_5a as select type,sector_id,yr,count(*) as jc
from jc_5 group by type,sector_id,yr;

create table jc_5b as select sector_id,yr,count(*) as jc
from jc_5 group by sector_id,yr;

create table jc_5c as select yr,count(*) as jc
from jc_5 group by yr;
quit;


proc sql;
create table jc_5d as select x.*,y.jc_drop
from (select type,yr,sector_id,max(id) as max_id from jc_5 group by type,yr,sector_id) as x
left join usj_7 as y on x.type = y.type and x.yr = y.yr and x.sector_id = y.sector_id
order by type,sector_id,yr;
quit;


proc sql;
create table jc_6 as select x.*,y.jc_drop
,case when y.jc_drop > 0 and x.id <= y.jc_drop then 1 else 0 end as drop
from jc_5 as x
left join usj_7 as y on x.type = y.type and x.yr = y.yr and x.sector_id = y.sector_id
order by type,sector_id,yr,id;

create table jc_6a as select * from jc_6 where drop=1
order by type,sector_id,yr,id;

create table jc_6b as select type,sector_id,id,min(yr) as min_yr,count(yr) as n
from jc_6 where drop=0 group by type,sector_id,id;

create table jc_6c as select min_yr,n,count(*) as m
from jc_6b group by min_yr,n;
quit;


proc sql;
create table test_0 as select * from jc_6c where
(min_yr = 2018 and n ^= 8) or
(min_yr = 2020 and n ^= 7) or
(min_yr = 2025 and n ^= 6) or
(min_yr = 2030 and n ^= 5) or
(min_yr = 2035 and n ^= 4) or
(min_yr = 2040 and n ^= 3) or
(min_yr = 2045 and n ^= 2) or
(min_yr = 2050 and n ^= 1);
quit;

proc sql;
create table test_1 as select * from jc_6b where min_yr=2018 and n=1;
create table test_2 as select * from jc_6 where type = "se" and sector_id = 5 and id = 4;
quit;

proc sql;
create table e1. as select yr,type,mgra,jur_id,cpa_id,sector_id,id
from jc_6 where drop = 0;
quit;

