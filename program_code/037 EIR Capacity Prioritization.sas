/* BEFORE RUNNING THIS FILE, MAKE SURE TO UPDATE THE VARIABLE rcver IN THE VARIABLES FILE */

/* References the 'Variables and Libaries' file */
%let a=%sysget(SAS_EXECFILEPATH);
%let b=%sysget(SAS_EXECFILENAME);
%let valib=%sysfunc(tranwrd(&a,&b,_ Variables and Libraries.sas));
%include "&valib";

/* Pulls parcel capacity and Scheduled Development site capacity by parcel */
proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table eir_parcel_0 as select *
from connection to odbc
(
SELECT e.*, p.centroid
FROM [urbansim].[urbansim].[eir_parcel] e
INNER JOIN [urbansim].[urbansim].[parcel] p
	on p.parcel_id = e.parcel_id
WHERE eir_scenario_id = &eirver
)
order by parcel_id;

create table rhna_target_0 as select *
from connection to odbc
(
SELECT
	[jurisdiction_id]
	,[jurisdiction]
	,[units_total_rhna6] as target
FROM [urbansim].[ref].[rhna_6th_housing_cycle]
)
order by jurisdiction_id;

create table adu_parcel_0 as select *
from connection to odbc
(
SELECT * 
FROM urbansim.urbansim.additional_capacity 
WHERE version_id = 111 and type = 'adu'
)
order by parcel_id;
disconnect from odbc;

quit;


/* Pull fire hazard data by parcel */
proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table fz_1 as select *
from connection to odbc
(
select a.parcel_id,a.du_cap,b.haz_class
from
(
select e.parcel_id,p.centroid,e.scenario_cap as du_cap
from [urbansim].[urbansim].[eir_parcel] e
INNER JOIN [urbansim].[urbansim].[parcel] p
	on p.parcel_id = e.parcel_id
WHERE eir_scenario_id = &eirver
) as a
inner join isam.dbo.fire_hazard_severity_zones as b
on a.centroid.STIntersects(b.shape) = 1
where a.du_cap > 0
)
;

/* Pulls parcel-specific jurisdiction and cpa */
create table cap_1 as select *,int(jur_cpa/100) as jur_id
,case when int(jur_cpa/100) * 100 = jur_cpa then 0 else jur_cpa end as cpa_id
from connection to odbc
(
select a.*,b.mgra,b.jur_&by1 as jur_cpa 
from
(
select e.parcel_id,p.centroid,e.scenario_cap as du_cap
from [urbansim].[urbansim].[eir_parcel] e
INNER JOIN [urbansim].[urbansim].[parcel] p
	on p.parcel_id = e.parcel_id
WHERE eir_scenario_id = &eirver
) as a
inner join (select mgra,jur_&by1,geometry::UnionAggregate(shape) as shape from [estimates].[dbo].[BLK2010_JUR_POST2010]
	group by mgra,jur_&by1) as b on a.centroid.STIntersects(b.shape) = 1

where a.du_cap > 0
)
;

/* Creates a parcel-level flag for the County Water Authority Boundary */
create table cwa_1 as select *
from connection to odbc
(
select a.parcel_id,a.du_cap
from
(
select e.parcel_id,p.centroid,e.scenario_cap as du_cap
from [urbansim].[urbansim].[eir_parcel] e
INNER JOIN [urbansim].[urbansim].[parcel] p
	on p.parcel_id = e.parcel_id
WHERE eir_scenario_id = &eirver
) as a
inner join (select shape
from openquery(sql2014b8,
'SELECT [Shape]
  FROM [lis].[gis].[CountyWaterAuthority]'
)) as b
on a.centroid.STIntersects(b.shape) = 1
where a.du_cap > 0
)
;

disconnect from odbc;
quit;

/* Joins fire hazard info to parcel-level geography info */
proc sql;
create table fz_2 as select x.*,y.haz_class
from cap_1 as x
left join fz_1 as y on x.parcel_id=y.parcel_id;

update fz_2 set haz_class = "Urban Unzoned" where parcel_id in (9556,9002470) and haz_class="";

create table fz_2a as select haz_class,sum(du_cap) as du_cap,count(parcel_id) as n
from fz_2 group by haz_class;
quit;

/* Joins CWA info to parcel-level geography info, and includes Coronado */
proc sql;
create table cwa_2 as select x.*,
CASE WHEN y.parcel_id is not null then 1
WHEN (x.jur_id = 3 AND y.du_cap > 0) then 1
ELSE 0 END as cwa_access
from cap_1 as x
left join cwa_1 as y on x.parcel_id=y.parcel_id;

create table cwa_2a as select cwa_access,sum(du_cap) as du_cap,count(parcel_id) as n
from cwa_2 group by cwa_access;
quit;

/* Pulls the HU target from the listed file. Please see 'HU Construction files - Note.txt' */
proc import out=hu_2
datafile="T:\socioec\Current_Projects\&xver\input_data\HU Construction and PPH projections_Feb2020.xlsx"
replace dbms=excelcs; RANGE='Sheet1$r1:s36'n;
run;


/* Pulls parcel-level data for distance to rail / rapid bus lines (in miles) */
proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table pc_rail_1 as select parcel_id,du_cap,round(min(dist/5280),0.01) as dist_rail
from connection to odbc
(
select a.*,a.centroid.STDistance(b.shape) as dist
from
(
select e.parcel_id,p.centroid,e.scenario_cap as du_cap
from [urbansim].[urbansim].[eir_parcel] e
INNER JOIN [urbansim].[urbansim].[parcel] p
	on p.parcel_id = e.parcel_id
WHERE eir_scenario_id = &eirver
) as a
cross join isam.dbo.rail_stops as b
where a.du_cap > 0
)
group by parcel_id,du_cap;

create table pc_rapid_1 as select parcel_id,du_cap,round(min(dist/5280),0.01) as dist_rapid
from connection to odbc
(
select a.*,a.centroid.STDistance(b.shape) as dist
from
(
select e.parcel_id,p.centroid,e.scenario_cap as du_cap
from [urbansim].[urbansim].[eir_parcel] e
INNER JOIN [urbansim].[urbansim].[parcel] p
	on p.parcel_id = e.parcel_id
WHERE eir_scenario_id = &eirver
) as a
cross join isam.dbo.rapid_stops as b
where a.du_cap > 0
)
group by parcel_id,du_cap;


disconnect from odbc;
quit;

/* Combine Rail and Rapid distance measures into one table */
proc sql;
create table pc_rail_rapid_1 as select x.*,y.dist_rapid
from pc_rail_1 as x
inner join pc_rapid_1 as y on x.parcel_id=y.parcel_id;

quit;

/* Pulls distance-to-coast measure by parcel */
proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table dist_to_coast as select *
from connection to odbc
(
SELECT e.[parcel_id]
	,[distance_to_coast] as dist_coast
FROM [urbansim].[urbansim].[eir_parcel] e
INNER JOIN [urbansim].[urbansim].[parcel] p
	on p.parcel_id = e.parcel_id
WHERE eir_scenario_id = &eirver
);
/* these distances reflect feet from coast, hence the very large numbers (e.g. 95000 is the east side of Poway ~ 18 miles)  */
disconnect from odbc;
quit;

/* This table combines all of the above metrics into one table at a parcel level:
Original Capacity
Rail, Rapid, Fire Hazard and Distance to Coast original metrics
Jur / Cpa information
Scheduled Development site_id and sched_dev capacity
Scoring (lower is more preferred):
Score 1 (Binary set to 1 for 'Very High' fire hazard, 0 otherwise )
Score 2 (Binary set to 1 when more than 5 miles from a rapid or rail stop, 0 otherwise)
Score 3 (Binary set to 1 when outside the CWA, 0 otherwise)
Total score
*/
proc sql;
create table pc_3 as select x.parcel_id,x.du_cap as du_cap_original
,x.du_cap /* this is where the below section was removed */
,x.dist_rail,x.dist_rapid,y.haz_class,z.cap_jurisdiction_id as jur_id
,y.cpa_id,y.jur_cpa
,d.dist_coast
,coalesce(z.site_id,0) as sd_site_id
,coalesce(z.capacity_3,0) as sd
,case when y.haz_class = "Very High" then 1 else 0 end as score_1 /* location inside Very High Fire Hazard zone */
,case when x.dist_rail > 5 and x.dist_rapid > 5 then 1 else 0 end as score_2 /* location away from transit */
,case when c.cwa_access = 1 then 0 else 1 end as score_3 /* in CWA shape, or in Coronado (jur_id = 4) */
,calculated score_1 + calculated score_2 + calculated score_3 as total_score
from eir_parcel_0 as z
inner join pc_rail_rapid_1 as x on x.parcel_id=z.parcel_id
inner join fz_2 as y on x.parcel_id=y.parcel_id
left join cwa_2 as c on x.parcel_id=c.parcel_id
left join dist_to_coast as d on x.parcel_id=d.parcel_id;

/* At some point questions about how much capacity received certain scores arose, so I began outputting the above table to the database */
delete from urb.urbansim_reduced_cap_scoring where version_id=&rcver;
insert into urb.urbansim_reduced_cap_scoring(bulkload=yes bl_options=TABLOCK) 
	select parcel_id,du_cap,dist_rail,dist_rapid,haz_class,jur_id,cpa_id,jur_cpa,sd_site_id,sd,score_1,score_2,total_score
	,&rcver as version_id 
	,score_3,dist_coast,du_cap_original from pc_3;
quit;

/* The below steps takes jurisdiction-level targets and assigns them to the 'more favorable' parcels (with lower scores) */
proc sql;
create table pc_4 as select a.*
,case when a.cpa_id = 0 then a.jur_id else a.cpa_id end as geo_id
,c.parcel_id,c.scenario_cap,a.total_score,b.target
,c.cap_priority
from pc_3 a
inner join rhna_target_0 b on a.jur_id = b.jurisdiction_id
inner join eir_parcel_0 c on a.parcel_id = c.parcel_id
order by a.jur_id,c.cap_priority,a.total_score,ranuni(&by1);

create table jb_0 as select
cap_jurisdiction_id, sum(baseline_cap) as baseline_cap
from eir_parcel_0
group by cap_jurisdiction_id;
quit;

data pc_5;set pc_4; by jur_id; retain tc;
if first.jur_id then do; duc2 = min(scenario_cap,target); tc=duc2; end;
else do; duc2 = min(scenario_cap,(target-tc));tc=tc+duc2; end;
run;

proc sql;
create table ps_1 as select 
jur_id, geo_id, parcel_id, scenario_cap as capacity
from pc_5
where duc2 > 0;

create table rhna_target_1 as select sum(capacity) as cap1
from ps_1;

create table hu_3 as select sum(hu_g) as hu_t
from hu_2 
where yr_built_during >2017;

create table hu_4 as select a.hu_t - b.cap1 as hu_t2
from hu_3 a
cross join rhna_target_1 b;

create table eir_jur_0 as select
cap_jurisdiction_id
from jb_0 a
inner join rhna_target_0 b
	on a.cap_jurisdiction_id = b.jurisdiction_id
where a.baseline_cap < b.target;

create table pc_6 as select *
from pc_5
where parcel_id not in (select parcel_id from ps_1)
and jur_id not in (select cap_jurisdiction_id from eir_jur_0);

create table test_0 as select distinct(jur_id) as jur_id from pc_6;
quit;

/* This table aggregates available capacity based on the binary scoring metrics defined above */
proc sql;
create table jur_1 as select x.*
,coalesce(x2.sched_cap,0) as sched_cap
,coalesce(y.transit_cap,0) as transit_cap
,coalesce(z.nofirehaz_cap,0) as nofirehaz_cap
,coalesce(c.cwa_cap,0) as cwa_cap
from (select jur_id,sum(scenario_cap) as total_cap from pc_6 where sd_site_id = 0 group by jur_id) as x
left join (select jur_id,sum(scenario_cap) as sched_cap from pc_6 where sd_site_id > 0 group by jur_id) as x2 on x.jur_id=x2.jur_id
left join (select jur_id,sum(scenario_cap) as transit_cap from pc_6
where sd_site_id = 0 and (dist_rail <= 5 or dist_rapid <= 5) group by jur_id) as y
	on x.jur_id=y.jur_id
left join (select jur_id,sum(scenario_cap) as nofirehaz_cap from pc_6
where sd_site_id=0 and haz_class <> "Very High" group by jur_id) as z
	on x.jur_id=z.jur_id
left join (select jur_id,sum(scenario_cap) as cwa_cap from pc_6
where score_3=0 and sd_site_id = 0group by jur_id) as c
	on x.jur_id=c.jur_id;

/* This is where the weighting of these metrics is used to decide how much capacity is assigned to each jurisdiction */
/* The first term is the total non-scheduled_dev capacity, the second is the lower of sched_dev or non-sched_dev capacity */
create table jur_2 as select *
,round(total_cap * 0.45 + min(sched_cap,total_cap) * 0.35 + transit_cap * 0.05 + nofirehaz_cap * 0.1 + cwa_cap * 0.05, 1) as c1
from jur_1;

/* This table takes the 'targets' generated by the weighting in the above table and creates reallocation metrics */
create table jur_3 as select *,total_cap - c1 as c2, sum(c1) as c1_sum,sum(calculated c2) as c2_sum
,calculated c2 / sum(calculated c2) as s2  format=percent8.2
,sum(sched_cap) as sc2
from jur_2;
quit;

/* Using the regional target and jurisdictional reallocation metrics, the below steps determine jurisdiction-level targets */
proc sql;
create table jur_4 as select x.*
,y.hu_t2 - sc2 as target_1, y.hu_t2 - sc2 - c1_sum as target_2
,x.c1 + round(calculated target_2 * x.s2) as hu0
from jur_3 as x
cross join hu_4 as y
order by hu0;
quit;

data jur_5;set jur_4;
hc+hu0;
run;

proc sort data=jur_5;by descending hu0;run;

data jur_6;set jur_5;
if _n_ = 1 then hu1 = hu0 + (target_1 - hc) + sched_cap;
else hu1 = hu0 + sched_cap;
run;

proc sql;
create table pc_7 as select
	a.jur_id, a.geo_id, a.parcel_id, a.scenario_cap, a.cap_priority, a.total_score,
	b.hu1
from pc_6 a
left join jur_6 b
	on a.jur_id = b.jur_id
order by jur_id, cap_priority, total_score, ranuni(&by1);
quit;


data pc_8;set pc_7; by jur_id; retain tc2;
if first.jur_id then do; duc3 = min(scenario_cap,hu1); tc2=duc3; end;
else do; duc3 = min(scenario_cap,(hu1-tc2));tc2=tc2+duc3; end;
run;

proc sql;
/*
1. Take out selected parcels and find total cap
2. subtract total cap from model target
3. remove parcels in jurs with cap_priority = 3 parcels
4. select remaining cap from remaining parcels
*/
create table pc_9 as select 
&rcver as version_id,
jur_id, geo_id, parcel_id, scenario_cap as capacity
from pc_8 where duc3 > 0
union all
select 
&rcver as version_id, * from ps_1;

create table test_02 as select sum(capacity) as tot_cap from pc_9;

create table test_03 as select a.jur_id, a.jur_cap 
,b.target, jur_cap / sum(jur_cap) as regional_percent
from (select jur_id, sum(capacity) as jur_cap from pc_9 group by jur_id) a
inner join rhna_target_0 b on a.jur_id = b.jurisdiction_id
order by jur_id;

quit;
proc sql;
create table test_04 as select a.parcel_id,a.capacity_2,a.capacity_3,a.scenario_cap,a.cap_priority
,b.capacity,
case when b.capacity > a.scenario_cap then 0 else 1 end as diff
,a.cap_jurisdiction_id,b.jur_id
from eir_parcel_0 a
left join pc_9 b
	on a.parcel_id = b.parcel_id;

create table test_05 as select *
from test_04 where jur_id <> cap_jurisdiction_id
and jur_id is not null;
quit;

proc sql;

delete from urb.urbansim_reduced_capacity where version_id=&rcver;

insert into urb.urbansim_reduced_capacity(bulkload=yes bl_options=TABLOCK) select * from pc_9;
quit;
