/* BEFORE RUNNING THIS FILE, MAKE SURE TO UPDATE THE VARIABLE rcver IN THE VARIABLES FILE */

/* References the 'Variables and Libaries' file */
%let a=%sysget(SAS_EXECFILEPATH);
%let b=%sysget(SAS_EXECFILENAME);
%let valib=%sysfunc(tranwrd(&a,&b,_ Variables and Libraries.sas));
%include "&valib";

/* Pulls parcel capacity and Scheduled Development site capacity by parcel */
proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table cap_0 as select parcel_id,sum(capacity_2) as du_cap
from connection to odbc
(
SELECT [parcel_id],[capacity_2]
FROM [urbansim].[urbansim].[parcel] where capacity_2 > 0
)
group by parcel_id
order by parcel_id;

create table sd_0 as select *
from connection to odbc
(
SELECT [parcel_id],site_id,sum(capacity_3) as sd
FROM [urbansim].[urbansim].[scheduled_development_parcel]
group by parcel_id,site_id
);

disconnect from odbc;

create table cap_0a as select sum(du_cap) as du_cap,count(parcel_id) as n
FROM cap_0;

quit;


proc sql;
/* this table should zero records */
create table sd_0b as select parcel_id,count(parcel_id) as n
from sd_0 group by parcel_id having calculated n>1;

create table sd_0c as select * from sd_0 where sd=0;
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
select parcel_id,centroid,capacity_2 as du_cap
from [urbansim].[urbansim].[parcel]
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
select parcel_id,centroid,capacity_2 as du_cap
from [urbansim].[urbansim].[parcel]
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
select parcel_id,centroid,capacity_2 as du_cap
from [urbansim].[urbansim].[parcel]
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
/* 
Note: During PRP 50.3 I had claimed that Coronado was included (jur_id = 3), but there was a typo. 
I had instead included Del Mar (jur_id = 4), which is already included in the boundary.
This fix was not implemented until AFTER XPEF29 (and may be advisable to remove, if it results
in a significant increase in Coronado's allocation)
*/
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

/* Removes total scheduled development from the total HU targets */
proc sql;
create table target_2 as select x.hu_target, y.sched_dev, x.hu_target - y.sched_dev as new_target
from (select sum(hu_g) as hu_target from hu_2 where yr_built_during >= &by1) as x
cross join (select sum(sd) as sched_dev from sd_0) as y;
quit;

/* Pulls parcel-level data for distance to rail / rapid bus lines (in miles) */
proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table pc_rail_1 as select parcel_id,du_cap,round(min(dist/5280),0.01) as dist_rail
from connection to odbc
(
select a.*,a.centroid.STDistance(b.shape) as dist
from
(
select parcel_id,centroid,capacity_2 du_cap
from [urbansim].[urbansim].[parcel]
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
select parcel_id,centroid,capacity_2 as du_cap
from [urbansim].[urbansim].[parcel]
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
SELECT [parcel_id]
	,[distance_to_coast] as dist_coast
FROM [urbansim].[urbansim].[parcel]
where capacity_2>0
);
/* these distances reflect feet from coast, hence the very large numbers (e.g. 95000 is the east side of Poway ~ 18 miles)  */
disconnect from odbc;
quit;

/* This table combines all of the above metrics into one table at a parcel level:
Original Capacity
Removed: Distance-scaled capacity (scaled from 100% just east of Poway down to a minimum of 5% near the Eastern county border) 
	used to limit Desert HU growth pre-2020 DOF, see commented-out section below table
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
,x.dist_rail,x.dist_rapid,y.haz_class,y.jur_id,y.cpa_id,y.jur_cpa,d.dist_coast
,coalesce(z.site_id,0) as sd_site_id
,coalesce(z.sd,0) as sd
,case when y.haz_class = "Very High" then 1 else 0 end as score_1 /* location inside Very High Fire Hazard zone */
,case when x.dist_rail > 5 and x.dist_rapid > 5 then 1 else 0 end as score_2 /* location away from transit */
,case when c.cwa_access = 1 then 0 else 1 end as score_3 /* in CWA shape, or in Coronado (jur_id = 4) */
,calculated score_1 + calculated score_2 + calculated score_3 as total_score
from pc_rail_rapid_1 as x
inner join fz_2 as y on x.parcel_id=y.parcel_id
left join sd_0 as z on x.parcel_id=z.parcel_id
left join cwa_2 as c on x.parcel_id=c.parcel_id
left join dist_to_coast as d on x.parcel_id=d.parcel_id;

/* This is removed because it is causing very dramatic declines in east county housing units with new DOF targets */
/*,CASE WHEN z.site_id > 0 THEN x.du_cap /* don't scale sched dev (but shouldnt make a diff since set to 0 below) */
/*	WHEN d.dist_coast < 95000 THEN x.du_cap /* east edge of Poway, don't scale cap down west to coast*/
/*	WHEN d.dist_coast > 350000 THEN round(x.du_cap * 0.05, 1) /* minimum cap scaling is 5% (county parcels w/ cap go out to ~355300) */
/*	ELSE round((x.du_cap * (((-0.95/255000) * d.dist_coast) + (1 + (0.95/255000) * 95000))), 1) END as du_cap
	/* scale cap down linearly from 95000 to 350000, this is a basic y=mx+b using those distances */

/* At some point questions about how much capacity received certain scores arose, so I began outputting the above table to the database */
delete from urb.urbansim_reduced_cap_scoring where version_id=&rcver;
insert into urb.urbansim_reduced_cap_scoring(bulkload=yes bl_options=TABLOCK) 
	select parcel_id,du_cap,dist_rail,dist_rapid,haz_class,jur_id,cpa_id,jur_cpa,sd_site_id,sd,score_1,score_2,total_score
	,&rcver as version_id 
	,score_3,dist_coast,du_cap_original from pc_3;

/* This sets the 'targetable' capacity to 0 if the parcel has a site_id (is scheduled development) */
update pc_3 set du_cap = 0 where sd_site_id > 0;
quit;

proc sql;
create table pc_3a as select * from pc_3 where sd_site_id > 0;
quit;

/* This table aggregates available capacity based on the binary scoring metrics defined above */
proc sql;
create table jur_1 as select x.*
,coalesce(x2.sched_cap,0) as sched_cap
,coalesce(y.transit_cap,0) as transit_cap
,coalesce(z.nofirehaz_cap,0) as nofirehaz_cap
,coalesce(c.cwa_cap,0) as cwa_cap
from (select jur_id,sum(du_cap) as total_cap from pc_3 where sd_site_id=0 group by jur_id) as x
left join (select jur_id,sum(sd) as sched_cap from pc_3 group by jur_id) as x2 on x.jur_id=x2.jur_id
left join (select jur_id,sum(du_cap) as transit_cap from pc_3
where sd_site_id = 0 and (dist_rail <= 5 or dist_rapid <= 5) group by jur_id) as y
	on x.jur_id=y.jur_id
left join (select jur_id,sum(du_cap) as nofirehaz_cap from pc_3
where sd_site_id=0 and haz_class <> "Very High" group by jur_id) as z
	on x.jur_id=z.jur_id
left join (select jur_id,sum(du_cap) as cwa_cap from pc_3
where score_3=0 group by jur_id) as c
	on x.jur_id=c.jur_id;

/* This is where the weighting of these metrics is used to decide how much capacity is assigned to each jurisdiction */
/* Originally (pre-CWA) each metric was given equal weight of 25%. Then, with CWA, each was given 20%. */
/* With the 2020 DOF, the much lower HU targets resulted in very little capacity from the Unincorporated County,
	so the weights were reallocated to result in ~10% of total capacity (including sched_dev) to be in the County */
/* The first term is the total non-scheduled_dev capacity, the second is the lower of sched_dev or non-sched_dev capacity */
create table jur_2 as select *
/*,round(total_cap * 0.25 + min(sched_cap,total_cap) * 0.25 + transit_cap * 0.25 + nofirehaz_cap * 0.25, 1) as c1*/
,round(total_cap * 0.35 + min(sched_cap,total_cap) * 0.35 + transit_cap * 0.1 + nofirehaz_cap * 0.1 + cwa_cap * 0.1, 1) as c1
from jur_1;

/* This table takes the 'targets' generated by the weighting in the above table and creates reallocation metrics */
create table jur_3 as select *,total_cap - c1 as c2, sum(c1) as c1_sum,sum(calculated c2) as c2_sum
,calculated c2 / sum(calculated c2) as s2  format=percent8.2
from jur_2;
quit;

/* Using the regional target and jurisdictional reallocation metrics, the below steps determine jurisdiction-level targets */

proc sql;
create table jur_4 as select x.*
,y.new_target as target_1, y.new_target - c1_sum as target_2
,x.c1 + round(calculated target_2 * x.s2) as hu0
from jur_3 as x
cross join target_2 as y
order by hu0;
quit;


data jur_5;set jur_4;
hc+hu0;
run;

proc sort data=jur_5;by descending hu0;run;

data jur_6;set jur_5;
if _n_ = 1 then hu1 = hu0 + (target_1 - hc);
else hu1 = hu0;
run;

/* The below steps takes jurisdiction-level targets and assigns them to the 'more favorable' parcels (with lower scores) */
/* Only parcels with (non-scheduled development) capacity are used, and all 'score 0' capacity is used before any 'score 1' capacity, etc */
proc sql;
create table pc_4 as select jur_id
,case when cpa_id = 0 then jur_id else cpa_id end as geo_id
,parcel_id,du_cap,total_score
from pc_3 where sd_site_id = 0
order by jur_id,total_score,ranuni(&by1);
quit;


data pc_5;set pc_4; by jur_id; retain duc2 i;
if first.jur_id then do; duc2 = du_cap; i = 1; end;
else do; duc2 = duc2 + du_cap; i = i + 1; end;
run;

proc sql;
create table pc_6 as select x.*, coalesce(y.duc2,0) as duc1
from pc_5 as x
left join pc_5 as y on x.jur_id=y.jur_id and x.i=y.i+1
order by jur_id,i;
quit;

proc sql;
create table pc_7 as select x.*, y.hu1
from pc_6 as x
inner join jur_6 as y on x.jur_id=y.jur_id
where x.duc2 <= y.hu1 or x.duc1 < y.hu1
order by jur_id,i;

create table pc_7a as select x.*
from pc_7 as x
inner join (select jur_id,max(i) as max_i from pc_7 group by jur_id) as y
on x.jur_id=y.jur_id and x.i=y.max_i
order by jur_id;
quit;

proc sql;
create table pc_8 as select jur_id,geo_id,parcel_id,du_cap
,case when hu1 >= duc2 then du_cap
else hu1 - duc1 end as du
from pc_7;

create table pc_8a as select * from pc_8 where du_cap <> du;
quit;

/* The parcel-level 'allowed capacities' are then loaded to a table in the database and given a version number */
proc sql;
create table pc_9 as select &rcver as version_id, jur_id, geo_id, parcel_id,du as capacity
from pc_8;

create table pc_9a as select x.*, y.hu1
from (select jur_id,sum(capacity) as capacity from pc_9 group by jur_id) as x
inner join jur_6 as y on x.jur_id=y.jur_id;

/* this table should have zero records */
create table pc_9b as select * 
from pc_9a where capacity <> hu1;
quit;


proc sql;

delete from urb.urbansim_reduced_capacity where version_id=&rcver;

insert into urb.urbansim_reduced_capacity(bulkload=yes bl_options=TABLOCK) select * from pc_9;
quit;
