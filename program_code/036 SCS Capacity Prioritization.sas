/* BEFORE RUNNING THIS FILE, MAKE SURE TO UPDATE THE VARIABLE rcver IN THE VARIABLES FILE */

/* References the 'Variables and Libaries' file */
%let a=%sysget(SAS_EXECFILEPATH);
%let b=%sysget(SAS_EXECFILENAME);
%let valib=%sysfunc(tranwrd(&a,&b,_ Variables and Libraries.sas));
%include "&valib";

/* Pulls SCS parcel capacity and other information */
proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table scs_parcel_0 as select *
,case when mohub is null then 'none' else mohub end as mohub2
from connection to odbc
(
SELECT 
	[parcel_id]
	,[scs_site_id]
	,[mgra]
	,[cap_jurisdiction_id]
	,[mohub]
	,[score]
	,[scenario_cap]
	,[cap_priority]
FROM [urbansim].[urbansim].[scs_parcel]
WHERE scenario_id = &scsver
)
order by parcel_id;

create table scs_target_0 as select *
,case when mohub is null then 'none' else mohub end as mohub2
from connection to odbc
(
SELECT
	[jur_id]
	,[mohub]
	,[scs_priority1_cap]
	,[scs_priority2_cap]
	,[scs_priority2_max]
	,[scs_distributed_tier2_cap]
FROM [urbansim].[urbansim].[scs_tier2_scenario]
WHERE scs_tier2_scenario_id = &scsver
)
order by jur_id, mohub;

disconnect from odbc;

quit;

/* This tests to make sure the target table agrees with parcel table on p1 and p2 capacity sums, both tables should have 0 rows */
proc sql;
create table z_jur_test_0 as select
	a.jur_id
	,target_priority1_cap
	,parcel_priority1_cap
	,target_priority2_cap
	,parcel_priority2_cap
from (
select jur_id
	,sum(scs_priority1_cap) as target_priority1_cap
	,sum(scs_priority2_cap) as target_priority2_cap
from scs_target_0 group by jur_id) a
inner join (
select cap_jurisdiction_id as jur_id
	,sum(case when cap_priority = 1 then scenario_cap else 0 end) as parcel_priority1_cap
	,sum(case when cap_priority = 2 then scenario_cap else 0 end) as parcel_priority2_cap
from scs_parcel_0 group by jur_id) b
on a.jur_id = b.jur_id
where (target_priority1_cap ^= parcel_priority1_cap) OR (target_priority2_cap ^= parcel_priority2_cap)
order by a.jur_id;

create table z_mohub_test_0 as select
	a.mohub
	,target_priority1_cap
	,parcel_priority1_cap
	,target_priority2_cap
	,parcel_priority2_cap
from (
select mohub
	,sum(scs_priority1_cap) as target_priority1_cap
	,sum(scs_priority2_cap) as target_priority2_cap
from scs_target_0 group by mohub) a
inner join (
select mohub
	,sum(case when cap_priority = 1 then scenario_cap else 0 end) as parcel_priority1_cap
	,sum(case when cap_priority = 2 then scenario_cap else 0 end) as parcel_priority2_cap
from scs_parcel_0 group by mohub) b
on a.mohub = b.mohub
where (target_priority1_cap ^= parcel_priority1_cap) OR (target_priority2_cap ^= parcel_priority2_cap)
order by a.mohub;

quit;

proc sql;

create table jur_mohub_ids_0 as select *
from (
select distinct(cap_jurisdiction_id) from scs_parcel_0) a
cross join (
select distinct(mohub2) from scs_parcel_0) b;

quit;

data jur_mohub_ids_1;set jur_mohub_ids_0; by cap_jurisdiction_id mohub2;
if first.mohub2 then id + 1;
run;

proc sql;

create table tier1_parcels as select
parcel_id
,scs_site_id
,scenario_cap
,cap_priority
from scs_parcel_0
where cap_priority = 1;

create table tier2_ranking_0 as select c.id, a.*, b.scs_distributed_tier2_cap as at
from scs_parcel_0 a
left join scs_target_0 b
on a.cap_jurisdiction_id = b.jur_id and a.mohub2 = b.mohub2
left join jur_mohub_ids_1 c
on a.cap_jurisdiction_id = c.cap_jurisdiction_id and a.mohub2 = c.mohub2
where a.cap_priority = 2
order by id, floor(score) desc, ranuni(&by1);

create table tier2_ranking_1 as select *
from tier2_ranking_0
where at > 0;

quit;


data tier2_ranking_2;set tier2_ranking_1;by id;retain tc;
if first.id then do;c1=min(scenario_cap,at);tc=c1;end;
else do;c1=min(scenario_cap,(at-tc));tc=tc+c1;end;
run;

proc sql;

create table tier2_parcels as select
parcel_id,mgra,cap_jurisdiction_id,mohub,score,scenario_cap,cap_priority,
c1 as target_cap
from tier2_ranking_2
where c1 > 0;

quit;

proc sql;

create table scs_scenario_parcels as select 
&scsver as scenario_id
,parcel_id
,scs_site_id
,scenario_cap
,cap_priority
from tier1_parcels

union all

select 
&scsver as scenario_id
,parcel_id
,. as scs_site_id
,target_cap
,cap_priority
from tier2_parcels;

quit;

/* Pulls the HU target from the listed file. Please see 'HU Construction files - Note.txt' */
proc import out=hu_2
datafile="T:\socioec\Current_Projects\&xver\input_data\HU Construction and PPH projections_Feb2020.xlsx"
replace dbms=excelcs; RANGE='Sheet1$r1:s36'n;
run;

proc sql;
create table z_test0 as select 
sum(hu_g) as target_cap
from hu_2 where yr_built_during >= &by1;

create table z_test1 as select sum(scenario_cap) as scenario_cap from scs_scenario_parcels;
create table z_test2 as select cap_priority, sum(scenario_cap) as scenario_cap from scs_scenario_parcels group by cap_priority;
quit;


proc sql;

delete from urb.scs_scenario_parcels where scenario_id=&scsver;

insert into urb.scs_scenario_parcels(bulkload=yes bl_options=TABLOCK) select * from scs_scenario_parcels;
quit;
