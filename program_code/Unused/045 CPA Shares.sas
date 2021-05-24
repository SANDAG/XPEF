/* BEFORE RUNNING THIS FILE, MAKE SURE TO UPDATE THE VARIABLE scver IN THE VARIABLES FILE */

/* References the 'Variables and Libaries' file */
%let a=%sysget(SAS_EXECFILEPATH);
%let b=%sysget(SAS_EXECFILENAME);
%let valib=%sysfunc(tranwrd(&a,&b,_ Variables and Libraries.sas));
%include "&valib";

%let phver = 155; /* phase_yr_version_id in [urbansim].[urbansim].[urbansim_lite_parcel_control] */

/* Pulls the subregional control allocation used for datasource 17 as a baseline */
/* Applys parcel-level phasing and sets missing phase years to 2018 */
proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table cpa_target_0 as select *
from connection to odbc
(select * from [urbansim].[urbansim].[urbansim_lite_subreg_control] where subregional_crtl_id=209);

create table sch_dev_0 as select *, int(jur_cpa/100) as jur_id
,case when int(jur_cpa/100) * 100 = jur_cpa then 0 else jur_cpa end as cpa_id
,case when int(jur_cpa/100) * 100 = jur_cpa then int(jur_cpa/100) else jur_cpa end as geo_id
from connection to odbc
(select x.parcel_id, x.capacity_3 as sd, y.jur_&by1 as jur_cpa
from [urbansim].[urbansim].[scheduled_development_parcel] as x
inner join (select jur_&by1,geometry::UnionAggregate(shape) as shp from [estimates].[dbo].[BLK2010_JUR_POST2010]
	group by jur_&by1) as y
on x.shape.STCentroid().STIntersects(y.shp) = 1
where x.capacity_3 > 0);

create table cap_0 as select *
from connection to odbc
(select * from [urbansim].[urbansim].[urbansim_reduced_capacity] where version_id = &rcver);

create table reg_target_0 as select *
from connection to odbc
(select * from [urbansim].[urbansim].[urbansim_target_housing_units] where version_id = &thuver);


create table sdy_0 as select *
from connection to odbc
(
SELECT parcel_id,phase_yr
FROM [urbansim].[urbansim].[urbansim_lite_parcel_control]
where phase_yr_version_id = &phver and capacity_type = 'sch'
)
;

update sdy_0 set phase_yr = &by1 where phase_yr = &by1 - 1;

disconnect from odbc;
quit;

proc sql;
create table sd_test_1 as select x.*
from sch_dev_0 as x
inner join (select parcel_id,count(parcel_id) as n from sch_dev_0 group by parcel_id having calculated n > 1) as y
on x.parcel_id=y.parcel_id
order by parcel_id;
quit;

proc sql;
create table sdy_test_1 as select x.*
from sdy_0 as x
inner join (select parcel_id,count(parcel_id) as n from sdy_0 group by parcel_id having calculated n > 1) as y
on x.parcel_id=y.parcel_id
order by parcel_id,phase_yr;
quit;


proc sql;
create table cpa_target_1 as select x.yr,x.geo_id,x.control as s0,y.housing_units_add as reg_target
,round(x.control * reg_target,1) as hu0
from cpa_target_0 as x
inner join reg_target_0 as y on x.yr=y.yr
where x.yr >= &by1
order by yr,hu0;
quit;

data cpa_target_2; set cpa_target_1;by yr; retain hc;
if first.yr then do; hu1 = hu0; hc = hu1; end;
else if last.yr then do; hu1 = reg_target - hc; hc = hu1 + hc; end;
else do; hu1 = hu0; hc = hu1 + hc; end;
run;

proc sql;
/* if phase_yr is missing it is set to 2018 */
create table sch_dev_1 as select x.*,coalesce(y.phase_yr, &by1) as phase_yr
from sch_dev_0 as x
left join sdy_0 as y on x.parcel_id=y.parcel_id;

create table sch_dev_2 as select geo_id,phase_yr,sum(sd) as sd
from sch_dev_1 group by geo_id,phase_yr;
quit;

data sch_dev_3; set sch_dev_2; by geo_id; retain sd1;
if first.geo_id then sd1 = sd; 
else sd1 = sd1 + sd;
run;


proc sql;
create table cpa_target_3 as select x.yr, x.geo_id, x.s0, x.hu1
,coalesce(y.cap,0) as cap
,z.sd1
from cpa_target_2 as x
left join (select geo_id,sum(capacity) as cap from cap_0 group by geo_id) as y
	on x.geo_id=y.geo_id
left join sch_dev_3 as z
	on x.geo_id=z.geo_id and x.yr=z.phase_yr
order by geo_id,yr;
quit;

data cpa_target_3a;set cpa_target_3;retain sd2;
if sd1^=. then sd2=sd1;
tc = cap + sd2; /* total capacity */
run;


data cpa_target_4; set cpa_target_3a; by geo_id; retain huc;
if first.geo_id then huc = hu1;
else huc = hu1 + huc;

tcl = tc - huc;

if tcl >= 0 then hu2 = hu1;
else if (hu1 + tcl)  > 0 then hu2 = hu1 + tcl;
else hu2 = 0;
run;



proc sql;
create table cpa_target_4a as select yr,geo_id,tcl
from cpa_target_4 where yr = 2050 and tcl > 0;

create table cpa_target_4b as select x.*,y.housing_units_add as reg_target, y.housing_units_add - x.hu2 as d
from (select yr,sum(hu2) as hu2 from cpa_target_4 group by yr) as x
inner join reg_target_0 as y on x.yr=y.yr
order by yr;
quit;

data slots_1(drop=d i);set cpa_target_4b(drop=hu2 reg_target);
do i=1 to d;
	output;
end;
run;

data candidates_1(drop=tcl i);set cpa_target_4a(drop=yr);
do i=1 to tcl;
	rn=ranuni(&by1);
	output;
end;
run;

proc sort data=candidates_1;by rn;run;

data candidates_1;set candidates_1; i= _n_; run;

data slots_1;set slots_1; i = _n_; run;

proc sql;
create table slots_2 as select x.yr,y.geo_id
from slots_1 as x
inner join candidates_1 as y on x.i=y.i;

create table slots_3 as select yr,geo_id,count(*) as n
from slots_2 group by yr,geo_id;
quit;

proc sql;
create table cap_target_5 as select x.yr,x.geo_id,x.hu2 + coalesce(y.n,0) as hu3
from cpa_target_4 as x
left join slots_3 as y on x.yr=y.yr and x.geo_id=y.geo_id
order by yr,geo_id;
quit;


proc sql;
create table cap_target_new as select
&scver as subregional_crtl_id,yr,"jur_and_cpa" as geo,geo_id
,hu3/sum(hu3) as control
,"percentage" as control_type
,"" as max_units
,"random_test" as scenario_desc
from cap_target_5 group by yr
order by yr,geo_id;
quit;

proc sql;
create table test_1 as select yr,sum(control) as c
from cap_target_new group by yr;
quit;



proc sql;
delete * from urb.urbansim_lite_subreg_control where subregional_crtl_id = &scver;

insert into urb.urbansim_lite_subreg_control(bulkload=yes bl_options=TABLOCK) select * from cap_target_new;
quit;
