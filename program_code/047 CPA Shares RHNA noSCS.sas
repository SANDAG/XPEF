/* BEFORE RUNNING THIS FILE, MAKE SURE TO UPDATE THE VARIABLE scver IN THE VARIABLES FILE */

/* References the 'Variables and Libaries' file */
%let a=%sysget(SAS_EXECFILEPATH);
%let b=%sysget(SAS_EXECFILENAME);
%let valib=%sysfunc(tranwrd(&a,&b,_ Variables and Libraries.sas));
%include "&valib";


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
(
select x.parcel_id, x.capacity_3 as sd, y.jur_&by1 as jur_cpa
from (select a.*, b.shape from [urbansim].[urbansim].[eir_parcel] a
inner join urbansim.urbansim.parcel b on a.parcel_id = b.parcel_id
where a.capacity_3 > 0 and a.eir_scenario_id = &eirver) as x
inner join (select jur_&by1,geometry::UnionAggregate(shape) as shp from [estimates].[dbo].[BLK2010_JUR_POST2010]
	group by jur_&by1) as y
on x.shape.STCentroid().STIntersects(y.shp) = 1);

create table cap_0 as select *
from connection to odbc
(SELECT 
	s.version_id
	,s.jur_id
	,CASE WHEN cpa_id = 0 then s.jur_id
		when cpa_id <> 0 and s.jur_id not in (14,19) then s.jur_id
		else cpa_id end as geo_id
	,s.[parcel_id]
	,x.cap_jurisdiction_id
	,x.site_id
	,s.capacity
FROM [urbansim].[urbansim].[urbansim_reduced_capacity] s
inner join [isam].[xpef04].[parcel2015_mgra_jur_cpa] p
	on s.parcel_id = p.parcel_id
inner join (select * from [urbansim].[urbansim].[eir_parcel] where eir_scenario_id = &eirver) x
	on x.parcel_id = s.parcel_id
where s.version_id = &rcver 
	and p.i = 1);

create table reg_target_0 as select *
from connection to odbc
(select * from [urbansim].[urbansim].[urbansim_target_housing_units] where version_id = &thuver);


create table sdy_0 as select *
from connection to odbc
(
/*
SELECT parcel_id,phase_yr
FROM [urbansim].[urbansim].[urbansim_lite_parcel_control]
where phase_yr_version_id =&phver and capacity_type = 'sch'
*/
SELECT
	parcel_id
	,case when phase_yr is null then startyear
		WHEN startyear > phase_yr THEN startyear
		ELSE phase_yr END as phase_yr
	,site_id
FROM (
SELECT
	coalesce(a.parcel_id,b.parcel_id) as parcel_id
	,case when year(startdate) > 2017 then year(startdate) else 2017 end as startyear
	,phase_yr
	,site_id
FROM (SELECT * FROM [urbansim].[urbansim].[urbansim_lite_parcel_control] where phase_yr_version_id = 155 and capacity_type = 'sch') a
FULL JOIN (SELECT * FROM urbansim.urbansim.eir_parcel where eir_scenario_id = &eirver and site_id is not null) b
	on a.parcel_id = b.parcel_id
) c
);

create table phase_0 as select *
from connection to odbc
(
SELECT
	parcel_id
	,case when (phase_yr is null and startyear is not null) then startyear
		when (phase_yr is null and startyear is null) then 2021
		WHEN startyear > phase_yr THEN startyear
		ELSE phase_yr END as phase_yr
	,phase_yr_version_id
	,capacity_type
	,site_id
FROM (
SELECT
	coalesce(a.parcel_id,b.parcel_id) as parcel_id
	,case when year(startdate) > 2017 then year(startdate) else 2017 end as startyear
	,phase_yr
	,capacity_type
	,site_id
	,phase_yr_version_id
FROM (SELECT * FROM [urbansim].[urbansim].[urbansim_lite_parcel_control] where phase_yr_version_id = 155 and capacity_type in ('jur','sch')) a
FULL JOIN (SELECT * FROM urbansim.urbansim.eir_parcel where eir_scenario_id = &eirver) b
	on a.parcel_id = b.parcel_id
) c
);


create table rhna_0 as select *
from connection to odbc
(
SELECT
	jurisdiction_id
	,rhna
	,cast(rhna as float)/cast(total as float) as proportion
FROM (
SELECT 
	jurisdiction_id
	,units_total_rhna6 as rhna
	,total
from [urbansim].[ref].[rhna_6th_housing_cycle] a
cross join (select sum(units_total_rhna6) as total from [urbansim].[ref].[rhna_6th_housing_cycle]) b
) c
);

update sdy_0 set phase_yr = &by1 where phase_yr = &by1 - 1;

disconnect from odbc;
quit;

proc sql;
create table cap_0b as select
	version_id,jur_id,geo_id,parcel_id,capacity
from cap_0
where site_id is null;
quit;


/* Update phasing for RHNA */
proc sql;

create table phase_update_0 as select
	c.parcel_id
	,c.jur_id
	,c.capacity
	,COALESCE(s.phase_yr,p.phase_yr,&by1) as phase_yr
	,case when c.site_id is not null then 'sch'
		else 'jur' end as capacity_type
	,s.site_id
	,geo_id
from cap_0 c
left join phase_0 p
	on c.parcel_id = p.parcel_id
left join sdy_0 s
	on s.parcel_id = c.parcel_id;

create table phase_update_1 as select
	a.*,b.rhna
from (select jur_id, sum(capacity) as cap1 from phase_update_0 group by jur_id) a
inner join rhna_0 b
	on a.jur_id = b.jurisdiction_id;

create table phase_update_2 as select
	a.*,b.rhna,b.cap1
from phase_update_0 a
inner join phase_update_1 b
	on a.jur_id = b.jur_id
order by jur_id
	,case when (phase_yr >= 2021) and (phase_yr <=2034) then 1
		when phase_yr < 2021 then 2
		else 3 end
	,case when capacity_type = 'jur' then 1 else 2 end
	,site_id
	,capacity
	,phase_yr, ranuni(&by1);
quit;

data phase_update_3;set phase_update_2;by jur_id;retain rc;
if first.jur_id then do;c1=min(capacity,rhna);rc=c1;end;
else do;c1=min(capacity,(rhna-rc));rc=rc+c1;end;
run;

proc sql;
create table phase_update_4 as select *
	,case /*when site_id < 2035 and site_id > 2020 then site_id*/
		when c1 > 0 and phase_yr > 2034 then (2035 - ceil((rhna-rc)/(rhna/14)))
		when rc < rhna then 2021
		when c1 > 0 and phase_yr < 2021 then 2021
		else phase_yr end as phase_yr2
from phase_update_3;

create table phase_update_5 as select
	a.*,b.phase_yr2
from phase_update_0 a
inner join phase_update_4 b
	on a.parcel_id = b.parcel_id;

create table phase_update_5a as select
	site_id, min(phase_yr2) as phase_yr2_site
from phase_update_5
where site_id is not null
group by site_id;

create table phase_update_6 as select
	x.*
	,CASE WHEN coalesce(y.phase_yr2_site,phase_yr2) < 2018 THEN 2018
		WHEN coalesce(y.phase_yr2_site,phase_yr2) > 2050 THEN 2045
		ELSE coalesce(y.phase_yr2_site,phase_yr2) END as phase_yr_final
from phase_update_5 x
left join phase_update_5a y
	on x.site_id = y.site_id;

create table phase_update_final as select
	parcel_id
	,phase_yr_final as phase_yr
	,&phver as phase_yr_version_id
	,capacity_type
from phase_update_6;

quit;

proc sql;
create table phase_test_1 as select
	capacity_type, sum(capacity) as c
from phase_update_6
group by capacity_type;

create table phase_test_2 as select
	phase_yr_final, sum(capacity) as c
from phase_update_6
group by phase_yr_final;
quit;

proc sql;
create table cpa_cap_phase_0 as select
	geo_id
	,phase_yr_final as phase_yr
	,sum(capacity) as cap_available
from phase_update_6
group by geo_id, phase_yr_final
order by geo_id, phase_yr_final;

create table cpa_cap_phase_1 as select
	a.yr,b.geo_id
	,coalesce(c.cap_available,0) as ca1
from (select distinct(yr) as yr from reg_target_0) a
cross join (select distinct(geo_id) as geo_id from cpa_cap_phase_0) b
left join cpa_cap_phase_0 c
	on a.yr=c.phase_yr and b.geo_id = c.geo_id
order by b.geo_id,a.yr;
quit;

data cpa_cap_phase_2;set cpa_cap_phase_1;by geo_id;retain ca2;
if first.geo_id then do;ca2=ca1;end;
else do;ca2=ca2+ca1;end;
run;

proc sql;
create table sdy_0a as select
	x.parcel_id, y.phase_yr2_site as phase_yr, x.site_id
from sdy_0 x
left join phase_update_5a y
	on x.site_id = y.site_id;
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
/* The following steps are added/changed to allocate the appropriate control percentages for RHNA */
proc sql;

create table cpa_target_0a as select 
	coalesce(b.subregional_crtl_id,209) as subregional_crtl_id
	,a.yr
	,coalesce(b.geo,'jur_and_cpa') as geo
	,a.geo_id
	,coalesce(b.control,0) as control
	,coalesce(b.control_type,'percentage') as control_type
	,coalesce(b.max_units,'') as max_units
	,coalesce(b.scenario_desc,'capacity_2') as scenario_desc
	,case when a.geo_id >= 1900 then 19 when a.geo_id >=1400 then 14 else a.geo_id end as jur_id 
	,coalesce(a.ca2,0) as ca2
	,coalesce(CASE WHEN a.ca2 = 0 THEN 0 
		WHEN (coalesce(b.control,0)  = 0 and a.ca2 > 0) THEN round((a.ca2/c.housing_units_add) * a.ca2,1)
		WHEN a.yr < 2021 THEN round((a.ca2/3),1)
		ELSE round(coalesce(b.control,0) * c.housing_units_add,1) end,0) as huc1
	,c.housing_units_add
from cpa_cap_phase_2 a
left join cpa_target_0 b
	on a.geo_id = b.geo_id and a.yr=b.yr
inner join reg_target_0 c
	on c.yr = b.yr;

quit;


proc sql;

create table cpa_target_0b as select *
	,sum(huc1) as huc2
from cpa_target_0a
group by yr;


create table cpa_target_0c as select *
	,round(huc1 * (housing_units_add/huc2),1) as huc3
from cpa_target_0b;


create table cpa_target_0d as select *
	,huc3 / housing_units_add as control2
from cpa_target_0c;

quit;

proc sql;
create table cpa_target_0e as select x.yr,x.jur_id
	,sum(x.control2) as control_jur
	,sum(x.ca2) as ca3
from cpa_target_0d as x
where x.yr >= 2021 and x.yr <= 2034
group by x.yr, x.jur_id;


create table cpa_target_0f as select x.yr,x.jur_id
	,x.control_jur,y.housing_units_add,z.rhna,x.ca3
	,round(x.control_jur * y.housing_units_add,1) as hue1
from cpa_target_0e as x
inner join reg_target_0 as y 
	on x.yr=y.yr
inner join rhna_0 as z
	on x.jur_id = z.jurisdiction_id
where x.yr >= 2021 and x.yr <= 2034;


create table cpa_target_0g as select *
	,sum(hue1) as hur1
from cpa_target_0f
group by jur_id;


create table cpa_target_0h as select *
	,round(hue1 * (rhna/hur1),1) as hue2
from cpa_target_0g;


create table cpa_target_0i as select *
	,hue2 / housing_units_add as control_jur2
from cpa_target_0h;


create table cpa_target_0j as select
	d.*
	,i.rhna
	,i.ca3
	,i.control_jur
	,i.control_jur2
from cpa_target_0d d
left join cpa_target_0i i
	on d.yr = i.yr and d.jur_id = i.jur_id;


create table cpa_target_0k as select
	*
	,(ca2/ca3) as rhna_share
	,case when control_jur2 is null then control2
		when geo_id > 20 then control2 * (control_jur2/control_jur)
		else control_jur2 end as control3
from cpa_target_0j;
quit;

proc sql;
create table cpa_target_1 as select yr,geo_id,control3 as s0,housing_units_add as reg_target
,round(control3 * reg_target,1) as hu0
,ca2
,rhna
,rhna_share
from cpa_target_0k
order by yr,geo_id;

create table cpa_target_1a as select yr,geo_id
	,reg_target
	,rhna
	,rhna_share
	,round(rhna * rhna_share,1) as rhna2
	,hu0
	,ca2
from cpa_target_1
order by geo_id,yr;
quit;

data cpa_target_1b;set cpa_target_1a;by geo_id;retain ct ru rr;
if first.geo_id then do;c1=min(hu0,ca2);ct=c1;ru=0;rr=0;end;
else if not missing(rhna2) then do;c1=min(max(hu0,ceil(rr/(2035-yr))),ca2-ct,rhna2-ru);ct=ct+c1;ru=ru+c1;rr=rhna2-ru;end;
else do;c1=min(max(hu0,hu0+ceil((ca2-ct)/(2051-yr))),ca2-ct);ct=ct+c1;ru=ru;rr=rr;end;
run;

proc sql;
create table cpa_target_1c as select *
from cpa_target_1b b
order by yr,ranuni(&by1);
quit;

data cpa_target_1d;set cpa_target_1c;by yr;retain yt;
if first.yr then do;hu2=min(c1,reg_target);yt=hu2;cr=0;end;
else if not missing(rhna2) then do;hu2=min(c1,reg_target-yt);yt=yt+hu2;cr=c1-hu2;end;
else do;hu2=min(c1,reg_target-yt);yt=yt+hu2;cr=0;end;
run;

proc sql;
create table cpa_target_1e as select
	a.yr,a.geo_id,a.reg_target,a.hu2
	,c.cr2
	,(reg_target-ytm) as rem
from cpa_target_1d a
inner join (select yr, max(yt) as ytm from cpa_target_1d group by yr) b
	on a.yr = b.yr
inner join (select geo_id, sum(cr) as cr2 from cpa_target_1d group by geo_id) c
	on a.geo_id = c.geo_id
where ytm < reg_target and cr2 > 0 and (a.yr <= 2034 and a.yr >= 2021)
order by yr,geo_id;
quit;
/* This really should be a macro */
proc sql;
create table m_2021a as select
	a.yr,a.geo_id,a.hu2,a.rem,a.cr2
from cpa_target_1e a
where yr = 2021
order by ranuni(2021);
quit;
data m_2021b;set m_2021a;by yr;retain rt;
if first.yr then do;nua=min(cr2,rem);rt=rem-nua;hu3=hu2+nua;end;
else do;nua=min(cr2,rt);rt=rt-nua;hu3=hu2+nua;end;
run;
proc sql;
create table m_2022a as select
	a.yr,a.geo_id,a.hu2,a.rem,case when b.cr2 is not null then(b.cr2 - b.nua) else a.cr2 end as cr2
from cpa_target_1e a
left join m_2021b b
	on a.geo_id = b.geo_id
where a.yr = 2022
order by ranuni(2022);
quit;
data m_2022b;set m_2022a;by yr;retain rt;
if first.yr then do;nua=min(cr2,rem);rt=rem-nua;hu3=hu2+nua;end;
else do;nua=min(cr2,rt);rt=rt-nua;hu3=hu2+nua;end;
run;
proc sql;
create table m_2023a as select
	a.yr,a.geo_id,a.hu2,a.rem,case when b.cr2 is not null then(b.cr2 - b.nua) else a.cr2 end as cr2
from cpa_target_1e a
left join m_2022b b
	on a.geo_id = b.geo_id
where a.yr = 2023
order by ranuni(2023);
quit;
data m_2023b;set m_2023a;by yr;retain rt;
if first.yr then do;nua=min(cr2,rem);rt=rem-nua;hu3=hu2+nua;end;
else do;nua=min(cr2,rt);rt=rt-nua;hu3=hu2+nua;end;
run;
proc sql;
create table m_2024a as select
	a.yr,a.geo_id,a.hu2,a.rem,case when b.cr2 is not null then(b.cr2 - b.nua) else a.cr2 end as cr2
from cpa_target_1e a
left join m_2023b b
	on a.geo_id = b.geo_id
where a.yr = 2024
order by ranuni(2024);
quit;
data m_2024b;set m_2024a;by yr;retain rt;
if first.yr then do;nua=min(cr2,rem);rt=rem-nua;hu3=hu2+nua;end;
else do;nua=min(cr2,rt);rt=rt-nua;hu3=hu2+nua;end;
run;
proc sql;
create table m_2025a as select
	a.yr,a.geo_id,a.hu2,a.rem,case when b.cr2 is not null then(b.cr2 - b.nua) else a.cr2 end as cr2
from cpa_target_1e a
left join m_2024b b
	on a.geo_id = b.geo_id
where a.yr = 2025
order by ranuni(2025);
quit;
data m_2025b;set m_2025a;by yr;retain rt;
if first.yr then do;nua=min(cr2,rem);rt=rem-nua;hu3=hu2+nua;end;
else do;nua=min(cr2,rt);rt=rt-nua;hu3=hu2+nua;end;
run;
proc sql;
create table m_2026a as select
	a.yr,a.geo_id,a.hu2,a.rem,case when b.cr2 is not null then(b.cr2 - b.nua) else a.cr2 end as cr2
from cpa_target_1e a
left join m_2025b b
	on a.geo_id = b.geo_id
where a.yr = 2026
order by ranuni(2026);
quit;
data m_2026b;set m_2026a;by yr;retain rt;
if first.yr then do;nua=min(cr2,rem);rt=rem-nua;hu3=hu2+nua;end;
else do;nua=min(cr2,rt);rt=rt-nua;hu3=hu2+nua;end;
run;
proc sql;
create table m_2027a as select
	a.yr,a.geo_id,a.hu2,a.rem,case when b.cr2 is not null then(b.cr2 - b.nua) else a.cr2 end as cr2
from cpa_target_1e a
left join m_2026b b
	on a.geo_id = b.geo_id
where a.yr = 2027
order by ranuni(2027);
quit;
data m_2027b;set m_2027a;by yr;retain rt;
if first.yr then do;nua=min(cr2,rem);rt=rem-nua;hu3=hu2+nua;end;
else do;nua=min(cr2,rt);rt=rt-nua;hu3=hu2+nua;end;
run;
proc sql;
create table m_2028a as select
	a.yr,a.geo_id,a.hu2,a.rem,case when b.cr2 is not null then(b.cr2 - b.nua) else a.cr2 end as cr2
from cpa_target_1e a
left join m_2027b b
	on a.geo_id = b.geo_id
where a.yr = 2028
order by ranuni(2028);
quit;
data m_2028b;set m_2028a;by yr;retain rt;
if first.yr then do;nua=min(cr2,rem);rt=rem-nua;hu3=hu2+nua;end;
else do;nua=min(cr2,rt);rt=rt-nua;hu3=hu2+nua;end;
run;
proc sql;
create table m_2029a as select
	a.yr,a.geo_id,a.hu2,a.rem,case when b.cr2 is not null then(b.cr2 - b.nua) else a.cr2 end as cr2
from cpa_target_1e a
left join m_2028b b
	on a.geo_id = b.geo_id
where a.yr = 2029
order by ranuni(2029);
quit;
data m_2029b;set m_2029a;by yr;retain rt;
if first.yr then do;nua=min(cr2,rem);rt=rem-nua;hu3=hu2+nua;end;
else do;nua=min(cr2,rt);rt=rt-nua;hu3=hu2+nua;end;
run;
proc sql;
create table m_2030a as select
	a.yr,a.geo_id,a.hu2,a.rem,case when b.cr2 is not null then(b.cr2 - b.nua) else a.cr2 end as cr2
from cpa_target_1e a
left join m_2029b b
	on a.geo_id = b.geo_id
where a.yr = 2030
order by ranuni(2030);
quit;
data m_2030b;set m_2030a;by yr;retain rt;
if first.yr then do;nua=min(cr2,rem);rt=rem-nua;hu3=hu2+nua;end;
else do;nua=min(cr2,rt);rt=rt-nua;hu3=hu2+nua;end;
run;
proc sql;
create table m_2031a as select
	a.yr,a.geo_id,a.hu2,a.rem,case when b.cr2 is not null then(b.cr2 - b.nua) else a.cr2 end as cr2
from cpa_target_1e a
left join m_2030b b
	on a.geo_id = b.geo_id
where a.yr = 2031
order by ranuni(2031);
quit;
data m_2031b;set m_2031a;by yr;retain rt;
if first.yr then do;nua=min(cr2,rem);rt=rem-nua;hu3=hu2+nua;end;
else do;nua=min(cr2,rt);rt=rt-nua;hu3=hu2+nua;end;
run;
proc sql;
create table m_2032a as select
	a.yr,a.geo_id,a.hu2,a.rem,case when b.cr2 is not null then(b.cr2 - b.nua) else a.cr2 end as cr2
from cpa_target_1e a
left join m_2031b b
	on a.geo_id = b.geo_id
where a.yr = 2032
order by ranuni(2032);
quit;
data m_2032b;set m_2032a;by yr;retain rt;
if first.yr then do;nua=min(cr2,rem);rt=rem-nua;hu3=hu2+nua;end;
else do;nua=min(cr2,rt);rt=rt-nua;hu3=hu2+nua;end;
run;
proc sql;
create table m_2033a as select
	a.yr,a.geo_id,a.hu2,a.rem,case when b.cr2 is not null then(b.cr2 - b.nua) else a.cr2 end as cr2
from cpa_target_1e a
left join m_2032b b
	on a.geo_id = b.geo_id
where a.yr = 2033
order by ranuni(2033);
quit;
data m_2033b;set m_2033a;by yr;retain rt;
if first.yr then do;nua=min(cr2,rem);rt=rem-nua;hu3=hu2+nua;end;
else do;nua=min(cr2,rt);rt=rt-nua;hu3=hu2+nua;end;
run;
proc sql;
create table m_2034a as select
	a.yr,a.geo_id,a.hu2,a.rem,case when b.cr2 is not null then(b.cr2 - b.nua) else a.cr2 end as cr2
from cpa_target_1e a
left join m_2033b b
	on a.geo_id = b.geo_id
where a.yr = 2034
order by ranuni(2034);
quit;
data m_2034b;set m_2034a;by yr;retain rt;
if first.yr then do;nua=min(cr2,rem);rt=rem-nua;hu3=hu2+nua;end;
else do;nua=min(cr2,rt);rt=rt-nua;hu3=hu2+nua;end;
run;
/* Someday I'll make that a macro... someday... */

proc sql;
create table cpa_target_1f as select 
	a.yr,a.geo_id,a.reg_target,a.ca2
	,coalesce(b21.hu3,b22.hu3,b23.hu3,b24.hu3,b25.hu3,b26.hu3,b27.hu3,b28.hu3,b29.hu3,b30.hu3,b31.hu3,b32.hu3,b33.hu3,b34.hu3,a.hu2) as hu3
	,min(b34.rt) as rem_2034
from cpa_target_1d a
left join m_2021b b21
	on a.yr=b21.yr and a.geo_id = b21.geo_id
left join m_2022b b22
	on a.yr=b22.yr and a.geo_id = b22.geo_id
left join m_2023b b23
	on a.yr=b23.yr and a.geo_id = b23.geo_id
left join m_2024b b24
	on a.yr=b24.yr and a.geo_id = b24.geo_id
left join m_2025b b25
	on a.yr=b25.yr and a.geo_id = b25.geo_id
left join m_2026b b26
	on a.yr=b26.yr and a.geo_id = b26.geo_id
left join m_2027b b27
	on a.yr=b27.yr and a.geo_id = b27.geo_id
left join m_2028b b28
	on a.yr=b28.yr and a.geo_id = b28.geo_id
left join m_2029b b29
	on a.yr=b29.yr and a.geo_id = b29.geo_id
left join m_2030b b30
	on a.yr=b30.yr and a.geo_id = b30.geo_id
left join m_2031b b31
	on a.yr=b31.yr and a.geo_id = b31.geo_id
left join m_2032b b32
	on a.yr=b32.yr and a.geo_id = b32.geo_id
left join m_2033b b33
	on a.yr=b33.yr and a.geo_id = b33.geo_id
left join m_2034b b34
	on a.yr=b34.yr and a.geo_id = b34.geo_id;

create table cpa_target_1g as select *
	,case when (yr=2034 and geo_id = 5) then hu3 + round(rem_2034/10,1)
		when (yr=2034 and geo_id = 1442) then hu3 + round(rem_2034/5,1)
		when (yr=2034 and geo_id = 1404) then hu3 + round((rem_2034/5),1)
		when (yr=2034 and geo_id = 1428) then hu3 + round((rem_2034/10),1)
		when (yr=2034 and geo_id = 1909) then hu3 + round((rem_2034/5),1)
		when (yr=2034 and geo_id = 1911) then hu3 + round((rem_2034/5),1)
		else hu3 end as hu4
	/*,hu3 as hu4*/
from cpa_target_1f
order by yr, geo_id;

quit;
/* If one jur is short for RHNA at the end, check this table output to see where it can go,
then go mess with the above table (1g) - I know this is a suboptimal method */
proc sql;
create table z_test_2034 as select 
a.geo_id,a.ca2-b.cu34 as cr34
from cpa_target_1f a
left join (select
	geo_id,sum(hu3) as cu34 from cpa_target_1f
	where yr<=2034 group by geo_id) b
	on a.geo_id = b.geo_id
where a.yr=2034
order by cr34 desc;
quit;

proc sql;
create table cpa_target_2 as select yr,geo_id
	,hu4
	,hu4 / reg_target as control4
	,reg_target
from cpa_target_1g
order by yr,geo_id;
quit;
/*
proc sql;
create table cpa_target_2a as select
	b.yr,b.geo_id,b.control4,b.hu4
	,b.reg_target,a.ca2,b.rem_2034
from cpa_target_1 a
inner join cpa_target_2 b
	on a.yr=b.yr and a.geo_id = b.geo_id
order by geo_id,yr;
quit;

data cpa_target_2b;set cpa_target_2a;by geo_id;retain ct eun rem2;
if first.geo_id then do;hu5=min(hu4,ca2);ct=hu5;eun=hu4-hu5;;rem2=rem_2034;end;
else if yr=2034 then do;hu5=min(hu4+eun,ca2-ct);ct=ct+hu5;eun=eun+(hu4-hu5);rem2=rem2+(hu4-hu5);end;
else do;hu5=min(hu4,ca2-ct);ct=ct+hu5;eun=eun+(hu4-hu5);rem2=rem2;end;
run;
*/

proc sql;
create table z_test1 as select 
	case when (yr<2021) then 'pre-RHNA'
		when (yr>=2021) and (yr<=2034) then 'RHNA'
		else 'post-RHNA' end as rhna
	,case when geo_id >= 1900 then 19
		when geo_id >= 1400 and geo_id <=1500 then 14
		else geo_id end as jur
	,sum(hu4) as hu4
from cpa_target_2 group by rhna,jur
order by rhna,jur;
quit;

proc sql;
create table test_1 as select yr,sum(hu4) as c
from cpa_target_2 group by yr;
quit;

proc sql;
create table cpa_target_new as select
&scver as subregional_crtl_id,yr,"jur_and_cpa" as geo,geo_id
/*,hu3/sum(hu3) as control*/
,hu4/sum(hu4) as control
,"percentage" as control_type
,"" as max_units
,"RHNA 2021-2034 v1" as scenario_desc
from cpa_target_2 /* cpa_target_5 */
group by yr
order by yr,geo_id;
quit;

proc sql;
create table test_2 as select yr,sum(control) as c
from cpa_target_new group by yr;
quit;

proc sql;
create table z_test2 as select 
	case when (a.yr<2021) then 'pre-RHNA'
		when (a.yr>=2021) and (a.yr<=2034) then 'RHNA'
		else 'post-RHNA' end as rhna
	,case when a.geo_id >= 1900 then 19
		when a.geo_id >= 1400 and a.geo_id <=1500 then 14
		else a.geo_id end as jur
	,sum(round(a.control * b.reg_target,1)) as huf
from cpa_target_new a
inner join cpa_target_2 b
	on a.geo_id = b.geo_id and a.yr=b.yr
group by rhna,jur;
quit;



proc sql;
delete * from urb.urbansim_lite_parcel_control where phase_yr_version_id = &phver;

insert into urb.urbansim_lite_parcel_control(bulkload=yes bl_options=TABLOCK) select * from phase_update_final;


delete * from urb.urbansim_lite_subreg_control where subregional_crtl_id = &scver;

insert into urb.urbansim_lite_subreg_control(bulkload=yes bl_options=TABLOCK) select * from cpa_target_new;
quit;

