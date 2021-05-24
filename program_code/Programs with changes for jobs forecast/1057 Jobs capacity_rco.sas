
proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table p_01 as select *
from connection to odbc
(
select a.parcel_id,b.mgra as mgra_c, b.BLOCKID10 as blk_c, b.jur_&by1 as jur_c
from urbansim.urbansim.parcel as a
inner join [estimates].[dbo].[BLK2010_JUR_POST2010] as b on a.centroid.STIntersects(b.shape) = 1
);

create table p_02 as select *
from connection to odbc
(
select a.parcel_id,b.mgra as mgra, b.BLOCKID10 as blk, b.jur_&by1 as jur, a.shape.STIntersection(b.shape).STArea() as area
from urbansim.urbansim.parcel as a
inner join [estimates].[dbo].[BLK2010_JUR_POST2010] as b on a.shape.STIntersects(b.shape) = 1
);
disconnect from odbc;
quit;

proc sort data = p_02; by mgra parcel_id; run; 

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;
create table p_03 as select *
from connection to odbc
(
select
x.parcel_id,x.shape.STArea() as parcel_area,x.mgra_id as mgra_p,x.block_id as blk_p,x.jurisdiction_id as jur_p
,case when v.parcel_id>0 then 1 else 0 end as hu_urb
FROM urbansim.urbansim.parcel as x
inner join (select distinct parcelid_2015 as parcel_id from [urbansim].[urbansim].[employment_capacity_scs]) as u on x.parcel_id=u.parcel_id /*changed table reference*/
left join (select distinct parcel_id from [urbansim].[urbansim].[urbansim_lite_output] where run_id=&usver) as v on x.parcel_id=v.parcel_id 
);

disconnect from odbc;
quit;

proc sql;
create table p_1 as select
a.parcel_id,a.parcel_area,a.mgra_p,a.blk_p,a.jur_p
,b.mgra_c,b.blk_c,b.jur_c
,c.area
,c.mgra,c.blk,c.jur
,a.hu_urb
from p_03 as a
left join p_01 as b on a.parcel_id=b.parcel_id
left join p_02 as c on a.parcel_id=c.parcel_id;
quit;


proc sql;
create table p_1a as 
select parcel_id,parcel_area,mgra_p,substr(blk_p,6,6) as ct_p,jur_p
,mgra_c,
substr(blk_c,6,6) as ct_c,
jur_c
,mgra,substr(blk,6,6) as ct
,jur
,sum(area) as area
from p_1 where area>0
group by parcel_id,parcel_area,mgra_p,ct_p,jur_p,mgra_c,ct_c,jur_c,mgra,ct,jur
order by parcel_id,area desc;
quit;


proc sql;
create table p_1a_location as select parcel_id,mgra
,int(jur/100) as jur_id
,case when int(jur/100) in (14,19) then jur else 0 end as cpa_id
,area
from p_1a;
quit;


proc sql;
create table p_1b as select *,count(parcel_id) as n
from (select * from p_1a where area>360)
group by parcel_id;
quit;

proc sql;
create table test_3 as select distinct parcel_id from p_1b where parcel_area<1000;
quit;


proc sql;
create table p_3 as select *,count(parcel_id) as n
from (select parcel_id,parcel_area,mgra,jur,substr(blk,6,6) as ct,hu_urb,sum(area) as area
	from p_1 where area>0 group by parcel_id,parcel_area,mgra,jur)
group by parcel_id
order by parcel_id,area desc;
quit;


proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

/* all parcels */
create table p_2 as select *
from connection to odbc
(
select x.parcel_id 
,x.development_type_id_2015 as dt_2015,x.lu_2015

,x.development_type_id_&by1 as dt_&by1
/* once development_type_id_2018 is ready replace with: x.development_type_id_&by1 as dt_&by1 */

,x.lu_&by1 as lu_&by1
/* once development_type_id_2018 is ready replace with: x.lu_&by1 as lu_&by1 */

,x.du_&by1

,y.gplu as lu_2099
FROM [urbansim].[urbansim].[parcel] as x
left join [urbansim].[urbansim].[general_plan_parcel] as y on x.parcel_id=y.parcel_id);

create table jcap_1 as select *
from connection to odbc
(
select parcelid_2015,mgra,emp_2012 as j_2012,cap_emp_civ as jcap, cap_emp_civ2 as jcap2
FROM [urbansim].[urbansim].[employment_capacity_scs] /*changed table reference*/
);

create table lu_names as select *
from connection to odbc
(select lu_code,lu_name FROM [urbansim].[ref].[lu_code]);

create table dt_names as select *
from connection to odbc
(select development_type_id as dt_code,name as dt_name FROM [urbansim].[ref].[development_type]);

create table dt_lu as select *
from connection to odbc
(select x.development_type_id as dt_id,x.lu_code as lu_id,y.lu_name,z.name as dt_name
FROM [urbansim].[ref].[development_type_lu_code] as x
inner join [urbansim].[ref].[lu_code] as y on x.lu_code=y.lu_code
inner join [urbansim].[ref].[development_type] as z on x.development_type_id=z.development_type_id);

disconnect from odbc;
quit;


proc sql;
create table jcap_1a as select sum(j_2012) as j_2012,sum(jcap) as jcap
from jcap_1;
quit;

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table bld_location_00 as select *
from connection to odbc
(
select a.building_id, b.mgra, b.jur_&by1
from [urbansim].[urbansim].[building] as a
inner join [estimates].[dbo].[BLK2010_JUR_POST2010] as b on a.centroid.STIntersects(b.shape) = 1
);

/* parcel-level jobs from 2016 */
create table pj_2016_0 as select x.job_id,x.building_id,x.parcel_id,coalesce(y.mgra,0) as mgra,coalesce(y.jur_&by1,0) as jur_&by1
,case
when x.job_id > 200000000 then 5 /* self-employed */
when x.job_id > 50000000  then 2 /* fed government owned */
when x.job_id > 40000000  then 3 /* state government owned */
when x.job_id > 30000000  then 4 /* local government owned */
else 1 /* private */
end as type
,case
when x.sector_id in (23,25) then 15 /* state and local education */
when x.sector_id in (21,24,26) then 28 /* federal, state, and local public administration */
else x.sector_id
end as sector_id
,case
when x.sector_id in (23,25) then 15 + (calculated type / 10)
when x.sector_id in (21,24,26) then 28 + (calculated type / 10)
else x.sector_id
end as sector_id_1

from connection to odbc
(
select a.job_id,a.sector_id,a.building_id,b.parcel_id
from [urbansim].[urbansim].[job] /* [job_2016] */as a
inner join [urbansim].[urbansim].[building] as b on a.building_id=b.building_id
)
as x

left join bld_location_00 as y on x.building_id=y.building_id;

/* job spaces */
create table js_2016_0 as select x.building_id,x.parcel_id,coalesce(y.mgra,0) as mgra,coalesce(y.jur_&by1,0) as jur_&by1
,x.development_type_id,x.source,x.job_spaces
,case
when x.sector_id = 23 then 15.3
when x.sector_id = 25 then 15.4
when x.sector_id = 21 then 28.2
when x.sector_id = 24 then 28.3
when x.sector_id = 26 then 28.4
else x.sector_id
end as sector_id_1
from connection to odbc
(
select a.building_id,b.development_type_id,a.sector_id,a.source,a.job_spaces,b.parcel_id
from [urbansim].[urbansim].[job_space] /*[job_space_2016]*/ as a
inner join [urbansim].[urbansim].[building] as b on a.building_id=b.building_id
) as x
left join  bld_location_00 as y on x.building_id=y.building_id;

disconnect from odbc;

update pj_2016_0 set mgra = 14542 where mgra=0 and parcel_id = 831901;
update js_2016_0 set mgra = 14542 where mgra=0 and parcel_id = 831901;
quit;


proc sql;
create table test_01 as select * from pj_2016_0 where mgra=0 or jur_&by1=0;
create table test_02 as select * from js_2016_0 where mgra=0 or jur_&by1=0;
quit;

/*
15.3 education - state
15.4 education - local
28.2 public admin - federal
28.3 public admin - state
28.4 public admin - local
*/

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table bld_location_0 as select x.building_id,x.parcel_id,x.js
,coalesce(z.mgra,0) as mgra,coalesce(z.jur_&by1,0) as jur_&by1
from connection to odbc
(
select
a.building_id,a.parcel_id,b.js
FROM [urbansim].[urbansim].[building] as a
inner join (select building_id,sum(job_spaces) as js from [urbansim].[urbansim].[job_space] group by building_id) as b
	on a.building_id=b.building_id
) as x

LEFT JOIN bld_location_00 as z on x.building_id = z.building_id

order by building_id;

disconnect from odbc;
quit;


proc sql;
create table test_03 as select * from bld_location_0 where mgra=0 or jur_&by1=0;
quit;

/* fishing pier in Oceanside */
proc sql;
update bld_location_0 set mgra = 14542 where parcel_id = 831901 and mgra=0;
quit;



data bld_location_1(drop=jur_&by1);set bld_location_0;
jur_id=int(jur_&by1/100);
if jur_id in (14,19) then cpa_id=jur_&by1; else cpa_id=0;
run;

proc sql;
create table pb_location_0 as select parcel_id,mgra,jur_id,cpa_id,sum(js) as jsp
from bld_location_1 group by parcel_id,mgra,jur_id,cpa_id
order by parcel_id,mgra,jsp desc;

create table pb_location_0a as select parcel_id,mgra,count(*) as n
from pb_location_0 group by parcel_id,mgra having calculated n>1;

create table pb_location_0b as select x.*
from pb_location_0 as x
inner join pb_location_0a as y on x.parcel_id=y.parcel_id and x.mgra=y.mgra
order by parcel_id;
quit;

/* only first instance of parcel is retained; may need to change that */

proc sql;
create table pb_location_1 as select * from pb_location_0;
quit;



proc sql;
create table js_2016_1 as select x.*,y.mgra, y.jur_id, y.cpa_id
from
(select building_id,parcel_id,sector_id_1,sum(job_spaces) as job_spaces
from js_2016_0 group by building_id,parcel_id,sector_id_1) as x
left join bld_location_1 as y
on x.building_id=y.building_id;

create table js_2016_1a as select sector_id_1,sum(job_spaces) as job_spaces
from js_2016_1 group by sector_id_1;
quit;

proc sql;
create table test_01 as select * from js_2016_1 where building_id=537150;
create table test_02 as select * from js_2016_1 where jur_id=. or cpa_id=.;
quit;

data pj_2016_1;set pj_2016_0;
jur_id=int(jur_&by1/100);
if jur_id in (14,19) then cpa_id=jur_&by1; else cpa_id=0;
run;


proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table mohub_mgra as select mgra, mohub
from connection to odbc
(select * from urbansim.ref.scs_mgra_xref); /*changed to include the mohub mgras*/
disconnect from odbc;
quit;

proc sql; 
create table mohub_mgra2 as 
select mgra, mohub from mohub_mgra
where mohub IS NOT NULL
order by mgra; 
quit; 


proc sql;
create table bsj_2016_1 as select
coalesce(x.building_id,y.building_id) as building_id
,coalesce(x.sector_id_1,y.sector_id_1) as sector_id_1
,coalesce(x.parcel_id,y.parcel_id) as parcel_id
,coalesce(x.mgra,y.mgra) as mgra
,coalesce(x.jur_id,y.jur_id) as jur_id
,coalesce(x.cpa_id,y.cpa_id) as cpa_id
,y.j_2016
,x.job_spaces as js
from (select * from js_2016_1 where job_spaces>0) as x
full join /*pj_2016_04*/
(select building_id,parcel_id,mgra,jur_id,cpa_id,sector_id_1,count(job_id) as j_2016
from pj_2016_1 group by building_id,parcel_id,mgra,jur_id,cpa_id,sector_id_1) as y
on x.building_id = y.building_id and x.parcel_id = y.parcel_id and x.sector_id_1 = y.sector_id_1;

create table bsj_2016_1b as select * from bsj_2016_1 where js < j_2016;
create table bsj_2016_1c as select distinct sector_id_1 from bsj_2016_1b;

create table bsj_2016_2 as select parcel_id, mgra, jur_id, cpa_id
,sector_id_1,sum(js - j_2016) as avl_js /* available jobs slots */
from bsj_2016_1 where js - j_2016 > 0 and mgra in(select mgra from mohub_mgra2)/*changed to include the mohub mgras*/
group by parcel_id, mgra, jur_id, cpa_id, sector_id_1;

update bsj_2016_2 set avl_js = round(avl_js *1.25,1); /*changed to increase job vacancy on current job spaces by 25%*/

create table bsj_2016_2a as select sector_id_1,sum(avl_js) as avl_js
from bsj_2016_2 group by sector_id_1;

create table bsj_2016_2b as select sum(avl_js) as avl_js
from bsj_2016_2;
quit;

proc sql;
/* adding military */
create table jf_00 as
select yr,sandag_industry_id,type, j as jobs from e1.employment_controls
	union all
select y.yr,x.sector_id as sandag_industry_id,x.type,x.jobs
from (select sector_id,type,count(job_id) as jobs from pj_2016_0 where sector_id = 27 group by sector_id,type) as x
cross join (select distinct yr from e1.employment_controls) as y;

create table jf_01 as select yr,sandag_industry_id,type
,case
when sandag_industry_id = 15 and type = 3 then 15.3
when sandag_industry_id = 15 and type = 4 then 15.4
when sandag_industry_id = 28 and type = 2 then 28.2
when sandag_industry_id = 28 and type = 3 then 28.3
when sandag_industry_id = 28 and type = 4 then 28.4
else sandag_industry_id
end as sector_id_1
,jobs
from jf_00;

create table jf_0 as select yr,sector_id_1,sum(jobs) as jobs
from jf_01
group by yr,sector_id_1;
quit;

proc sql;
create table jf_0a as select yr,sum(jobs) as jobs
from jf_0 group by yr order by yr;

create table jf_0b as select sector_id_1,max(jobs) as jobs
from (select yr,sector_id_1,sum(jobs) as jobs from jf_0 group by yr,sector_id_1)
group by sector_id_1;

create table jf_0c as select x.*,y.yr
from jf_0b as x
inner join (select yr,sector_id_1,sum(jobs) as jobs from jf_0 group by yr,sector_id_1) as y
on x.sector_id_1=y.sector_id_1 and x.jobs=y.jobs;

delete from jf_0c where sector_id_1=27 and yr<2050;
quit;


proc sql;
create table pj_2016_6 as select x.*, y.lu_&by1,y.dt_&by1
from
(
select parcel_id, mgra, jur_id, cpa_id, sector_id_1, count(job_id) as jt
from pj_2016_1 group by parcel_id, sector_id_1, mgra, jur_id, cpa_id
) as x
inner join p_2 as y on x.parcel_id = y.parcel_id;
quit;


proc sql;
create table dt_sec_1 as select dt_&by1,sector_id_1,sum(jt) as jt
from pj_2016_6
where sector_id_1 ^= 27
group by dt_&by1,sector_id_1;

create table dt_sec_2 as select *,jt/sum(jt) as f format=percent8.1 /* share of dt in each sector_id */ 
from dt_sec_1 group by dt_&by1 order by dt_&by1;

create table dt_sec_2a as select distinct dt_&by1 from dt_sec_2;
create table dt_sec_2b as select distinct sector_id_1 from dt_sec_2;

create table dt_sec_3 as select dt_&by1,sector_id_1,f/sum(f) as f
from dt_sec_2 where f>=0.05 group by dt_&by1;

create table dt_sec_3a as select distinct dt_&by1 from dt_sec_3;
create table dt_sec_3b as select distinct sector_id_1 from dt_sec_3;

create table dt_sec_3c as select * from dt_sec_3 where sector_id_1=19 order by f desc;
quit;

/* reading in employment events */
proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;
create table dev_j_0 as select *
from connection to odbc
(
SELECT x.siteid,x.compdate as yr
,y.[parcel_id],y.[civemp_imputed] as j,y.[sector_id]
,z.mgra,z.jur_&by1
,y.shape.STIntersection(z.shape).STArea() as area
FROM [urbansim].[ref].[non_res_sched_dev_sites_scs] as x
inner join [urbansim].[urbansim].[non_res_sched_dev_parcel_scs_2] as y on x.siteid=y.site_id
LEFT JOIN [estimates].[dbo].[BLK2010_JUR_POST2010] as z on y.shape.STIntersects(z.shape) = 1
where y.civemp_imputed >0 and x.civemp >0
)
order by siteid,parcel_id,mgra,yr,sector_id ,area desc;

disconnect from odbc;

update dev_j_0 set sector_id = 17 where sector_id = . and siteid in (19000,19001);

update dev_j_0 set yr = 2022 where yr = . and siteid in (4014);
quit;

proc sort data = dev_j_0; by siteid parcel_id descending area mgra yr sector_id;run; 

data dev_j_0a; 
set dev_j_0; 
by siteid parcel_id descending area mgra yr sector_id; 
if first.parcel_id then count = 0; 
count + 1; 
run; 

proc sql; 
create table dev_j_0b as 
select siteid, yr, parcel_id, j, sector_id, mgra, jur_2018, area
from dev_j_0a 
where (count = 1 and siteid <> 19020) or (count in(1,2,3,4) and siteid = 19020)
order by siteid, parcel_id, mgra; 
quit; 

proc sort data = dev_j_0b; by siteid parcel_id mgra yr sector_id;run; 

data dev_j_1(drop=jur_&by1 area);set dev_j_0b;by siteid parcel_id mgra yr sector_id;
if first.sector_id;
jur_id=int(jur_&by1/100);
if jur_id in (14,19) then cpa_id=jur_&by1; else cpa_id=0;
run;

proc sql;
create table dev_j_1a as select distinct yr from dev_j_1;
create table dev_j_1b as select distinct sector_id from dev_j_1;
create table dev_j_1c as select distinct siteid,sector_id from dev_j_1 where sector_id in (21,24,26);

create table dev_j_2 as select yr,siteid,parcel_id,mgra,jur_id,cpa_id
,case
when siteid = 12057 then 16 /* VA clinic */
when siteid = 14037 then 28 /* reclassified from 26; local gov */
else sector_id end as sandag_industry_id
,case
when siteid = 12057 then 2 /* federal gov */
when siteid = 14037 then 4 /* local gov */
when siteid in (19000,19001) then 4 /* Tribal Casinos; local gov */
else 1 /* private */ end as type
,j
from dev_j_1;

create table dev_j_3 as 
select parcel_id,mgra,jur_id,cpa_id,sum(j) as j
from dev_j_2 group by parcel_id,mgra,jur_id,cpa_id;
quit;

proc sql; 
create table dev_j_3_test as 
select sum(j) as jobs 
from dev_j_3; 
quit; 

/*
Create total job capacity by parcel by sector
	attach dt (development type)
	include parcels where (hu_urb=0 and hu_existing=0) or lu_2099 in (1200,9700)
	split jobs by sector (using shares in dt_sec_2)

Calculate demand for additional jobs by sector
	compare this demand with available capacity by sector
	reallocate capacity across sectors
*/

/* this should be used for capacity only; base jobs and vacant slots should go to specific parcel-mgra pairs */


/*
proc sort data=p_3;by parcel_id mgra descending area;run;
data p_3a;set p_3;by parcel_id mgra;
if first.mgra;
run;
*/

/*
proc sql;
create table p_1b_location as select x.*, coalesce(y.j,0) as j
from p_1a_location as x
left join (select parcel_id,mgra,jur_id,cpa_id, count(job_id) as j from pj_2016_1 group by parcel_id,mgra,jur_id,cpa_id) as y
on x.parcel_id=y.parcel_id and x.mgra=y.mgra and x.jur_id=y.jur_id and x.cpa_id=y.cpa_id
order by parcel_id,j desc, area desc;
quit;
*/



/*
data p_3c;set p_3b;by parcel_id;retain i;
if first.parcel_id then i=1;
else i=i+1;
run;

proc sql;
create table p_3d as select * from p_3c where i=1 or j>0
order by parcel_id,i;

create table p_3d_ as select * from p_3d where i>1;
quit;
*/


/*
data p_4;set p_3(drop=n area);by parcel_id;
if first.parcel_id;
jur_id=int(jur/100);
if jur_id in (14,19) then cpa_id=jur;else cpa_id=0;
run;
*/

proc sql;
create table p_1b_location as select *, area/sum(area) as s
from p_1a_location group by parcel_id;
quit;
/**/
/*proc sql; */
/*create table test_jcap_2 as */
/*select **/
/*from jcap_2 */
/*where mgra in(3633,6781)*/
/*order by mgra, parcelid_2015; */
/*quit; */


proc sql;
create table jcap_2 as select x.parcelid_2015, x.mgra as mgra_p, x.j_2012, x.jcap, x.jcap2 /*changed this statement to bring in all the variables from jcap_1*/
,y.area as parcel_area,y.mgra,y.jur_id,y.cpa_id, y.s
,y1.hu_urb
,z.lu_2099
,v.dt_id as dt_2099
,case when z.du_&by1>0 then 1 else 0 end as hu_existing

,round(x.jcap2 * y.s,1) as jcap3 /*changed this to jcap3 and updated it to be calculated from jcap2 (the SCS increased cap)*/
,round(x.j_2012 * y.s,1) as j_2012_2

from jcap_1 as x
left join p_1b_location as y on x.parcelid_2015=y.parcel_id
left join (select distinct parcel_id, hu_urb from p_1) as y1 on x.parcelid_2015=y1.parcel_id
inner join p_2 as z on x.parcelid_2015=z.parcel_id
left join dt_lu as v on z.lu_2099=v.lu_id
order by parcelid_2015,jcap3 desc; /*changed to jcap3*/
quit;


proc sql;
create table test_201 as select * from jcap_2 where jcap3 = .; /*changed to jcap3*/
quit;

proc sql;
create table jcap_3 as select parcelid_2015, hu_urb, hu_existing, lu_2099, dt_2099
,mgra, jur_id, cpa_id
,jcap3 as jcap /*changed to jcap3*/
,j_2012_2 as j_2012
from jcap_2
where jcap3>0; /*changed to jcap3*/

create table jcap_3_sum_1 as select hu_urb,hu_existing,sum(jcap) as jcap
from jcap_3 group by hu_urb,hu_existing;

create table jcap_3_sum_2 as select case
when (hu_urb=0 and hu_existing=0) or lu_2099 in (1200,9700) then "Jobs Allowed" else "Jobs Not Allowed" 
end as type
,sum(jcap) as jcap
from jcap_3 group by type;
quit;

proc sql; 
create table test_jcap_3 as 
select sum(jcap) as jcap 
from jcap_3; 
quit; 

/* include only certain parcels */
proc sql;
create table jcap_4 as select 
x.parcelid_2015 as parcel_2015, x.mgra, x.jur_id, x.cpa_id
,x.j_2012,x.jcap,x.lu_2099,x.dt_2099
,coalesce(y.j_2016_t,0) as j_2016_t
,x.j_2012 - coalesce(y.j_2016_t,0) as jd /*2012 jobs from sr13 minus the 2016 jobs from base year jobs file*/
,abs(calculated jd) as abs_jd
from (select * from jcap_3 where (hu_urb=0 and hu_existing=0) or (lu_2099 in (1200,9700))) as x

left join (select parcel_id,mgra,jur_id,cpa_id,count(job_id) as j_2016_t from pj_2016_1 group by parcel_id,mgra,jur_id,cpa_id) as y
on x.parcelid_2015 = y.parcel_id

left join dev_j_3 as z on x.parcelid_2015 = z.parcel_id

where z.parcel_id = . /* if there's a dev event on the same parcel, the capacity is excluded */ 

order by /*abs_jd*/x.parcelid_2015 desc;
delete from jcap_4 where mgra in(3633,6781); /*this deletes those mgras with capacity that are in the water*/ 

create table jcap_4a as select sum(jcap) as jcap
from jcap_4;
quit;

proc sql; 
create table test_jcap_4 as 
select sum(jcap) as jcap 
from jcap_4; 
quit; 

proc sql;
create table dev_j_3a as select x.*, y.jcap
from dev_j_3 as x
left join (select * from jcap_3 where (hu_urb=0 and hu_existing=0) or lu_2099 in (1200,9700) ) as y
	on x.parcel_id = y.parcelid_2015 and x.mgra=y.mgra and x.jur_id=y.jur_id and x.cpa_id=y.cpa_id;
quit;

proc sql;
create table jf_5 as select x.sector_id_1,x.jobs - y.j as tj_c
from jf_0c as x
left join (select sector_id_1,count(job_id) as j from pj_2016_0 group by sector_id_1) as y
on x.sector_id_1 = y.sector_id_1;
quit;

/* since these sectors will not use capacity for future jobs, their slots are re-allocated among other sectors */
proc sql;
create table dt_sec_4 as select dt_&by1,sector_id_1,f,f/sum(f) as f1 format=percent8.1
from dt_sec_2 where sector_id_1 not in (15.3, 15.4, 22, 27, 28.2, 28.3, 28.4)
group by dt_&by1;
quit;

proc sql;
create table jcap_5 as select x.parcel_2015, x.mgra, x.jur_id, x.cpa_id
,x.lu_2099, x.dt_2099, x.jcap
,y.sector_id_1, y.f
,ceil(x.jcap * y.f) as sl1 /* slots */
,round(x.jcap * y.f,1) as sl2 /* slots */
from jcap_4 as x
inner join dt_sec_4 as y on x.dt_2099=y.dt_&by1
order by parcel_2015,sector_id_1;

create table jcap_5a as select x.sector_id_1,x.tj_c,coalesce(y.sl1,0) as sl1
,coalesce(y.sl1,0) - x.tj_c as d1
,coalesce(z.avl_js,0) as avl_js
,coalesce(z.avl_js,0) + calculated d1 as d1_

from jf_5 as x

left join (select sector_id_1,sum(sl1) as sl1
from jcap_5 where sl1>=5 group by sector_id_1) as y
on x.sector_id_1=y.sector_id_1
left join bsj_2016_2a as z on x.sector_id_1 = z.sector_id_1
order by sector_id_1;

create table jcap_5b as select x.*,y.*
from (select sum(jcap) as jcap from jcap_4) as x
cross join (select sum(tj_c) as jg_t,sum(sl1) as sl1
from jcap_5a) as y;
quit;

/*proc sql; */
/*create table test_jcap_5 as */
/*select sum(sl1) as sl1 */
/*from jcap_5; */
/*quit; */
/**/
/*proc sql; */
/*create table test_jcap_5 as */
/*select count(*) */
/*from jcap_5*/
/*where sl1 <5; */
/*quit; */

/*
parcel- and sector-level job capacity
	only parcels with at least 5 slots are considered

special treatment
sectors 27 (military), 28.2 (fed padm), 28.3 (state padm), 28.4 (local padm), 22 (DOD), 15.3 (state edu), 15.4 (local edu)
	new jobs will go to existing sites

when new job capacity by sector is exhausted, currently unoccupied job slots will be filled

if after that there's still need for more job capacity, the remaining unplaced jobs will be placed into existing locations
	15 Private education
     3 Utilities
    13 Management
     1 Farm	 
*/


proc sql;
create table jcap_6 as select sector_id_1
,case
when sector_id_1 in (15.3, 15.4, 22, 27, 28.2, 28.3, 28.4) then 0
when tj_c <= sl1 then tj_c
else sl1
end as slots_cap

,case
when sector_id_1 in (15.3, 15.4, 22, 27, 28.2, 28.3, 28.4) then 0
when d1 >= 0 then 0
when avl_js > d1 * -1 then d1 * -1
else avl_js
end as slots_vac

,case
when sector_id_1 in (15.3, 15.4, 22, 27, 28.2, 28.3, 28.4) then tj_c
when d1_ >= 0 then 0
else d1_ * -1
end as slots_new

from jcap_5a
order by sector_id_1;
quit;


/*
capacity:
slots_vac:
slots_new:
*/

data jcap_5c; 
set jcap_5; 
id = ranuni(&by1-1);
run; 

proc sort data = jcap_5c; by id; run; 

proc sql; 
create table test_jcap_5c as 
select sum(sl1) 
from jcap_5c; 
quit; 


proc sql;
create table slots_cap_1 as 
select x.parcel_2015 as parcel_id, x.mgra, x.jur_id, x.cpa_id
,x.lu_2099,x.dt_2099,x.sector_id_1,x.sl1 as slots_cap
from jcap_5c as x
inner join jcap_6 as y on x.sector_id_1=y.sector_id_1
where x.sl1>=5 and y.slots_cap>0 
order by sector_id_1,id;

create table slots_vac_1 as select x.parcel_id, x.mgra, x.jur_id, x.cpa_id
,x.sector_id_1,x.avl_js as slots_vac
from bsj_2016_2 as x
inner join jcap_6 as z on x.sector_id_1=z.sector_id_1
where z.slots_vac > 0
order by sector_id_1,ranuni(&by1);

create table slots_new_1 as select x.parcel_id, x.mgra, x.jur_id, x.cpa_id
,x.sector_id_1,x.jobs as slots_new
/* x.j_2016_ws + x.j_2016_se as slots_new*/
from (select parcel_id,mgra,jur_id,cpa_id,sector_id_1,count(job_id) as jobs from pj_2016_1 group by parcel_id,mgra,jur_id,cpa_id,sector_id_1) as x
inner join jcap_6 as y on x.sector_id_1=y.sector_id_1
where y.slots_new > 0 and /*(x.j_2016_ws + x.j_2016_se)*/ x.jobs >= 10;
quit;

data slots_cap_2;set slots_cap_1;by sector_id_1;retain s2;
if first.sector_id_1 then do; s1=0;s2=slots_cap;end;
else do;s1=s2;s2=s1+slots_cap;end;
run;

proc sql; create table test_slots_cap_1 as 
select sum(slots_cap) as sc 
from slots_cap_1; 
quit;

data slots_vac_2;set slots_vac_1;by sector_id_1;retain s2;
if first.sector_id_1 then do; s1=0;s2=slots_vac;end;
else do;s1=s2;s2=s1+slots_vac;end;
run;

proc sql;
create table slots_cap_3 as select x.parcel_id, x.mgra, x.jur_id, x.cpa_id
,x.lu_2099,x.dt_2099,x.sector_id_1,x.slots_cap,x.s2,y.slots_cap as target
from slots_cap_2 as x
inner join jcap_6 as y on x.sector_id_1=y.sector_id_1
where y.slots_cap > 0 and x.s1 < y.slots_cap;

create table slots_cap_3a as select * from slots_cap_3 where s2 > target;

update slots_cap_3 set slots_cap = slots_cap - (s2 - target) where s2 > target;

create table slots_cap_3b as select * from slots_cap_3 where s2 > target;
quit;

proc sql;
create table slots_vac_3 as select x.parcel_id, x.mgra, x.jur_id, x.cpa_id
,x.sector_id_1,x.slots_vac,x.s2,y.slots_vac as target
from slots_vac_2 as x
inner join jcap_6 as y on x.sector_id_1=y.sector_id_1
where y.slots_vac > 0 and x.s1 < y.slots_vac;

create table slots_vac_3a as select * from slots_vac_3 where s2 > target;

update slots_vac_3 set slots_vac = slots_vac - (s2 - target) where s2 > target;

create table slots_vac_3b as select * from slots_vac_3 where s2 > target;
quit;

proc sql;
create table slots_new_2 as select *,slots_new/sum(slots_new) as f
from slots_new_1 group by sector_id_1;

create table slots_new_2a as select x.*,ceil(x.f * y.slots_new) as sn,y.slots_new as target
from slots_new_2 as x
inner join jcap_6 as y on x.sector_id_1 = y.sector_id_1
where y.slots_new > 0;

create table slots_new_2b as select parcel_id, mgra, jur_id, cpa_id, sector_id_1,sn,target
from slots_new_2a where sn>0
order by sector_id_1,sn desc;
quit;

data slots_new_2c; set slots_new_2b; by sector_id_1; retain c;
if first.sector_id_1 then do; sn1 = sn; c = sn1; end;
else do; sn1 = min(sn, target - c) ;c = c+sn1;end;
run;

proc sql;
create table slots_new_3 as select parcel_id, mgra, jur_id, cpa_id
,sector_id_1,sn1 as slots_new,target
from slots_new_2c where sn1>0;
quit;

proc sql;
create table slots_done_0 as select
coalesce(x.parcel_id, y.parcel_id) as parcel_id
,coalesce(x.mgra, y.mgra) as mgra
,coalesce(x.jur_id, y.jur_id) as jur_id
,coalesce(x.cpa_id, y.cpa_id) as cpa_id
,coalesce(x.sector_id_1, y.sector_id_1) as sector_id_1
,x.slots_cap
,y.slots_vac
from slots_cap_3 as x
full join slots_vac_3 as y on x.parcel_id=y.parcel_id and x.mgra=y.mgra and x.jur_id=y.jur_id and x.cpa_id=y.cpa_id
and x.sector_id_1=y.sector_id_1;

create table slots_done_1 as select
coalesce(x.parcel_id, y.parcel_id) as parcel_id
,coalesce(x.mgra, y.mgra) as mgra
,coalesce(x.jur_id, y.jur_id) as jur_id
,coalesce(x.cpa_id, y.cpa_id) as cpa_id
,coalesce(x.sector_id_1, y.sector_id_1) as sector_id_1
,x.slots_cap
,x.slots_vac
,y.slots_new
from slots_done_0 as x
full join slots_new_3 as y on x.parcel_id=y.parcel_id and x.mgra=y.mgra and x.jur_id=y.jur_id and x.cpa_id=y.cpa_id
and x.sector_id_1=y.sector_id_1;
quit;

proc sql;
create table test_1 as select * from slots_done_1 where
(slots_cap>0 and slots_vac>0) or (slots_cap>0 and slots_new>0) or (slots_vac>0 and slots_new>0);
quit;

proc sql;
create table test_2 as select * from slots_cap_3 where parcel_id=896 and sector_id_1=7;
create table test_3 as select * from slots_vac_3 where parcel_id=896 and sector_id_1=7;
quit;

proc sql;
create table slots_done_2 as select parcel_id, mgra, jur_id, cpa_id
,sector_id_1
,coalesce(slots_cap,0) as slots_cap
,coalesce(slots_vac,0) as slots_vac
,coalesce(slots_new,0) as slots_new
from slots_done_1 as x;

create table old_jobs as select parcel_id, mgra, jur_id, cpa_id
,sector_id_1,count(job_id) as j_old
from pj_2016_1 group by parcel_id, mgra, jur_id, cpa_id, sector_id_1;

create table final_j_2050_1 as
select 
coalesce(x.parcel_id,y.parcel_id) as parcel_id
,coalesce(x.mgra, y.mgra) as mgra
,coalesce(x.jur_id, y.jur_id) as jur_id
,coalesce(x.cpa_id, y.cpa_id) as cpa_id
,coalesce(x.sector_id_1,y.sector_id_1) as sector_id_1
,coalesce(slots_cap,0) as slots_cap
,coalesce(slots_vac,0) as slots_vac
,coalesce(slots_new,0) as slots_new
,coalesce(slots_cap,0) + coalesce(slots_vac,0) + coalesce(slots_new,0) as j_new
,coalesce(j_old,0) as j_old
,calculated j_new + calculated j_old as j_2050
from slots_done_2 as x
full join old_jobs as y on x.parcel_id=y.parcel_id  and x.mgra=y.mgra and x.jur_id=y.jur_id and x.cpa_id=y.cpa_id and x.sector_id_1=y.sector_id_1;
quit;

proc sql; 
create table test_slots_done_1 as 
select sum(slots_cap) as slots_cap 
from slots_done_1; 
quit; 


proc sql;
create table final_j_2050_1a as select * from final_j_2050_1 where mgra=. or jur_id=. or cpa_id=.;

create table final_j_2050_1b as select parcel_id,sector_id_1,count(parcel_id) as n
from final_j_2050_1 group by parcel_id,sector_id_1 having calculated n>1;
quit;

proc sql;
create table test_01 as select jur_id
,sum(slots_cap) as slots_cap
,sum(slots_vac) as slots_vac
,sum(slots_new) as slots_new
,sum(j_new) as j_new
,sum(j_old) as j_old
,sum(j_2050) as j_2050
from final_j_2050_1
group by jur_id;

create table test_02 as select *
,slots_cap/sum(slots_cap) as cap_s format=percent8.1
,slots_vac/sum(slots_vac) as vac_s format=percent8.1
,slots_new/sum(slots_new) as new_s format=percent8.1
,j_new/sum(j_new) as j_new_s format=percent8.1
,j_old/sum(j_old) as j_old_s format=percent8.1
from test_01;
quit;

proc sql;
create table final_j_2050_1c as select sector_id_1,sum(j_2050) as j_2050,sum(j_new) as j_new
from final_j_2050_1 group by sector_id_1;
quit;


proc sql;
create table p_j_1 as select parcel_id,sector_id_1,mgra,jur_id,cpa_id,j_new
,slots_cap, slots_vac, slots_new as slots_clone
from final_j_2050_1 where j_new > 0;
quit;

data p_j_2(drop=i j_new);set p_j_1(drop = slots_cap slots_vac slots_clone);
do i=1 to j_new;
	output;
end;
run;

proc sql;
create table p_j_3 as select * from p_j_2
order by sector_id_1,ranuni(&by1 - 18);
quit;

data p_j_4;set p_j_3;by sector_id_1;retain i;
if first.sector_id_1 then i=1;else i=i+1;
run;

proc sql;
create table jf_02 as select x.yr,x.sandag_industry_id,x.sector_id_1,x.type,x.jobs, y.j_2016, x.jobs - y.j_2016 as jc /* job change */
from jf_01 as x
left join
(
select sector_id as sandag_industry_id,sector_id_1,type,count(job_id) as j_2016
from pj_2016_0
group by sandag_industry_id,sector_id_1,type
) as y
on x.sandag_industry_id = y.sandag_industry_id and x.sector_id_1 = y.sector_id_1 and x.type = y.type
order by sector_id_1,type,yr;

create table jf_02a as select * from jf_02 where jc < 0;
quit;

/*
step 1: assign sandag_industry_id and type
step 2: assign year of activation
*/

proc sql;
create table jf_03 as select sector_id_1,type,sum(jc) as jc
from jf_02 where yr = 2050 
group by sector_id_1,type
order by sector_id_1,ranuni(&by1 + 1);
quit;

data jf_03a;set jf_03;by sector_id_1; retain i2;
if first.sector_id_1 then do; i1 = 1; i2 = jc; end;
else do; i1 = i2 + 1; i2 = i1 + jc - 1; end;
run;

proc sql;
create table test_10 as select x.*, y.jc
from (select sector_id_1,count(*) as j from p_j_4 group by sector_id_1) as x
inner join (select sector_id_1,sum(jc) as jc from jf_03 group by sector_id_1) as y
on x.sector_id_1 = y.sector_id_1
where x.j ^= y.jc;
quit;

proc sql;
create table p_j_5 as select x.*,y.type
from p_j_4 as x
left join jf_03a as y on x.sector_id_1 = y.sector_id_1
where y.i1 <= x.i <= y.i2;

create table p_j_5a as select * from p_j_5 where type = .;

create table p_j_6 as select * from p_j_5(drop=i)
order by sector_id_1, type, ranuni(&by1 + 2);
quit;

data p_j_6;set p_j_6;by sector_id_1 type;retain i;
if first.type then i = 1;else i = i + 1;
run;

data jf_04;set jf_02(keep=yr sector_id_1 type jc);by sector_id_1 type; retain i2;
if first.type then do; i1 = 1; i2 = jc; end;
else do; i1 = i2 + 1; i2 = jc; end;
run;

proc sql;
create table p_j_7 as select x.*,y.yr
from p_j_6 as x
left join jf_04 as y on x.sector_id_1 = y.sector_id_1 and x.type = y.type
where y.i1 <= x.i <= y.i2
order by sector_id_1,type,yr;

create table p_j_7a as select * from p_j_7 where yr = .;
quit;


proc sql;
create table p_j_9 as select parcel_id,mgra,jur_id,cpa_id,int(sector_id_1) as sandag_industry_id,type,yr
from p_j_7
order by parcel_id; 
quit;

proc sql;
create table dev_j_4 as select parcel_id,mgra,jur_id,cpa_id
,case
when sandag_industry_id = 15 and type = 3 then 15.3
when sandag_industry_id = 15 and type = 4 then 15.4
when sandag_industry_id = 28 and type = 2 then 28.2
when sandag_industry_id = 28 and type = 3 then 28.3
when sandag_industry_id = 28 and type = 4 then 28.4
else sandag_industry_id
end as sandag_industry_id_1
,sum(j) as j
from dev_j_2 group by parcel_id,mgra,jur_id,cpa_id,sandag_industry_id_1;
quit;

proc sql;
create table job_slots_by_source as 
select
coalesce(x.parcel_id,y.parcel_id) as parcel_id
,coalesce(x.sector_id_1,y.sandag_industry_id_1) as sandag_industry_id_1
,coalesce(x.mgra,y.mgra) as mgra
,coalesce(x.jur_id,y.jur_id) as jur_id
,coalesce(x.cpa_id,y.cpa_id) as cpa_id
,coalesce(x.slots_cap,0) as slots_capacity
,coalesce(x.slots_vac,0) as slots_vacancy
,coalesce(x.slots_clone,0) as slots_cloned
,coalesce(y.j,0) as slots_events
from p_j_1 as x
full join dev_j_4 as y
on x.parcel_id = y.parcel_id and x.sector_id_1 = y.sandag_industry_id_1 and x.mgra=y.mgra and x.jur_id=y.jur_id and x.cpa_id=y.cpa_id
order by parcel_id;
quit;

proc sql; 
create table test_by_source as 
select sum(slots_capacity) as sc, sum(slots_vacancy) as sv, sum(slots_cloned) as slc, sum(slots_events) as se
from job_slots_by_source; 
quit; 

data e1.jobs_from_capacities; set p_j_9;run;

data e1.job_slots_by_source; set job_slots_by_source;run;
