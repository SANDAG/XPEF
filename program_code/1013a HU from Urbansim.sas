
proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table p_01 as select *
from connection to odbc
(
select
x.parcel_id,x.shape.STArea() as parcel_area,x.mgra_id as mgra_p,x.block_id as blk_p,x.jurisdiction_id as jur_p
FROM urbansim.urbansim.parcel as x
inner join (select distinct parcel_id from [urbansim].[urbansim].[urbansim_lite_output] where run_id=&usver) as v on x.parcel_id=v.parcel_id 
);

create table p_1 as select *
from connection to odbc
(
select
x.parcel_id,x.shape.STArea() as parcel_area,x.mgra_id as mgra_p,x.block_id as blk_p,x.jurisdiction_id as jur_p
,y.mgra as mgra_c,y.BLOCKID10 as blk_c,y.jur_&by1 as jur_c
,x.shape.STIntersection(z.shape).STArea() as area
,z.mgra as mgra,z.BLOCKID10 as blk,z.jur_&by1 as jur
FROM urbansim.urbansim.parcel as x
inner JOIN [estimates].[dbo].[BLK2010_JUR_POST2010] as y on x.centroid.STIntersects(y.shape) = 1
inner JOIN [estimates].[dbo].[BLK2010_JUR_POST2010] as z on x.shape.STIntersects(z.shape) = 1
inner join (select distinct parcel_id from [urbansim].[urbansim].[urbansim_lite_output] where run_id=&usver) as v on x.parcel_id=v.parcel_id 
);

disconnect from odbc;
quit;

proc sql;
create table p_01_test as select x.*
from (select distinct parcel_id from p_01) as x
left join (select distinct parcel_id from p_1) as y on x.parcel_id=y.parcel_id
where y.parcel_id=.;
quit;

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table p_2 as select *
from connection to odbc
(
select x.parcel_id,x.development_type_id_2015 as dt_2015,x.lu_2015
,x.development_type_id_2017 /* change to x.development_type_id_&by1 */ as dt_&by1
,x.lu_2017  /* change to x.lu_&by1 */ as lu_&by1
,y.gplu as lu_2099
FROM [urbansim].[urbansim].[parcel] as x
left join [urbansim].[urbansim].[general_plan_parcel] as y on x.parcel_id=y.parcel_id);

create table urb_hu_1 as select *
from connection to odbc
(
select parcel_id,year_simulation as yr,unit_change as du,capacity_type
FROM [urbansim].[urbansim].[urbansim_lite_output]
where run_id=&usver 
);

create table lu_names as select *
from connection to odbc
(select lu_code,lu_name FROM [urbansim].[ref].[lu_code]);

create table dt_names as select *
from connection to odbc
(select development_type_id as dt_code,name as dt_name FROM [urbansim].[ref].[development_type]);

create table dt_lu as select *
from connection to odbc
(select x.development_type_id as dt_code,x.lu_code,y.lu_name,z.name as dt_name
FROM [urbansim].[ref].[development_type_lu_code] as x
inner join [urbansim].[ref].[lu_code] as y on x.lu_code=y.lu_code
inner join [urbansim].[ref].[development_type] as z on x.development_type_id=z.development_type_id);

disconnect from odbc;
quit;

proc sql;
create table p_2_test as select * from p_2 where lu_2099=.;
quit;


proc sql;
create table p_1a as select parcel_id,parcel_area,mgra_p,substr(blk_p,6,6) as ct_p,jur_p
,mgra_c,substr(blk_c,6,6) as ct_c,jur_c
,mgra,substr(blk,6,6) as ct,jur
,sum(area) as area
from p_1 where area>0
group by parcel_id,parcel_area,mgra_p,ct_p,jur_p,mgra_c,ct_c,jur_c,mgra,ct,jur
order by parcel_id,area desc;
quit;

data p_1a;set p_1a;by parcel_id;retain i;
if first.parcel_id then i=1;else i=i+1;
run;

proc sql;
create table test_1 as select min(area) as mn from p_1a where i=1;
create table test_2 as select min(area) as mn from p_1a where i=2;
quit;

proc sql;
create table p_1b as select *,count(parcel_id) as n
from (select * from p_1a where area>4)
group by parcel_id;
quit;

proc sql;
create table test_3 as select distinct parcel_id from p_1b where parcel_area<500;

create table test_4 as select x.*
from urb_hu_1 as x
inner join test_3 as y on x.parcel_id=y.parcel_id;
quit;

proc sql;
create table test_5_j as select *
from p_1b where i=1 and
(jur_p^=int(jur_c/100) or jur_p^=int(jur/100) or int(jur_c/100)^=int(jur/100));

create table test_5_m as select *
from p_1b where i=1 and
(mgra_p^=mgra);

create table test_5_c as select *
from p_1b where i=1 and
(ct_p^=ct);
quit;

proc sql;
create table test_6 as select * from p_1b
where parcel_id in (691540,5049170,5049171) and i=1;
quit;

proc sql;
create table test_7 as select * from urb_hu_1 where parcel_id=5308189;
quit;

/*
parcel ... should be in jur/cpa ...
73610 700
258510 500
699257 1909
712884 500
722882 700
1547986 700
5123363 500
5291540 700

roadways
5308189
*/

proc sql;
create table test_01 as select distinct jur from p_1b;
quit;


proc sql;
create table p_3 as select parcel_id,mgra,int(jur/100) as jur
,case when int(jur/100) in (14,19) then jur else 0 end as end as cpa
,ct
from p_1b where i=1;

create table p_3a as select distinct cpa from p_3;
quit;



proc sql;
drop table sql_xpef.parcel_du_xref_post2017;

create table sql_xpef.parcel_du_xref_post2017(bulkload=yes bl_options=TABLOCK) as
select parcel_id,mgra,jur as jur_id,cpa as cpa_id, ct from p_3;
quit;


proc sql;
create table p_2a as select distinct lu_2099
from p_2;
quit;

proc sql;
create table urb_hu_1a as select parcel_id,count(distinct capacity_type) as c
from urb_hu_1 group by parcel_id having calculated c>1;

create table urb_hu_1b as select x.*
from urb_hu_1 as x
inner join urb_hu_1a as y on x.parcel_id=y.parcel_id
order by parcel_id,capacity_type,yr;

create table urb_hu_1c as select distinct capacity_type from urb_hu_1;
quit;

proc sql;
create table urb_hu_1_test_1 as select yr,sum(du) as du
from urb_hu_1 group by yr;

create table urb_hu_1_test_2 as select sum(du) as du
from urb_hu_1;
quit;


proc sql;
create table urb_hu_2 as select x.*
,y.dt_&by1,y.lu_&by1,y.lu_2099
,z.lu_name,z.dt_code,z.dt_name
from urb_hu_1 as x
left join p_2 as y on x.parcel_id=y.parcel_id
left join dt_lu as z on y.lu_2099=z.lu_code;

create table urb_hu_2_test_1 as select yr,sum(du) as du from urb_hu_1 group by yr;
create table urb_hu_2_test_2 as select * from urb_hu_2 where lu_2099=.;
quit;

proc sql;
create table urb_hu_2a as select distinct capacity_type,dt_&by1,lu_&by1,lu_2099,lu_name,dt_code,dt_name
from urb_hu_2 where capacity_type in ("jur","sch");

create table urb_hu_2b as select distinct capacity_type,lu_2099,lu_name,dt_code,dt_name
from urb_hu_2 where capacity_type in ("jur","sch");

create table urb_hu_2c as select distinct dt_code,dt_name from urb_hu_2b;
quit;


proc sql;
create table urb_hu_3 as select *
,case
when capacity_type="adu" then "SFA"
when capacity_type in ("cc","mc","tc","tco","uc") then "MF"
when dt_code=19 then "SFD"
when dt_code=20 then "SFA"
else "MF"
end as du_type
from urb_hu_2;

create table urb_hu_3a as select du_type,min(du) as min_du,max(du) as max_du
from urb_hu_3 group by du_type;

create table urb_hu_3b as select * from urb_hu_3 where du_type="MF" and du=1;
quit;

proc sql;
create table urb_hu_4 as select x.parcel_id,x.yr,x.du,x.capacity_type,x.du_type
,y.mgra,y.jur,y.cpa,y.ct
from urb_hu_3 as x
left join p_3 as y on x.parcel_id=y.parcel_id;

create table urb_hu_5 as select mgra,jur,cpa,ct,du_type,yr,sum(du) as du
from urb_hu_4 group by mgra,jur,cpa,ct,du_type,yr;

create table urb_hu_5a as select du_type,yr,sum(du) as du
from urb_hu_5 group by du_type,yr;

create table urb_hu_5b as select du_type,sum(du) as du
from urb_hu_5 group by du_type;

create table urb_hu_5c as select yr,sum(du) as du
from urb_hu_5 group by yr;

create table urb_hu_5d as select sum(du) as du
from urb_hu_5;
quit;

proc sql;
create table test_02 as select distinct cpa from urb_hu_5;
quit;
