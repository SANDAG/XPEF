libname id "T:\socioec\Current_Projects\estimates\input_data";

libname usr odbc noprompt="driver=SQL Server; server=sql2014a8; database=urbansim;
Trusted_Connection=yes" schema=ref;

libname us odbc noprompt="driver=SQL Server; server=sql2014a8; database=urbansim;
Trusted_Connection=yes" schema=urbansim;




proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table p_sga_1 as
select *,put(inside_x*10000,11.)||"_"||put(inside_y*10000,11.) as poly_id
from connection to odbc
(
select x.p_id as parcel_id,x.objectid,x.shape.STArea() as area
,y.shape.STArea() as area_p
,x.development_type_id_2017
,x.namecc,x.namerv,x.namesu,x.nametc,x.nametco,x.nameuc,x.namemc
,x.inside_x,x.inside_y
,y.jurisdiction_id as p_jur_id
FROM OPENQUERY (socioeca8, 'SELECT * FROM ws_archive.dbo.sga_2016_p') as x
left join [urbansim].[urbansim].[parcel] as y
on x.p_id=y.parcel_id
);

disconnect from odbc;
quit;


proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table p_us_1a as select * from connection to odbc
(select parcel_id,du_2017,shape.STArea() as p_area
,parcel_acres,proportion_undevelopable
,development_type_id_2017 as dev_id
,du_2017,capacity_2 as capacity,site_id,max_res_units_2017
,jurisdiction_id as p_jur_id
from [urbansim].[urbansim].[parcel]
);

create table p_us_1b as select * from connection to odbc
(select parcel_id,site_id,capacity_3
from [urbansim].[urbansim].[scheduled_development_parcel]
);

disconnect from odbc;
quit;

proc sql;
create table test_01 as select parcel_id,count(parcel_id) as n
from p_us_1b group by parcel_id having calculated n>1;
quit;

proc sql;
create table p_us_1 as select x.parcel_id,x.du_2017,x.p_area,x.parcel_acres
,x.proportion_undevelopable
,x.dev_id
,coalesce(y.capacity_3,x.capacity) as capacity
,x.site_id,x.max_res_units_2017
,x.p_jur_id
from p_us_1a as x
left join p_us_1b as y on x.parcel_id=y.parcel_id;
quit;

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table cwa_1 as
select *
from connection to odbc
(
select x.parcel_id
FROM [urbansim].[urbansim].[parcel] as x
inner join OPENQUERY(sql2014b8, 'SELECT * FROM [lis].[GIS].[CountyWaterAuthority]')  as y
on x.centroid.STIntersects(y.shape) = 1
where x.jurisdiction_id=19
);

disconnect from odbc;
quit;





proc sql;
create table p_us_3 as select parcel_id
,du_2017 as du_2017
,capacity as remaining_cap
,site_id
,p_area as parcel_sqft
,dev_id
,p_jur_id
from p_us_1;

create table p_us_3a as select sum(remaining_cap) as remaining_cap from p_us_3;
create table p_us_3b as select sum(remaining_cap) as remaining_cap from p_us_3 where site_id=.;
quit;



proc sql;
create table p_sga_2 as select distinct parcel_id,area_p as total_parcel_area
from p_sga_1;

create table p_sga_2a as select parcel_id,count(*) as n
from p_sga_2 group by parcel_id having calculated n>1;
quit;

/* pull name,Potential from lis.GIS.SmartGrowthAreas2016 */

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014b8;Trusted_Connection=yes;") ;

create table sga_prop_0 as
select *
from connection to odbc
(
select distinct sg_type,name,proposed
FROM [lis].[gis].[SmartGrowthAreas2016]
);

disconnect from odbc;

create table sga_prop_0a as select name,count(name) as n
from sga_prop_0 group by name having calculated n>1;
quit;

proc sql;
create table sga_du(sga_type char format=$6.,du_per_acre num format=5.1);
/* source http://www.sandag.org/programs/land_use_and_regional_growth/comprehensive_land_use_and_regional_growth_projects/RCP/targets.pdf*/
insert into sga_du
values("cc", 20)
values("mc", 75)
values("rv", 10.9)
values("tc", 20)
values("tco", 25)
values("uc", 40)
;
update sga_du set du_per_acre = du_per_acre /*+ 10*/;
quit;

proc sql;
create table sga_jur_1 as select distinct substr(name,1,2) as jur2
from sga_prop_0 order by jur2;

create table sga_jur_2 as select x.*,y.name as jur_name
from (select
jur2
,case
when jur2='CB' then 1
when jur2='CN' then 19
when jur2='CO' then 3
when jur2='CV' then 2
when jur2='DM' then 4
when jur2='EC' then 5
when jur2='EN' then 6
when jur2='ES' then 7
when jur2='IB' then 8
when jur2='LM' then 9 /* corrected from LG in prior versions */
when jur2='LG' then 10 /* corrected from LM in prior versions */
when jur2='NC' then 11
when jur2='OC' then 12
when jur2='PW' then 13
when jur2='SB' then 17
when jur2='SD' then 14
when jur2='SM' then 15
when jur2='ST' then 16
when jur2='VS' then 18
end as jur_id
from sga_jur_1) as x
inner join id.sf1_place as y on x.jur_id=y.jur;
quit;



proc sql;
create table poly_1 as select parcel_id as p_id,development_type_id_2017 as dev_id
,p_jur_id
,namecc,nametc,nametco,nameuc,namemc
,poly_id
,area
from p_sga_1 where (namecc^="" /*or namerv^="" or  namesu^=""*/ or nametc^="" or nametco^="" or nameuc^="" or namemc^="") and
development_type_id_2017 not in (15,24,27,28,29);
quit;

proc sql;
create table poly_2 as
select distinct p_id, p_jur_id, dev_id,"cc" as sga_type,namecc as sga_name,poly_id,area
from poly_1 where namecc^=""
	union all
select distinct p_id, p_jur_id, dev_id,"tc" as sga_type,nametc as sga_name,poly_id,area
from poly_1 where nametc^=""
	union all
select distinct p_id, p_jur_id, dev_id,"tco" as sga_type,nametco as sga_name,poly_id,area
from poly_1 where nametco^=""
	union all
select distinct p_id, p_jur_id, dev_id,"uc" as sga_type,nameuc as sga_name,poly_id,area
from poly_1 where nameuc^=""
	union all
select distinct p_id, p_jur_id, dev_id,"mc" as sga_type,namemc as sga_name,poly_id,area
from poly_1 where namemc^="";
quit;

proc sql;
create table p_sga_4 as
select p_id,p_jur_id,dev_id,sga_type,sga_name,sum(area) as area
from poly_2 group by p_id,p_jur_id,dev_id,sga_type,sga_name;
quit;

proc sql;
create table p_sga_4a as select x.p_id,x.dev_id,x.p_jur_id
,u.name as dev_type_name length=24 format=$24.
,y.total_parcel_area,x.sga_type,x.sga_name,z.proposed,x.area
,round(x.area / y.total_parcel_area,0.001) as f
from p_sga_4 as x
inner join p_sga_2 as y on x.p_id=y.parcel_id
inner join sga_prop_0 as z on x.sga_name=z.name
inner join usr.development_type as u on x.dev_id=u.development_type_id;

create table p_sga_4b as select * from p_sga_4a where f>=0.5;

create table p_sga_4b_test_1 as select * from p_sga_4a where f>1;
quit;


proc sql;
create table p_sga_4c as select x.*
,u.jur_name as p_jur_name
,v.jur_id as sga_jur_id
,y.du_per_acre,z.du_2017,z.remaining_cap,z.site_id
,round((x.total_parcel_area * x.f)/43560,.01) as usable_acres
/* rasing du+per_acre by 10% for city of San Diego */
/*
,case
when x.p_jur_id = 14 then round((x.total_parcel_area * x.f)/43560 * (y.du_per_acre * 1.1),1)
else round((x.total_parcel_area * x.f)/43560 * y.du_per_acre,1)
end as du_sga
*/
,round((x.total_parcel_area * x.f)/43560 * y.du_per_acre,1) as du_sga

,case
when calculated du_sga > (z.du_2017 + z.remaining_cap)
	then calculated du_sga - z.du_2017 - z.remaining_cap
else 0
end as du_1
,ranuni(2018) as rn1
,ranuni(2019) as rn2
from p_sga_4b as x
inner join sga_du as y on x.sga_type=y.sga_type
left join p_us_3 as z on x.p_id=z.parcel_id
inner join sga_jur_2 as u on x.p_jur_id=u.jur_id

inner join sga_jur_2 as v on substr(x.sga_name,1,2)=v.jur2
order by p_id,sga_type,du_1 desc,rn1;
quit;

proc sql;
create table p_sga_4d as select distinct p_jur_name,proposed
from p_sga_4c where du_1>0;
quit;

proc sql;
create table test_delmar as select * from p_sga_4c where p_jur_name="Del Mar"
order by proposed;
quit;


/* for each parcel, for each sga_type, select sga_name with the most du_1 */
/* proposed are excluded */
data p_max_1(drop=sga_jur_id);set p_sga_4c;by p_id sga_type;
where du_1>0 and (proposed=0 or p_jur_name in ("Del Mar", "Poway")) and p_jur_name^="Unincorporated";
if first.sga_type;
run;



proc sql;
create table p_max_1a as select x.*
from p_max_1 as x
inner join (select p_id,count(p_id) as n from p_max_1 group by p_id having calculated n>1) as y
on x.p_id=y.p_id
order by p_id,du_1 desc;

create table p_max_1b as select distinct p_jur_id from p_max_1;
quit;

proc sql;
create table p_max_1c as select sga_type,sum(du_1) as du_1,count(distinct sga_name) as n
from p_max_1 group by sga_type;

create table p_max_1d as select p_jur_name,sga_type,sum(du_1) as du_1,count(distinct sga_name) as n
from p_max_1 group by p_jur_name,sga_type;
quit;

proc transpose data=p_max_1d out=p_max_1d1(drop=_name_);by p_jur_name;var du_1;id sga_type;run;
proc transpose data=p_max_1d1 out=p_max_1d2;by p_jur_name;run;

proc sql;
create table p_max_1d3 as select p_jur_name,_name_,coalesce(col1,0) as col1
from p_max_1d2 order by p_jur_name,_name_;
quit;

proc transpose data=p_max_1d3 out=p_max_1d4(drop=_name_);by p_jur_name;var col1;id _name_;run;

proc sql;
create table p_max_1_lu as
select dev_type_name
,sum(area)/43560 as acres format=comma7.,sum(du_1) as new_capacity format=comma7.
from p_max_1 group by dev_type_name
	union all
select "All" as dev_type_name,sum(area)/43560 as acres format=comma6.,sum(du_1) as new_capacity format=comma7.
from p_max_1

order by new_capacity desc;
quit;


proc sort data=p_max_1;by p_id descending du_1 rn2;run;

/*
for each parcel, select sga_name with the most du_1
Only keep the following dev types
(4      ,5       ,18            ,21                     ,25             ,31)
Office | Retail | Mixed Use | Multi-Family Residential | Parking Lot | Vacant Developable Land
*/

data p_max_2;set p_max_1;where dev_id in (4,5,18,21,25,31) and site_id=. and du_1>=5;
by p_id;if first.p_id;
run;


proc sql;
create table p_max_2a as select sga_type,sga_name,sum(du_1) as du_1,count(p_id) as n
from p_max_2 group by sga_type,sga_name
order by sga_type,sga_name;

create table p_max_2b as select p_jur_name,sga_type,sum(du_1) as du_1,count(sga_name) as n
from p_max_2 group by p_jur_name,sga_type order by p_jur_name,sga_type;

create table p_max_2c as select sga_type,sum(du_1) as du_1,count(sga_name) as n
from p_max_2 group by sga_type order by du_1 desc;

create table p_max_2d as select p_jur_id,p_jur_name,sum(du_1) as du_1,count(sga_name) as n
from p_max_2 group by p_jur_id,p_jur_name order by p_jur_id,p_jur_name;

create table p_max_2e as select sum(du_1) as du_1 from p_max_2b;

create table p_max_2f as select sum(du_1) as du_1 from p_max_2 where proposed=0;
quit;

proc sql;
create table p_max_rem_cap as select sum(remaining_cap) as rem_cap
from p_max_2;
quit;

/*
proc export data=p_max_2a outfile="T:\socioec\Current_Projects\Urbansim\lu_scenarios\p_max_2a.dbf"
dbms=dbf replace;run;
*/

/*
proc export data=p_max_2(keep=p_id dev_type_name usable_acres du_2017 du_1)
outfile="T:\socioec\Current_Projects\Urbansim\lu_scenarios\p_max_2.dbf"
dbms=dbf replace;run;
*/



proc sql noprint;
select sum(du_1) into :sgoa from p_max_2;
quit;

%let sgoa = %sysfunc(tranwrd(%quote(&sgoa),%str(,),));
%let sgoa = %sysfunc(compress(&sgoa));
%put &sgoa;



proc sql;
create table cwa_2 as
select x.parcel_id,x.p_jur_id,x.parcel_sqft
from p_us_3 as x
inner join cwa_1 as y on x.parcel_id=y.parcel_id
where x.du_2017=1 and x.remaining_cap=0 and x.site_id=. and x.parcel_sqft>=5000 and x.dev_id=19 and x.p_jur_id=19;
quit;



proc sql;
create table adu_1 as
select parcel_id,p_jur_id,parcel_sqft
from p_us_3 where du_2017=1 and remaining_cap=0 and site_id=. and parcel_sqft>=5000 and dev_id=19 and p_jur_id^=19
	union all
select x.parcel_id,x.p_jur_id,x.parcel_sqft
from p_us_3 as x
inner join cwa_1 as y on x.parcel_id=y.parcel_id
where x.du_2017=1 and x.remaining_cap=0 and x.site_id=. and x.parcel_sqft>=5000 and x.dev_id=19 and x.p_jur_id=19;

create table adu_1a as select p_jur_id,count(parcel_id) as sfd_parcels_1 format=comma7.
from adu_1 group by p_jur_id
order by p_jur_id;

create table adu_1b as select y.name,x.p_jur_id,x.sfd_parcels_1
from adu_1a as x
inner join id.sf1_place as y on x.p_jur_id=y.jur;

create table adu_1c as select *
,sfd_parcels_1/sum(sfd_parcels_1) as sfd_share_1 format=percent8.2
from adu_1b order by name;

/* setting 5% for ADU's */
create table adu_1d as select sum(sfd_parcels_1) as sfd_parcels_1
,round(sum(sfd_parcels_1) * 0.05,1) as sfd_parcels_2
from adu_1b;
quit;


proc sql;
create table adu_2 as select x.*, y.sfd_parcels_2 as adut
,round(x.sfd_share_1 * y.sfd_parcels_2,1) as adu0
from adu_1c as x
cross join adu_1d as y
order by adu0;
quit;

data adu_2a;set adu_2;
aduc+adu0;
run;

proc sort data=adu_2a;by descending adu0;run;

data adu_3;set adu_2a;
if _n_ = 1 then adu1=adu0 + (adut - aduc);
else adu1=adu0;
run;




/* setting total demand for ADUs */
/* %let adut = 39000;  */
proc sql noprint;
select sum(adu1) into :adut from adu_3;
quit;



proc sql;
create table control_1 as select a.*,b.*,a.hug - b.tug as tac,&adut as adut,calculated tac - &adut as sgoat
from (SELECT sum(housing_units_add) as HUG
FROM us.urbansim_target_housing_units where version_id=108) as a
cross join 
(
select sum(cap) as tug
from (select x.parcel_id,coalesce(y.capacity_3,x.capacity_2) as cap
from us.parcel as x
left join us.scheduled_development_parcel as y on x.parcel_id=y.parcel_id
)) as b;
quit;

proc sql;
create table control_2 as select * from control_1;

/*update control_2 set hug = 534000;*/
update control_2 set tac = hug - tug;
update control_2 set sgoat = tac - adut;
quit;

/*
compare sgoat with du_1 in p_max_2e
sgoat is the target
du_1 is max possible
*/

proc sql;
create table sgoa_1 as select p_jur_id,p_jur_name,du_1 as sgoa_max,du_1/sum(du_1) as f
from p_max_2d;

create table sgoa_1a as select x.*,y.sgoat,round(x.f * y.sgoat,1) as sgoa0
from sgoa_1 as x
cross join control_2 as y
order by sgoa0;
quit;

data sgoa_1b;set sgoa_1a;
sgoac+sgoa0;
run;

proc sort data=sgoa_1b;by descending sgoa0;run;

data sgoa_2;set sgoa_1b;
if _n_ = 1 then sgoa1 = sgoa0 + (sgoat - sgoac);
else sgoa1 = sgoa0;
run;


proc sql;
create table sgoa_3 as select x.*,y.name,coalesce(z.sgoa1,0) as sgoa1,u.sgoat
,case
when calculated sgoa1 > x.cap and y.name in ("Del Mar","Coronado","Lemon Grove") then x.cap
else calculated sgoa1 end as sgoa2
from (select p_jur_id,sum(capacity) as cap from p_us_1 group by p_jur_id) as x
left join adu_3 as y on x.p_jur_id=y.p_jur_id
left join sgoa_2 as z on x.p_jur_id=z.p_jur_id
cross join control_2 as u
order by sgoa2;
quit;

data sgoa_3a;set sgoa_3;
c+sgoa2;
run;

data sgoa_4;set sgoa_3a;
if name="San Diego" then sgoa3 = sgoa2 + (sgoat - c);
else sgoa3 = sgoa2;
run;




proc sql;
create table jur_sgoa_adu_1 as select x.p_jur_id,x.name,y.cap as capacity_provided
,x.adu1 as adu_allocated
,y.sgoa3 as sgoa_allocated
,x.adu1 + y.sgoa3 as total_allocated
,y.cap + calculated total_allocated as provided_and_allocated
from adu_3 as x
left join sgoa_4 as y on x.p_jur_id=y.p_jur_id
order by name;

create table jur_sgoa_adu_2 as
select * from jur_sgoa_adu_1 
	union all
select 99 as p_jur_id,"Region" as name
,sum(capacity_provided) as capacity_provided
,sum(adu_allocated) as adu_allocated
,sum(sgoa_allocated) as sgoa_allocated
,sum(total_allocated) as total_allocated
,sum(provided_and_allocated) as provided_and_allocated
from jur_sgoa_adu_1;
quit;

proc sql;
create table jur_sgoa_adu_3 as select name
,round(capacity_provided,100) as capacity_provided format=comma8.
,round(adu_allocated,100) as adu_allocated format=comma8.
,round(sgoa_allocated,100) as sgoa_allocated format=comma8.
,round(total_allocated,100) as total_allocated format=comma8.
,round(provided_and_allocated,100) as provided_and_allocated format=comma8.
from jur_sgoa_adu_2;
quit;

/*
proc export data=jur_sgoa_adu_2(drop=p_jur_id)
outfile="T:\socioec\Current_Projects\Urbansim\lu_scenarios\provided and allocated capacity 05252018.xlsx"
dbms=xlsx replace;run;

proc export data=jur_sgoa_adu_3
outfile="T:\socioec\Current_Projects\Urbansim\lu_scenarios\provided and allocated capacity (rounded) 0525018.xlsx"
dbms=xlsx replace;run;
*/



/*
proc import out=old_cap
datafile="M:\RES\estimates & forecast\SR14 Forecast\Admin\Reports\RPC\2018_05_04_RPC\provided and allocated capacity 04202018.xlsx"
dbms=xlsx replace;run;

proc import out=oldest_cap
datafile="M:\RES\estimates & forecast\SR14 Forecast\Admin\Reports\RPC\2018_05_04_RPC\provided and allocated capacity.xlsx"
dbms=xlsx replace;run;
*/

/*
proc sql;
create table jur_sgoa_adu_4 as select y.p_jur_id,x.name
,x.capacity_provided as cp_old
,y.capacity_provided as cp_new

,x.adu_allocated as adu_old
,y.adu_allocated as adu_new

,x.sgoa_allocated as sgoa_old
,y.sgoa_allocated as sgoa_new
,coalesce(z.du_1,u.du_1,0) as max_sgoa

from old_cap as x
left join jur_sgoa_adu_2 as y on x.name=y.name
left join p_max_2d as z on x.name=z.jur_name
left join (select "Region" as name, * from p_max_2e) as u on x.name=u.name
order by p_jur_id;
quit;

proc sql;
create table jur_sgoa_adu_4a as select p_jur_id,name
,cp_new as capacity_provided
,adu_old as adu_allocated
,case
when name = "Solana Beach" then 200
when name = "San Diego" then 53303 
else min(sgoa_old,max_sgoa)
end as sgoa_allocated
,adu_old + calculated sgoa_allocated as total_allocated
,cp_new + calculated total_allocated as provided_and_allocated
from jur_sgoa_adu_4 where name^="Region"
order by name;
quit;
*/



/*
proc sql;
create table parcels_sgoa_0 as select y.jur as jur_id,p_id as parcel_id,sga_type,sga_name,du_1 as du
from (select * from p_max_2) as x
left join id.sf1_place as y on x.jur_name=y.name
order by jur_id,ranuni(2020);
quit;
*/

proc sql;
create table parcels_sgoa_0 as select p_jur_id,p_id as parcel_id,sga_type,sga_name,du_1 as du
from p_max_2
order by p_jur_id,ranuni(2020);
quit;


data parcels_sgoa_1;set parcels_sgoa_0;by p_jur_id;retain i2;
if first.p_jur_id then i2=du;else i2=i2+du;
i1=i2 - du + 1;
run;

proc sql;
create table parcels_sgoa_2 as select x.p_jur_id,parcel_id,sga_type,sga_name,du
from parcels_sgoa_1 as x
inner join jur_sgoa_adu_2 as y on x.p_jur_id=y.p_jur_id
where x.i1<=y.sgoa_allocated;

create table parcels_sgoa_2a as select x.*,y.sgoa_allocated as du_target,x.du_selected - y.sgoa_allocated as d
from (select p_jur_id,sum(du) as du_selected from parcels_sgoa_2 group by p_jur_id) as x
left join jur_sgoa_adu_2 as y on x.p_jur_id=y.p_jur_id;
quit;

data parcels_sgoa_3(drop=du i);set parcels_sgoa_2;
do i=1 to du;output;end;
run;

proc sql;
create table parcels_sgoa_3a as select *
from parcels_sgoa_3 order by p_jur_id,ranuni(2040);
quit;

data parcels_sgoa_3b;set parcels_sgoa_3a;by p_jur_id;retain i;
if first.p_jur_id then i=1;else i=i+1;
run;

proc sql;
create table parcels_sgoa_3c as select x.*
from parcels_sgoa_3b as x
inner join parcels_sgoa_2a as y on x.p_jur_id=y.p_jur_id
where x.i<=y.du_target;

create table parcels_sgoa_4 as select p_jur_id,parcel_id,sga_type,sga_name,count(i) as du
from parcels_sgoa_3c group by p_jur_id,parcel_id,sga_type,sga_name;

create table parcels_sgoa_4a as select x.*,y.sgoa_allocated as du_target,x.du_selected - y.sgoa_allocated as d
from (select p_jur_id,sum(du) as du_selected from parcels_sgoa_4 group by p_jur_id) as x
left join jur_sgoa_adu_2 as y on x.p_jur_id=y.p_jur_id;

create table parcels_sgoa_4b as select * from parcels_sgoa_4a where d^=0;
quit;


proc sql;
create table parcels_adu_0 as select p_jur_id,parcel_id
from adu_1 order by p_jur_id,ranuni(2050);
quit;

data parcels_adu_1; set parcels_adu_0;by p_jur_id;retain i;
if first.p_jur_id then i=1;else i=i+1;
run;

/*
proc sql;
create table parcels_adu_1a as select jur_id,count(parcel_id) as n
from parcels_adu_1 group by jur_id;
quit;
*/


proc sql;
create table parcels_adu_2 as select x.p_jur_id,x.parcel_id
from parcels_adu_1 as x
inner join jur_sgoa_adu_2 as y on x.p_jur_id=y.p_jur_id
where x.i<=y.adu_allocated;
quit;

/* setting version */
%let ver=107;


proc sql;
create table additional_capacity as
select &ver as version_id,p_jur_id as jur_id,parcel_id,sga_type as type,sga_name as name,du from parcels_sgoa_4
	union all
select &ver as version_id,p_jur_id as jur_id,parcel_id,"adu" as sga_type as type,"" as name,1 as du from parcels_adu_2;

create table additional_capacity_test_1 as select jur_id,type,sum(du) as du
from additional_capacity group by jur_id,type;

create table additional_capacity_test_2 as select type,sum(du) as du
from additional_capacity group by type;

create table additional_capacity_test_3 as select sum(du) as du
from additional_capacity;
quit;


/*
proc sql;
delete * from us.additional_capacity where version_id=&ver;

insert into us.additional_capacity(bulkload=yes bl_options=TABLOCK) select * from additional_capacity;
quit;
*/

/* create table sql_urb.additional_capacity(bulkload=yes bl_options=TABLOCK) as select * from additional_capacity; */
