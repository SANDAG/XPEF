/* %let xprev=xpef04; */

libname xprev odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;
Trusted_Connection=yes" schema=&xprev;



/*
%let xver=xpef05;

libname sql_xpef odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;
Trusted_Connection=yes" schema=&xver;

libname e1 "T:\socioec\Current_Projects\&xver\input_data";

libname e2 "T:\socioec\Current_Projects\&xprev\input_data";
*/

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table dof_est_0 as select
case when area_type="City" then area_name else summary_type end as Name
,vintage_yr,est_yr as yr,household_pop as hp_dof
from connection to odbc
(
select * FROM [socioec_data].[ca_dof].[population_housing_estimates]
where county_name= 'San Diego' and est_yr in (2016,2017,2018) and vintage_yr in (2017,2018) and (area_type = 'City' or summary_type = 'Unincorporated')
);

disconnect from odbc;
quit;

proc sql;
create table dof_est_1 as select y.jur,x.*
from dof_est_0 as x
left join e1.sf1_place as y on x.name = y.name;

create table dof_est_1a as select yr,vintage_yr,sum(hp_dof) as hp_dof
from dof_est_1 group by yr,vintage_yr;

create table dof_est_2 as select x.jur,x.name,x.vintage_yr,x.yr,x.hp_dof - y.hp_dof as d
from dof_est_1 as x
inner join dof_est_1 as y on x.jur=y.jur and x.vintage_yr=y.vintage_yr and x.yr=y.yr+1
order by jur,vintage_yr,yr;
quit;

proc sql;
create table dof_est_3 as select *,d/sum(d) as d_p
from dof_est_2 where vintage_yr=2018 and yr=2018;
quit;

proc sql;
create table hp_0 as select yr,count(hp_id) as hp0
from xprev.household_population group by yr order by yr;
quit;

/*
proc sql;
create table hp_test_1 as select x.*,y.hh_id as hh_id_hh,z.hh_id as hh_id_hu
from xprev.household_population as x
left join xprev.households as y on x.yr=y.yr and x.hh_id=y.hh_id
left join xprev.housing_units as z on x.yr=z.yr and x.hh_id=z.hh_id;

create table hp_test_2 as select * from hp_test_1 where hh_id_hh = .
order by hh_id,yr;
quit;

proc sql;
create table hp_test_3 as select distinct hh_id
from hp_test_2 where yr=2018;
quit;
*/


proc sql;
create table hh_1 as select x.yr,y.jur,count(distinct x.hh_id) as hh
from xprev.households as x
inner join xprev.housing_units as y on x.yr=y.yr and x.hh_id=y.hh_id
group by x.yr,y.jur;

create table hp_1 as select x.yr,y.jur,count(x.hp_id) as hp
,count(distinct x.hh_id) as hh
from xprev.household_population as x
inner join xprev.households as y on x.yr=y.yr and x.hh_id=y.hh_id
/*inner join xprev.housing_units as z on y.yr=z.yr and y.hh_id=z.hh_id*/
group by x.yr,y.jur;

/*
create table hu_1 as select x.*,coalesce(y.hu_sto,0) as hu_sto,coalesce(z.hu_vac,0) as hu_vac
from (select yr,jur,count(hu_id) as hu_all, count(hh_id) as hu_occ
	from xprev.housing_units group by yr,jur) as x

left join (select yr,jur,count(hu_id) as hu_sto
	from xprev.housing_units where sto_flag=1 group by yr,jur) as y
	on x.yr=y.yr and x.jur=y.jur
left join (select yr,jur,count(hu_id) as hu_vac
	from sql_xpef.housing_units where sto_flag=0 and hh_id=. group by yr,jur) as z
	on x.yr=z.yr and x.jur=z.jur;
*/
quit;

proc sql;
create table hp_1a as select yr,sum(hp) as hp
from hp_1 group by yr;
quit;



/*
proc sql;
create table jur_1 as select x.*,y.hh,z.hp,z.hp/y.hh as pph format=5.2
from hu_1 as x
left join hh_1 as y on x.yr=y.yr and x.jur=y.jur
left join hp_1 as z on x.yr=z.yr and x.jur=z.jur
order by jur,yr;
quit;
*/

proc sql;
create table jur_1 as select *,hp/hh as pph format=5.2
from hp_1;
quit;

proc sql;
create table jur_2 as select x.*,x.hp - y.hp as d1
,case when (x.hp - y.hp) <0 then 0 else (x.hp - y.hp) end as d2
from jur_1 as x
left join jur_1 as y on x.jur=y.jur and x.yr=y.yr+1
order by jur,yr;
quit;

data jur_3(drop=hpc);set jur_2/*(drop=hu_all hu_sto hu_occ)*/;by jur;retain hpc;
if first.jur then do; hp2 = hp; hpc = hp2; end;
else do; hp2 = hpc + d2; hpc = hp2; end;
run;

proc sql;
create table jur_4 as select *,d2/sum(d2) as d2_p
from jur_3 group by yr
order by jur,yr;
quit;

proc sql;
create table jur_4a as select x.*,y.d as est_d,y.d_p as est_p
from jur_4 as x
left join dof_est_3 as y on x.jur=y.jur and x.yr=y.yr
order by jur,yr;
quit;


proc sql;
create table reg_1 as select yr,sum(hp) as hp
from jur_3 group by yr;

create table reg_2 as select x.*,x.hp - y.hp as reg_d
from reg_1 as x
inner join reg_1 as y on x.yr=y.yr+1
order by yr;
quit;

proc sql;
create table jur_5 as select x.jur,x.yr/*,x.hu_vac*/,x.hh,x.hp as hp0,x.pph as pph0,x.d1,x.d2
,y.reg_d
,case
when x.yr=2018 then round(x.est_p * y.reg_d,1) 
else round(x.d2_p * y.reg_d,1) 
end as d3
from jur_4a as x
inner join reg_2 as y on x.yr=y.yr
order by yr,d3;
quit;

data jur_6/*(drop=dc d3 reg_d)*/;set jur_5;by yr;retain dc;
if first.yr then do; d4 = d3; dc = d4; end;
else if last.yr then do; d4 = reg_d - dc; dc = dc + d4; end;
else do; d4 = d3; dc = dc + d4; end;
run;

data jur_6_test;set jur_6;by yr;if last.yr;
if reg_d = dc then delete;
run;

proc sort data=jur_6;by jur yr;run;

data jur_6a;set jur_6;by jur;retain d5;
if first.jur then d5=d4;else d5=d5+d4;
run;


proc sql;
create table jur_7 as select x.jur,x.yr,x.d5+y.hp as hp4
from jur_6a as x
inner join (select * from jur_4 where yr=2017) as y on x.jur=y.jur
order by jur,yr;
quit;

proc sql;
create table jur_8 as select x.yr,x.jur/*,x.hu_vac*/,x.hh,x.hp as hp0,x.pph as pph0
,case when x.yr=2017 then x.hp else y.hp4 end as hp1
,case when x.yr=2017 then x.pph else y.hp4/x.hh end as pph1 format=5.2
,calculated hp1 - x.hp as d
from jur_4 as x
left join jur_7 as y on x.jur=y.jur and x.yr=y.yr
order by jur,yr;

create table jur_8a as select yr,sum(hp0) as hp0,sum(hp1) as hp1
from jur_8 group by yr;

create table jur_8b as select * from jur_8a where hp0 ^= hp1;
quit;


/*
proc sql;
create table jur_8c as select x.*, y.hp_future, z.hp_future_2
from jur_8a as x
inner join (select yr,sum(hp) as hp_future from e1.future_hp group by yr) as y
	on x.yr = y.yr
inner join (select yr,sum(hp) as hp_future_2 from e2.future_hp group by yr) as z
	on x.yr = z.yr
order by yr;

create table jur_8d as select * from jur_8c
where hp0 ^= hp1 or hp0 ^= hp_future or hp1 ^= hp_future;
quit;
*/



proc sql;
create table dof_update_jur as select yr,jur,hh,hp1 as hp
from jur_8
order by yr,jur;
quit;

data e1.dof_update_jur;set dof_update_jur;run;
