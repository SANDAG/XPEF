/* %let xprev=xpef04; */

/*
libname xprev odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;
Trusted_Connection=yes" schema=&xprev;
*/


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
,vintage_yr,est_yr as yr,household_pop as hp_dof,occupied as hh_dof
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

/*
proc sql;
create table test_sto as select count(hu_id) as sto from sd.hu_2017 where sto_flag=1;
quit;
*/


/*---new section----*/

/*
proc sql;
create table dof_est_4 as select jur,name,yr,hp_dof,hh_dof from dof_est_1
where vintage_yr = &by2 and yr=&by2;
quit;
*/

/* removing units in DeAnza cove */
/* these three tables are not needed
create table yr as select distinct yr from hu_old_1;

create table hu_old_0a as select jur,count(hu_id) as hu
from sd.hu_&by1 where mgra not in (3644,3631) and sto_flag=0
group by jur;

create table urb_hu_6 as select jur,yr,sum(du) as du
from urb_hu_4 group by jur,yr;
*/

proc import out=hh_target_0(drop=hu_g hu2 f4 f5 rename=(yr_built_during=yr hh2=hh_rt)) /* hh_rt: hh regional target */
datafile="T:\socioec\Current_Projects\&xver\input_data\HU Construction and PPH projections 7.xlsx"
replace dbms=excelcs; RANGE='Data2$k1:p36'n;
run;

proc sql;
create table hh_target_1 as select x.yr,x.hh_rt - y.hh_rt as hhd_rt
from hh_target_0 as x
inner join hh_target_0 as y on x.yr = y.yr + 1
order by yr;
quit;

proc sql;
create table fut_hu_0 as select jur,yr,count(hu_id) as hu from sd.ludu where sto_flag=0 group by jur,yr;

create table fut_hu_1 as select x.jur,x.yr,x.hu, x.hu - y.hu as hu_d
from fut_hu_0 as x
inner join fut_hu_0 as y
on x.jur=y.jur and x.yr = y.yr + 1
order by jur,yr;

create table fut_hp as select yr,sum(hp) as hp
from e1.future_hp
group by yr;
quit;


proc sql;
create table tab_0 as select z.yr,x.jur,x.hp,x.hh,y.hu,z.hu as hu2,z.hu - y.hu as hu_d
,x.hp / x.hh as pph
,x.hh / y.hu as or /* occupancy rate */
,calculated hu_d * calculated or as hh_d0
from (select jur,count(hh_id) as hh,sum(size) as hp from sd.hh_2017 group by jur) as x
inner join (select jur,count(hu_id) as hu from sd.hu_2017 where sto_flag=0 group by jur) as y
	on x.jur=y.jur
inner join fut_hu_0 as z on x.jur=z.jur
where z.yr=2018;

create table tab_1 as select x.*, y.hhd_rt, round(x.s_hh * y.hhd_rt,1) as hhd0 
from (select *,hh_d0/sum(hh_d0) as s_hh from tab_0) as x
cross join (select * from hh_target_1 where yr=2017) as y
order by hhd0;
quit;

data tab_1a;set tab_1;hhc+hhd0;run;
proc sort data=tab_1a;by descending hhd0;run;

data tab_1b;set tab_1a;
if _n_ = 1 then hhd1 = hhd0 + (hhd_rt - hhc);
else hhd1=hhd0;
hh2 = hh + hhd1;
hp0 = hh2 * pph;
run;

proc sql;
create table tab_2 as select x.*,y.hp as hpt
,max(x.hp, round(x.hp0_s * y.hp,1)) as hp1
from (select *,hp0/sum(hp0) as hp0_s from tab_1b) as x
cross join (select * from fut_hp where yr=2018) as y
order by hp0;
quit;

data tab_2a;set tab_2;hpc + hp1;run;
proc sort data=tab_2a;by descending hp1;run;

data tab_2b;set tab_2a;
if _n_ = 1 then hp2 = hp1 + (hpt - hpc);
else hp2 = hp1;
pph2 = hp2 / hh2;
or2 = hh2 / hu2;
run;

/* this is for 2018 */
proc sql;
create table tab_2018 as select yr,jur,hp,hh,hu,hp2,hh2,hu2,pph format=5.2, pph2 format=5.2
,or format=percent8.1, or2 format=percent8.1
from tab_2b;
quit;

%macro fut;

data tab_4;set tab_2018;run;

%do yr = 2019 %to 2051;

/* %let yr = 2019; */

proc sql;
create table tab_0 as select z.yr, x.jur, x.hp2 as hp, x.hh2 as hh, x.hu2 as hu
,z.hu as hu2,z.hu - x.hu as hu_d
/*,y.hu,z.hu as hu2,z.hu - y.hu as hu_d*/
,x.pph2 as pph
,x.or2 as or /* occupancy rate */
,calculated hu_d * x.or as hh_d0
from (select * from tab_4 where yr = &yr - 1) as x
/* inner join (select jur,count(hu_id) as hu from sd.hu_2017 where sto_flag=0 group by jur) as y
	on x.jur=y.jur */
inner join fut_hu_0 as z on x.jur=z.jur
where z.yr = &yr;

create table tab_1 as select x.*, y.hhd_rt, round(x.s_hh * y.hhd_rt,1) as hhd0 
from (select *,hh_d0/sum(hh_d0) as s_hh from tab_0) as x
cross join (select * from hh_target_1 where yr = &yr - 1) as y
order by hhd0;
quit;

data tab_1a;set tab_1;hhc+hhd0;run;
proc sort data=tab_1a;by descending hhd0;run;

data tab_1b;set tab_1a;
if _n_ = 1 then hhd1 = hhd0 + (hhd_rt - hhc);
else hhd1=hhd0;
hh2 = hh + hhd1;
hp0 = hh2 * pph;
run;

proc sql;
create table tab_2 as select x.*,y.hp as hpt
,max(x.hp, round(x.hp0_s * y.hp,1)) as hp1
from (select *,hp0/sum(hp0) as hp0_s from tab_1b) as x
cross join (select * from fut_hp where yr = &yr) as y
order by hp0;
quit;

data tab_2a;set tab_2;hpc + hp1;run;
proc sort data=tab_2a;by descending hp1;run;

data tab_2b;set tab_2a;
if _n_ = 1 then hp2 = hp1 + (hpt - hpc);
else hp2 = hp1;
pph2 = hp2 / hh2;
or2 = hh2 / hu2;
run;

proc sql;
create table tab_3 as select yr,jur,hp,hh,hu,hp2,hh2,hu2,pph format=5.2, pph2 format=5.2
,or format=percent8.1, or2 format=percent8.1
from tab_2b;
quit;

proc append base=tab_4 data=tab_3;run;

%end;

%mend fut;

%fut;

proc sql;
create table tab_4a as select * from tab_4
where hu2 < hu or hh2 < hh or hp2 < hp;
quit;

proc sort data=tab_4;by jur yr;run;

proc sql;
create table tab_4b as
select x.jur,x.pph2 as pph_2018 format=5.2,y.pph2 as pph_2051 format=5.2
, 1 - x.or2 as vr_2018 format=percent8.1, 1 - y.or2 as vr_2051 format=percent8.1
from tab_4 as x
inner join tab_4 as y on x.jur=y.jur
where x.yr=2018 and y.yr=2051
	union all
select 20 as jur,x.pph2 as pph_2018 format=5.2,y.pph2 as pph_2051 format=5.2
, 1 - x.or2 as vr_2018 format=percent8.1, 1 - y.or2 as vr_2051 format=percent8.1
from (select sum(hp2)/sum(hh2) as pph2,sum(hh2)/sum(hu2) as or2 from tab_4 where yr=2018) as x
cross join (select sum(hp2)/sum(hh2) as pph2,sum(hh2)/sum(hu2) as or2 from tab_4 where yr=2051) as y

order by jur;
quit;


proc sql;
create table e1.dof_update_jur as select yr,jur,hh2 as hh, hp2 as hp
from tab_4 order by yr,jur;
quit;

/*

proc sql;
create table hp_0 as select yr,count(hp_id) as hp0
from xprev.household_population group by yr order by yr;
quit;

proc sql;
create table hh_1 as select x.yr,y.jur,count(distinct x.hh_id) as hh
from xprev.households as x
inner join xprev.housing_units as y on x.yr=y.yr and x.hh_id=y.hh_id
group by x.yr,y.jur;

create table hp_1 as select x.yr,y.jur,count(x.hp_id) as hp
,count(distinct x.hh_id) as hh
from xprev.household_population as x
inner join xprev.households as y on x.yr=y.yr and x.hh_id=y.hh_id
group by x.yr,y.jur;
quit;

proc sql;
create table hp_1a as select yr,sum(hp) as hp
from hp_1 group by yr;
quit;


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

data jur_3(drop=hpc);set jur_2;by jur;retain hpc;
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
create table jur_5 as select x.jur,x.yr,x.hh,x.hp as hp0,x.pph as pph0,x.d1,x.d2
,y.reg_d
,case
when x.yr=2018 then round(x.est_p * y.reg_d,1) 
else round(x.d2_p * y.reg_d,1) 
end as d3
from jur_4a as x
inner join reg_2 as y on x.yr=y.yr
order by yr,d3;
quit;

data jur_6;set jur_5;by yr;retain dc;
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
create table jur_8 as select x.yr,x.jur,x.hh,x.hp as hp0,x.pph as pph0
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


proc sql;
create table dof_update_jur as select yr,jur,hh,hp1 as hp
from jur_8
order by yr,jur;
quit;

data e1.dof_update_jur;set dof_update_jur;run;
*/
