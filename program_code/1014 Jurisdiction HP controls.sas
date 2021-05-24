
proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table dof_est_00 as select
case when area_type="City" then area_name else summary_type end as Name
,vintage_yr,est_yr as yr,household_pop as hp_dof,occupied as hh_dof, group_quarters as gq_dof
from connection to odbc
(
select * FROM [socioec_data].[ca_dof].[population_housing_estimates]
where county_name= 'San Diego' /*and est_yr in (2016,2017,2018)*/
and vintage_yr in (&by1 - 1,&by1) and (area_type = 'City' or summary_type = 'Unincorporated')
);

disconnect from odbc;

create table dof_est_0 as select * from dof_est_00 where yr >= &by1-2;
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
from dof_est_2 where vintage_yr=&by1 and yr=&by1;
quit;

/* hh_rt: hh regional target */
/*
proc import out=hh_target_0(drop=hu_g hu2 f4 f5 rename=(yr_built_during=yr hh2=hh_rt)) 
datafile="T:\socioec\Current_Projects\&xver\input_data\HU Construction and PPH projections.xlsx"
replace dbms=excelcs; RANGE='Data2$k1:p36'n;
run;
*/

proc import out=hh_target_0(drop=hu_g hu2 f4 f5 rename=(yr_built_during=yr hh2=hh_rt)) 
datafile="T:\socioec\Current_Projects\&xver\input_data\HU Construction and PPH projections_Feb2020.xlsx"
replace dbms=excelcs; RANGE='Sheet1$r1:t36'n;
run;

proc sql;
create table hh_target_1 as select x.yr,x.hh_rt - y.hh_rt as hhd_rt
from hh_target_0 as x
inner join hh_target_0 as y on x.yr = y.yr + 1
order by yr;
quit;

proc sql;
create table fut_hu_0 as select jur,yr,count(hu_id) as hu
from sd.ludu where sto_flag=0 group by jur,yr;

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
create table tab_0 as select z.yr,x.jur,x.hp,x.hh
,y.hu
,z.hu as hu2
,z.hu - y.hu as hu_d
,x.hp / x.hh as pph
,x.hh / y.hu as or /* occupancy rate */
,calculated hu_d * calculated or as hh_d0
from (select jur,count(hh_id) as hh,sum(size) as hp from sd.hh_&by1 group by jur) as x
inner join (select jur,count(hu_id) as hu from sd.hu_&by1 where sto_flag=0 group by jur) as y
	on x.jur=y.jur
inner join fut_hu_0 as z on x.jur=z.jur
where z.yr=&by2;

create table tab_1 as select x.*, y.hhd_rt, round(x.s_hh * y.hhd_rt,1) as hhd0 
from (select *,hh_d0/sum(hh_d0) as s_hh from tab_0) as x
cross join (select * from hh_target_1 where yr=&by1) as y
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
cross join (select * from fut_hp where yr=&by2) as y
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

/* this is for &by2 */
proc sql;
create table tab_&by2 as select yr,jur,hp,hh,hu,hp2,hh2,hu2,pph format=5.2, pph2 format=5.2
,or format=percent8.1, or2 format=percent8.1
from tab_2b;
quit;

%macro fut;

data tab_4;set tab_&by2;
vu = hu2 - hh2;
run;

%do yr = &by2+1 %to 2051;

proc sql;
create table tab_0 as select z.yr, x.jur, x.hp2 as hp, x.hh2 as hh, x.hu2 as hu, x.vu
,z.hu as hu2
,z.hu - x.hu2 as hu_d
,x.pph2 as pph
,x.or2 as or /* occupancy rate */
,calculated hu_d * x.or as hh_d0
from (select * from tab_4 where yr = &yr - 1) as x
inner join fut_hu_0 as z on x.jur=z.jur
where z.yr = &yr;

create table tab_1 as select x.*, y.hhd_rt
,case
when hu_d = 0 and vu = 0 then 0
when hu_d = 0 then ceil (ceil(vu/10) * ranuni (&yr)) /* random integer between 1 and one tenth of the vacant stock */
else min(hu_d, round(x.s_hh * y.hhd_rt,1)) end as hhd0 
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
,max(x.hp + ceil (20 * ranuni (&yr)), round(x.hp0_s * y.hp,1)) as hp1 /* random number between 1 and 20 */
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
,hu2 - hh2 as vu
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
select x.jur,x.pph2 as pph_&by2 format=5.2,y.pph2 as pph_2051 format=5.2
, 1 - x.or2 as vr_&by2 format=percent8.2, 1 - y.or2 as vr_2051 format=percent8.2
from tab_4 as x
inner join tab_4 as y on x.jur=y.jur
where x.yr=&by2 and y.yr=2051
	union all
select 20 as jur,x.pph2 as pph_&by2 format=5.2,y.pph2 as pph_2051 format=5.2
, 1 - x.or2 as vr_&by2 format=percent8.2, 1 - y.or2 as vr_2051 format=percent8.2
from (select sum(hp2)/sum(hh2) as pph2,sum(hh2)/sum(hu2) as or2 from tab_4 where yr=&by2) as x
cross join (select sum(hp2)/sum(hh2) as pph2,sum(hh2)/sum(hu2) as or2 from tab_4 where yr=2051) as y

order by jur;
quit;

proc sql;
create table tab_4d as select x.yr,x.jur
,x.hp2 as hp_curr,y.hp2 as hp2_prev
,x.hh2 as hh_curr,y.hh2 as hh2_prev
,x.hp2 - y.hp2 as hp2d
,x.hh2 - y.hh2 as hh2d
,x.hu - x.hh2 as vac_hu

from tab_4 as x
inner join tab_4 as y on x.jur=y.jur and x.yr=y.yr+1
where x.hp2=y.hp2 or x.hh2=y.hh2
order by jur,yr;
quit;


proc sql;
create table e1.dof_update_jur as select yr,jur,hh2 as hh, hp2 as hp
from tab_4 order by yr,jur;
quit;

proc sql;
create table atest_01 as select x.*,y.hh
from (select jur,yr,count(hu_id) as hu_ludu from sd.ludu where sto_flag=0 group by jur,yr) as x
inner join e1.dof_update_jur as y on x.jur=y.jur and x.yr=y.yr
where x.hu_ludu < y.hh
order by jur,yr;
quit;
