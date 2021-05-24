options notes;

%let xver=xpef23;

libname &xver odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;
Trusted_Connection=yes" schema=&xver;

libname sql_de odbc noprompt="driver=SQL Server; server=sql2014a8; database=socioec_data;
Trusted_Connection=yes" schema=ca_dof;

libname e1 "T:\socioec\Current_Projects\&xver\input_data";

proc sql;
create table dof_0 as select yr,age101,sex,r7,p_cest as tp_dof
from e1.dof_pop_proj_r7_age101 where yr in (2018:2051);
quit;

proc sql; 
create table dof_test as 
select yr, sum(p_cest) as pop
from e1.dof_pop_proj_r7_age101
where yr = 2051
group by yr; 
quit; 


/*
proc sql;
create table dof_0 as select y.jur,x.*
from 
(select 
case when area_type="City" then area_name else summary_type end as Name
,est_yr as yr,total_pop as tp_dof,household_pop as hp_dof,occupied as hh_dof,group_quarters as gq_dof,total_hu as hu_dof
from sql_de.population_housing_estimates
where county_name="San Diego" and vintage_yr=&vyr and (area_type="City" or summary_type="Unincorporated")) as x
inner join id.sf1_place as y on x.name=y.name
order by jur,yr;
quit;
*/


proc sql;
create table est_0 as
select "HP" as type,yr
,case when age<=101 then age else 101 end as age101
,sex
,case
when hisp="H" then "H"
when r="R10" then "W"
when r="R02" then "B"
when r="R03" then "I"
when r="R04" then "S"
when r="R05" then "P"
else "M" end as r7
from &xver..household_population
	union all
select "GQ" as type,yr
,case when age<=101 then age else 101 end as age101
,sex
,case
when hisp="H" then "H"
when r="R10" then "W"
when r="R02" then "B"
when r="R03" then "I"
when r="R04" then "S"
when r="R05" then "P"
else "M" end as r7
from &xver..gq_population;

create table est_1 as 
select yr,age101,sex,r7,type,count(*) as p
from est_0 group by yr,age101,sex,r7,type;
quit;

/*proc sql; */
/*create table est_1_test as */
/*select yr, sum(p) as pop*/
/*from est_1 */
/*group by yr; */
/*quit; */

proc transpose data=est_1 out=est_2;by yr age101 sex r7;var p;id type;run;

proc sql;
create table est_2a as select * from est_2 where yr=2018;
quit;

proc sql;
create table est_3 as select yr,age101,sex,r7,coalesce(gq,0) + coalesce(hp,0) as tp_est
,coalesce(gq,0) as gq_est,coalesce(hp,0) as hp_est
from est_2
order by yr,age101,sex,r7;

create table est_3a as select yr,sum(tp_est) as tp_est,sum(gq_est) as gq_est,sum(hp_est) as hp_est
from est_3 group by yr;

create table future_hp_2 as select yr,sum(tp) as future_tp,sum(hp) as future_hp
from e1.future_hp group by yr;

create table dof_update as select yr,sum(hp) as hp_dof_update
from e1.dof_update_jur group by yr;
quit;

proc sql;
create table est_4 as select x.*,y.future_hp,z.hp_dof_update
from est_3a as x
inner join future_hp_2 as y on x.yr=y.yr
inner join dof_update as z on x.yr=z.yr
order by yr;

create table est_4a as select * from est_4 where hp_est ^= future_hp;
create table est_4b as select * from est_4 where hp_est ^= hp_dof_update;
quit;


proc sql;
create table comp_0 as select 
coalesce(x.yr,y.yr) as yr
,coalesce(x.age101,y.age101) as age101
,coalesce(x.sex,y.sex) as sex
,coalesce(x.r7,y.r7) as r7
,coalesce(x.tp_dof,0) as tp_dof
,coalesce(y.tp_est,0) as tp_est
,coalesce(y.hp_est,0) as hp_est
,coalesce(y.gq_est,0) as gq_est
from dof_0 as x
full join (select * from est_3 where yr>2017) as y on x.yr=y.yr and x.age101=y.age101 and x.sex=y.sex and x.r7=y.r7
order by yr,age101,sex,r7;

create table comp_1 as select x.*
,coalesce(y.gq_est,0) as gq_base
,x.gq_est - coalesce(y.gq_est,0) as gq_new
from comp_0 as x
left join (select * from est_3 where yr=2017) as y on x.age101=y.age101 and x.sex=y.sex and x.r7=y.r7
order by yr,age101,sex,r7;

create table comp_1a as select * from comp_1 where tp_dof=. or tp_est=.;
quit;


proc sql;
create table comp_1_test_1 as select *,tp_dof - hp_est - gq_base as dof_less_est
from comp_1
where tp_dof ^= (hp_est + gq_base) order by abs(tp_dof - hp_est - gq_base) desc;

create table comp_1_test_1a as select *,tp_dof - hp_est - gq_base as dof_less_est
from comp_1
where tp_dof ^= (hp_est + gq_base) order by yr;
quit;

/*proc sql; */
/*create table test_test as */
/*select yr, sum(tp_dof) as tp_dof, sum(tp_est) as tp_est, sum(hp_est) as hp_est, sum(gq_est) as gq_est, sum(gq_base) as gq_base, */
/*sum(gq_new) as gq_new, sum(dof_less_est) as dof_less_est */
/*from comp_1_test_1a*/
/*group by yr*/
/*order by yr; */
/*quit; */



/*
proc sql;
create table comp_2 as select yr,sum(hp_est) as hp
from comp_1 group by yr;
quit;
*/



/*
proc sql;
create table hu_test_1 as select yr,count(hu_id) as hu_all,count(hh_id) as hu_occ,sum(sto_flag) as hu_sto
,sum(case when sto_flag=0 and hh_id=. then 1 else 0 end) as hu_vac
from &xver..housing_units
group by yr order by yr;

create table hu_test_1a as select *,hu_vac / (hu_occ + hu_vac) as vr1 format=percent7.2
,(hu_vac + hu_sto) / (hu_occ + hu_vac + hu_sto) as vr2 format=percent7.2
from hu_test_1;
quit;
*/



/*
proc sql;
create table comp_2_test_2 as select * 
from (select yr,sum(tp_dof) as tp_dof,sum(tp_est) as tp_est from comp_1 group by yr)
where tp_dof^=tp_est;

create table comp_2_test_3 as select * 
from (select yr,age101,sum(tp_dof) as tp_dof,sum(tp_est) as tp_est from comp_1 group by yr,age101)
where tp_dof^=tp_est;

create table comp_2_test_4 as select * 
from (select yr,sex,sum(tp_dof) as tp_dof,sum(tp_est) as tp_est from comp_1 group by yr,sex)
where tp_dof^=tp_est;

create table comp_2_test_5 as select * 
from (select yr,r7,sum(tp_dof) as tp_dof,sum(tp_est) as tp_est from comp_1 group by yr,r7)
where tp_dof^=tp_est;
quit;
*/
