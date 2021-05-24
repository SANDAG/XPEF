/* References the 'Variables and Libaries' file */
%let a=%sysget(SAS_EXECFILEPATH);
%let b=%sysget(SAS_EXECFILENAME);
%let valib=%sysfunc(tranwrd(&a,&b,_ Variables and Libraries.sas));
%include "&valib";

/* This step takes the GQ population from the estimates, but uses 2017 for Coronado */
/* This is because in 2018 there was a deployed ship, which lowered the GQ estimate for that year */
/* If the base year were to be changed, the overwrite could be removed */
proc sql;
create table gq_baseyear_0 as
select * from sql_est.gq_population(drop=dob) where yr = &by1 and jur <> 3
	union all
select * from sql_est.gq_population(drop=dob) where yr = &by1 -1  and jur = 3;
quit;

/* Combines DOF total pop with GQ estimates to create household pop */
proc sql;
create table gq_base as select r7,sex,age,count(gq_id) as gq
from gq_baseyear_0
group by r7,sex,age;

create table p_1 as select x.yr
,coalesce(x.r7,y.r7) as r7
,coalesce(x.sex,y.sex) as sex
,coalesce(x.age101,y.age) as age
,x.p_cest as tp
,coalesce(y.gq,0) as gq
,x.p_cest - coalesce(y.gq,0) as hp
from e1.Dof_pop_proj_r7_age101 as x
full join gq_base as y on x.r7=y.r7 and x.sex=y.sex and x.age101=y.age
where x.yr > &by1;

create table p_1a as select * from p_1 where tp=. or hp=.;
create table p_1b as select * from p_1 where hp<0 order by yr,age;
quit;

proc sql;
create table p_1c as select r7,sex,age,gq,max(gq-tp) as d
from p_1b group by r7,sex,age,gq;
quit;

proc sql;
create table p_2 as select x.r7,x.sex,x.age,x.gq
from gq_base as x
inner join p_1c as y on x.sex=y.sex and x.age=y.age
order by sex,age,r7;
quit;

proc transpose data=p_2 out=p_2a(drop=_name_);by sex age;var gq;id r7;run;

proc sql;
create table gq_baseyear_1 as select x.*
from (select * from gq_baseyear_0) as x
inner join p_1c as y on x.sex=y.sex and x.age=y.age and x.r7=y.r7
order by sex,age,gq_id;
quit;

data gq_baseyear_2;set gq_baseyear_1;by sex age;retain i;
if first.age then i=1;else i=i+1;
run;

proc sql;
create table gq_baseyear_3 as select x.*
from gq_baseyear_2 as x
inner join p_1c as y on x.sex=y.sex and x.age=y.age
where x.i<=y.d;
quit;

/*
changing r from "Two or more" to "White" for 1 gq individual in a specific cohort
this is needed because DOF data has less people in that cohort
*/

proc sql;
update gq_baseyear_3 set r="R10", r7="W";
quit;


proc sql;
create table gq_baseyear_4 as select x.jur,coalesce(y.r,x.r) as r length=3,coalesce(y.r7,x.r7) as r7 length=1
,x.hisp,x.sex,x.age,x.gq_type,x.gq_id
from (select * from gq_baseyear_0) as x
left join gq_baseyear_3 as y on x.gq_id=y.gq_id and x.jur=y.jur;

create table test_01 as select x.*
from gq_baseyear_4 as x
inner join gq_baseyear_3 as y on x.gq_id=y.gq_id and x.jur=y.jur;

quit;


proc sql;
create table gq_base_n as select r7,sex,age,count(gq_id) as gq
from gq_baseyear_4 
group by r7,sex,age;

create table p_1n as select x.yr
,coalesce(x.r7,y.r7) as r7
,coalesce(x.sex,y.sex) as sex
,coalesce(x.age101,y.age) as age
,x.p_cest as tp
,coalesce(y.gq,0) as gq
,x.p_cest - coalesce(y.gq,0) as hp
from e1.Dof_pop_proj_r7_age101 as x
full join gq_base_n as y on x.r7=y.r7 and x.sex=y.sex and x.age101=y.age
where x.yr > &by1;

create table p_1n_a as select * from p_1n where tp=. or hp=.;
create table p_1n_b as select * from p_1n where hp<0 order by yr,age;
quit;

/*
Assume that gq pop in future years is exactly the same as in the base year 
Subtract base year gq from DOF projections to get future hp
Apply headship rates to hp to get hh
*/

proc sql;
create table hp_1 as select yr
,case
when r7 in ("I","P","M") then "O" else r7 end as r5
,age,sex
,sum(hp) as hp
from p_1n
group by yr,r5,age,sex;

create table hr_1 as
select * from pdsr.householder_rates where householder_rate_id=101
	union all
select householder_rate_id,2051 as yr,age,race,sex,householder_rate
from pdsr.householder_rates where householder_rate_id=101 and yr=2050;

create table hr_2 as
select * from pdsr.householder_rates where householder_rate_id=102 and yr in (2018:2034);

create table hp_2 as select x.*
,y.householder_rate as hr_a
,z.householder_rate as hr_b
,round(x.hp * coalesce(y.householder_rate,0),1) as hh_a
,round(x.hp * coalesce(z.householder_rate,0),1) as hh_b
from hp_1 as x
left join hr_1 as y on x.yr=y.yr and x.r5=y.race and x.age=y.age and x.sex=y.sex
left join hr_2 as z on x.yr=z.yr and x.r5=z.race and x.age=z.age and x.sex=z.sex;

create table hp_2a as select * from hp_2 where hr_a=.;
create table hp_2b as select distinct age from hp_2 where hr_a=.;

create table hp_2c as select yr,sum(hp) as hp,sum(hh_a) as hh_a,sum(hh_b) as hh_b
from hp_2 group by yr;
quit;

/* Unsure if any of this DOF table is used but updated the vintage year in the query on 1/30/2020 */
proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table dof_hh_0 as select
case when area_type="City" then area_name else summary_type end as Name
,est_yr,household_pop as hp,occupied as hh,total_hu as hu,group_quarters as gq
from connection to odbc
(
select * FROM [socioec_data].[ca_dof].[population_housing_estimates]
where county_name= 'San Diego' and vintage_yr = 2019 and est_yr >= 2017 and (area_type = 'City' or summary_type = 'Unincorporated')
);

DISCONNECT FROM odbc;
quit;

proc sql;
create table dof_hu_1 as select x.name as jurisdiction, x.est_yr as yr
,y.hu - x.hu as hu_construction
,y.gq - x.gq as gq_change
from dof_hh_0 as x
inner join dof_hh_0 as y on x.name=y.name
where x.est_yr = 2017 and y.est_yr = 2018
order by jurisdiction;

create table dof_hu_1a as select yr
,sum(hu_construction) as hu_construction
,sum(gq_change) as gq_change
from dof_hu_1 group by yr;
quit;


proc sql;
create table hp_3 as
select yr,count(hp_id) as hp,count(distinct hh_id) as hh
from sql_est.household_population where yr in (2017,2018) group by yr
	union all
select yr,hp,hh_a as hh from hp_2c where yr>2018
order by yr;
quit;

/* This outputs the target number of households and household population by year, which are used as inputs to determining the target housing units */
proc export data=hp_3 outfile="T:\socioec\Current_Projects\&xver\input_data\HH_Initial_Estimate.xlsx" dbms=xlsx replace;sheet="HP_HH";run;

proc sql;
create table hu_1 as select yr,sto_flag,count(hu_id) as hu,count(hh_id) as hh, calculated hu - calculated hh as vac_hu
from sql_est.housing_units where yr in (2017,2018) group by yr,sto_flag;

create table hu_2 as select x.hu - y.hu as hu_g
from hu_1 as x
cross join hu_1 as y
where x.yr=2018 and y.yr=2017 and x.sto_flag=0 and y.sto_flag=0;
quit;
