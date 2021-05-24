
%let xver=xpef06;

libname sql_est odbc noprompt="driver=SQL Server; server=sql2014a8; database=estimates;
Trusted_Connection=yes" schema=est_2017_04;

libname e1 "T:\socioec\Current_Projects\&xver\input_data";

libname pdsr odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;
Trusted_Connection=yes" schema=demographic_rates;


proc sql;
create table rh as select distinct r,hisp from sql_est.hp_aggregated;
create table r7 as select distinct r7 from e1.Dof_pop_proj_r7_age102;
quit;

proc sql;
create table gq_base as select 
case
when hisp="H" then "H"
when r="R10" then "W"
when r="R02" then "B"
when r="R03" then "I"
when r="R04" then "S"
when r="R05" then "P"
when r="R06" then "M" /* Other becomes Multiple */
when r="R07" then "M"
end as r7
,sex,age,count(gq_id) as gq
from sql_est.gq_population where yr=2017
group by r7,sex,age;

create table p_1 as select x.yr
,coalesce(x.r7,y.r7) as r7
,coalesce(x.sex,y.sex) as sex
,coalesce(x.age102,y.age) as age
,x.p_cest as tp
,coalesce(y.gq,0) as gq
,x.p_cest - coalesce(y.gq,0) as hp
from e1.Dof_pop_proj_r7_age102 as x
full join gq_base as y on x.r7=y.r7 and x.sex=y.sex and x.age102=y.age
where x.yr>2017;

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
create table gq_2017_1 as select x.*
from (select * from sql_est.gq_population where yr=2017) as x
inner join p_1c as y on x.sex=y.sex and x.age=y.age
where x.r in ("R06","R07") and x.hisp="NH"
order by sex,age,gq_id;
quit;

data gq_2017_2;set gq_2017_1;by sex age;retain i;
if first.age then i=1;else i=i+1;
run;

proc sql;
create table gq_2017_3 as select x.*
from gq_2017_2 as x
inner join p_1c as y on x.sex=y.sex and x.age=y.age
where x.i<=y.d;
quit;


/*
changing r from "Two or more" to "White" for 1 gq individual in a specific cohort
this is needed because DOF data has less people in that cohort
*/

proc sql;
update gq_2017_3 set r="R10";
quit;


proc sql;
create table gq_2017_4 as select x.jur,coalesce(y.r,x.r) as r length=3,x.hisp,x.sex,x.age,x.dob,x.gq_type,x.gq_id
from (select * from sql_est.gq_population where yr=2017) as x
left join gq_2017_3 as y on x.gq_id=y.gq_id;

create table test_01 as select x.*
from gq_2017_4 as x
inner join gq_2017_3 as y on x.gq_id=y.gq_id;

/*create table sd.gq_2017 as select * from gq_2017_4;*/
quit;





proc sql;
create table gq_base_n as select 
case
when hisp="H" then "H"
when r="R10" then "W"
when r="R02" then "B"
when r="R03" then "I"
when r="R04" then "S"
when r="R05" then "P"
when r="R06" then "M" /* Other becomes Multiple */
when r="R07" then "M"
end as r7
,sex,age,count(gq_id) as gq
from gq_2017_4 /*sd.gq_2017*/
group by r7,sex,age;

create table p_1n as select x.yr
,coalesce(x.r7,y.r7) as r7
,coalesce(x.sex,y.sex) as sex
,coalesce(x.age102,y.age) as age
,x.p_cest as tp
,coalesce(y.gq,0) as gq
,x.p_cest - coalesce(y.gq,0) as hp
from e1.Dof_pop_proj_r7_age102 as x
full join gq_base_n as y on x.r7=y.r7 and x.sex=y.sex and x.age102=y.age
where x.yr>2017;

create table p_1n_a as select * from p_1n where tp=. or hp=.;
create table p_1n_b as select * from p_1n where hp<0 order by yr,age;
quit;




/*
Assume that gq pop in future years is exactly the same as in 2017 (1/1/2017)
Subtract 2017 gq from DOF projections to get future hp
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

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table dof_hh_0 as select
case when area_type="City" then area_name else summary_type end as Name
,est_yr,household_pop as hp,occupied as hh,total_hu as hu,group_quarters as gq
from connection to odbc
(
select * FROM [socioec_data].[ca_dof].[population_housing_estimates]
where county_name= 'San Diego' and vintage_yr = 2018 and est_yr >= 2017 and (area_type = 'City' or summary_type = 'Unincorporated')
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
from sql_est.household_population where yr=2017 group by yr
	union all
select est_yr as yr,sum(hp) as hp,sum(hh) as hh 
from dof_hh_0 where est_yr = 2018 group by yr
	union all
select yr,hp,hh_a as hh from hp_2c where yr>2018
order by yr;
quit;

proc export data=hp_3 outfile="T:\socioec\Current_Projects\&xver\input_data\HH_Initial_Estimate.xlsx" dbms=xlsx replace;sheet="HP_HH";run;
