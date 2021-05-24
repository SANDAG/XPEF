/*
proc import out=hh_urb_1
datafile="T:\socioec\Current_Projects\&xver\input_data\HU Construction and PPH projections.xlsx"
replace dbms=excelcs; RANGE='Data2$k1:p36'n;
run;
*/

proc import out=hh_urb_1
datafile="T:\socioec\Current_Projects\&xver\input_data\HU Construction and PPH projections_Feb2020.xlsx"
replace dbms=excelcs; RANGE='Sheet1$r1:t36'n;
run;

/* this is needed to keep Conodado GQ at 2017 level
in 2018 Coronado GQ is much lower because of the deployed ship */

proc sql;
create table gq_base as select r7,sex,age,count(gq_id) as gq
from sd.gq_&by1
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

/* the following sections needs to be edited if p_1b has records
otherwise, no editing is needed */

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
create table gq_&by1._1 as select x.*
from sd.gq_&by1 as x
inner join p_1c as y on x.sex=y.sex and x.age=y.age
where x.r7 in ("M") and x.hisp="NH"
order by sex,age,gq_id;
quit;

data gq_&by1._2;set gq_&by1._1;by sex age;retain i;
if first.age then i=1;else i=i+1;
run;

proc sql;
create table gq_&by1._3 as select x.*
from gq_&by1._2 as x
inner join p_1c as y on x.sex=y.sex and x.age=y.age
where x.i<=y.d;
quit;

/*
changing r7 from "M" to "W" (and r from "R07" to "R10") for 1 gq individual in a specific cohort
this is needed because DOF data has less people in that cohort
*/

proc sql;
update gq_&by1._3 set r7="W", r="R10";
quit;

proc sql;
create table gq_&by1._4 as select x.gq_id,x.gq_type,x.jur,x.ct,x.cpa,x.mgra
,x.age
,coalesce(y.r,x.r) as r length=3
,coalesce(y.r7,x.r7) as r7 length=1
,x.hisp,x.sex,x.dob
from sd.gq_&by1 as x
left join gq_&by1._3 as y on x.gq_id=y.gq_id;

create table test_01 as select x.*
from gq_&by1._4 as x
inner join gq_&by1._3 as y on x.gq_id=y.gq_id;

create table sd.gq_&by1 as select * from gq_&by1._4;
quit;


proc sql;
/* this is needed to keep Conodado GQ at 2017 level
in 2018 Coronado GQ is much lower because of the deployed ship */
create table gq_base_n as select r7,sex,age,count(gq_id) as gq
from 
(
select r7,sex,age,gq_id from sd.gq_&by1 where jur <> 3
	union all
select r7,sex,age,gq_id from sql_est.gq_population where yr = &by1 -1  and jur = 3
)
group by r7,sex,age;

/* Changing this as of Feb 5, 2020 to set negative hp to 0 */
/* This was because new DOF ASE estimate was inconsistant with GQ estimate,
was trying to remove more pop from specific groups than DOF said existed */
create table p_1n as select x.yr
,coalesce(x.r7,y.r7) as r7
,coalesce(x.sex,y.sex) as sex
,coalesce(x.age101,y.age) as age
,x.p_cest as tp
,coalesce(y.gq,0) as gq
/* ,x.p_cest - coalesce(y.gq,0) as hp */
,CASE WHEN x.p_cest - coalesce(y.gq,0) < 0 THEN 0
	ELSE x.p_cest - coalesce(y.gq,0) END as hp
from e1.Dof_pop_proj_r7_age101 as x
full join gq_base_n as y on x.r7=y.r7 and x.sex=y.sex and x.age101=y.age
where x.yr > &by1
order by yr,r7,sex,age;


create table p_1n_a as select * from p_1n where tp=. or hp=.;

/* this table should be blank */
create table p_1n_b as select * from p_1n where hp<0 order by yr,age;
quit;

proc sql;
create table e1.future_hp as select *
from p_1n;
quit;

/*
Assume that gq pop in future years is exactly the same as in 2018 plus the Coronado correction
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
select * from pdsr.householder_rates where householder_rate_id=102 and yr in (&by1:2034);

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
create table hh_&by1 as select yr,count(hh_id) as hh,sum(size) as hp
from sql_est.households where yr = &by1 group by yr;

create table hu_&by1 as select yr,count(hu_id) as hu
from sql_est.housing_units where yr = &by1 group by yr;
quit;


proc sql;
create table hh_urb_2 as select yr_built_during+1 as yr
,round(hh2,1) as hh
from hh_urb_1 where yr_built_during in (&by1:2050) order by yr;
quit;

proc sql;
create table hh_part_1 as
select x.yr,x.hp,y.hh
from hp_2c as x
inner join hh_urb_2 as y on x.yr=y.yr
order by yr;
quit;


proc sql;
create table gq_2 as select x.yr,y.gq
from (select distinct yr from hh_part_1) as x
cross join (select sum(gq) as gq from gq_base_n) as y;
quit;


proc sql;
create table dof_update as select x.yr,x.hp,x.hh,y.gq
from hh_part_1 as x
inner join gq_2 as y on x.yr=y.yr
order by yr;
quit; 


%include "T:\socioec\Current_Projects\&xver\program_code\1013a HU from Urbansim.sas";

/* old units */
proc sql;
create table hu_old_0 as select * from sd.hu_&by1(drop=hh_id size)
order by ranuni(&by1);

/* removing units in DeAnza cove */
delete from hu_old_0 where mgra in (3644,3631);

quit;

data hu_old_1;length yr 3;set hu_old_0;
do yr = &by2 to 2051;
	output;
end;
run;

data urb_hu_5;set urb_hu_5;
informat mgra cpa yr;
run;

proc sql;
create table urb_hu_5a as select yr,sum(du) as du
from urb_hu_5 group by yr;
quit;

proc sql;
create table hu_new_0 as select yr+1 as yr1 length=3
,mgra length=4 format=5.
,jur length=3 format=2.
,cpa length=3 format=4.
,ct length=6 format=$6.
,du_type length=3 format=$3.
,0 as sto_flag length=3 format=1.
,du
from urb_hu_5
order by yr1;
quit;

data hu_new_1(drop=du i);set hu_new_0;
do i=1 to du;
	output;
end;
run;

data hu_new_1(drop=i);set hu_new_1;length hu_id 5;format hu_id 8.;
i+1;
hu_id=i+2000000;
run;


data hu_new_2(drop=yr1);length yr 3;set hu_new_1;
do yr = yr1 to 2051;
	output;
end;
run;

data sd.ludu;set hu_old_1 hu_new_2;run;
