%include "T:\socioec\Current_Projects\&xver\program_code\1055a Data Prep for Synthetic Households.sas";

/* libname sd "T:\socioec\Current_Projects\&xver\simulation_data\"; */

/*libname sh1 "T:\socioec\Current_Projects\Synthetic Households\";*/

/*libname sql_dc odbc noprompt="driver=SQL Server; server=sql2014a8; database=data_cafe; Trusted_Connection=yes" schema=ref;*/
/*libname sql_dim odbc noprompt="driver=SQL Server; server=sql2014a8; database=demographic_warehouse;
Trusted_Connection=yes" schema=dim;*/

/*
options set=SAS_HADOOP_RESTFUL 1;

libname xpef odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;
Trusted_Connection=yes" schema=xpef03;*/


/* load households and people */
/* xpef.household_income_upgraded containes only certain years
(defined in global macro list1 in "1043 Income Imputation and Assignment.sas" 
because of that, households and people are for these years only */

proc sql;
create table hh_0 as select x.*
,y.inc_2010
,y.income_group_id_2010
,case
when y.income_group_id_2010 in (11,12) then 1
when y.income_group_id_2010 in (13,14) then 2
when y.income_group_id_2010 in (15,16) then 3
when y.income_group_id_2010 in (17,18) then 4
when y.income_group_id_2010 in (19,20) then 5 end as hh_income_cat_id length=3 format=1.
from sql_xpef.households as x
inner join sql_xpef.household_income_upgraded as y
on x.yr=y.yr and x.hh_id=y.hh_id;

create table hp_0 as select x.*,y.ct,y.hh_income_cat_id
,case
when x.age<=4 then .
when x.age<=9 then 0509
when x.age<=14 then 1014
when x.age<=17 then 1517
when x.age<=19 then 1819
when x.age<=24 then 2024
when x.age<=34 then 2534
when x.age<=59 then 3599 end as age8 length=3 format=4.
,case
when x.age<=16 then .
when x.age<=17 then 1517
when x.age<=24 then 1824
when x.age<=34 then 2534
when x.age<=59 then 3599 end as age4 length=3 format=4.
,y.mgra

/* for school assignment purposes, ages 0-4 and 60+ are excluded */
/* for college assignment purposes, ages 0-16 and 60+ are excluded */
from sql_xpef.household_population(drop=dob) as x
inner join hh_0 as y on x.yr=y.yr and x.hh_id=y.hh_id;

create table gq_0 as select x.*
,case
when x.age<=4 then .
when x.age<=9 then 0509
when x.age<=14 then 1014
when x.age<=17 then 1517
when x.age<=19 then 1819
when x.age<=24 then 2024
when x.age<=34 then 2534
when x.age<=59 then 3599 end as age8 length=3 format=4.
,case
when x.age<=16 then .
when x.age<=17 then 1517
when x.age<=24 then 1824
when x.age<=34 then 2534
when x.age<=59 then 3599 end as age4 length=3 format=4.
from sql_xpef.gq_population(drop=jur dob) as x
inner join (select distinct yr from hh_0) as y on x.yr=y.yr;
quit;

/*
proc sql;
create table test_01 as select * from hh_0 where hh_income_cat_id=.;
create table test_02 as select * from hh_0 where hh_income_cat_id^=.;
quit;
*/


proc sql;
create table max_hpid as select yr,max(hp_id) as mx format=comma12. from hp_0 group by yr;
create table max_gqid as select yr,max(gq_id) as mx format=comma12. from gq_0 group by yr;
quit;

proc sql;
create table hp_00 as select yr length=3 format=4.
,hh_id length=5 format=8.
,hp_id as id length=5 format=8.
,age length=3 format=3.
,sex
,ct
,"HHP" as type
,age8
,age4
,hh_income_cat_id
,r
,hisp
from hp_0;

create table gq_00 as select yr length=3 format=4.
/* check table max_hpid */
,gq_id + 30000000 as hh_id length=5 format=8.
,gq_id + 30000000 as id length=5 format=8.
,age length=3 format=3.
,sex
,ct length=6
,gq_type as type
,age8
,age4
,1 as hh_income_cat_id length=3 format=1.
,r
,hisp
from gq_0;
quit;

data tp_0;set hp_00 gq_00;
informat yr hh_id id age sex ct type age8 age4 hh_income_cat_id r hisp;
run;

proc sql;
create table tp_0_sum as select yr,type,count(id) as n
from tp_0 group by yr,type;

create table tp_0_sum_2 as select yr,count(id) as n
from tp_0 group by yr;
quit;

proc transpose data=tp_0_sum out=tp_0_sum_1(drop=_name_);by yr; var n;id type;run;

proc sql;
create table hh_0a as select yr length=3 format=4.
,hh_id length=5 format=8.
,"HHP" as type
,size length=3 format=2.
,inc_2010 length=4 format=6.
,hh_income_cat_id length=3 format=1.
,income_group_id_2010-10 as inc10 length=3
,mgra length=4 format=5.
from hh_0;

create table gq_h as select yr length=3 format=4.
,gq_id + 30000000 as hh_id length=5 format=8.
,gq_type as type
,1 as size length=3
,14000 as inc_2010 length=4 format=6.
,1 as hh_income_cat_id length=3
,1 as inc10 length=3
,mgra length=4 from gq_0;
quit;

/*
proc sql;
select max(inc_2010) from hh_0;
quit;
*/


data hh_01;set hh_0a gq_h;
informat yr hh_id type size inc_2010 hh_income_cat_id inc10 mgra;
run;

proc sql;
create table hp_age8_0 as select yr,ct,age8,sex,count(id) as p
from tp_0 where age8^=. and type="HHP" group by yr,ct,age8,sex;

create table hp_age8_1 as select x.*
,y.f as f_ct,y.p_acs as p_acs_ct
,z.f as f_cn,z.p_acs as p_acs_cn
,coalesce(y.f,z.f) as f
,round(calculated f * p,1) as p_school
from hp_age8_0 as x
left join tab_senr_ct5_2 as y on /*x.yr=y.yr_est and*/ x.ct=y.ct and x.age8=y.age8 and x.sex=y.sex
left join tab_senr_cn5_2 as z on /*x.yr=z.yr_est and*/ x.age8=z.age8 and x.sex=z.sex;

create table hp_age8_1a as select * from hp_age8_1 where f=.;
create table hp_age8_1b as select * from hp_age8_1 where f_ct=.;
quit;

proc sql;
create table hp_school_1 as select yr,ct,age8,sex,id
,case
when age in (15:17) then ranuni(2010) + (17 - age) /* 15-year olds are assigned to school first, then 16-year olds, then 17-year olds */
when age in (18) then ranuni(2010) * 1.25 /* 18-year olds are 1.25 times more likely to be in school than 19-year olds */
when age in (19) then ranuni(2010) * 1
when age in (20) then ranuni(2010) * 2
when age in (21) then ranuni(2010) * 1.75
when age in (22) then ranuni(2010) * 1.5
when age in (23) then ranuni(2010) * 1.25
when age in (24) then ranuni(2010) * 1
when age in (25:29) then ranuni(2010) * 3
when age in (35:39) then ranuni(2010) * 6
when age in (40:49) then ranuni(2010) * 3
when age in (30:34,50:59) then ranuni(2010) * 1
else ranuni(2010) end as sort_order
from tp_0 where type="HHP" order by yr,ct,age8,sex,sort_order desc;
quit;

data hp_school_1;set hp_school_1;by yr ct age8 sex;retain i;
if first.sex then i=1;else i=i+1;
run;

proc sql;
create table hp_school_2 as select x.yr,x.ct,x.age8,x.sex,x.id,z.age4,z.age
,case when x.i<=y.p_school then 1 else 0 end as in_school length=3
from hp_school_1 as x
left join hp_age8_1 as y on x.yr=y.yr and x.ct=y.ct and x.age8=y.age8 and x.sex=y.sex
inner join tp_0 as z on x.yr=z.yr and x.id=z.id;

create table hp_school_2a as select yr,age8,count(id) as p,sum(in_school) as in_school
,calculated in_school / calculated p as pct_in_school format=percent8.1
from hp_school_2 group by yr,age8;
quit;


proc sql;
create table hp_age4_0 as select yr,ct,age4,sex,count(id) as p
from hp_school_2 where age4^=. group by yr,ct,age4,sex;

create table hp_age4_1 as select x.*
,y.f as f_ct,y.p_acs as p_acs_ct
,z.f as f_cn,z.p_acs as p_acs_cn
,coalesce(y.f,z.f) as f
,round(calculated f * p,1) as p_college
from hp_age4_0 as x
left join tab_cenr_ct5_2 as y on /*x.yr=y.yr_est and*/ x.ct=y.ct and x.age4=y.age4 and x.sex=y.sex
left join tab_cenr_cn5_2 as z on /*x.yr=z.yr_est and*/ x.age4=z.age4 and x.sex=z.sex;

create table hp_age4_1a as select * from hp_age4_1 where f=.;
create table hp_age4_1b as select * from hp_age4_1 where f_ct=.;
quit;


proc sql;
create table hp_college_1 as select yr,ct,age,age4,sex,in_school,id
,case
when age in (15:17) then ranuni(2020) + (age-15) /* first 17-year olds are assigned to college, then 16-year olds, then 15-year olds */
when age in (18) then ranuni(2020) * 1.25
when age in (19) then ranuni(2020) * 2
when age in (20) then ranuni(2020) * 2
when age in (21) then ranuni(2020) * 1.75
when age in (22) then ranuni(2020) * 1.5
when age in (23) then ranuni(2020) * 1.25
when age in (24) then ranuni(2020) * 1
when age in (35:39) then ranuni(2020) * 6
when age in (40:49) then ranuni(2020) * 3
when age in (30:34,50:59) then ranuni(2020) * 1
else ranuni(2020) end as sort_order
from hp_school_2 order by yr,ct,age4,sex,in_school desc,sort_order desc;
quit;


data hp_college_1;set hp_college_1;by yr ct age4 sex;retain i;
if first.sex then i=1;else i=i+1;
run;

proc sql;
create table hp_college_2 as select x.yr,x.ct,x.age,x.age4,x.sex,x.id,x.in_school
,case when x.i<=y.p_college and x.in_school=1 then 1 else 0 end as in_college length=3
from hp_college_1 as x
left join hp_age4_1 as y on x.yr=y.yr and x.ct=y.ct and x.age4=y.age4 and x.sex=y.sex;

/* age 19+ can be only in college */
update hp_college_2 set in_school=0 where in_school=1 and in_college=0 and age>=19;

create table hp_college_2a as select yr,age4,count(id) as p,sum(in_college) as in_college
,calculated in_college / calculated p as pct_in_college format=percent8.1
from hp_college_2 group by yr,age4;
quit;

proc sql;
create table hp_school_2b as select x.yr,x.age8,x.pct_in_school,y.f as pct_acs format=percent8.1
from hp_school_2a as x
inner join tab_senr_cn5_3a as y on /*x.yr=y.yr_est and*/ x.age8=y.age8
where x.age8^=. and y.enr_school=1
order by yr,age8;

create table hp_college_2b as select x.yr,x.age4,x.pct_in_college,y.f as pct_acs format=percent8.1
from hp_college_2a as x
inner join tab_cenr_cn5_3a as y on /*x.yr=y.yr_est and*/ x.age4=y.age4
where x.age4^=. and y.enr_college=1
order by yr,age4;
quit;

proc sql;
create table tp_1 as select x.yr,x.hh_id,x.id,x.age,x.sex,x.r,x.hisp
,x.type,x.ct,x.hh_income_cat_id
,y.in_school,y.in_college
,case 
when y.in_school=0 then 0
when y.in_college=1 then 6
when y.in_school=1 and 5<=x.age<=12 then 2
when y.in_school=1 and y.in_college=0 and 13<=x.age<=19 then 5
end as grade_id length=3

from tp_0 as x
left join hp_college_2 as y on x.yr=y.yr and x.id=y.id;

update tp_1 set in_school=1 where type="COL" and in_school=.;
update tp_1 set in_college=1 where type="COL" and in_college=.;
update tp_1 set grade_id=6 where type="COL" and grade_id=.;

update tp_1 set in_school=0 where type^="HHP" and in_school=.;
update tp_1 set in_college=0 where type^="HHP" and in_college=.;
update tp_1 set grade_id=0 where type^="HHP" and grade_id=.;
quit;

/*
set grade_id=0 where age<=4; nobody in grades K-8 below age 4
set grade_id=2 where 5<=age<=12; ages 5-12 go to grades K-8
set grade_id=5 where 13<=age<=19; ages 13-19 go to grades 9-12
set grade_id=0 where age>=60; ages 60+ do not go to school

ages below 22 can't have a college degree 
those attending college should have at least a HS degree
military must have at least a HS degree 
*/

proc sql;
create table tp_1_test_1 as select in_school,in_college,grade_id,min(age) as min_age,max(age) as max_age
from tp_1 where type="HHP" group by in_school,in_college,grade_id;

create table tp_1_test_2 as select age,in_school,in_college,grade_id,count(id) as n
from tp_1 where type="HHP" and age in (16:19) group by age,in_school,in_college,grade_id;

create table tp_1_test_3 as select yr,count(id) as p,count(distinct id) as n
from tp_1 group by yr;
quit;





proc sql;
create table hp_age1619_0 as select yr,ct,sex,in_school,type,count(id) as hp
from tp_1 where age in (16:19) and type="HHP" group by yr,ct,sex,in_school,type;

create table hp_age1619_0a as select x.*,y.stat1
from hp_age1619_0 as x
inner join (select distinct in_school,stat1 from tab_stat1_cn5_2) as y on x.in_school=y.in_school;

create table hp_age1619_1 as select x.*
,y.f as f_ct,y.p_acs as p_acs_ct
,z.f as f_cn,z.p_acs as p_acs_cn
,coalesce(y.f,z.f) as f
,ceil(calculated f * x.hp) as hp_stat1

from hp_age1619_0a as x
left join tab_stat1_ct5_2 as y on /*x.yr=y.yr_est and*/ x.ct=y.ct and x.sex=y.sex and x.in_school=y.in_school and x.stat1=y.stat1
left join tab_stat1_cn5_2 as z on /*x.yr=z.yr_est and*/ x.sex=z.sex and x.in_school=z.in_school and x.stat1=z.stat1
order by yr,ct,sex,in_school,stat1, f desc;

create table hp_age1619_1a as select * from hp_age1619_1 where f=.;
create table hp_age1619_1b as select * from hp_age1619_1 where f_ct=.;
quit;

data hp_age1619_2(drop=c);set hp_age1619_1; where hp>0;
by yr ct sex in_school;retain c;
if first.in_school then do;hp_stat1_ = min(hp, hp_stat1);c = hp_stat1_;end;
else do;hp_stat1_ = min(hp_stat1, hp - c);c = hp_stat1_ + c;end;
run;

proc sql;
create table hp_age1619_3 as select yr,ct,sex,in_school,stat1,hp,hp_stat1_
from hp_age1619_2 where hp_stat1_>0 order by yr,ct,sex,in_school,stat1;

create table hp_age1619_3a as select yr,ct,sex,in_school,hp,sum(hp_stat1_) as psum
from hp_age1619_3 group by yr,ct,sex,in_school,hp having calculated psum ^= hp;
quit;


data hp_age1619_4;set hp_age1619_3;by yr ct sex in_school;retain b;
age2=1619;
if first.in_school then do; a = 1;b = hp_stat1_; end;
else do;a = b + 1; b = a + hp_stat1_ - 1;end;
run;

proc sql;
create table hp_stat1_1 as select yr,ct,sex,in_school,id
,case when age in (16:19) then 1619 else . end as age2 length=3
,case 
when in_school=1 then ranuni(2030) + (19-age) /* younger ages first (in descending order ) */
when age in (16:17) then ranuni(2030)+1 /* ages 16-17 are dispatched first, ages 18-19 after */
else ranuni(2030)
end as sort_order
from tp_1 where type in ("HHP") order by yr,ct,age2,sex,in_school,sort_order desc;
quit;

data hp_stat1_1;set hp_stat1_1;by yr ct age2 sex in_school;retain i;
if first.in_school then i=1;else i=i+1;
run;

proc sql;
create table hp_stat1_2 as select x.yr,x.ct,x.sex,x.id,y.stat1
from hp_stat1_1 as x
left join hp_age1619_4 as y on x.yr=y.yr and x.ct=y.ct and x.age2=y.age2 and x.sex=y.sex and x.in_school=y.in_school
and y.a<=x.i<=y.b;
quit;

proc sql;
create table hp_stat1_2a as select x.*
from (select yr,id,count(id) as n from hp_stat1_2 group by yr,id having calculated n>1) as x
inner join hp_stat1_2 as y on x.yr=y.yr and x.id=y.id;

create table hp_stat1_2b as select yr,count(id) as m
from hp_stat1_2a group by yr;
quit;


proc sql;
create table tp_2 as select x.*,y.stat1 length=3
,case
when 16<=x.age<=19 then 1619
when 20<=x.age<=21 then 2021
when 22<=x.age<=24 then 2224
when 25<=x.age<=29 then 2529
when 30<=x.age<=34 then 3034
when 35<=x.age<=44 then 3544
when 45<=x.age<=54 then 4554
when 55<=x.age<=59 then 5559
when 60<=x.age<=61 then 6061
when 62<=x.age<=64 then 6264
when 65<=x.age<=69 then 6569
when 70<=x.age<=74 then 7074
when 75<=x.age then  7599
end as age13 length=3
from tp_1 as x
left join hp_stat1_2 as y on x.yr=y.yr and x.id=y.id;

update tp_2 set stat1=6 where stat1=. and type="MIL" and age in (16:19);
/* military gq ages 16-19 are always "Employed HS Grad" */

update tp_2 set stat1=1 where stat1=. and type in ("INS","OTH") and age in (16:19);
/* INS and OTH gq ages 16-19 are always "NILF HS NoGrad" */

update tp_2 set stat1=4 where stat1=. and type in ("COL") and age in (16:19);
/* COL ages 16-19 are always "NILF HS Grad" */

create table tp_2_test_1 as select * from tp_2 where age in (16:19) and stat1=.;
quit;

proc sql;
create table tp_2_test_2 as select yr,count(id) as p
from tp_2 group by yr;
quit;


proc sql;
create table tp_2a as select yr,sex,in_school,stat1,count(id) as n
from tp_2 where age in (16:19) group by yr,sex,in_school,stat1;

create table tp_2b as select *,n/sum(n) as pct format=percent8.1
from tp_2a group by yr,sex,in_school;

create table tp_2c as select x.*,y.f as pct_acs format=percent8.1,y.p_acs
,x.pct - y.f as d format=percent8.1
from tp_2b as x
inner join tab_stat1_cn5_2 as y on /*x.yr=y.yr_est and*/ x.sex=y.sex and x.in_school=y.in_school and x.stat1=y.stat1
order by abs(d) desc;
quit;

proc sql;
create table tp_2d as select in_school,in_college,grade_id,stat1,count(id) as n
from tp_2 where age in (16:19) group by in_school,in_college,grade_id,stat1;

create table tp_2e as select in_school,in_college,grade_id,stat1
,case
when stat1 = 6 then "Employed HS Grad"
when stat1 = 5 then "Unemployed HS Grad"
when stat1 = 4 then "NILF HS Grad"
when stat1 = 3 then "Employed HS NoGrad"
when stat1 = 2 then "Unemployed HS NoGrad"
when stat1 = 1 then "NILF HS NoGrad"
when stat1 = 30 then "Employed student"
when stat1 = 20 then "Unemployed student"
when stat1 = 10 then "NILF student" end as stat1_name
,count(id) as n
from tp_2 where age in (16:19) group by in_school,in_college,grade_id,stat1;
quit;

proc sql;
create table wstat_test_1 as select * from b23001_cn5_1 where age13=1619 and yr=2010;
create table wstat_test_2 as select * from b14005_cn5_1 where yr=2010;
quit;

proc sql;
create table hp_age2099_0 as select yr,ct,sex,age13,count(id) as hp
from tp_2 where age>19 and type="HHP" group by yr,ct,sex,age13;
quit;

proc sql;
create table hp_age2099_0a as select x.*,y.wstat_new
from hp_age2099_0 as x
inner join (select distinct age13,wstat_new from tab_wstat_cn5_2) as y on x.age13=y.age13;

create table hp_age2099_1 as select x.*
,y.f as f_ct,y.p_acs as p_acs_ct
,z.f as f_cn,z.p_acs as p_acs_cn
,coalesce(y.f,z.f) as f
,ceil(calculated f * x.hp) as hp_stat1
/* when any of the GQ are present, use the county-wide distribution
otherwise, use ct-specific distribution (if available) */
from hp_age2099_0a as x
left join tab_wstat_ct5_2 as y on /*x.yr=y.yr_est and*/ x.ct=y.ct and x.sex=y.sex and x.age13=y.age13 and x.wstat_new=y.wstat_new
left join tab_wstat_cn5_2 as z on /*x.yr=z.yr_est and*/ x.sex=z.sex and x.age13=z.age13 and x.wstat_new=z.wstat_new
order by yr,ct,sex,age13,wstat_new, f desc;

create table hp_age2099_1a as select * from hp_age2099_1 where f=.;
create table hp_age2099_1b as select * from hp_age2099_1 where f_ct=.;
quit;

data hp_age2099_2(drop=c);set hp_age2099_1; where hp>0;
by yr ct sex age13;retain c;
if first.age13 then do;hp_stat1_=min(hp,hp_stat1);c=hp_stat1_;end;
else do;hp_stat1_=min(hp_stat1,hp-c);c=hp_stat1_+c;end;
run;

proc sql;
create table hp_age2099_3 as select yr,ct,sex,age13,wstat_new,hp,hp_stat1_
from hp_age2099_2 where hp_stat1_>0 order by yr,ct,sex,age13,wstat_new;

create table hp_age2099_3a as select yr,ct,sex,age13,hp,sum(hp_stat1_) as psum
from hp_age2099_3 group by yr,ct,sex,age13,hp
having calculated psum ^= hp;
quit;

proc sql;
create table hp_age2099_3b as select yr,wstat_new,sum(hp_stat1_) as est
from hp_age2099_3 group by yr,wstat_new
order by wstat_new,yr;
quit;

/* SH.SD_PUMS_MARGINS_2 (covers 2006-2016) is generated in T:\socioec\Current_Projects\Synthetic Households\PUMS Margins 2.sas */

proc sql;
create table margins_wstat_new_1 as select sex,age13,wstat_new,hh_income_cat_id,count(*) as n
from SH.SD_PUMS_MARGINS_2 where age13^=0015
group by sex,age13,wstat_new,hh_income_cat_id;

create table scale_1 as select x1.*,x2.*,x3.*,x4.*
from (select distinct sex from margins_wstat_new_1) as x1
cross join (select distinct age13 from margins_wstat_new_1) as x2
cross join (select distinct wstat_new from margins_wstat_new_1) as x3
cross join (select distinct hh_income_cat_id from margins_wstat_new_1) as x4;

create table margins_wstat_new_2 as select x.*,coalesce(y.n,0.01) as n
from scale_1 as x
left join margins_wstat_new_1 as y 
on x.sex=y.sex and x.age13=y.age13 and x.wstat_new=y.wstat_new and x.hh_income_cat_id=y.hh_income_cat_id
order by sex,age13,wstat_new,hh_income_cat_id;
quit;


/*------------IPF SECTION---------------------------------------*/

/*
A: yr/ct/age/sex/work_status
B: yr/ct/age/sex/income
The purpose is to create a joint distribution of A and B yr/ct/age/sex/work_status/income
*/

proc sql;
create table inp_a_00 as
select put(yr,4.)||ct||"_"||put(age13,4.)||sex as i,wstat_new as a,hp_stat1_ as h,sex,age13
from hp_age2099_3 order by i,a;

create table inp_b_00 as
select put(yr,4.)||ct||"_"||put(age13,4.)||sex as i,hh_income_cat_id as b,count(*) as h,sex,age13
from tp_2 where age>19 and type="HHP"
group by i,b,sex,age13;

create table inp_AB_m_0 as select x.i,x.a,y.b,z.n as h
from inp_a_00 as x
inner join inp_b_00 as y on x.i=y.i 
inner join margins_wstat_new_2 as z
on x.sex=z.sex and x.age13=z.age13 and x.a=z.wstat_new
and y.sex=z.sex and y.age13=z.age13 and y.b=z.hh_income_cat_id
order by i,a,b;

create table inp_a_0 as select i,a,h from inp_a_00 order by i,a;
create table inp_b_0 as select i,b,h from inp_b_00 order by i,b;
quit;

/*
proc sql;
create table test_01 as select x.*,y.*,z.*
from (select count(distinct i) as a from inp_a_0) as x
cross join (select count(distinct i) as b from inp_b_0) as y
cross join (select count(distinct i) as ab from inp_ab_m_0) as z;
quit;

proc sql;
create table test_01a as select a,substr(i,1,4) as yr,sum(h) as h
from inp_a_0 group by a,yr;
quit;
*/

/*
proc sql;
delete from inp_a_0 where substr(i,5,6)^="000100";
delete from inp_b_0 where substr(i,5,6)^="000100";
delete from inp_ab_m_0 where substr(i,5,6)^="000100";
quit;
*/

/*
proc sql;
create table test_02 as select x.*,y.*,z.*
from (select count(distinct i) as a from inp_a_0) as x
cross join (select count(distinct i) as b from inp_b_0) as y
cross join (select count(distinct i) as ab from inp_ab_m_0) as z;
quit;
*/

/* these table should have zero records */
proc sql;
create table inp_A_0_test as select i,A,count(i) as c from inp_A_0 group by i,A having calculated c>1;
create table inp_B_0_test as select i,B,count(i) as c from inp_B_0 group by i,B having calculated c>1;
create table inp_AB_m_0_test_1 as select i,A,B,count(i) as c from inp_AB_m_0 group by i,A,B having calculated c>1;

create table inp_AB_m_0_test_2 as select coalesce(x.i,y.i) as i,x.h_a,y.h_b
from (select i,sum(h) as h_a from inp_a_0 group by i) as x
full join (select i,sum(h) as h_b from inp_b_0 group by i) as y on x.i=y.i
where x.h_a=. or y.h_b=. or x.h_a^=y.h_b;
quit;

%include "T:\socioec\Demographic_Model\Work\ipf\new ipf.sas";

/*%include "T:\socioec\Demographic_Model\Work\ipf\new ipf 2.sas";*/

%include "T:\socioec\Demographic_Model\Work\ipf\postipf.sas";

options nonotes;
%ipf1(mx=100);
/* sets the number of iterations */

/*%ipf2(mx=100, p=0.001);*/
/* sets the number of iterations and precision */


%postipf;
options notes;

options nonotes;

proc sql;
create table hp_age2099_4 as select
input(substr(i,1,4),4.0) as yr length=3
,substr(i,5,6) as ct
,substr(i,16,1) as sex
,input(substr(i,12,4),4.0) as age13
,b as hh_income_cat_id
,a as wstat_new
,c as hp_stat1_
from pii_06 where c>0 order by yr,ct,sex,age13,sex,hh_income_cat_id,wstat_new;

create table hp_age2099_4a as select wstat_new,yr,sum(hp_stat1_) as n
from hp_age2099_4 group by wstat_new,yr;
quit;

data hp_age2099_5;set hp_age2099_4;by yr ct sex age13 hh_income_cat_id;retain b;
if first.hh_income_cat_id then do; a = 1;b = hp_stat1_; end;
else do;a = b + 1; b = a + hp_stat1_ - 1;end;
run;

proc sql;
create table hp_wstat_1 as select yr,ct,sex,age13,hh_income_cat_id,id
,case 
when age13 in (6569,7074,7599) then ranuni(2040) + (100-age) /* younger ages first (in descending order ) */
/* this ensures that older ages are put into NILF 
wstat_new is dispatched in the following order: 1-Employed, 2-Unemployed, 6-NILF, 9-Military 
(there is no military for 65+ */
else ranuni(2040)
end as sort_order
from tp_2 where age>19 and type in ("HHP") order by yr,ct,sex,age13,hh_income_cat_id,sort_order desc;
quit;

data hp_wstat_1;set hp_wstat_1;by yr ct sex age13 hh_income_cat_id;retain i;
if first.hh_income_cat_id then i=1;else i=i+1;
run;


proc sql;
create table hp_wstat_2 as select x.yr,x.ct,x.sex,x.id,y.wstat_new
from hp_wstat_1 as x
left join hp_age2099_5 as y on x.yr=y.yr and x.ct=y.ct and x.sex=y.sex and x.age13=y.age13 and x.hh_income_cat_id=y.hh_income_cat_id
and y.a<=x.i<=y.b;

create table hp_wstat_2a as select * from hp_wstat_2 where wstat_new=.;

create table hp_wstat_2b as select wstat_new,yr,count(*) as n
from hp_wstat_2 group by wstat_new,yr;
quit;


proc sql;
create table tp_3 as select x.*,y.wstat_new length=3
from tp_2 as x
left join hp_wstat_2 as y on x.yr=y.yr and x.id=y.id;

update tp_3 set wstat_new=6 where wstat_new=. and age<16;
/* everybody under 16 is NILF */

update tp_3 set wstat_new=9 where wstat_new=. and type in ("MIL");
/* Military gq are always MIL */

update tp_3 set wstat_new=6 where wstat_new=. and type in ("INS","OTH");
/* INS and OTH gq are always NILF */

update tp_3 set wstat_new=6 where type="HHP" and wstat_new=. and age in (16:19) and stat1 in (1,10,4); /*NILF*/
update tp_3 set wstat_new=1 where type="HHP" and wstat_new=. and age in (16:19) and stat1 in (3,6,30);/* Employed */
update tp_3 set wstat_new=3 where type="HHP" and wstat_new=. and age in (16:19) and stat1 in (2,5,20);/* Unemployed */

update tp_3 set in_school=0,in_college=0,grade_id=0 where in_college=1 and wstat_new=9;
/* college attendees can not be military */
quit;

proc sql;
create table tp_3_test_1 as select * from tp_3 where wstat_new=.;

create table tp_3_test_2 as select * from tp_3 where wstat_new=. and type^="COL";

create table tp_3_test_3 as select yr,in_school,in_college,grade_id,wstat_new,count(id) as n
from tp_3 group by yr,in_school,in_college,grade_id,wstat_new;
quit;


/*
some in wstat_new=1 (if age in (16:19) and stat1=6) need to be reclassified as Military (wstat_new=9)
*/


/*
1 NILF HS NoGrad
2 Unemployed HS NoGrad
3 Employed HS NoGrad
4 NILF HS Grad
5 Unemployed HS Grad
6 Employed HS Grad

10 NILF student
20 Unemployed student
30 Employed student
*/



proc sql;
create table tab_wstat_ct5_3 as select x.yr_est,y.*
from (select distinct yr as yr_est from tp_3) as x
cross join tab_wstat_ct5_2 as y;

create table mil_6 as select x.*,coalesce(y.emp_acs,0) as emp_acs,x.mil_acs / (x.mil_acs + coalesce(y.emp_acs,0)) as pct_mil
,z.gq_mil,z1.emp1617,coalesce(z2.emp1819,0) as emp1819
from (select yr_est,ct,sex,p_acs as mil_acs from tab_wstat_ct5_3 where wstat_new=9 and age13=1619 and p_acs>0) as x

left join (select yr_est,ct,sex,p_acs as emp_acs from tab_wstat_ct5_3 where wstat_new=1 and age13=1619) as y
on x.yr_est=y.yr_est and x.ct=y.ct and x.sex=y.sex

left join (select yr,ct,sex,count(*) as gq_mil from tp_3 where age13=1619 and type="MIL" group by yr,ct,sex) as z
on x.yr_est=z.yr and x.ct=z.ct and x.sex=z.sex

left join (select yr,ct,sex,count(*) as emp1617 from tp_3 where age in (16:17) and stat1=6 group by yr,ct,sex) as z1
on x.yr_est=z1.yr and x.ct=z1.ct and x.sex=z1.sex

left join (select yr,ct,sex,count(*) as emp1819 from tp_3 where age in (18:19) and stat1=6 group by yr,ct,sex) as z2
on x.yr_est=z2.yr and x.ct=z2.ct and x.sex=z2.sex

order by ct,sex,yr_est;

create table mil_7 as select *,min(mil_acs,emp1819) as mil_1819a,min(emp1819,round(emp1819 * pct_mil,1)) as mil_1819b
from mil_6;

create table mil_7a as select yr_est,sum(mil_acs) as mil_acs,sum(mil_1819a) as mil_1819a,sum(mil_1819b) as mil_1819b
from mil_7 group by yr_est;
quit;




/*
Assign military for 18-19
*/

proc sql;
create table hp_age1819_0 as select yr,ct,sex,id
from tp_3 where age in (18:19) and type="HHP" and stat1=6
order by yr,ct,sex,ranuni(2050);
quit;

data hp_age1819_0;set hp_age1819_0;by yr ct sex;retain i;
if first.sex then i=1;else i=i+1;
run;

proc sql;
create table hp_age1819_1 as select x.*,y.mil_1819b
from hp_age1819_0 as x
left join mil_7 as y on x.yr=y.yr_est and x.ct=y.ct and x.sex=y.sex
order by yr,ct,sex,i;

create table hp_age1819_2 as select yr,ct,sex,id,9 as wstat_new
from hp_age1819_1 where i<=mil_1819b;
quit;

/* see table college_gq_4b generated in "T:\socioec\Current_Projects\Synthetic Households\PUMS Margins 2.sas" */
proc sql;
create table college_gq as select yr,id,ranuni(2060) as rn
,case
when calculated rn < 0.69 then 6 /* 68% of colege gq are NILF */
when calculated rn < 0.95 then 1 /* 26% (95-68) of colege gq are employed */
else 3 /* 5% of college gq are unemployed */
end as wstat_new length=3
from tp_3 where wstat_new=. and type="COL";
quit;

proc sql;
create table tp_4 as select x.yr,x.hh_id,x.id,x.age,x.sex,x.r,x.hisp
,x.type,x.ct,x.in_school,x.in_college
,x.grade_id,x.stat1,coalesce(y.wstat_new,z.wstat_new,x.wstat_new) as wstat_new length=3
,x.hh_income_cat_id
,case
when x.age<=15 then 0015
when x.age<=18 then 1618
when x.age<=24 then 1924
when x.age<=29 then 2529
when x.age<=39 then 3039
when x.age<=49 then 4049
when x.age<=59 then 5059
when x.age<=69 then 6069
when x.age>=70 then 7099 end as age9 length=3
,ranuni(2070) as rn
from tp_3 as x
left join hp_age1819_2 as y on x.yr=y.yr and x.id=y.id
left join college_gq as z on x.yr=z.yr and x.id=z.id;

/* for gq college and gq military income is set to the lowest category */
/* this is redundant because income is set earlier for all GQ */
update tp_4 set hh_income_cat_id=1 where hh_income_cat_id=. and type in ("COL","MIL");

update tp_4 set wstat_new=6 where type="COL" and age>22;

create table tp_4a as select * from tp_4 where wstat_new=.;
quit;

proc sql;
create table test_01 as select age9,yr,count(id) as p
from tp_4 where type="COL" group by age9,yr;
quit;

/*
proc sql;
create table tp_4b as select * from tp_4 where type="MIL" and wstat_new^=9;
quit; 
*/


/* put(weeks_worked_id,z2.)||"_"||put(hours_worked,z2.)||"_"||put(educ_id,z2.)||"_"||occ as wheo */

/* Table sh.sd_pums_margins_1 is created in "T:\socioec\Current_Projects\Synthetic Households\PUMS Margins 1.sas" */
/* Table sh.sd_pums_margins_2 is created in "T:\socioec\Current_Projects\Synthetic Households\PUMS Margins 2.sas" */

proc sql;
create table wheo_0 as select sex,age9
,grade_id,hh_income_cat_id,wstat_new,wheo
,count(*) as n
from sh.sd_pums_margins_2 where age9^=0015
group by sex,age9,grade_id,hh_income_cat_id,wstat_new,wheo;

update wheo_0 set wheo="05_00_"||substr(wheo,7,5) where wstat_new in (3,6);
quit;

proc sql;
create table mil_occ_1 as select occ,count(*) as n
from sh.sd_pums_margins_2
where mil_ind=1 and military_id=0 and wstat=1
group by occ;
quit;

proc sql;
create table wheo_1 as select sex,age9
,grade_id,hh_income_cat_id,wstat_new,wheo
,sum(n) as n
from wheo_0
group by sex,age9,grade_id,hh_income_cat_id,wstat_new,wheo;

create table wheo_2 as select *,n/sum(n) as s
from wheo_1 group by sex,age9,grade_id,hh_income_cat_id,wstat_new;
quit;

proc sql;
create table wheo_test_0 as select wstat_new,scan(wheo,1,"_") as weeks,scan(wheo,2,"_") as hours,count(*) as n
from sh.sd_pums_margins_2 group by wstat_new,weeks,hours;

create table wheo_test_1 as select wstat_new,scan(wheo,1,"_") as weeks,scan(wheo,2,"_") as hours,sum(n) as n
from wheo_1 group by wstat_new,weeks,hours;

create table wheo_test_2 as select distinct wheo from wheo_1;
quit;



data wheo_3;set wheo_2;by sex age9 grade_id hh_income_cat_id wstat_new;retain b;
if first.wstat_new then do;a=0;b=s;end;
else do;a=b;b=a+s;end;
run;

proc sql;
create table wheo_3a as select x.*,y.*
from (select distinct sex,age9,grade_id,wstat_new from wheo_3) as x
cross join (select distinct hh_income_cat_id from wheo_3) as y;

create table wheo_3b as select x.*
from wheo_3a as x
left join (select distinct sex,age9,grade_id,hh_income_cat_id,wstat_new from wheo_3) as y
on x.sex=y.sex and x.age9=y.age9 and x.grade_id=y.grade_id and x.wstat_new=y.wstat_new and x.hh_income_cat_id=y.hh_income_cat_id
where x.hh_income_cat_id=5 and y.hh_income_cat_id=.;

create table wheo_3c as select x.sex,x.age9,x.grade_id,x.hh_income_cat_id,x.wstat_new
,y.wheo,y.n,y.s,y.b,y.a
from wheo_3b as x
inner join (select * from wheo_3 where hh_income_cat_id=4) as y
on x.sex=y.sex and x.age9=y.age9 and x.grade_id=y.grade_id and x.wstat_new=y.wstat_new;
quit;

data wheo_4;set wheo_3 wheo_3c;run;

proc sql;
create table test_wheo as select distinct wheo from wheo_4 where wstat_new=1 and scan(wheo,4,"_")="";
quit;


/*
proc sql;
create table test_01 as select sex,grade_id,wstat_new,hh_income_cat_id
,sum(n) as n
from wheo_1 where age9=1924
group by sex,grade_id,wstat_new,hh_income_cat_id;
quit;

proc transpose data=test_01 out=test_02(drop=_name_);by sex grade_id wstat_new;var n;id hh_income_cat_id;run;
*/


proc sql;
create table tp_5(drop=rn age9) as select x.*
,case 
when x.age9=0015 or x.type in ("INS","OTH") then "05_00_01_"

/*
This is a remnant of old code; disabled on 9/29/2017
when x.stat1 in (1,4,10) then "05_00_01_" 
when x.stat1 in (2,5,20) then "05_00_01_" 
*/
/* stat1 in (1,4,10) NILF ages1619 */
/* stat1 in (2,5,20 Unemployed ages1619 */

when x.type="MIL" or (/*x.age=18 and*/ x.wstat_new=9) then "05_35_09_ML"
else y.wheo end as wheo
from tp_4 as x
left join wheo_4 as y on x.sex=y.sex and x.age9=y.age9 and x.grade_id=y.grade_id and x.hh_income_cat_id=y.hh_income_cat_id
and x.wstat_new=y.wstat_new and y.a<=x.rn<=y.b;

update tp_5 set wheo="05_00_09_" where wheo="" and type="COL" and age>22;

/* somehow there are 2 15-year old military gq; trace the origin */
update tp_5 set age=18,wstat_new=9,wheo="05_35_09_ML" where age=15 and type="MIL";

create table tp_5a as select * from tp_5 where wheo="";

create table tp_5b as select type,wstat_new,wheo,count(*) as n
from tp_5 group by type,wstat_new,wheo;

create table tp_5c as select type,wstat_new,substr(wheo,1,5) as whe,count(*) as n
from tp_5 group by type,wstat_new,whe;
quit;


proc sql;
create table tp_5d as select wstat_new,scan(wheo,4,"_") as occ,count(*) as n
from tp_5 group by wstat_new,occ;
quit;

/*
PROC IMPORT OUT=abm_p_0 DATAFILE="T:\ABM\release\ABM\archive\version_13_3_1\input\2012\persons.csv"
DBMS=CSV REPLACE; GETNAMES=YES; DATAROW=2;
RUN; 

PROC IMPORT OUT=abm_h_0 DATAFILE="T:\ABM\release\ABM\archive\version_13_3_1\input\2012\households.csv"
DBMS=CSV REPLACE; GETNAMES=YES; DATAROW=2;
RUN; 

PROC IMPORT OUT=abm_lu_0 DATAFILE="T:\ABM\release\ABM\archive\version_13_3_1\input\2012\mgra13_based_input2012.csv"
DBMS=CSV REPLACE; GETNAMES=YES; DATAROW=2;
RUN; 
*/


proc sql;
create table tp_5_1 as select yr
,hh_id
,id from tp_5 where type not in ("INS","OTH")
order by yr,hh_id,id;

create table tp_5_1a as select distinct yr,hh_id from tp_5_1;
quit;

data tp_5_1a;set tp_5_1a;by yr;retain hhid;length hhid 5;
if first.yr then hhid=1;
else hhid=hhid+1;
run;

proc sql;
create table tp_5_2 as select 
x.yr,y.hhid,x.hh_id,x.id
from tp_5_1 as x
inner join tp_5_1a as y on x.yr=y.yr and x.hh_id=y.hh_id
order by yr,hhid,id;
quit;

data tp_5_2;set tp_5_2;by yr hhid;retain pnum;length pnum 5;
if first.hhid then pnum=1;
else pnum=pnum+1;
run;

data tp_5_2;set tp_5_2;by yr;retain perid;length perid 5;
if first.yr then perid=1;
else perid=perid+1;
run;

proc sql;
create table tp_6 as select
x.yr,x.hh_id,x.id,x.type
,scan(x.wheo,4,"_") as occ length=2

/*output-ready*/
,z.hhid
,z.perid
,z.pnum

,x.age
,case 
when x.sex="M" then 1 else 2 end as sex length=3
,case when x.wstat_new=9 then 1 else 0 end as miltary length=3
,case when x.wstat_new in (1,9) then 1 else x.wstat_new end as wstat length=3
,input(scan(x.wheo,1,"_"),2.) as weeks length=3
,input(scan(x.wheo,2,"_"),2.) as hours length=3
,input(scan(x.wheo,3,"_"),2.) as educ length=3
,x.grade_id as grade
,case
when y.r in ("R10","R11") then 1
when y.r in ("R02") then 2
when y.r in ("R03") then 5
when y.r in ("R04") then 6
when y.r in ("R05") then 7
when y.r in ("R06") then 8
when y.r in ("R07") then 9
end as rac1p length=3
,case
when y.hisp="H" then 2 else 1 end as hisp length=3
from tp_5 as x
inner join tp_0 as y on x.yr=y.yr and x.id=y.id
inner join tp_5_2 as z on x.yr=z.yr and x.id=z.id
where x.type not in ("INS","OTH");
quit;

proc sql;
create table tp_6_test_1 as select yr,count(pnum) as p format=comma12.
from tp_6 group by yr;
quit;


proc sql;
create table tp_6_gq as select
x.yr,x.hh_id,x.id,x.type
,scan(x.wheo,4,"_") as occ length=2

/*output-ready*/
,0 as hhid
,0 as perid
,0 as pnum

,x.age
,case 
when x.sex="M" then 1 else 2 end as sex length=3
,case when x.wstat_new=9 then 1 else 0 end as miltary length=3
,case when x.wstat_new in (1,9) then 1 else x.wstat_new end as wstat length=3
,input(scan(x.wheo,1,"_"),2.) as weeks length=3
,input(scan(x.wheo,2,"_"),2.) as hours length=3
,input(scan(x.wheo,3,"_"),2.) as educ length=3
,x.grade_id as grade
,case
when y.r in ("R10","R11") then 1
when y.r in ("R02") then 2
when y.r in ("R03") then 5
when y.r in ("R04") then 6
when y.r in ("R05") then 7
when y.r in ("R06") then 8
when y.r in ("R07") then 9
end as rac1p length=3
,case
when y.hisp="H" then 2 else 1 end as hisp length=3
from tp_5 as x
left join tp_0 as y on x.yr=y.yr and x.id=y.id
/* left join tp_5_2 as z on x.yr=z.yr and x.id=z.id */
where x.type in ("INS","OTH");
quit;

proc sql;
create table relchildren as select yr,hh_id,count(*) as related_children
from tp_6 where age<18 group by yr,hh_id;

update relchildren set related_children=8 where related_children>8;

create table hworkers as select yr,hh_id,count(*) as hworkers
from tp_6 where wstat=1 group by yr,hh_id;
quit;

/* Need to ramdomly select non-military working White Collar people and assign them to a military industry (indcen=9770) */

proc sql;
create table mil_wc_0 as select yr,id
from tp_6 where miltary=0 and wstat=1 and occ="WC" and type="HHP" and weeks=1 and hours=35
order by yr,ranuni(3010);
quit;

data mil_wc_1;set mil_wc_0;by yr;retain i;
if first.yr then i=1;else i=i+1;
run;

/* need 22000 white collar workers assigned to work for DoD */
proc sql;
create table mil_wc_2 as select yr,id,"ML" as occ1 from mil_wc_1
where i<=22000;
quit;

proc sql;
create table tp_6_1 as select x.*,y.occ1
from tp_6 as x
left join mil_wc_2 as y on x.yr=y.yr and x.id=y.id;

update tp_6_1 set occ=occ1 where occ1^="";
quit;


proc sql;
create table persons_1 as select
yr length=3 format=4.
,hh_id length=5 format=8.
,id length=5 format=8.
,type
/* do not include in final */

/* variable sequence gleaned from "T:\ABM\release\ABM\version_13_3_1\input\2012\persons.csv" */

,hhid format=7./*sequential household id*/
,perid format=7./*sequential person id */
,0 as household_serial_no length=3 format=1./* original id from pums; use 0 */
,pnum length=3 format=2./*sequence of persons within household */
,age length=3 format=3.
,sex length=3 format=1.
,miltary /*as military*/ length=3 format=1.

,case
WHEN age < 16 then 4
WHEN wstat IN (1,2,4,5) AND weeks IN (1) AND hours >= 35 THEN 1
WHEN wstat IN (1,2,4,5) AND (weeks IN (5) OR hours < 35) THEN 2
WHEN wstat IN (3,6) THEN 3
end as pemploy length=3 format=1.

,case
WHEN grade > 0 AND grade < 6 THEN 1
WHEN grade IN (6,7) THEN 2
ELSE 3
end as pstudent length=3 format=1.

,case
WHEN age < 6 THEN 8
WHEN age >= 6 AND age <= 15 THEN 7
WHEN wstat IN (1,2,4,5) AND weeks IN (1) AND hours >= 35 THEN 1
WHEN grade IN (6,7) OR (age>=20 AND grade > 0 AND grade < 6) THEN 3
WHEN grade > 0 AND grade < 6 THEN 6
WHEN wstat IN (1,2,4,5) AND (weeks IN (5) OR hours < 35) THEN 2
WHEN age < 65 THEN 4
ELSE 5
end as ptype length=3 format=1.

,educ length=3 format=2.
,grade length=3 format=1.
,0 as occen5 length=3 format=1.
,case
when occ="WC" then "11-1021"
when occ="BC" then "31-1010"
when occ="SC" then "41-1011"
when occ="CO" then "45-1010"
when occ="PT" then "51-1011"
when occ="ML" then "55-1010"
else "00-0000"
end as occsoc5 length=7 format=$7.
,case when occ1^="" or miltary=1 then 9770 else 0 end as indcen length=3 format=4.
,weeks length=3 format=1.
,hours length=3 format=2.
,rac1p length=3 format=1.
,hisp length=3 format=1.
,0 as version length=3 format=1.
from tp_6_1
order by yr,hhid,perid;

create table persons_1_gq as select
yr length=3 format=4.
,hh_id length=5 format=8.
,id length=5 format=8.
,type
/* do not include in final */

/* variable sequence gleaned from "T:\ABM\release\ABM\version_13_3_1\input\2012\persons.csv" */

,hhid length=5 format=7./*sequential household id*/
,perid length=5 format=7./*sequential person id */
,0 as household_serial_no length=3 format=1./* original id from pums; use 0 */
,pnum length=3 format=2./*sequence of persons within household */
,age length=3 format=3.
,sex length=3 format=1.
,miltary as military length=3 format=1.

,case
WHEN age < 16 then 4
WHEN wstat IN (1,2,4,5) AND weeks IN (1) AND hours >= 35 THEN 1
WHEN wstat IN (1,2,4,5) AND (weeks IN (5) OR hours < 35) THEN 2
WHEN wstat IN (3,6) THEN 3
end as pemploy length=3 format=1.

,case
WHEN grade > 0 AND grade < 6 THEN 1
WHEN grade IN (6,7) THEN 2
ELSE 3
end as pstudent length=3 format=1.

,case
WHEN age < 6 THEN 8
WHEN age >= 6 AND age <= 15 THEN 7
WHEN wstat IN (1,2,4,5) AND weeks IN (1) AND hours >= 35 THEN 1
WHEN grade IN (6,7) OR (age>=20 AND grade > 0 AND grade < 6) THEN 3
WHEN grade > 0 AND grade < 6 THEN 6
WHEN wstat IN (1,2,4,5) AND (weeks IN (5) OR hours < 35) THEN 2
WHEN age < 65 THEN 4
ELSE 5
end as ptype length=3 format=1.

,educ length=3 format=2.
,grade length=3 format=1.
,0 as occen5 length=3 format=1.
,case
when occ="WC" then "11-1021"
when occ="BC" then "31-1010"
when occ="SC" then "41-1011"
when occ="CO" then "45-1010"
when occ="PT" then "51-1011"
when occ="ML" then "55-1010"
else "00-0000"
end as occsoc5 length=7 format=$7.
,0 as indcen length=3 format=4.
,weeks length=3 format=1.
,hours length=3 format=2.
,rac1p length=3 format=1.
,hisp length=3 format=1.
,0 as version length=3 format=1.
from tp_6_gq
order by yr,hhid,perid;

create table persons_1a as select yr,perid,count(perid) as n
from persons_1 group by yr,perid having calculated n>1;
quit;


proc sql;
create table test_01 as select yr,weeks,hours,count(*) as n
from tp_6 group by yr,weeks,hours;

create table test_02 as select yr,occsoc5,count(*) as n
from persons_1 where pemploy in (1,2) and miltary=0 group by yr,occsoc5;

create table ptype_test as select yr,ptype,count(*) as n
from persons_1 group by yr,ptype order by ptype,yr;

create table pemploy_test as select * from persons_1 where pemploy=.;
quit;


/*
proc sql;
create table mgra_taz as select mgra_13 as mgra,taz_13 as taz,luz_13 as luz,sra_1990 as sra
from sql_dc.vi_xref_geography_mgra_13
order by mgra;
quit;
*/

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table mgra_taz as select mgra length=4 format=5., taz length=3 format=4.,luz length=3 format=3.,sra length=3 format=2.
from connection to odbc
(select mgra_13 as mgra,taz_13 as taz,luz_13 as luz,sra_1990 as sra FROM [data_cafe].[ref].[vi_xref_geography_mgra_13])
;

disconnect from odbc;
quit;

data mgra_taz;set mgra_taz;
informat mgra taz luz sra;
run;

/*
proc sql;
select max(luz) as maz_luz,max(sra) as max_sra from mgra_taz;
quit;
*/





proc import out=pov_0 datafile="T:\socioec\Current_Projects\Popsyn_Related\Federal Poverty Thresholds (2010).xlsx"
replace dbms=excelcs;sheet="SAS";run;

proc transpose data=pov_0 out=pov_1;by hh_size hh_age;run;

proc sql;
create table pov_2 as select hh_size,hh_age
,input(substr(_label_,4,1),1.0) as related_children,col1 as poverty_threshold
from pov_1 where col1>0
order by hh_size,hh_age,related_children;

create table pov_2a as select hh_size,hh_age,related_children,count(*) as c
from pov_2 group by hh_size,hh_age,related_children
having calculated c>1;
quit;

/*
proc sql;
create table inc10 as select income_group_id - 10 as inc10,lower_bound,upper_bound
from sql_dim.income_group where constant_dollars_year=2010;
quit;
*/

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table inc10 as select *
from connection to odbc
(select income_group as inc10,lower_bound,upper_bound FROM [demographic_warehouse].[dim].[income_group]
 where categorization = 10 and constant_dollars_year=2010)
;

disconnect from odbc;
quit;

/* find household head */
proc sql;
create table hh_head_1 as select x.yr,x.hh_id,x.type,x.hh_income_cat_id,x.inc10,x.inc_2010,x.mgra
,x.size as persons length=3
,case when x.size>9 then 9 else size end as hh_size length=3
,y.hhid
,coalesce(z.related_children,0) as related_children length=3
,coalesce(u.hworkers,0) as hworkers length=3
,case
when x.size in (1,2) and v.age>64 then 65.99
when x.size in (1,2) and v.age<=64 then 0.64
else 0.99 end as hh_age 
from (select * from hh_01 where type in ("HHP","COL","MIL")) as x
left join tp_5_1a as y on x.yr=y.yr and x.hh_id=y.hh_id
left join relchildren as z on x.yr=z.yr and x.hh_id=z.hh_id
left join hworkers as u on x.yr=u.yr and x.hh_id=u.hh_id
left join (select * from hp_0 where role="H") as v on x.yr=v.yr and x.hh_id=v.hh_id;

update hh_head_1 set related_children=0 where hh_size=1 and related_children>0;

create table hh_head_1a as select * from hh_head_1 where related_children>=hh_size;

create table hh_head_1b as select yr,hh_id,count(hh_id) as n
from hh_head_1 group by yr,hh_id having calculated n>1;
quit;


proc sql;
create table hh_head_2 as select x.*,y.poverty_threshold
,round(x.inc_2010/y.poverty_threshold,0.001) as poverty
from hh_head_1 as x 
left join pov_2 as y on x.hh_size=y.hh_size and x.hh_age=y.hh_age and x.related_children=y.related_children;

create table hh_head_2a as select * from hh_head_2 where poverty_threshold=.;

create table hh_head_2b as select yr,hh_id,count(hh_id) as n
from hh_head_2 group by yr,hh_id having calculated n>1;
quit;


proc sql;
create table tp_0_ct as select distinct yr,ct,hh_id from tp_0;
create table tp_0_yr as select distinct yr from tp_0;
quit;


%include "T:\socioec\Current_Projects\&xver\program_code\1055b HHT Imputation.sas";


proc sql;
create table unit_type_test_1 as select yr,du_type,count(hu_id) as hu
from sql_xpef.housing_units group by yr,du_type
order by yr,du_type;

create table unit_type_test_2 as select du_type,yr,hu
from unit_type_test_1 where yr in (2017,2018,2050) order by du_type,yr;
quit;

/*
proc sql;
create table units_type_1 as select x.yr,x.hh_id,y.hu_id,coalesce(z.du_type2,y.du_type) as du_type3
from xpef.households as x
inner join xpef.housing_units as y on x.yr=y.yr and x.hh_id=y.hh_id
left join xpef.housing_units_sf as z on y.hu_id=z.hu_id;
quit;
*/

proc sql;
create table units_type_1 as select x.yr,x.hh_id,y.hu_id,y.du_type as du_type3
from sql_xpef.households as x
inner join sql_xpef.housing_units as y on x.yr=y.yr and x.hh_id=y.hh_id
where x.yr in (&list1) or x.yr = &by1;
quit;


proc sql;
create table households_1 as select
x.yr length=3 format=4.
,x.hh_id length=5 format=8.
,u.ct
/* do not include in final */

/* variable sequence gleaned from "T:\ABM\release\ABM\version_13_3_1\input\2012\households.csv" */
,z.hhid format=7.
,0 as household_serial_no length=3 format=1.
,y.taz length=3 format=4.
,x.mgra length=4 format=5.
,x.hh_income_cat_id as hinccat1 format=1.
,x.inc_2010 as hinc length=4
,x.hworkers length=3 format=2.
,0 as veh length=3 format=1.
,x.persons 
,coalesce(w.hht,0) as hht length=3 format=1. /* household/family type from PUMS */
,case
when t.du_type3="SFD" then 2
when t.du_type3="SFA" then 3
when t.du_type3="MH" then 1
when t.du_type3="MF" then 8
else 9
end as bldgsz length=3 format=1.
,case
when x.hh_id < 30000000 then 0 else 1 end as unittype length=3 format=1. 
,0 as version length=3 format=1.
,x.poverty format=7.3
from hh_head_2 as x
inner join mgra_taz as y on x.mgra=y.mgra
inner join tp_5_1a as z on x.yr=z.yr and x.hh_id=z.hh_id
inner join tp_0_ct as u on x.yr=u.yr and x.hh_id=u.hh_id
left join est_config_11 as w on x.yr=w.yr and x.hh_id=w.hh_id
left join units_type_1 as t on x.yr=t.yr and x.hh_id=t.hh_id
order by yr,hhid;

create table households_1a as select yr,hhid,count(hhid) as n
from households_1 group by yr,hhid having calculated n>1;
quit;

proc sql;
create table zztest_1 as select distinct yr from hh_head_2;
create table zztest_2 as select distinct yr from est_config_11;
create table zztest_3 as select distinct yr from units_type_1;
quit;


proc sql;
create table households_1_gq as select x.yr length=3 format=4.
,x.hh_id length=5 format=8.
,x.type,x.size
,y.taz length=3 format=4.
,z.ct
from hh_01 as x
inner join mgra_taz as y on x.mgra=y.mgra
inner join gq_0 as z on x.yr=z.yr and x.hh_id=z.gq_id + 30000000
where x.type in ("OTH","INS");
quit;

proc sql;
create table ztest_1 as select * from gq_0 where gq_type in ("OTH","INS");
create table ztest_2 as select * from hh_01 where hh_id = 263 + 30000000;
quit;


proc sql;
create table ztest_01 as select yr,hht,count(*) as n
from households_1 group by yr,hht;
quit;

proc sql;
create table p_len_1 as select
max(hh_id) as hh_id format=comma12.
,max(id) as id format=comma12.
,max(hhid) as hhid format=comma12.
,max(perid) as perid
,max(household_serial_no) as household_serial_no
,max(pnum) as pnum
,max(sex) as sex
,max(miltary) as military
,max(pemploy) as pemploy
,max(pstudent) as pstudent
,max(ptype) as ptype
,max(educ) as educ
,max(grade) as grade
,max(occen5) as occen5
,max(length(occsoc5)) as occsoc5
,max(indcen) as indcen
,max(weeks) as weeks
,max(hours) as hours
,max(rac1p) as rac1p
,max(hisp) as hisp
,max(version) as version
from persons_1;
quit;

proc sql;
create table h_len_1 as select
max(hinccat1) as hinccat1
,max(hworkers) as hworkers
,min(poverty) as min_poverty
,max(poverty) as max_poverty

,max(taz) as max_taz
,max(mgra) as max_mgra
from households_1;
quit;


proc sql;
create table ztest_100 as select pemploy,occsoc5,count(*) as n
from persons_1 group by pemploy,occsoc5;
quit;

options notes;

PROC DATASETS LIB=SD nolist; delete hh_01 tp_5 persons_1 households_1 persons_1_gq households_1_gq;
RUN; QUIT;

/* saving key tables */
data sd.hh_01;set hh_01;run;
data sd.tp_5;set tp_5;run;

data sd.persons_1;set persons_1;run; /* does not include GQ OTH and INS */
data sd.households_1;set households_1;run; /* does not include GQ OTH and INS */


data sd.persons_1_gq;set persons_1_gq;run; /* includes only GQ OTH and INS */
data sd.households_1_gq;set households_1_gq;run; /* includes only GQ OTH and INS */


proc sql;
drop table sql_xpef.abm_syn_persons;
drop table sql_xpef.abm_syn_households;

create table sql_xpef.abm_syn_persons(bulkload=yes bl_options=TABLOCK) as select * from persons_1;
create table sql_xpef.abm_syn_households(bulkload=yes bl_options=TABLOCK) as select * from households_1;
quit;

options nonotes;

/*
proc sql;
create table ztest_01 as select yr,count(*) as n
from sql_xpef.abm_syn_persons group by yr;
quit;
*/




/* when age9=0015 then wheo="05_00_01_" */
/* ,case when wstat_new in  (3,6) then "05_00_01_" else wheo end as wheo */

/*
"military" industries
when indcen in (9670,9680,9690,9770,9780,9790,9870)
crosstab occsoc5 with military industries
*/


