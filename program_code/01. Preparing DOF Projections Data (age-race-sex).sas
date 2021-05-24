libname e1 "T:\socioec\Current_Projects\XPEF06\input_data";

/*libname sql_dof odbc noprompt="driver=SQL Server; server=sql2014a8; database=socioec_data;
Trusted_Connection=yes" schema=ca_dof; */

/*
Census has race categories "other" and "two or more"
DOF has "multi-race"
Presumably, "multi-race" includes "other"
Using Census data, add "other" and "two or more", then find the share of "other" in that total
Use these share to split DOF's "multi-race" into "other" and "two or more"
*/

libname sf1 "T:\socioec\socioec_data_test\Census_2010\SF1";

%let vyr=2017;/* set vintage year for DOF estimates*/

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table dof_est_0 as select
case when area_type="City" then area_name else summary_type end as Name
,est_yr,total_pop as tp
from connection to odbc
(
select * FROM [socioec_data].[ca_dof].[population_housing_estimates]
where county_name= 'San Diego' and vintage_yr = &vyr and (area_type = 'City' or summary_type = 'Unincorporated')
);

create table dof_proj_1 as select fiscal_yr
,case
when race_code=1 then "W"
when race_code=2 then "B"
when race_code=3 then "I"
when race_code=4 then "S"
when race_code=5 then "P" 
when race_code=6 then "M"
when race_code=7 then "H"
end as r7
,case
when age <= 101 then age
else 101 end as age102,sex
,sum(population) as p
from connection to odbc
(
select * FROM [socioec_data].[ca_dof].[population_proj_2018_1_20]
where county_fips_code = 6073 and fiscal_yr >= 2009 and fiscal_yr <= 2051
)
group by fiscal_yr,r7,age102,sex;

DISCONNECT FROM odbc;
quit;


proc transpose data=sf1.sandag_050(keep=PCT012:) out=pct012_0;run;

data pct012_1;set pct012_0;
if anyalpha(_name_,7)>0 then type=substr(_name_,7,1);
if type="" then i=input(substr(_name_,7),4.0);else i=input(substr(_name_,8),4.0);
if type="" then delete; /* dropping tables without the race breakdown */
if i in (1,2,106) then delete; /* dropping totals (all, males,females)*/;
if i<106 then sex="M";else sex="F";
if sex="M" then age=i-3;else age=i-107;
/* age=100 covers 100-104 */
if i in (104,208) then age=105; /* covers 105-109 */
if i in (105,209) then age=110; /* covers 110 and over */

if age<=4 then age18="00_04";
else if age<=9 then age18="05_09";
else if age<=14 then age18="10_14";
else if age<=19 then age18="15_19";
else if age<=24 then age18="20_24";
else if age<=29 then age18="25_29";
else if age<=34 then age18="30_34";
else if age<=39 then age18="35_39";
else if age<=44 then age18="40_44";
else if age<=49 then age18="45_49";
else if age<=54 then age18="50_54";
else if age<=59 then age18="55_59";
else if age<=64 then age18="60_64";
else if age<=69 then age18="65_69";
else if age<=74 then age18="70_74";
else if age<=79 then age18="75_79";
else if age<=84 then age18="80_84";
else age18="85_99";

/*
if age<=4 then age23= 0.04;
else if age<=9 then age23= 5.09;
else if age<=14 then age23= 10.14;
else if age<=17 then age23= 15.17;
else if age<=19 then age23= 18.19;
else if age<=20 then age23= 20.20;
else if age<=21 then age23= 21.21;
else if age<=24 then age23= 22.24;
else if age<=29 then age23= 25.29;
else if age<=34 then age23= 30.34;
else if age<=39 then age23= 35.39;
else if age<=44 then age23= 40.44;
else if age<=49 then age23= 45.49;
else if age<=54 then age23= 50.54;
else if age<=59 then age23= 55.59;
else if age<=61 then age23= 60.61;
else if age<=64 then age23= 62.64;
else if age<=66 then age23= 65.66;
else if age<=69 then age23= 67.69;
else if age<=74 then age23= 70.74;
else if age<=79 then age23= 75.79;
else if age<=84 then age23= 80.84;
else age23=85.99;
*/

if type="A" then r="R01";
if type="B" then r="R02";
if type="C" then r="R03";
if type="D" then r="R04";
if type="E" then r="R05";
if type="F" then r="R06";
if type="G" then r="R07";

if type="I" then r="R01";
if type="J" then r="R02";
if type="K" then r="R03";
if type="L" then r="R04";
if type="M" then r="R05";
if type="N" then r="R06";
if type="O" then r="R07";

if type in ("I","J","K","L","M","N","O") then h1="NH";
if type in ("A","B","C","D","E","F","G") then h1="T";
run;

/*
"R10" non-hispanic white
"R11" hispanic white
*/

proc sql;
create table pct012_2 as select
x.age as age101,x.age18,x.sex,x.r,y.col1 as NH,x.col1-y.col1 as H
from (select age,age18,sex,r,col1 from pct012_1 where h1="T" and col1>0) as x
left join (select age,age18,sex,r,col1 from pct012_1 where h1="NH") as y
on x.age=y.age and x.sex=y.sex and x.r=y.r
order by age101,age18,sex,r;

create table pct012_3 as
select * from pct012_2 where r not in ("R01")
	union all
select age101,age18,sex,"R10" as r,nh,0 as h from pct012_2 where r="R01"
	union all
select age101,age18,sex,"R11" as r,0 as nh,h from pct012_2 where r="R01"
order by age101,age18,sex,r;
quit;

proc sql;
create table pct012_4 as 
select r,sex,age18,age101,"NH" as h_nh,nh as p from pct012_3 where nh>0
	union all
select r,sex,age18,age101,"H" as h_nh,h as p from pct012_3 where h>0
order by r,sex,age18,age101,h_nh;
quit;

proc sql;
create table oth_1 as select sex,age18,age101,r,p from pct012_4 where h_nh="NH" and r in ("R06","R07")
order by sex,age101,r;
create table oth_1a as select r,sex,sum(p) as p from oth_1 group by r,sex;
quit;

proc transpose data=oth_1 out=oth_2(drop=_name_);by sex age18 age101;var p;id r;run;

proc sql;
create table oth_3 as select sex,age18,age101
,coalesce(R06,0) as R06,coalesce(R07,0) as R07
,coalesce(R06,0) / (coalesce(R06,0)+ coalesce(R07,0)) as R06_ format=percent8.1
from oth_2 order by sex,age101;

create table oth_3a as select sex
,sum(R06) as R06,sum(R07) as R07
,sum(R06) / (sum(R06)+ sum(R07)) as R06_ format=percent8.1
from oth_2 group by sex;

create table oth_3b as select age18
,sum(R06) as R06,sum(R07) as R07
,sum(R06) / (sum(R06)+ sum(R07)) as R06_ format=percent8.1
from oth_2 group by age18;

create table oth_3c as select 
sum(R06) as R06,sum(R07) as R07
,sum(R06) / (sum(R06)+ sum(R07)) as R06_ format=percent8.1
from oth_2;

create table oth_3d as select sex,age18
,sum(R06) as R06,sum(R07) as R07
,sum(R06) / (sum(R06)+ sum(R07)) as R06_ format=percent8.1
from oth_2 group by sex,age18;
quit;




proc sql;
create table dof_est_1 as select est_yr,sum(tp) as tp
from dof_est_0 where est_yr>2010 /* 2010 estimate is for April 1st; we use only Jan 1st */
group by est_yr;

create table dof_proj_2 as select fiscal_yr,sum(p) as p
from dof_proj_1 group by fiscal_yr;

create table dof_proj_3 as select x.fiscal_yr as yr
,round((x.p+y.p)/2,1) as p
from dof_proj_2 as x
inner join dof_proj_2 as y on x.fiscal_yr=y.fiscal_yr+1
order by yr;
quit;


proc sql;
create table dof_proj_t as
select est_yr as yr,tp from dof_est_1
	union all
select yr,p as tp from dof_proj_3 where yr>&vyr
order by yr;
quit;


proc sql;
create table dof_proj_4 as select 
coalesce(x.fiscal_yr,y.fiscal_yr+1) as yr
,coalesce(x.age102,y.age102) as age102
,coalesce(x.sex,y.sex) as sex
,coalesce(x.r7,y.r7) as r7
,round((coalesce(x.p,0) + coalesce(y.p,0))/2,1) as p
from dof_proj_1 as x
full join dof_proj_1 as y on x.r7=y.r7 and x.age102=y.age102 and x.sex=y.sex and x.fiscal_yr=y.fiscal_yr+1
order by yr,r7,age102,sex;

create table dof_proj_4a as select *,p/sum(p) as s
from dof_proj_4 group by yr
order by yr;

create table dof_proj_5 as select x.yr,y.age102,y.sex,y.r7,x.tp,round(x.tp * y.s,1) as p1,y.p as p0
from dof_proj_t as x
inner join dof_proj_4a as y on x.yr=y.yr
order by yr,p;
quit;

data dof_proj_6;set dof_proj_5; by yr; retain cp;
if first.yr then do;p2=p1;cp=p2;end;
else if last.yr then do;p2=tp - cp;cp=p2+cp;end;
else do;p2=min(p1,(tp - cp));cp=p2+cp;end;run;
run;

proc sql;
create table dof_proj_7 as select yr,age102,sex,r7,p2 as p_cest
from dof_proj_6 order by yr,age102,sex,r7;
quit;

proc sql;
create table dof_proj_7a as select x1.*,x2.*,x3.*,x4.*
from (select distinct yr from dof_proj_7) as x1
cross join (select distinct age102 from dof_proj_7) as x2
cross join (select distinct sex from dof_proj_7) as x3
cross join (select distinct r7 from dof_proj_7) as x4;

create table dof_proj_8 as select x.*,coalesce(y.p_cest,0) as p_cest
from dof_proj_7a as x
left join dof_proj_7 as y on
x.yr=y.yr and x.age102=y.age102 and x.sex=y.sex and x.r7=y.r7
order by yr,age102,sex,r7;
quit;



data e1.dof_pop_proj_r7_age102;set dof_proj_8;run;

/*
proc sql;
create table dof_A_01 as select yr
,case
when r7 in ("W","H","S","B") then r7 else "O" 
end as r5
,case
when age102<=4 then "00_04"
when age102<=9 then "05_09"
when age102<=14 then "10_14"
when age102<=19 then "15_19"
when age102<=24 then "20_24"
when age102<=29 then "25_29"
when age102<=34 then "30_34"
when age102<=39 then "35_39"
when age102<=44 then "40_44"
when age102<=49 then "45_49"
when age102<=54 then "50_54"
when age102<=59 then "55_59"
when age102<=64 then "60_64"
when age102<=69 then "65_69"
when age102<=74 then "70_74"
when age102<=79 then "75_79"
when age102<=84 then "80_84"
else "85_99" end as age18,sex
,sum(p_cest) as p_cest
from dof_proj_8
group by yr,r5,age18,sex;
quit;
*/

/* data id.dof_pop_est_r5_age18;set dof_A_01;run; */

/*
proc sql;
create table dof_B_01 as select yr,r7
,case
when age102<=4 then "00_04"
when age102<=9 then "05_09"
when age102<=14 then "10_14"
when age102<=19 then "15_19"
when age102<=24 then "20_24"
when age102<=29 then "25_29"
when age102<=34 then "30_34"
when age102<=39 then "35_39"
when age102<=44 then "40_44"
when age102<=49 then "45_49"
when age102<=54 then "50_54"
when age102<=59 then "55_59"
when age102<=64 then "60_64"
when age102<=69 then "65_69"
when age102<=74 then "70_74"
when age102<=79 then "75_79"
when age102<=84 then "80_84"
else "85_99" end as age18,sex
,sum(p_cest) as p_cest
from dof_proj_8
group by yr,r7,age18,sex;
quit;
*/

/*data id.dof_pop_est_r7_age18;set dof_B_01;run;*/












