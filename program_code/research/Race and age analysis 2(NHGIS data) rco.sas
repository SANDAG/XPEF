libname nhgis "T:\socioec\socioec_data_test\NHGIS_Extracts\Time series (1990-2000-2010)";

proc sql;
create table age22 as select distinct age22 from nhgis.tract_age;
create table race as select distinct race from nhgis.tract_race;
quit;

proc sql;
create table cnt_race_1 as
select input(datayear,4.) as yr
,case
when race in ("SOR","TOMR") then "O" else race end as r
,round(sum(p),1) as p
from nhgis.county_race where hisp="NH" group by yr,r
	union all
select input(datayear,4.) as yr
,"H" as r
,round(sum(p),1) as p
from nhgis.county_race where hisp="H" group by yr
order by r,yr;

create table ct_race_1 as
select tract as ct_2010,input(datayear,4.) as yr
,case
when race in ("SOR","TOMR") then "O" else race end as r
,round(sum(p),1) as p
from nhgis.tract_race where hisp="NH" group by ct_2010,yr,r
	union all
select tract as ct_2010,input(datayear,4.) as yr
,"H" as r
,round(sum(p),1) as p
from nhgis.tract_race where hisp="H" group by ct_2010,yr
order by ct_2010,r,yr;


/*
create table ct_age22_1 as
select tract as ct_2010,input(datayear,4.) as yr
,age22
,round(p,1) as p
from nhgis.tract_age order by ct_2010,yr,age22;
*/

create table cnt_age_1 as
select input(datayear,4.) as yr
,case
when age22 < 20 then 0.19
when age22 < 40 then 20.39
when age22 < 60 then 40.59
when age22 < 80 then 60.79
else 80.99 end as age5
,round(sum(p),1) as p
from nhgis.county_age group by yr,age5;

create table ct_age_1 as
select tract as ct_2010,input(datayear,4.) as yr
,case
when age22 < 20 then 0.19
when age22 < 40 then 20.39
when age22 < 60 then 40.59
when age22 < 80 then 60.79
else 80.99 end as age5
/*
,case
when age22 < 18 then 0.17
when age22 < 25 then 18.24
when age22 < 45 then 25.44
when age22 < 65 then 45.64
else 65.99 end as age5
*/
,round(sum(p),1) as p
from nhgis.tract_age group by ct_2010,yr,age5;
quit;

proc sql;
create table cnt_race_2 as select *,p/sum(p) as s format=percent8.0
from cnt_race_1 group by yr order by r,yr;

create table cnt_age_2 as select *,p/sum(p) as s format=percent8.0
from cnt_age_1 group by yr order by age5,yr;

create table ct_race_2 as select *,p/sum(p) as s format=percent8.0
from ct_race_1 group by ct_2010,yr
order by ct_2010,r,yr;

create table ct_age_2 as select *,p/sum(p) as s format=percent8.0
from ct_age_1 group by ct_2010,yr
order by ct_2010,age5,yr;
quit;

proc transpose data=ct_race_2 out=ct_race_3(drop=_name_);by ct_2010 r;var s;id yr;run;
proc transpose data=ct_age_2 out=ct_age_3(drop=_name_);by ct_2010 age5;var s;id yr;run;

proc transpose data=cnt_race_2 out=cnt_race_3(drop=_name_);by r;var s;id yr;run;
proc transpose data=cnt_age_2 out=cnt_age_3(drop=_name_);by age5;var s;id yr;run;


proc sql;
create table ct_race_4 as select x.ct_2010,x.r
,x._1990 as t_1990
,x._2000 as t_2000
,x._2010 as t_2010
,y._1990 as cnt_1990
,y._2000 as cnt_2000
,y._2010 as cnt_2010
from ct_race_3 as x
inner join cnt_race_3 as y on x.r=y.r
order by ct_2010,r;

create table ct_age_4 as select x.ct_2010,x.age5
,x._1990 as t_1990
,x._2000 as t_2000
,x._2010 as t_2010
,y._1990 as cnt_1990
,y._2000 as cnt_2000
,y._2010 as cnt_2010
from ct_age_3 as x
inner join cnt_age_3 as y on x.age5=y.age5
order by ct_2010,age5;
quit;

/*
ac_20: absolute change over 20 years
ac_10: absolute change over 10 years
r_1990: relative difference (with region)

rc_20: relative change over 20 years
rc_10: relative change over 10 year
*/

proc sql;
create table ct_race_5 as select *
,t_2010 - t_1990 as ac_20 format=percent8.0
,t_2010 - t_2000 as ac_10 format=percent8.0
,t_1990 - cnt_1990 as r_1990 format=percent8.0
,t_2000 - cnt_2000 as r_2000 format=percent8.0
,t_2010 - cnt_2010 as r_2010 format=percent8.0

,calculated r_2010 - calculated r_1990 as rc_20 format=percent8.0
,calculated r_2010 - calculated r_2000 as rc_10 format=percent8.0
from ct_race_4;

create table ct_age_5 as select *
,t_2010 - t_1990 as ac_20 format=percent8.0
,t_2010 - t_2000 as ac_10 format=percent8.0
,t_1990 - cnt_1990 as r_1990 format=percent8.0
,t_2000 - cnt_2000 as r_2000 format=percent8.0
,t_2010 - cnt_2010 as r_2010 format=percent8.0

,calculated r_2010 - calculated r_1990 as rc_20 format=percent8.0
,calculated r_2010 - calculated r_2000 as rc_10 format=percent8.0
from ct_age_4;
quit;


proc sql;
create table ct_race_6 as select ct_2010,r,ac_20,r_2010,rc_20
,round(ac_20*100,5) as ac_20r
from ct_race_5
order by ct_2010,r;
quit;

proc transpose data=ct_race_6 out=ct_race_7(drop=_name_);by ct_2010;var ac_20r;id r;run;

proc sql;
create table ct_race_7a as select W,H,B,API,AIAN,O,count(ct_2010) as n
from ct_race_7 where W^=. group by W,H,B,API,AIAN,O;
quit;


/* are there any age-stable tracks ? */

proc sql;
create table ct_age_6 as select ct_2010,age5,ac_20,r_2010,rc_20 from ct_age_5;
quit;

/*
if rc_20 is zero then 
*/
