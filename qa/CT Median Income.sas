libname xpef odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;
Trusted_Connection=yes" schema=xpef03;

proc sql;
create table test_1 as select distinct yr from xpef.household_income;
quit;


proc sql;
create table inc_1 as select x.yr,x.hh_id,x.inc_2010,y.ct
from xpef.household_income as x
inner join xpef.housing_units as y on x.yr=y.yr and x.hh_id=y.hh_id;

create table ct_regular_1 as select ct,yr,round(mean(inc_2010),1) as avg,round(median(inc_2010),1) as med,count(hh_id) as hh
from inc_1 group by ct,yr;

create table cnt_regular_1 as select yr,round(mean(inc_2010),1) as avg,round(median(inc_2010),1) as med,count(hh_id) as hh
from inc_1 group by yr;
quit;

proc transpose data=ct_regular_1 out=ct_regular_1a(drop=_name_);by ct;var med;id yr;run;


proc sql;
create table ct_upgraded_1 as select ct,yr,round(mean(inc_2010),1) as avg,round(median(inc_2010),1) as med,count(hh_id) as hh
from xpef.household_income_upgraded
group by ct,yr;

create table cnt_upgraded_1 as select yr,round(mean(inc_2010),1) as avg,round(median(inc_2010),1) as med,count(hh_id) as hh
from xpef.household_income_upgraded
group by yr;
quit;

proc transpose data=ct_upgraded_1 out=ct_upgraded_1a(drop=_name_);by ct;var med;id yr;run;

/*
proc means data=inc_2018_1 noprint;
class yr ct;
ways 1;
var inc_2010;
output out=inc_2018_1b median(inc_2010)=median_inc2010;
run;

proc means data=xpef.household_income_upgraded noprint;
class yr ct;
ways 1;
var inc_2010;
output out=upgraded_inc_1 median(inc_2010)=median_inc2010;
run;
*/

