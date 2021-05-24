%let xver=xpef04;

libname sql_xpef odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;
Trusted_Connection=yes" schema=&xver;

/* mgra-jur-cpa jobs by sector */

proc sql;
create table inc_0 as select x.yr as yr_id,x.hh_id,x.inc_2010
,z.mgra,z.jurisdiction_id,z.cpa_id
from sql_xpef.household_income_upgraded as x
left join sql_xpef.housing_units as y on x.hh_id=y.hh_id and x.yr=y.yr
left join sql_xpef.mgra_id_new as z on y.mgra=z.mgra and y.cpa=z.cpa_id and y.jur=z.jurisdiction_id;
quit;

proc sql;
create table inc_mjc_1 as select mgra,jurisdiction_id,cpa_id,yr_id
,round(median(inc_2010),1) as med_inc format=comma8.
,round(mean(inc_2010),1) as avg_inc format=comma8.
,count(inc_2010) as n format=comma8.
from inc_0
group by mgra,jurisdiction_id,cpa_id,yr_id
order by mgra,jurisdiction_id,cpa_id,yr_id;

create table inc_jc_1 as select jurisdiction_id,cpa_id,yr_id
,round(median(inc_2010),1) as med_inc format=comma8.
,round(mean(inc_2010),1) as avg_inc format=comma8.
,count(inc_2010) as n format=comma8.
from inc_0
group by jurisdiction_id,cpa_id,yr_id
order by jurisdiction_id,cpa_id,yr_id;

create table inc_j_1 as select jurisdiction_id,yr_id
,round(median(inc_2010),1) as med_inc format=comma8.
,round(mean(inc_2010),1) as avg_inc format=comma8.
,count(inc_2010) as n format=comma8.
from inc_0
group by jurisdiction_id,yr_id
order by jurisdiction_id,yr_id;

create table inc_r_1 as select yr_id
,round(median(inc_2010),1) as med_inc format=comma8.
,round(mean(inc_2010),1) as avg_inc format=comma8.
,count(inc_2010) as n format=comma8.
from inc_0
group by yr_id
order by yr_id;
quit;
