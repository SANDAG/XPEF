options notes;

/* sd.hu_2022 */

proc sql;
create table a_test_01 as 
select 2022 as yr,count(hu_id) as hu,count(hh_id) as hh
from hu_base where cpa=1414 and sto_flag=0
	union all
select 2023 as yr,count(hu_id) as hu,count(hh_id) as hh
from hu_next_3 where cpa=1414 and sto_flag=0;
quit;

proc sql;
create table a_test_02 as 
select 2022 as yr,count(hu_id) as hu,count(hh_id) as hh
from hu_base where cpa=1414 and sto_flag=1
	union all
select 2023 as yr,count(hu_id) as hu,count(hh_id) as hh
from hu_next_3 where cpa=1414 and sto_flag=1;
quit;

proc sql;
create table a_test_03 as select x.*,y.hu_2,y.hh_2
from 
(select cpa,count(hu_id) as hu_1,count(hh_id) as hh_1
from hu_base where jur=14 and sto_flag=0 group by cpa) as x
left join 
(select cpa,count(hu_id) as hu_2,count(hh_id) as hh_2
from hu_next_3 where jur=14 and sto_flag=0 group by cpa) as y on x.cpa=y.cpa
order by cpa;
quit;



proc sql;
create table atest_04 /*hu_new_0*/ as
select x.jur,x.ct,x.cpa
,case when x.hu_id ^= . then 1 else 0 end as hu_id1
,case when y.hu_id ^= . then 1 else 0 end as hu_id2
,case when x.hh_id ^= . then 1 else 0 end as hh_id1
,case when y.hh_id ^= . then 1 else 0 end as hh_id2
,count(x.hu_id) as hu
from (select * from hu_next_2 where sto_flag=0) as x
left join hu_base as y on x.hu_id=y.hu_id
group by x.jur,x.ct,x.cpa,hu_id1,hu_id2,hh_id1,hh_id2;

create table atest_05 as select x.*
from atest_04 as x
inner join (select distinct ct from atest_04 where cpa=1414) as y 
on x.ct=y.ct
order by cpa,ct;
quit;



proc sql;
create table a_test_hu_new_1 as select x0.*
,coalesce(x1.hu_new,0) as hu_new
,coalesce(x2.hu_remain,0) as hu_remain
,coalesce(x3.hu_reocc,0) as hu_reocc
,coalesce(x4.hu_vac,0) as hu_vac
,coalesce(x1.hu_new,0) + coalesce(x2.hu_remain,0) + coalesce(x3.hu_reocc,0) + coalesce(x4.hu_vac,0) as hu_all
from (select distinct jur,ct,cpa from atest_04) as x0

left join (select jur,ct,cpa,hu as hu_new from atest_04 where hu_id2=0) as x1 /* new construction */
	on x0.jur=x1.jur and x0.ct=x1.ct and x0.cpa=x1.cpa

left join (select jur,ct,cpa,hu as hu_remain from atest_04 where hh_id1=1) as x2 /* hh remaining in place */
	on x0.jur=x2.jur and x0.ct=x2.ct and x0.cpa=x2.cpa

left join (select jur,ct,cpa,hu as hu_reocc from atest_04 where hh_id1=0 and hh_id2=1) as x3 /* hu needs to be reoccupied  */
	on x0.jur=x3.jur and x0.ct=x3.ct and x0.cpa=x3.cpa

left join (select jur,ct,cpa,hu as hu_vac from atest_04 where hh_id1=0 and hh_id2=0 and hu_id2=1) as x4 /* hu remains vacant */
	on x0.jur=x4.jur and x0.ct=x4.ct and x0.cpa=x4.cpa;

create table a_test_hu_new_1a as select jur
,sum(hu_all) as hu_all
,sum(hu_new) as hu_new
,sum(hu_remain) as hu_remain
,sum(hu_reocc) as hu_reocc
,sum(hu_vac) as hu_vac
from a_test_hu_new_1 group by jur;

create table a_test_hu_new_1a_ as select cpa
,sum(hu_all) as hu_all
,sum(hu_new) as hu_new
,sum(hu_remain) as hu_remain
,sum(hu_reocc) as hu_reocc
,sum(hu_vac) as hu_vac
from a_test_hu_new_1 group by cpa;

create table a_test_hu_new_1b as select x.*,y.hh as hh_dof
,y.hh - (x.hu_remain + x.hu_reocc) as hu_occ /* to be occupied by new hh */
,case
when calculated hu_occ < 0 then hu_reocc + calculated hu_occ else hu_reocc end as hu_reocc2 /* hus to be occupied */
,y.hh - (x.hu_remain + calculated hu_reocc2) as hu_occ2 /* new units to be occupied; essentially vacant available*/
from a_test_hu_new_1a as x
left join (select jur,hh_dof as hh from dof_3 where est_yr=&yrn) as y
on x.jur=y.jur;
quit;

proc sql;
create table atest_base_vr_jurct_1 as select x.*
from base_vr_jurct_1 as x
inner join (select distinct ct from atest_04 where cpa=1414) as y 
on x.ct=y.ct
order by ct;

create table atest_base_vr_ct_1 as select x.*
from base_vr_ct_1 as x
inner join (select distinct ct from atest_04 where cpa=1414) as y 
on x.ct=y.ct
order by ct;
quit;

proc sql;
create table atest_base_vr_jurct_2 as select x.*
from base_vr_jurct_2 as x
inner join (select distinct ct from atest_04 where cpa=1414) as y 
on x.ct=y.ct
order by ct;

create table atest_base_vr_jurct_3 as select x.*
from base_vr_jurct_3 as x
inner join (select distinct ct from atest_04 where cpa=1414) as y 
on x.ct=y.ct
order by ct;
quit;



proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table atest_101 as select *,hh/hu as or format=percent8.1
from connection to odbc
(
select yr,jur,ct,cpa,count(hh_id) as hh,count(hu_id) as hu FROM [isam].[xpef06].[housing_units] where sto_flag=0
group by yr,jur,ct,cpa
)
order by jur,ct,cpa,yr
;

disconnect from odbc;
quit;

proc sql;
create table atest_102 as select * from atest_101 where cpa=1414;

create table atest_102a as select distinct ct from atest_102;

create table atest_103 as select x.*
from atest_101 as x
inner join atest_102a as y on x.ct=y.ct
order by ct,jur,cpa,yr;

create table atest_104 as select x.*
from atest_101 as x
inner join atest_102a as y on x.ct=y.ct
order by ct,yr,jur,cpa;
quit;

proc transpose data=atest_104 out=atest_104a(drop=_name_);where ct="006500";by ct yr;var or;id cpa;run;
proc transpose data=atest_104 out=atest_104b(drop=_name_ _label_);where ct="006500";by ct yr;var hu;id cpa;run;

proc sql;
create table atest_104c as select x.ct,x.yr
,x._1414 as or_1414,x._1424 as or_1424,x._1442 as or_1442
,y._1414 as hu_1414,y._1424 as hu_1424,y._1442 as hu_1442
from atest_104a as x
inner join atest_104b as y on x.yr=y.yr
order by yr;

create table atest_105 as select yr,ct
,count(*) as jurcpa,min(or) as min_or format=percent8.1
,max(or) as max_or format=percent8.1,mean(or) as avg_or format=percent8.1
from atest_101 group by yr,ct
order by max_or - min_or desc;

create table atest_105a as select * from atest_105
where min_or>0
order by max_or - min_or desc;

create table atest_105b as select * from atest_105
where yr=2017 and min_or>0
order by max_or - min_or desc;
quit;





proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table atest_201 as select *,hh/hu as or format=percent8.1
from connection to odbc
(
select yr,jur,ct,cpa,count(hh_id) as hh,count(hu_id) as hu FROM [estimates].[est_2017_04].[housing_units] where sto_flag=0
group by yr,jur,ct,cpa
)
order by jur,ct,cpa,yr
;

disconnect from odbc;
quit;

proc sql;
create table atest_202 as select * from atest_201 where cpa=1414;

create table atest_202a as select distinct ct from atest_202;

create table atest_203 as select x.*
from atest_201 as x
inner join atest_202a as y on x.ct=y.ct
order by ct,jur,cpa,yr;

create table atest_204 as select x.*
from atest_201 as x
inner join atest_202a as y on x.ct=y.ct
order by ct,yr,jur,cpa;
quit;

proc transpose data=atest_204 out=atest_204a(drop=_name_);where ct="006500";by ct yr;var or;id cpa;run;
proc transpose data=atest_204 out=atest_204b(drop=_name_ _label_);where ct="006500";by ct yr;var hu;id cpa;run;
proc transpose data=atest_204 out=atest_204c(drop=_name_ _label_);where ct="006500";by ct yr;var hh;id cpa;run;

proc sql;
create table atest_204d as select x.ct,x.yr
,x._1414 as or_1414,x._1424 as or_1424,x._1442 as or_1442
,y._1414 as hu_1414,y._1424 as hu_1424,y._1442 as hu_1442
,z._1414 as hh_1414,z._1424 as hh_1424,z._1442 as hh_1442
from atest_204a as x
inner join atest_204b as y on x.yr=y.yr
inner join atest_204c as z on x.yr=z.yr
order by yr;

create table atest_205 as select yr,ct
,count(*) as jurcpa,min(or) as min_or format=percent8.1
,max(or) as max_or format=percent8.1,mean(or) as avg_or format=percent8.1
from atest_201 group by yr,ct
order by max_or - min_or desc;

create table atest_205a as select * from atest_205
where min_or>0
order by max_or - min_or desc;

create table atest_205b as select * from atest_205
where yr=2017 and min_or>0
order by max_or - min_or desc;
quit;

proc sql;
create table atest_101a as select * from atest_101 where cpa=1466;
create table atest_101b as select * from atest_101 where cpa=1467;
quit;


proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table atest_301 as select yr,jur,ct,cpa,mgra,count(hh_id) as hh
,mean(inc_2010) as avg_inc format=comma8.0,median(inc_2010) as med_inc format=comma8.0
from connection to odbc
(
select x.*,y.jur,y.ct,y.cpa,y.mgra
FROM [isam].[xpef06].[household_income_upgraded] as x
inner join [isam].[xpef06].[housing_units] as y on x.hh_id=y.hh_id and x.yr=y.yr
)
group by yr,jur,ct,cpa,mgra
;

disconnect from odbc;
quit;

proc sql;
create table atest_301a as select * from atest_301 where cpa in (1466)
order by ct,mgra,yr;
quit;


proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table atest_401 as select yr,cpa,count(hh_id) as hh
,mean(inc_2010) as avg_inc format=comma8.0,median(inc_2010) as med_inc format=comma8.0
from connection to odbc
(
select x.*,y.cpa
FROM [isam].[xpef06].[household_income_upgraded] as x
inner join [isam].[xpef06].[housing_units] as y on x.hh_id=y.hh_id and x.yr=y.yr
)
group by yr,cpa
;

disconnect from odbc;

quit;

proc sql;
create table atest_401a as select * from atest_401 where cpa in (1466,1467) order by cpa,yr;
quit;


proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table atest_501 as select yr,mgra,count(hh_id) as hh
,mean(inc_2010) as avg_inc format=comma8.0,median(inc_2010) as med_inc format=comma8.0
from connection to odbc
(
select x.*,y.mgra
FROM [isam].[xpef06].[household_income_upgraded] as x
inner join [isam].[xpef06].[housing_units] as y on x.hh_id=y.hh_id and x.yr=y.yr
)
group by yr,mgra
;

disconnect from odbc;

quit;

proc sql;
create table atest_501a as select * from atest_501 where mgra in (4419,4420,4422) order by mgra,yr;
quit;
