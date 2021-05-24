/*libname e0 "T:\socioec\Current_Projects\estimates\input_data";*/

/*libname inc "T:\socioec\Income_Reconciliation_Model\Inputs";*/

/*libname sql_xpef odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;Trusted_Connection=yes" schema=xpef03;*/

proc sql;
create table cnt_inc_1 as select est as medinc from e1.Acs_medinc_cnt_1y
where yr=&acs_yr;
quit;


proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table cpi_2010 as select cpi_2010
from connection to odbc
(
select * FROM [socioec_data].[bls_su].[cpi_u_rs_1977_&acs_yr]
)
where yr=&acs_yr
;

create table income_group_id as select income_group as income_group_id,name,lower_bound,upper_bound
from connection to odbc
(select * FROM [demographic_warehouse].[dim].[income_group] where categorization = 10 and constant_dollars_year=2010);

disconnect from odbc;
quit;

proc sql;
create table cnt_inc_2 as select round(x.medinc/y.cpi_2010,1) as mi
from cnt_inc_1 as x
cross join cpi_2010 as y;
quit;

/*ata cnt_inc_2(rename=(medinc=mi));set cnt_inc_1;run;*/

data cnt_inc_3; set cnt_inc_2;
do yr=&by2 to &yy3;
	output;
end;
run;

/* real income is assumed to grow at 0.3% a year (per expert panel recommendations) */
data cnt_inc_4(drop=mi m);set cnt_inc_3; retain m;
if _n_ = 1 then do;medinc = mi + round(mi * 0.003,1); m = medinc;end;
else do; medinc = m + round(m * 0.003,1); m = medinc;end;
medinc1k=round(medinc,100);
run;

/* global macro list1 is defined and assigned in 1043 Income Imputation and Assignment.sas */

/*

%global list1;
%let list1=2019,2021,2026,2031,2036,2041,2046,2051;
%put &list1;

*/

proc sql;
create table cnt_inc_5 as select * from cnt_inc_4 where yr in (&list1);
quit;

/*
proc sql;
create table medinc_avginc as select x.medinc1k,round(x.avginc,1000) as avginc1k,y.yr
from e1.medinc1k_avginc as x
inner join (select distinct yr,medinc1k from cnt_inc_5) as y on x.medinc1k=y.medinc1k
order by medinc1k;

create table inc_dist_1 as select x.*,y.yr,y.medinc1k
from e1.avginc1k_inc10 as x
inner join medinc_avginc as y on x.avginc1k=y.avginc1k
order by medinc1k,inc10;
quit;
*/

proc sql;
create table inc_dist_1 as select x.yr,y.medinc1k,y.inc10,y.inc_cat,y.hh_share /*,y.medinc1k*/
from cnt_inc_5 as x
left join e1.medinc1k_inc10_sd as y on x.medinc1k=y.medinc1k
order by yr,inc10;
quit;

proc sql;
create table inc_dist_1a as select x.*,y.lower_bound as l,y.upper_bound as u
from inc_dist_1 as x
inner join income_group_id as y on x.inc10=y.income_group_id
order by yr,inc10;
quit;

data inc_dist_1b;set inc_dist_1a;by yr;retain s2;
if first.yr then do;s1=0;s2=hh_share;end;
else do;s1=s2;s2=s1+hh_share;end;
run;

proc sql;
create table inc_dist_1c as select yr /*,avginc1k*/,medinc1k,l,u,s1,s2
from inc_dist_1b where s1<=0.5 and s2>=0.5;
quit;



proc sql;
create table hh_base as select x.yr length=3,x.hh_id,x.income_group_id_2010-10 as inc10_0 length=3,x.inc_2010
,y.ct
from sql_xpef.household_income as x
inner join sql_xpef.housing_units as y on x.hh_id=y.hh_id and x.yr=y.yr
where x.yr in (&list1);
quit;


proc sql;
create table hh_0_sum_1 as select yr,inc10_0,count(hh_id) as hh
from hh_base group by yr,inc10_0;

create table hh_0_sum_2 as select yr,sum(hh) as hh from hh_0_sum_1 group by yr;
quit;

proc sql;
create table inc_dist_2 as select x.yr,x.medinc1k,x.inc10,x.inc_cat,y.hh as hht,round(x.hh_share * y.hh,1) as hh0,z.hh as hh_old
from inc_dist_1b as x
left join hh_0_sum_2 as y on x.yr=y.yr
left join hh_0_sum_1 as z on x.yr=z.yr and x.inc10=z.inc10_0
order by yr,hh0;
quit;

data inc_dist_3;set inc_dist_2;by yr;retain c;
if first.yr then do;hh1=hh0;c=hh1;end;
else if last.yr then do;hh1=hht-c;c=c+hh1;end;
else do;hh1=hh0;c=c+hh1;end;
run;

proc sql;
create table inc_target_1 as select yr,inc10,hh1 as hh_target
from inc_dist_3 order by yr,inc10;

create table inc_target_1a as select x.*,y.hh,x.hh_target - y.hh as d
from inc_target_1 as x
left join hh_0_sum_1 as y on x.yr=y.yr and x.inc10=y.inc10_0
order by yr,inc10;
quit;



/* start cycle */

%macro inc10;

proc sql;
create table hh_0 as select yr,hh_id,inc10_0,inc_2010,ct from hh_base;
quit;


%do j=1 %to 9;
%let jj=%eval(&j-1);
%let jjj=%eval(&jj-1);

proc sql noprint;
create table inc_target_2 as select x.yr,x.inc10,x.hh_target,y.hh
,x.hh_target - y.hh as d /* add if positive, reduce if negative */
from inc_target_1 as x
inner join (select yr,inc10_&jj,count(hh_id) as hh from hh_0 group by yr,inc10_&jj) as y on x.yr=y.yr and x.inc10=y.inc10_&jj
order by yr,inc10;

create table d as select yr,inc10,d from inc_target_2 where inc10=&j;

quit;

%if &j=1 %then %do;
proc sql;
create table hh_1 as select * from hh_0 order by yr,inc10_&jj,ranuni(&j+100);
quit;
%end;

%else %do;
proc sql;
create table hh_1 as select * from hh_0 order by yr,inc10_&jj,inc10_&jjj desc,ranuni(&j+100);
quit;
%end;

data hh_2;set hh_1;length i 5;retain i;by yr inc10_&jj;
if first.inc10_&jj then i=1;else i=i+1;
run;

%put j= &j /*d = &d*/;

proc sql;
create table hh_3(drop=i) as select x.*
,case
when y.d<0 and x.inc10_&jj = &j and i <= (y.d * -1) then x.inc10_&jj + 1
when y.d>0 and inc10_&jj = &j+1 and i <= (y.d * 1) then x.inc10_&jj - 1
else inc10_&jj
end as inc10_&j length=3
from hh_2 as x
left join d as y on x.yr=y.yr;

create table hh_0 as select * from hh_3;
quit;

%end;

%mend inc10;

%inc10;



proc sql;
create table test_1 as select yr,inc10_0,inc10_1,inc10_2,inc10_3,inc10_4,inc10_5,inc10_6,inc10_7,inc10_8,inc10_9,count(hh_id) as n
from hh_0 group by yr,inc10_0,inc10_1,inc10_2,inc10_3,inc10_4,inc10_5,inc10_6,inc10_7,inc10_8,inc10_9;

create table test_2 as select * from hh_0 where inc10_9=.;
quit;

proc sql;
create table test_3 as select distinct inc10_0 from hh_0;
create table test_4 as select distinct inc10_1 from hh_0;
quit;

proc sql;
create table test_5 as select x.*,y.hh_target
from (select yr,inc10_9,count(hh_id) as n from hh_0 group by yr,inc10_9) as x
left join inc_target_1 as y on x.yr=y.yr and x.inc10_9=y.inc10;

create table test_6 as select * from test_5 where n <> hh_target;
quit;


proc sql;
create table hh_4 as select x.yr,x.hh_id,x.ct,x.inc10_9 as inc10_new,y.inc10_0 as inc10_old
from hh_0 as x
inner join hh_base as y on x.yr=y.yr and x.hh_id=y.hh_id;
quit;




proc sql;
create table hh_4_1 as select x.*,y.lower_bound as l,y.upper_bound as u
from (select yr,inc10_new,count(hh_id) as hh1 from hh_4 group by yr,inc10_new) x
left join income_group_id as y on x.inc10_new = y.income_group_id
order by yr,inc10_new;

create table hh_4_1a as select sum(hh1) as hh1
from hh_4_1;
quit;


data hh_4_2 (drop=l u);set hh_4_1;where hh1>0;
/*
l=int(inc16) * 1000;
u=(inc16 - int(inc16)) * 1000000;
*/
do i=1 to hh1;
	inc=round((u-l) / (hh1+1) * i + l,1);
	output;
end;
run;

proc sql;
create table hh_5 as select *
from hh_4 order by yr,inc10_new,ranuni(2060);
quit;

data hh_5;set hh_5;by yr inc10_new;retain i;
if first.inc10_new then i=1;else i=i+1;
run;

proc sql;
create table hh_6 as select x.yr,x.hh_id,x.ct,x.inc10_new as income_group_id_2010,y.inc as inc_2010
from hh_5 as x
left join hh_4_2 as y on x.yr=y.yr and x.inc10_new=y.inc10_new and x.i=y.i;

create table hh_6a as select * from hh_6 where inc_2010=.;
quit;

proc sql;
create table summary_test_1 as select ct,yr,round(mean(inc_2010),1) as avg,round(median(inc_2010),1) as med,count(hh_id) as hh
from hh_6 group by ct,yr;

create table summary_test_2 as select yr,round(mean(inc_2010),1) as avg,round(median(inc_2010),1) as med,count(hh_id) as hh
from hh_6 group by yr;
quit;


proc sql;
create table hh_7 as select x.yr,x.ct,x.hh_id,x.inc10_0 as inc10_1,y.income_group_id_2010 as inc10_2
,x.inc_2010 as inc_2010_1,y.inc_2010 as inc_2010_2
,case
when y.income_group_id_2010 > x.inc10_0 then y.inc_2010
else x.inc_2010 end as inc_2010_3
/* income value is changed when a household is moved to an upper income category */
from hh_base as x
inner join hh_6 as y on x.yr=y.yr and x.hh_id=y.hh_id
order by yr,ct,hh_id;
quit; 

proc sql;
create table hh_7_test_01 as select yr,
round(mean(inc_2010_1),1) as avg_1
,round(mean(inc_2010_2),1) as avg_2
,round(mean(inc_2010_3),1) as avg_3
,round(median(inc_2010_1),1) as med_1
,round(median(inc_2010_2),1) as med_2
,round(median(inc_2010_3),1) as med_3
from hh_7 group by yr;

create table hh_7_test_02 as select ct,yr,
round(mean(inc_2010_1),1) as avg_1
,round(mean(inc_2010_2),1) as avg_2
,round(mean(inc_2010_3),1) as avg_3
,round(median(inc_2010_1),1) as med_1
,round(median(inc_2010_2),1) as med_2
,round(median(inc_2010_3),1) as med_3
from hh_7 group by ct,yr
order by ct,yr;
quit;

proc transpose data=hh_7_test_02 out=hh_7_test_03(drop=_name_);by ct;var med_2;id yr;run;

/*
proc sql;
create table hh_7_test_04 as select *,_2050 / _2020 as g format=percent8.1
from hh_7_test_03;

create table hh_7_test_05 as select round(g*100,1) as g1,count(ct) as n
from hh_7_test_04 group by g1;
quit;
*/


proc sql;
create table hh_8 as select yr length=3 format=4.,hh_id,ct,inc10_2 + 10 as income_group_id_2010 length=3 format=2.,inc_2010_2 as inc_2010
from hh_7;
quit;


proc sql;
create table hh_baseyear_inc as select x.yr length=3 format=4.,x.hh_id,y.ct,x.income_group_id_2010 length=3 format=2.,x.inc_2010
from sql_xpef.household_income as x
inner join sql_xpef.housing_units as y on x.hh_id=y.hh_id and x.yr=y.yr
where x.yr in (&by1);
quit;

data hh_9;set hh_8 hh_baseyear_inc;run;

proc sql;
create table hh_9a as select ct,yr,count(*) as n
from hh_9 where inc_2010 = .
group by ct,yr;
quit;


options notes;

proc sql;
CONNECT TO ODBC(noprompt="driver=SQL Server; server=sql2014a8; database=isam; DBCOMMIT=10000; Trusted_Connection=yes;") ;

EXECUTE ( drop table if exists isam.&xver..household_income_upgraded; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE
(
CREATE TABLE isam.&xver..household_income_upgraded(
yr smallint
,hh_id int
,ct varchar(6)
,income_group_id_2010 tinyint
,inc_2010 int
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;

DISCONNECT FROM ODBC ;
quit;


proc sql;
/*drop table sql_xpef.household_income_upgraded;*/

insert into sql_xpef.household_income_upgraded(bulkload=yes bl_options=TABLOCK) select * from hh_9;
quit;

options nonotes;


/*THIS IS THE STEP THAT CHANGED TO INCLUDE THE SELF EMPLOYED JOBS FOR 2016*/
proc sql;
delete from sql_xpef.household_income_upgraded where yr in(2016,2017);

create table inc_2016 as select x.yr, x.hh_id, y.ct, x.income_group_id_2010, x.inc_2010
from sql_est.household_income as x
inner join sql_est.housing_units as y on x.hh_id=y.hh_id and x.yr=y.yr
where x.yr = 2016;

create table inc_2017 as select x.yr, x.hh_id, y.ct, x.income_group_id_2010, x.inc_2010
from sql_est.household_income as x
inner join sql_est.housing_units as y on x.hh_id=y.hh_id and x.yr=y.yr
where x.yr = 2017;

insert into sql_xpef.household_income_upgraded(bulkload=yes bl_options=TABLOCK) select * from inc_2016;
insert into sql_xpef.household_income_upgraded(bulkload=yes bl_options=TABLOCK) select * from inc_2017;
quit;

proc sql; 
create table test_inc as 
select yr, count(hh_id) as households 
from sql_xpef.household_income_upgraded
group by yr
order by yr; 
quit;
