
proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;database=census") ;

create table B11001_cn1_0 as select line_number,line_desc,estimate,moe
from connection to odbc
(select * from acs.vw_summary_file
where yr=&acs_yr and release_type='1Y' and summary_level='050' and st='06' and county='073' and subject_table = 'B11001');

create table B11001_cn5_0 as select line_number,line_desc,estimate,moe
from connection to odbc
(select * from acs.vw_summary_file
where yr=&acs_yr and release_type='5Y' and summary_level='050' and st='06' and county='073' and subject_table = 'B11001');

create table B11001_ct5_0 as select line_number,line_desc,estimate,moe,tract as ct
from connection to odbc
(select * from acs.vw_summary_file
where yr=&acs_yr and release_type='5Y' and summary_level='140' and county='073' and subject_table =  'B11001');

disconnect from odbc;
quit;



proc sql;
create table B11001_cn1_1 as select
case 
when line_number=3 then "F1"
when line_number=5 then "F2"
when line_number=6 then "F3"
when line_number=8 then "N1"
when line_number=9 then "N2" end as h_type
,estimate as est
,moe
from B11001_cn1_0 where line_number in (3,5,6,8,9)
order by h_type;

create table B11001_cn5_1 as select
case 
when line_number=3 then "F1"
when line_number=5 then "F2"
when line_number=6 then "F3"
when line_number=8 then "N1"
when line_number=9 then "N2" end as h_type
,estimate as est
,moe
from B11001_cn5_0 where line_number in (3,5,6,8,9)
order by h_type;

create table B11001_ct5_1 as select ct
,case 
when line_number=3 then "F1"
when line_number=5 then "F2"
when line_number=6 then "F3"
when line_number=8 then "N1"
when line_number=9 then "N2" end as h_type
,estimate as est
,moe
from B11001_ct5_0 where line_number in (3,5,6,8,9)
order by ct,h_type;

create table B11001_cn1_1a as select estimate as est,moe
from B11001_cn1_0 where line_number in (1);

create table B11001_cn5_1a as select estimate as est,moe
from B11001_cn5_0 where line_number in (1);

create table B11001_ct5_1a as select ct,estimate as est,moe
from B11001_ct5_0 where line_number in (1) order by ct;
quit;

proc sql;
create table B11001_cn1_2 as select *,coalesce(est/sum(est),0) as s1_acs format=percent8.1
from B11001_cn1_1 order by h_type;

create table B11001_cn5_2 as select *,coalesce(est/sum(est),0) as s1_acs format=percent8.1
from B11001_cn5_1 order by h_type;

create table B11001_ct5_2 as select *,coalesce(est/sum(est),0) as s1_acs format=percent8.1
from B11001_ct5_1 group by ct order by ct,h_type;
quit;

proc sql;
create table B11001_cn1_2a as select x.*
,max(0,(x.est - x.moe)) / (y.est + y.moe) as s2_acs format=percent8.1
,min(1, (x.est + x.moe) / (y.est - y.moe)) as s3_acs format=percent8.1
from B11001_cn1_2 as x
cross join B11001_cn1_1a as y;

create table B11001_cn5_2a as select x.*
,max(0,(x.est - x.moe)) / (y.est + y.moe) as s2_acs format=percent8.1
,min(1, (x.est + x.moe) / (y.est - y.moe)) as s3_acs format=percent8.1
from B11001_cn5_2 as x
cross join B11001_cn5_1a as y;

create table B11001_ct5_2a as select x.*
,max(0,(x.est - x.moe)) / (y.est + y.moe) as s2_acs format=percent8.1
,case
when (y.est - y.moe) > 0 then min(1, (x.est + x.moe) / (y.est - y.moe))
else 0 end as s3_acs format=percent8.1
from B11001_ct5_2 as x
inner join B11001_ct5_1a as y on x.ct=y.ct;
quit;

proc sql;
create table B11001_ct5_2b as select * from B11001_ct5_2a where ct="005300";
quit;


/*
h_type: 
F1 married couple family
F2 family household with male householder, no wife
F3 family household with female householder, no hisband
N1 nonfamily household, householder living alone
N2 nonfamily household, householder not living alone
*/

proc sql;
create table p_h_1 as select x.yr,x.serialno,x.hh_size,x.h_type
,x.hh_age
,case
when x.hh_age<=19 then 1519
when x.hh_age<=24 then 2024
when x.hh_age<=29 then 2529
when x.hh_age<=34 then 3034
when x.hh_age<=39 then 3539
when x.hh_age<=44 then 4044
when x.hh_age<=49 then 4549
when x.hh_age<=54 then 5054
when x.hh_age<=59 then 5559
when x.hh_age<=64 then 6064
when x.hh_age<=69 then 6569
else 7099 end as hh_age15
,case
when x.hh_age<=18 then  018
when x.hh_age<=39 then 1939
when x.hh_age<=59 then 4059
when x.hh_age>=60 then 6099 end as hh_age5
,case
when x.hh_age<=18 then 18
when x.hh_age<=24 then 1924
when x.hh_age<=34 then 2534
when x.hh_age<=44 then 3544
when x.hh_age<=54 then 4554
when x.hh_age<=64 then 5564
when x.hh_age<=74 then 6574
else 7599 end as hh_age8

,case when y.sex_id=1 then "M" else "F" end as hh_sex
from (select * from shp.sd_pums_h_2006_&acs_yr._0 where hh_age>=18 and hh_size>1) as x
left join (select * from shp.sd_pums_p_2006_&acs_yr._0 where sporder=1) as y
on x.yr=y.yr and x.serialno=y.serialno;

create table p_m_1 as select x.yr,x.serialno,x.role
,x.age as hm_age
,case
when x.age<=4 then 004
when x.age<=9 then 509
when x.age<=14 then 1014
when x.age<=19 then 1519
when x.age<=24 then 2024
when x.age<=29 then 2529
when x.age<=34 then 3034
when x.age<=39 then 3539
when x.age<=44 then 4044
when x.age<=49 then 4549
when x.age<=54 then 5054
when x.age<=59 then 5559
when x.age<=64 then 6064
when x.age<=69 then 6569
else 7099 end as hm_age15
,case
when x.age<=9 then  009
when x.age<=18 then 1018
when x.age<=39 then 1939
when x.age<=59 then 4059
when x.age>=60 then 6099 end as hm_age5
,case
when x.age<=18 then 18
when x.age<=24 then 1924
when x.age<=34 then 2534
when x.age<=44 then 3544
when x.age<=54 then 4554
when x.age<=64 then 5564
when x.age<=74 then 6574
else 7599 end as hm_age8
,case when x.sex_id=1 then "M" else "F" end as hm_sex
from (select * from shp.sd_pums_p_2006_&acs_yr._0 where sporder>1 )as x
left join shp.sd_pums_h_2006_&acs_yr._0 as y
on x.yr=y.yr and x.serialno=y.serialno;
quit;

proc sql;
create table p_hm_1 as select x.*,y.*
from p_h_1 as x inner join p_m_1 as y
on x.yr=y.yr and x.serialno=y.serialno;
quit;

proc sql;
create table mcf_0 as select * from p_hm_1 where h_type="F1" and role="S" and hh_sex^=hm_sex;
create table mcf_0a as select yr,serialno,count(*) as n
from mcf_0 group by yr,serialno
having calculated n>1;
quit;

proc sql;
create table mcf_1 as select hh_sex,hh_age8,hm_age8,hm_sex,count(*) as n
from mcf_0 group by hh_sex,hh_age8,hm_age8,hm_sex;

create table mcf_2 as select *,n/sum(n) as s format=percent8.2
from mcf_1 group by hh_sex,hh_age8;
quit;

proc transpose data=mcf_2 out=mcf_2a(drop=_name_);by hh_sex hh_age8;var s;id hm_age8;run;

proc sql;
create table mcf_3 as select hh_sex,hh_age8,hm_age8,hm_sex,n,n/sum(n) as s format=percent8.2
from mcf_2 where s>=0.01 and hh_age8>18
group by hh_sex,hh_age8;
quit;

/*
h_type: 
F1 married couple family
F2 family household with male householder, no wife
F3 family household with female householder, no hisband
N1 nonfamily household, householder living alone
N2 nonfamily household, householder not living alone
*/

proc sql;
create table config_0 as select yr,serialno,hh_age8,hh_sex,hh_size,hm_sex,hm_age8,h_type
,strip(hm_sex||put(hm_age8,z4.)) as l length=5
from p_hm_1;

create table config_1 as select yr,serialno,hh_age8,hh_sex,hh_size,h_type,l,hm_sex,hm_age8,count(*) as n
from config_0
group by yr,serialno,hh_age8,hh_sex,hh_size,h_type,l,hm_sex,hm_age8
order by yr,serialno,hh_age8,hh_sex,hh_size,h_type;

create table config_1a as select x.*,coalesce(y.s,0) as mcf_prob
from config_1 as x
left join mcf_3 as y on x.hh_sex=y.hh_sex and x.hh_age8=y.hh_age8 and x.hm_sex=y.hm_sex and x.hm_age8=y.hm_age8;

create table config_1b as select yr,serialno,sum(mcf_prob) as m
from config_1a group by yr,serialno;

create table config_1c as select x.*
from config_1a as x
inner join config_1b as y on x.yr=y.yr and x.serialno=y.serialno and y.m=0 and x.h_type="F1"
order by yr,serialno;
quit;

/* exclude hh that cannot be mcf yet are coded as mcf */
proc sql;
create table config_1d as select x.*
from config_1 as x
left join (select distinct yr,serialno from config_1c) as y on x.yr=y.yr and x.serialno=y.serialno
where y.serialno="";
quit;

proc sql;
create table config_1e as select x.*
,case
when y.m=0 then 0 
when y.m>0 then 1 end as can_be_mcf
from config_1d as x
left join config_1b as y on x.yr=y.yr and x.serialno=y.serialno
order by yr,serialno,hh_age8,hh_sex,hh_size,can_be_mcf,h_type;
quit;

proc sql;
create table test_03 as select * from config_1e where can_be_mcf=.;
quit;

proc transpose data=config_1e out=config_2(drop=_name_);by yr serialno hh_age8 hh_sex hh_size can_be_mcf h_type;var n;id l;run;

proc transpose data=config_2 out=config_2a;by yr serialno hh_age8 hh_sex hh_size can_be_mcf h_type;run;

data config_2a;set config_2a;
if col1=. then col1=0;
run;

proc sort data=config_2a;by yr serialno hh_age8 hh_sex hh_size can_be_mcf h_type _name_;run;

proc transpose data=config_2a out=config_2b(drop=_name_);by yr serialno hh_age8 hh_sex hh_size can_be_mcf h_type;var col1;id _name_;run;

proc sql;
create table config_A_3 as select hh_age8,hh_sex,hh_size,can_be_mcf
,F0018,F1924,F2534,F3544,F4554,F5564,F6574,F7599
,M0018,M1924,M2534,M3544,M4554,M5564,M6574,M7599
,h_type
,count(*) as hh
from config_2b
group by hh_age8,hh_sex,hh_size,can_be_mcf
,F0018,F1924,F2534,F3544,F4554,F5564,F6574,F7599
,M0018,M1924,M2534,M3544,M4554,M5564,M6574,M7599
,h_type;

create table config_B_2 as select hh_age8,hh_sex,can_be_mcf
,case when F0018>=1 then 1 else 0 end as F0018
,case when F1924>=1 then 1 else 0 end as F1924
,case when F2534>=1 then 1 else 0 end as F2534
,case when F3544>=1 then 1 else 0 end as F3544
,case when F4554>=1 then 1 else 0 end as F4554
,case when F5564>=1 then 1 else 0 end as F5564
,case when F6574>=1 then 1 else 0 end as F6574
,case when F7599>=1 then 1 else 0 end as F7599
,case when M0018>=1 then 1 else 0 end as M0018
,case when M1924>=1 then 1 else 0 end as M1924
,case when M2534>=1 then 1 else 0 end as M2534
,case when M3544>=1 then 1 else 0 end as M3544
,case when M4554>=1 then 1 else 0 end as M4554
,case when M5564>=1 then 1 else 0 end as M5564
,case when M6574>=1 then 1 else 0 end as M6574
,case when M7599>=1 then 1 else 0 end as M7599
,h_type,hh
from config_A_3; 

create table config_B_3 as select hh_age8,hh_sex,can_be_mcf
,F0018,F1924,F2534,F3544,F4554,F5564,F6574,F7599
,M0018,M1924,M2534,M3544,M4554,M5564,M6574,M7599
,h_type
,sum(hh) as hh
from config_B_2
group by hh_age8,hh_sex,can_be_mcf
,F0018,F1924,F2534,F3544,F4554,F5564,F6574,F7599
,M0018,M1924,M2534,M3544,M4554,M5564,M6574,M7599
,h_type;

create table config_A_3a as select hh_age8,hh_sex,hh_size,can_be_mcf
,F0018,F1924,F2534,F3544,F4554,F5564,F6574,F7599
,M0018,M1924,M2534,M3544,M4554,M5564,M6574,M7599
,sum(hh) as hh_a
from config_A_3
group by hh_age8,hh_sex,hh_size,can_be_mcf
,F0018,F1924,F2534,F3544,F4554,F5564,F6574,F7599
,M0018,M1924,M2534,M3544,M4554,M5564,M6574,M7599;

create table config_B_3a as select hh_age8,hh_sex,can_be_mcf
,F0018,F1924,F2534,F3544,F4554,F5564,F6574,F7599
,M0018,M1924,M2534,M3544,M4554,M5564,M6574,M7599
,sum(hh) as hh_b
from config_B_3
group by hh_age8,hh_sex,can_be_mcf
,F0018,F1924,F2534,F3544,F4554,F5564,F6574,F7599
,M0018,M1924,M2534,M3544,M4554,M5564,M6574,M7599;
quit;

/* horizontal config */
data config_A_3a;set config_A_3a;config_id_a=_n_;run;
data config_B_3a;set config_B_3a;config_id_b=_n_;run;

proc transpose data=config_A_3a(drop=hh_a) out=config_A_3b;by config_id_a hh_age8 hh_sex hh_size can_be_mcf;run;
proc transpose data=config_B_3a(drop=hh_b) out=config_B_3b;by config_id_b hh_age8 hh_sex can_be_mcf;run;

proc sql;
create table config_A_4 as select x.config_id_a,y.h_type,y.hh
from config_A_3a as x
inner join config_A_3 as y on
x.hh_size=y.hh_size and x.hh_age8=y.hh_age8 and x.hh_sex=y.hh_sex and x.can_be_mcf=y.can_be_mcf
and x.F0018 = y.F0018
and x.F1924 = y.F1924
and x.F2534 = y.F2534
and x.F3544 = y.F3544
and x.F4554 = y.F4554
and x.F5564 = y.F5564
and x.F6574 = y.F6574
and x.F7599 = y.F7599
and x.M0018 = y.M0018
and x.M1924 = y.M1924
and x.M2534 = y.M2534
and x.M3544 = y.M3544
and x.M4554 = y.M4554
and x.M5564 = y.M5564
and x.M6574 = y.M6574
and x.M7599 = y.M7599
order by config_id_a;

create table config_B_4 as select x.config_id_b,y.h_type,y.hh
from config_B_3a as x
inner join config_B_3 as y on
x.hh_age8=y.hh_age8 and x.hh_sex=y.hh_sex and x.can_be_mcf=y.can_be_mcf
and x.F0018 = y.F0018
and x.F1924 = y.F1924
and x.F2534 = y.F2534
and x.F3544 = y.F3544
and x.F4554 = y.F4554
and x.F5564 = y.F5564
and x.F6574 = y.F6574
and x.F7599 = y.F7599
and x.M0018 = y.M0018
and x.M1924 = y.M1924
and x.M2534 = y.M2534
and x.M3544 = y.M3544
and x.M4554 = y.M4554
and x.M5564 = y.M5564
and x.M6574 = y.M6574
and x.M7599 = y.M7599
order by config_id_b;
quit;

proc sql;
/* composition of configuration A (age, sex, and count of members) */
create table config_A_id_1 as select config_id_a,hh_age8,hh_sex,hh_size,can_be_mcf,strip(_name_) as age8,col1 as persons
,strip(_name_)||"_"||strip(put(col1,2.0)) as age8_count
from config_A_3b where col1>0 order by config_id_a;

/* composition of configuration B (age, sex) */
create table config_B_id_1 as select config_id_b,hh_age8,hh_sex,can_be_mcf,strip(_name_) as age8
from config_B_3b where col1>0 order by config_id_b;
quit;

data config_A_id_2;set config_A_id_1;by config_id_a;
length age8_string age8_count_string $100;
retain age8_string age8_count_string;

if first.config_id_a then age8_string=strip(age8);
else age8_string=strip(age8_string)||" "||strip(age8);

if first.config_id_a then age8_count_string=strip(age8_count);
else age8_count_string=strip(age8_count_string)||" "||strip(age8_count);
run;

data config_B_id_2;set config_B_id_1;by config_id_b;
length age8_string $100;
retain age8_string;

if first.config_id_b then age8_string=strip(age8);
else age8_string=strip(age8_string)||" "||strip(age8);
run;


data config_A_id_3(drop=age8 age8_count persons);set config_A_id_2;by config_id_a;
if last.config_id_a;
run;

data config_B_id_3(drop=age8);set config_B_id_2;by config_id_b;
if last.config_id_b;
run;


/* probability that a household configuration will resolve to a specific household type */

proc sql;
/* household configuration is based on bin (age8/sex) and count of members in each bin */
create table config_A_rates_1 as select x.config_id_a,y.hh_age8,y.hh_sex,y.hh_size,y.can_be_mcf
,x.h_type,x.hh,x.prob
,y.age8_string,y.age8_count_string
from (select config_id_a,h_type,hh,hh/sum(hh) as prob from config_a_4 group by config_id_a) as x
inner join config_A_id_3 as y on x.config_id_a=y.config_id_a
order by config_id_a,ranuni(&by1 - 1);

/* household configuration is based on bin (age8/sex) and presense of members in each bin */
create table config_B_rates_1 as select x.config_id_b,y.hh_age8,y.hh_sex,y.can_be_mcf
,x.h_type,x.hh,x.prob
,y.age8_string
from (select config_id_b,h_type,hh,hh/sum(hh) as prob from config_B_4 group by config_id_b) as x
inner join config_B_id_3 as y on x.config_id_b=y.config_id_b
order by config_id_b,ranuni(&by1);
quit;

data config_A_rates_2;set config_A_rates_1;by config_id_a;retain p1 p2;
if first.config_id_a then do;p1 = 0;p2 = prob;end;
else do;p1 = p2;p2 = p1 + prob;end;
run;

data config_B_rates_2;set config_B_rates_1;by config_id_b;retain p1 p2;
if first.config_id_b then do;p1 = 0;p2 = prob;end;
else do;p1 = p2;p2 = p1 + prob;end;
run;


proc sql;
create table type_rates_0 as select distinct yr,serialno,hh_age8,hh_sex,hh_size,can_be_mcf,h_type
from config_1e;

create table type3_rates_1 as select *,n/sum(n) as prob,sum(n) as hh_c
from (select hh_age8,hh_sex,hh_size,can_be_mcf,h_type,count(*) as n from type_rates_0 group by hh_age8,hh_sex,hh_size,can_be_mcf,h_type)
group by hh_age8,hh_sex,hh_size,can_be_mcf
order by hh_age8,hh_sex,hh_size,can_be_mcf,ranuni(&by1 + 1);

create table type2_rates_1 as select *,n/sum(n) as prob,sum(n) as hh_d
from (select hh_age8,hh_sex,can_be_mcf,h_type,count(*) as n from type_rates_0 group by hh_age8,hh_sex,can_be_mcf,h_type)
group by hh_age8,hh_sex,can_be_mcf
order by hh_age8,hh_sex,can_be_mcf,ranuni(&by1 + 2);
quit;

data type3_rates_2;set type3_rates_1;by hh_age8 hh_sex hh_size can_be_mcf;retain p1 p2;
if first.can_be_mcf then do;p1 = 0;p2 = prob;end;
else do;p1 = p2;p2 = p1 + prob;end;
run;

data type2_rates_2;set type2_rates_1;by hh_age8 hh_sex can_be_mcf;retain p1 p2;
if first.can_be_mcf then do;p1 = 0;p2 = prob;end;
else do;p1 = p2;p2 = p1 + prob;end;
run;

/*
h_type: 
F1 married couple family
F2 family household with male householder, no wife
F3 family household with female householder, no hisband
N1 nonfamily household, householder living alone
N2 nonfamily household, householder not living alone
*/

proc sql;
create table test_A_02 as select * from config_A_rates_1
where (hh_sex="F" and h_type="F2") or (hh_sex="M" and h_type="F3");

create table test_B_02 as select * from config_B_rates_1
where (hh_sex="F" and h_type="F2") or (hh_sex="M" and h_type="F3");
quit;


proc sql;
create table est_h_1 as select x.yr length=3,y.ct,x.hh_id,x.sex as hh_sex
,y.size as hh_size length=3
,case
when x.age<=18 then 18
when x.age<=24 then 1924
when x.age<=34 then 2534
when x.age<=44 then 3544
when x.age<=54 then 4554
when x.age<=64 then 5564
when x.age<=74 then 6574
else 7599 end as hh_age8 length=3
from sql_xpef.household_population as x
inner join sql_xpef.households as y
on x.yr=y.yr and x.hh_id=y.hh_id
inner join tp_0_yr as z on x.yr=z.yr
where x.role="H";

create table est_m_1 as select x.yr length=3,x.hh_id,x.sex as hm_sex
,case
when x.age<=18 then 18
when x.age<=24 then 1924
when x.age<=34 then 2534
when x.age<=44 then 3544
when x.age<=54 then 4554
when x.age<=64 then 5564
when x.age<=74 then 6574
else 7599 end as hm_age8 length=3
from sql_xpef.household_population as x
inner join tp_0_yr as z on x.yr=z.yr
where x.role="M";

/* single person households are not included */
create table est_hm_1 as select x.*,y.*
from est_h_1 as x
inner join est_m_1 as y
on x.yr=y.yr and x.hh_id=y.hh_id;

create table est_hh_single as select *,"N1" as h_type length=2
,case when hh_sex="M" then 4 else 6 end as hht length=3
from est_h_1 where hh_size=1;
quit;

proc sql;
create table est_config_0 as select yr,hh_id,hh_age8,hh_sex,hh_size,hm_age8,hm_sex
,strip(hm_sex||transtrn(put(hm_age8,z4.),".","")) as l length=5
from est_hm_1
order by yr,hh_id;

create table est_config_1 as select yr,hh_id,hh_age8,hh_sex,hh_size,hm_age8,hm_sex,l
,count(*) as n length=3
from est_config_0
group by yr,hh_id,hh_age8,hh_sex,hh_size,hm_age8,hm_sex,l;

create table est_config_mcf_0 as select x.*,coalesce(y.s,0) as mcf_prob
from (select distinct yr,hh_id,hh_age8,hh_sex,hm_age8,hm_sex from est_config_1) as x
left join mcf_3 as y on x.hh_sex=y.hh_sex and x.hh_age8=y.hh_age8 and x.hm_sex=y.hm_sex and x.hm_age8=y.hm_age8;

create table est_config_mcf_1 as select distinct yr,hh_id
from est_config_mcf_0 where mcf_prob>0;

create table est_config_1a as select x.*
,case when y.hh_id^=. then 1 else 0 end as can_be_mcf
from est_config_1 as x
left join est_config_mcf_1 as y on x.yr=y.yr and x.hh_id=y.hh_id
order by yr,hh_id,hh_age8,hh_sex,hh_size,can_be_mcf;
quit;

proc transpose data=est_config_1a out=est_config_2(drop=_name_);by yr hh_id hh_age8 hh_sex hh_size can_be_mcf;var n;id l;run;

proc transpose data=est_config_2 out=est_config_2a;by yr hh_id hh_age8 hh_sex hh_size can_be_mcf;run;

data est_config_2a;set est_config_2a;
if col1=. then col1=0;
run;

proc sql;
create table est_age8 as select distinct _name_ from est_config_2a;
quit;

proc sort data=est_config_2a;by yr hh_id hh_age8 hh_sex hh_size can_be_mcf _name_;run;


proc transpose data=est_config_2a out=est_config_A_1(drop=_name_);by yr hh_id hh_age8 hh_sex hh_size can_be_mcf;var col1;id _name_;run;

proc sql;
create table est_config_B_1 as select yr,hh_id,hh_age8,hh_sex,can_be_mcf
,case when F0018>=1 then 1 else 0 end as F0018 length=3
,case when F1924>=1 then 1 else 0 end as F1924 length=3
,case when F2534>=1 then 1 else 0 end as F2534 length=3
,case when F3544>=1 then 1 else 0 end as F3544 length=3
,case when F4554>=1 then 1 else 0 end as F4554 length=3
,case when F5564>=1 then 1 else 0 end as F5564 length=3
,case when F6574>=1 then 1 else 0 end as F6574 length=3
,case when F7599>=1 then 1 else 0 end as F7599 length=3
,case when M0018>=1 then 1 else 0 end as M0018 length=3
,case when M1924>=1 then 1 else 0 end as M1924 length=3
,case when M2534>=1 then 1 else 0 end as M2534 length=3
,case when M3544>=1 then 1 else 0 end as M3544 length=3
,case when M4554>=1 then 1 else 0 end as M4554 length=3
,case when M5564>=1 then 1 else 0 end as M5564 length=3
,case when M6574>=1 then 1 else 0 end as M6574 length=3
,case when M7599>=1 then 1 else 0 end as M7599 length=3
from est_config_A_1;
quit; 

proc sql;
create table est_config_A_2 as select x.yr,x.hh_id,x.hh_age8,x.hh_sex,x.hh_size,x.can_be_mcf
,y.config_id_a,y.hh_a
from est_config_A_1 as x
left join config_A_3a as y on
x.hh_size=y.hh_size and x.hh_age8=y.hh_age8 and x.hh_sex=y.hh_sex and x.can_be_mcf=y.can_be_mcf
and x.F0018 = y.F0018
and x.F1924 = y.F1924
and x.F2534 = y.F2534
and x.F3544 = y.F3544
and x.F4554 = y.F4554
and x.F5564 = y.F5564
and x.F6574 = y.F6574
and x.F7599 = y.F7599
and x.M0018 = y.M0018
and x.M1924 = y.M1924
and x.M2534 = y.M2534
and x.M3544 = y.M3544
and x.M4554 = y.M4554
and x.M5564 = y.M5564
and x.M6574 = y.M6574
and x.M7599 = y.M7599;

create table est_config_B_2 as select x.yr,x.hh_id,x.hh_age8,x.hh_sex,x.can_be_mcf
,y.config_id_b,y.hh_b
from est_config_B_1 as x
left join config_B_3a as y on
x.can_be_mcf=y.can_be_mcf and x.hh_age8=y.hh_age8 and x.hh_sex=y.hh_sex
and x.F0018 = y.F0018
and x.F1924 = y.F1924
and x.F2534 = y.F2534
and x.F3544 = y.F3544
and x.F4554 = y.F4554
and x.F5564 = y.F5564
and x.F6574 = y.F6574
and x.F7599 = y.F7599
and x.M0018 = y.M0018
and x.M1924 = y.M1924
and x.M2534 = y.M2534
and x.M3544 = y.M3544
and x.M4554 = y.M4554
and x.M5564 = y.M5564
and x.M6574 = y.M6574
and x.M7599 = y.M7599;

create table est_config_3 as select x.*,y.config_id_b,y.hh_b
from est_config_A_2 as x
left join est_config_B_2 as y on x.yr=y.yr and x.hh_id=y.hh_id;
quit;


proc sql;
create table est_config_4 as select x.*
,y1.h_type as h_type_a
,y2.h_type as h_type_b
,y3.hh_c,y3.h_type as h_type_c
,y4.hh_d,y4.h_type as h_type_d
from (select *,ranuni(&by1 + 3) as rn from est_config_3) as x
left join config_a_rates_2 as y1 on x.config_id_a=y1.config_id_a and y1.p1<=x.rn<=y1.p2
left join config_b_rates_2 as y2 on x.config_id_b=y2.config_id_b and y2.p1<=x.rn<=y2.p2
left join type3_rates_2 as y3 on x.hh_age8=y3.hh_age8 and x.hh_sex=y3.hh_sex and x.hh_size=y3.hh_size and x.can_be_mcf=y3.can_be_mcf
	and y3.p1<=x.rn<=y3.p2
left join type2_rates_2 as y4 on x.hh_age8=y4.hh_age8 and x.hh_sex=y4.hh_sex and x.can_be_mcf=y4.can_be_mcf and y4.p1<=x.rn<=y4.p2;
quit;

proc sql;
create table est_config_5 as select x.yr,y.ct,x.hh_id,x.hh_age8,x.hh_sex,x.hh_size,x.can_be_mcf
,case
when x.hh_a>9 then x.h_type_a
when x.hh_b>9 then x.h_type_b
when x.hh_c>9 then x.h_type_c
else x.h_type_d end as h_type
,case
when calculated h_type="F1" then 1
when calculated h_type="F2" then 2
when calculated h_type="F3" then 3
when calculated h_type="N2" and x.hh_sex="M" then 5
when calculated h_type="N2" and x.hh_sex="F" then 7
end as hht length=3
from est_config_4 as x
inner join est_h_1 as y on x.yr=y.yr and x.hh_id=y.hh_id;

create table est_config_5a as select * from est_config_5 where hht=.;

create table est_config_5b as select x.*
from est_config_4 as x
inner join est_config_5a as y on x.yr=y.yr and x.hh_id=y.hh_id;
quit;

proc sql;
create table est_config_6 as 
select * from est_config_5
	union all
select yr,ct,hh_id,hh_age8,hh_sex,hh_size,0 as can_be_mcf,h_type,hht from est_hh_single;
quit;

proc sql;
create table test_04 as select can_be_mcf,h_type,count(*) as n
from est_config_6 group by can_be_mcf,h_type;
quit;


proc sql;
create table est_config_6a as select *,hh_est/sum(hh_est) as s_est format=percent8.1
from (select yr,ct,h_type,count(*) as hh_est from est_config_6 group by yr,ct,h_type)
group by yr,ct;

create table est_config_6b as select *,hh_est/sum(hh_est) as s_est format=percent8.1
from (select yr,h_type,count(*) as hh_est from est_config_6 group by yr,h_type)
group by yr;
quit;

proc sql;
create table B11001_cn1_3 as select * from B11001_cn1_2a;

create table B11001_cn5_3 as select * from B11001_cn5_2a;

create table B11001_ct5_3 as select * from B11001_ct5_2a;
quit;

/* this excludes singles */
proc sql;
create table B11001_cn1_4 as select h_type,est,moe
,coalesce(s1_acs/sum(s1_acs),0) as s1_acs
,coalesce(s2_acs/sum(s2_acs),0) as s2_acs
,coalesce(s3_acs/sum(s3_acs),0) as s3_acs
from B11001_cn1_3 where h_type^="N1";

create table B11001_cn5_4 as select h_type,est,moe
,coalesce(s1_acs/sum(s1_acs),0) as s1_acs
,coalesce(s2_acs/sum(s2_acs),0) as s2_acs
,coalesce(s3_acs/sum(s3_acs),0) as s3_acs
from B11001_cn5_3 where h_type^="N1";

create table B11001_ct5_4 as select ct,h_type,est,moe
,coalesce(s1_acs/sum(s1_acs),0) as s1_acs
,coalesce(s2_acs/sum(s2_acs),0) as s2_acs
,coalesce(s3_acs/sum(s3_acs),0) as s3_acs
from B11001_ct5_3 where h_type^="N1"
group by ct;
quit;



proc sql;
create table est_config_6a_1 as select x.*
,y.s1_acs as s1_acs_5,y.s2_acs as s2_acs_5,y.s3_acs as s3_acs_5
,min(abs(x.s_est - y.s1_acs),abs(x.s_est - y.s2_acs),abs(x.s_est - y.s3_acs)) as d_5
from est_config_6a as x
left join B11001_ct5_3 as y on /*x.yr=y.est_yr and*/ x.ct=y.ct and x.h_type=y.h_type
order by d_5 desc;

create table est_config_6a_2 as select * from est_config_6a_1
where s_est < min(s1_acs_5,s2_acs_5,s3_acs_5) or s_est > max(s1_acs_5,s2_acs_5,s3_acs_5)
order by d_5 desc;

create table est_config_6b_1 as select x.*
,y.s1_acs as s1_acs_1,y.s2_acs as s2_acs_1,y.s3_acs as s3_acs_1
,min(abs(x.s_est - y.s1_acs),abs(x.s_est - y.s3_acs),abs(x.s_est - y.s3_acs)) as d_1

,y.s1_acs as s1_acs_5,y.s2_acs as s2_acs_5,y.s3_acs as s3_acs_5
,min(abs(x.s_est - y.s1_acs),abs(x.s_est - y.s3_acs),abs(x.s_est - y.s3_acs)) as d_5

from est_config_6b as x
left join B11001_cn1_3 as y on /*x.yr=y.est_yr and*/ x.h_type=y.h_type
left join B11001_cn5_3 as z on /*x.yr=z.est_yr and*/ x.h_type=z.h_type
order by d_1 desc;

create table est_config_6b_2 as select * from est_config_6b_1
where s_est < min(s1_acs_5,s2_acs_5,s3_acs_5) or s_est > max(s1_acs_5,s2_acs_5,s3_acs_5)
order by d_5 desc;
quit;

/*
HHT
Household/family type:
0.		Not in universe (vacant or GQ)
1.	Family household:married-couple
2.	Family household:male householder,no wife present
3.	Family household:female householder,no husband present
4.		Nonfamily household:male householder, living alone
5.	Nonfamily household:male householder, not living alone
6.		Nonfamily household:female householder, living alone
7.	Nonfamily household:female householder, not living alone
*/

/*
h_type: 
F1 married couple family
F2 family household with male householder, no wife
F3 family household with female householder, no hisband
	N1 nonfamily household, householder living alone
N2 nonfamily household, householder not living alone
*/

proc sql;
create table est_hh_ct_1 as select yr,ct,count(hh_id) as hh
from est_h_1 where hh_size>1
group by yr,ct;

create table est_hh_ct_2 as select x.yr,x.ct,x.hh,y.h_type,round(x.hh*y.s1_acs,1) as h0
from est_hh_ct_1 as x
left join B11001_ct5_4 as y on /*x.yr=y.est_yr and*/ x.ct=y.ct
order by yr,ct,h0;

create table est_hh_ct_2a as select * from est_hh_ct_2 where h_type="";
quit;

data est_hh_ct_3;set est_hh_ct_2;by yr ct;retain hc;
if first.ct then do;h1=h0;hc=h1;end;
else if last.ct then do;h1=hh-hc;hc=h1+hc;end;
else do;h1=min(h0,hh-hc);hc=h1+hc;end;
run;

proc transpose data=config_a_rates_2 out=config_a_rates_3(drop=_name_);by config_id_a;var prob;id h_type;run;
proc transpose data=config_b_rates_2 out=config_b_rates_3(drop=_name_);by config_id_b;var prob;id h_type;run;

proc transpose data=type3_rates_2 out=type3_rates_3(drop=_name_);by hh_age8 hh_sex hh_size can_be_mcf;var prob;id h_type;run;
proc transpose data=type2_rates_2 out=type2_rates_3(drop=_name_);by hh_age8 hh_sex can_be_mcf;var prob;id h_type;run;

proc sql;
create table config_a_rates_4 as select x.config_id_a
,coalesce(x.F1,0) as F1_a,coalesce(x.F2,0) as F2_a,coalesce(x.F3,0) as F3_a,coalesce(x.N2,0) as N2_a
,y.hh_a
from config_a_rates_3 as x
left join (select config_id_a,sum(hh) as hh_a from config_a_rates_2 group by config_id_a) as y
on x.config_id_a=y.config_id_a;

update config_a_rates_4 set F1_a=., F2_a=., F3_a=., N2_a=. where hh_a<30;

create table config_b_rates_4 as select x.config_id_b
,coalesce(x.F1,0) as F1_b,coalesce(x.F2,0) as F2_b,coalesce(x.F3,0) as F3_b,coalesce(x.N2,0) as N2_b
,y.hh_b
from config_b_rates_3 as x
left join (select config_id_b,sum(hh) as hh_b from config_b_rates_2 group by config_id_b) as y
on x.config_id_b=y.config_id_b;

update config_b_rates_4 set F1_b=., F2_b=., F3_b=., N2_b=. where hh_b<30;
quit;

proc sql;
create table type3_rates_4 as select hh_age8,hh_sex,hh_size,can_be_mcf
,coalesce(F1,0) as F1_3,coalesce(F2,0) as F2_3,coalesce(F3,0) as F3_3,coalesce(N2,0) as N2_3
from type3_rates_3;

create table type2_rates_4 as select hh_age8,hh_sex,can_be_mcf
,coalesce(F1,0) as F1_2,coalesce(F2,0) as F2_2,coalesce(F3,0) as F3_2,coalesce(N2,0) as N2_2
from type2_rates_3;
quit;


proc sql;
create table est_config_7 as select x.*
,y1.*
,y2.*
,y3.*
,y4.*
from est_config_3(drop=hh_a hh_b) as x
left join config_a_rates_4 as y1 on x.config_id_a=y1.config_id_a
left join config_b_rates_4 as y2 on x.config_id_b=y2.config_id_b
left join type3_rates_4 as y3 on x.hh_age8=y3.hh_age8 and x.hh_sex=y3.hh_sex and x.hh_size=y3.hh_size and x.can_be_mcf=y3.can_be_mcf
left join type2_rates_4 as y4 on x.hh_age8=y4.hh_age8 and x.hh_sex=y4.hh_sex and x.can_be_mcf=y4.can_be_mcf;
quit;

proc sql;
create table est_config_8 as select x.yr,y.ct,x.hh_id
,coalesce(x.F1_a,x.F1_b,x.F1_3,x.F1_2) as F1
,coalesce(x.F2_a,x.F2_b,x.F2_3,x.F2_2) as F2
,coalesce(x.F3_a,x.F3_b,x.F3_3,x.F3_2) as F3
,coalesce(x.N2_a,x.N2_b,x.N2_3,x.N2_2) as N2
from est_config_7 as x
left join est_h_1 as y on x.yr=y.yr and x.hh_id=y.hh_id
order by yr,ct;
quit;

proc sql;
create table est_config_8a as select * from est_config_8
where f1>0
order by yr,ct,f1 desc,ranuni(&by1 + 4);
quit;

data est_config_8a;set est_config_8a;by yr ct;retain i;
if first.ct then i=1;else i=i+1;
run;

proc sql;
create table est_config_8a_ as select x.yr,x.ct,x.hh_id,x.i,y.h1,y.h_type
from est_config_8a as x
inner join (select * from est_hh_ct_3 where h_type="F1") as y on x.yr=y.yr and x.ct=y.ct and x.i<=y.h1;
quit;



proc sql;
create table est_config_8b as select x.*
from est_config_8 as x
left join est_config_8a_ as y on x.yr=y.yr and x.hh_id=y.hh_id
where x.f2>0 and y.hh_id=.
order by yr,ct,f2 desc,ranuni(&by1 + 5);
quit;

data est_config_8b;set est_config_8b;by yr ct;retain i;
if first.ct then i=1;else i=i+1;
run;

proc sql;
create table est_config_8b_ as select x.yr,x.ct,x.hh_id,x.i,y.h1,y.h_type
from est_config_8b as x
inner join (select * from est_hh_ct_3 where h_type="F2") as y on x.yr=y.yr and x.ct=y.ct and x.i<=y.h1;
quit;


proc sql;
create table est_config_8c as select x.*
from est_config_8 as x
left join est_config_8a_ as y on x.yr=y.yr and x.hh_id=y.hh_id
left join est_config_8b_ as z on x.yr=z.yr and x.hh_id=z.hh_id
where x.f3>0 and y.hh_id=. and z.hh_id=.
order by yr,ct,f3 desc,ranuni(&by1 + 6);
quit;

data est_config_8c;set est_config_8c;by yr ct;retain i;
if first.ct then i=1;else i=i+1;
run;

proc sql;
create table est_config_8c_ as select x.yr,x.ct,x.hh_id,x.i,y.h1,y.h_type
from est_config_8c as x
inner join (select * from est_hh_ct_3 where h_type="F3") as y on x.yr=y.yr and x.ct=y.ct and x.i<=y.h1;
quit;



proc sql;
create table est_config_8d as select x.*
from est_config_8 as x
left join est_config_8a_ as y on x.yr=y.yr and x.hh_id=y.hh_id
left join est_config_8b_ as z on x.yr=z.yr and x.hh_id=z.hh_id
left join est_config_8c_ as w on x.yr=w.yr and x.hh_id=w.hh_id
where x.n2>0 and y.hh_id=. and z.hh_id=. and w.hh_id=.
order by yr,ct,n2 desc,ranuni(&by1 + 7);
quit;

data est_config_8d;set est_config_8d;by yr ct;retain i;
if first.ct then i=1;else i=i+1;
run;

proc sql;
create table est_config_8d_ as select x.yr,x.ct,x.hh_id,x.i,y.h1,y.h_type
from est_config_8d as x
inner join (select * from est_hh_ct_3 where h_type="N2") as y on x.yr=y.yr and x.ct=y.ct and x.i<=y.h1;
quit;

proc sql;
create table est_config_9 as select x.*
,coalesce(y1.h_type,y2.h_type,y3.h_type,y4.h_type) as h_type
from est_config_8 as x
left join est_config_8a_ as y1 on x.yr=y1.yr and x.hh_id=y1.hh_id
left join est_config_8b_ as y2 on x.yr=y2.yr and x.hh_id=y2.hh_id
left join est_config_8c_ as y3 on x.yr=y3.yr and x.hh_id=y3.hh_id
left join est_config_8d_ as y4 on x.yr=y4.yr and x.hh_id=y4.hh_id
order by yr,ct,hh_id;
quit;

proc sql;
create table B11001_ct5_5 as select x.yr as est_yr,y.*
from tp_0_yr as x
cross join B11001_ct5_4 as y;
quit;

proc sql;
create table ztest_01 as select y.est_yr,y.ct,y.h_type
,y.est as est_acs,y.moe as moe_acs,y.s1_acs,y.s2_acs,y.s3_acs
,z.h1 as target
,x.hh
,x1.F1_avl,x2.F2_avl,x3.F3_avl,x4.N2_avl

from (select * from B11001_ct5_5 where ct="005300") as y

left join (select yr,ct,h_type,count(hh_id) as hh from est_config_9 where ct="005300" group by yr,ct,h_type) as x
on x.yr=y.est_yr and x.ct=y.ct and x.h_type=y.h_type

left join (select yr,ct,"F1" as h_type,count(hh_id) as F1_avl
	from est_config_9 where h_type="" and F1>0 and ct="005300" group by yr,ct,h_type) as x1
	on y.est_yr=x1.yr and y.ct=x1.ct and y.h_type=x1.h_type

left join (select yr,ct,"F2" as h_type,count(hh_id) as F2_avl
	from est_config_9 where h_type="" and F2>0 and ct="005300" group by yr,ct,h_type) as x2
	on y.est_yr=x2.yr and y.ct=x2.ct and y.h_type=x2.h_type

left join (select yr,ct,"F3" as h_type,count(hh_id) as F3_avl
	from est_config_9 where h_type="" and F3>0 and ct="005300" group by yr,ct,h_type) as x3
	on y.est_yr=x3.yr and y.ct=x3.ct and y.h_type=x3.h_type

left join (select yr,ct,"N2" as h_type,count(hh_id) as N2_avl
	from est_config_9 where h_type="" and N2>0 and ct="005300" group by yr,ct,h_type) as x4
	on y.est_yr=x4.yr and y.ct=x4.ct and y.h_type=x4.h_type

left join (select * from est_hh_ct_3 where ct="005300") as z
	on y.est_yr=z.yr and y.ct=z.ct and y.h_type=z.h_type
;
quit;

proc sql;
create table ztest_02 as select * from est_config_9 where ct="005300" and h_type="";
quit;



proc sql;
create table test_10 as select x.yr,x.ct,x.h_type,x.h1 as target,y.hh_actual
from est_hh_ct_3 as x
left join (select yr,ct,h_type,count(hh_id) as hh_actual from est_config_9 where h_type^="" group by yr,ct,h_type) as y
on x.yr=y.yr and x.ct=y.ct and x.h_type=y.h_type
order by yr,ct,h_type;
quit;



proc transpose data=est_config_9 out=est_config_9a;by yr ct hh_id;var f1 f2 f3 n2;where h_type="";run;

proc sql;
create table est_config_9b as select yr,ct,hh_id,_name_ as h_type,col1/sum(col1) as prob
from est_config_9a where col1>0
group by yr,ct,hh_id
order by yr,ct,hh_id,prob;
quit;

data est_config_9c;set est_config_9b;retain p1 p2;by yr ct hh_id;
if first.hh_id then do;p1=0;p2=prob;end;
else do;p1=p2;p2=p2+prob;end;
run;

proc sql;
create table est_config_9d as select x.*,ranuni(&by1 + 8) as rn
from (select distinct yr,ct,hh_id from est_config_9c) as x;

create table est_config_9e as select x.yr,x.ct,x.hh_id,y.h_type
from est_config_9d as x
left join est_config_9c as y on x.yr=y.yr and x.hh_id=y.hh_id and y.p1<=x.rn<=y.p2
order by yr,ct,hh_id;
quit;

proc sql;
create table est_config_10 as select x.*
,z.hh_age8,z.hh_sex,z.hh_size
,coalesce(y1.h_type,y2.h_type,y3.h_type,y4.h_type,y5.h_type) as h_type
from est_config_8 as x
left join est_config_8a_ as y1 on x.yr=y1.yr and x.hh_id=y1.hh_id
left join est_config_8b_ as y2 on x.yr=y2.yr and x.hh_id=y2.hh_id
left join est_config_8c_ as y3 on x.yr=y3.yr and x.hh_id=y3.hh_id
left join est_config_8d_ as y4 on x.yr=y4.yr and x.hh_id=y4.hh_id
left join est_config_9e  as y5 on x.yr=y5.yr and x.hh_id=y5.hh_id

left join est_config_7 as z on x.yr=z.yr and x.hh_id=z.hh_id
order by yr,ct,hh_id;

create table est_config_10a as select * from est_config_10 where h_type="";
quit;


proc sql;
create table est_config_10_ct as select *,hh_est/sum(hh_est) as s_est format=percent8.1
from (select yr,ct,h_type,count(*) as hh_est from est_config_10 group by yr,ct,h_type)
group by yr,ct;

create table est_config_10_cn as select *,hh_est/sum(hh_est) as s_est format=percent8.1
from (select yr,h_type,count(*) as hh_est from est_config_10 group by yr,h_type)
group by yr;
quit;


proc sql;
create table est_config_10_ct_1 as select x.*
,y.s1_acs as s1_acs_5,y.s2_acs as s2_acs_5,y.s3_acs as s3_acs_5
,min(abs(x.s_est - y.s1_acs),abs(x.s_est - y.s2_acs),abs(x.s_est - y.s3_acs)) as d_5 format=percent8.2
from est_config_10_ct as x
left join B11001_ct5_3 as y on x.ct=y.ct and x.h_type=y.h_type
order by d_5 desc;

create table est_config_10_cn_1 as select x.*
,y.s1_acs as s1_acs_1,y.s2_acs as s2_acs_1,y.s3_acs as s3_acs_1
,z.s1_acs as s1_acs_5,z.s2_acs as s2_acs_5,z.s3_acs as s3_acs_5
,min(abs(x.s_est - y.s1_acs),abs(x.s_est - y.s2_acs),abs(x.s_est - y.s3_acs)) as d_1 format=percent8.2
,min(abs(x.s_est - z.s1_acs),abs(x.s_est - z.s2_acs),abs(x.s_est - z.s3_acs)) as d_5 format=percent8.2
from est_config_10_cn as x
left join B11001_cn1_3 as y on x.h_type=y.h_type
left join B11001_cn5_3 as z on x.h_type=z.h_type
order by d_1 desc;
quit;

proc sql;
create table est_config_11 as 
select yr,ct,hh_id,hh_sex,hh_size,h_type
,case
when h_type="F1" then 1
when h_type="F2" then 2
when h_type="F3" then 3
when h_type="N2" and hh_sex="M" then 5
when h_type="N2" and hh_sex="F" then 7
end as hht length=3
from est_config_10
	union all
select yr,ct,hh_id,hh_sex,hh_size,h_type,hht from est_hh_single;
quit;

/* 2 methods were tested

Method 1 (result: est_config_6): household type was imputed based on the probabilities from PUMS
	Given a set of householder characteristics a characteristics of members, what is the probability that a household
is of a particular type?

Method 2 (result: est_config_11): ct-level distribution of households by type were borrowed from the ACS and applied to the households
	Some costraint were applied: for married couple families (opposite sex and reasonable age differential)
	male householders can't be type F3, while female householders can't be type F2 

Method 2 produces better results at the county-level

*/

proc sql;
create table method_1_test_1 as select yr,h_type,hh_est/sum(hh_est) as s_est format=percent8.1
from (select yr,h_type,count(*) as hh_est from est_config_6 group by yr,h_type)
group by yr;

create table method_1_test_2 as select x.*,y.s1_acs as s1_acs_1,abs(x.s_est - y.s1_acs) as d
from method_1_test_1 as x
left join B11001_cn1_3 as y on x.h_type=y.h_type
order by d desc;

create table method_1_test_3 as select yr,sum(d) as d
from method_1_test_2 group by yr
order by yr;
quit;

proc sql;
create table method_2_test_1 as select yr,h_type,hh_est/sum(hh_est) as s_est format=percent8.1
from (select yr,h_type,count(*) as hh_est from est_config_11 group by yr,h_type)
group by yr;

create table method_2_test_2 as select x.*,y.s1_acs as s1_acs_1,abs(x.s_est - y.s1_acs) as d
from method_2_test_1 as x
left join B11001_cn1_3 as y on x.h_type=y.h_type
order by d desc;

create table method_2_test_3 as select yr,sum(d) as d
from method_2_test_2 group by yr
order by yr;
quit;

proc sql;
create table method_12_test_2 as select x.yr,x.h_type
,x.s_est as s_est1,y.s_est as s_est2,x.s1_acs_1
,x.d as d1 format=percent8.1,y.d as d2 format=percent8.1
from method_1_test_2 as x
left join method_2_test_2 as y on x.yr=y.yr and x.h_type=y.h_type
order by h_type,yr;

create table method_12_test_3 as select yr
,sum(d1) as d1 format=percent8.1,sum(d2) as d2 format=percent8.1
from method_12_test_2 group by yr;
quit;

