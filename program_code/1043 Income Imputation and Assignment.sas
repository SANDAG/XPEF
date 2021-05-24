
data inc_yr;length inc_yr 3;
do inc_yr=&by1 to &yy3;
	output;
end;
run;


proc sql;
create table acs_inc_ct_1 as
select x.inc_yr,y.ct_id as ct,y.inc16,y.est
from inc_yr as x
cross join (select * from e1.acs_inc_ct_1 where est>0 and yr=&acs_yr) as y;

create table acs_inc_ct_2 as select *,est/sum(est) as f
from acs_inc_ct_1 group by inc_yr,ct;
quit;

proc sql;
create table acs_inc_ct_3 as select inc_yr,ct,count(est) as n
from acs_inc_ct_2 group by inc_yr,ct;

create table acs_inc_ct_3a as select x.*,y.n
from acs_inc_ct_2 as x
inner join acs_inc_ct_3 as y on x.inc_yr=y.inc_yr and x.ct=y.ct
where y.n^=16
order by inc_yr,ct,inc16;
quit;


proc sql;
create table acs_inc_ct_4 as select x.*,z.ct,coalesce(y.est,0) as est,coalesce(y.f,0) as f
from (select distinct inc_yr,inc16 from acs_inc_ct_2) as x
cross join (select distinct ct from acs_inc_ct_2) as z
left join acs_inc_ct_2 as y on x.inc_yr=y.inc_yr and z.ct=y.ct and x.inc16=y.inc16
order by inc_yr,ct,inc16;
quit;


proc sql;
create table hh_2 as select x.yr as yr_id length=3 format=4.,x.hh_id length=5 format=7.
,y.ct, y.jur length=3 format=2., y.mgra length=4 format=5., y.cpa length=3 format=4.
from (select * from sql_xpef.households where yr in (&list1)) as x
left join sql_xpef.housing_units as y on x.yr=y.yr and x.hh_id=y.hh_id;
/*
mgra,cpa,jur
5229,1429,14
16080,1920,19
18034,0,1
19555,0,7

all in cpa 1915
19218,19224,19231,19271
*/
quit;


proc sql;
create table hh_02 as select yr_id,mgra,ct,jur,count(hh_id) as hh
from hh_2 group by yr_id, mgra, ct, jur;

create table hh_02a as select yr_id,sum(hh) as hh
from hh_02 group by yr_id;
quit;


/* this table should have zero records */
proc sql;
create table test_001 as select distinct x.inc_yr,x.ct
from (select distinct yr_id,yr_id as inc_yr length=3, ct from hh_02) as x
left join (select distinct inc_yr,ct from acs_inc_ct_4) as y
on x.inc_yr=y.inc_yr and "6073"||x.ct=y.ct
where y.ct=""
order by ct,inc_yr;

create table test_002 as select ct,count(ct) as n
from test_001 group by ct order by n;
quit;

proc sql;
create table hh_3 as select *, yr_id as inc_yr length=3
from hh_2 order by yr_id,ct,ranuni(2050);
quit;

data hh_3;set hh_3;by yr_id ct;retain i;length i 4;
if first.ct then i=1;else i=i+1;
run;

proc sql;
create table hh_04 as select yr_id,inc_yr,ct,count(hh_id) as hht
from hh_3 group by yr_id,inc_yr,ct;

create table hh_05 as select x.*,y.inc16,y.est as acs_hh,y.f
,round(x.hht * y.f,1) as hh0
from hh_04 as x
left join acs_inc_ct_4 as y on x.inc_yr=y.inc_yr and "6073"||x.ct=y.ct
order by yr_id,ct,hh0;
quit;

data hh_05a;set hh_05;by yr_id ct;retain hhc;
if first.ct then do;hh1=min(hh0,hht);hhc=hh1;end;
else if last.ct then do;hh1=hht-hhc;hhc=hh1+hhc;end;
else do;hh1=min(hh0,hht-hhc);hhc=hh1+hhc;end;
if hhc=0 then a=0;else a=hhc-hh1+1;
b=hhc;
run;


data hh_05b (drop=l u);set hh_05a(keep=yr_id ct inc16 hh1);where hh1>0;
l=int(inc16) * 1000;
u=(inc16 - int(inc16)) * 1000000;
do i=1 to hh1;
	inc=round((u-l) / (hh1+1) * i + l,1);
	output;
end;
run;

/* assigning an income category */
proc sql;
create table hh_6 as select x.yr_id,x.mgra,x.ct,x.jur,x.hh_id,y.inc16,x.inc_yr
from hh_3 as x
left join hh_05a as y on x.yr_id=y.yr_id and x.ct=y.ct and y.a<=x.i<=y.b
order by yr_id,ct,inc16,ranuni(2051);

create table hh_6a as select x.*,y.*
from (select count(hh_id) as n1 from hh_3) as x
cross join (select count(inc16) as n2 from hh_6) as y;
quit;

data hh_7;set hh_6;by yr_id ct inc16;retain i;
if first.inc16 then i=1;else i=i+1;
run;


proc sql noprint;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

select cpi_2010 into :cpi2010
from connection to odbc
(select * FROM [socioec_data].[bls_su].[cpi_u_rs_1977_&acs_yr] where yr = &acs_yr);

create table income_group_id as select income_group_id,name,lower_bound,upper_bound
from connection to odbc
(select * FROM [demographic_warehouse].[dim].[income_group] where categorization = 10 and constant_dollars_year=2010);

disconnect from odbc;
quit;

%put cpi_2010 = &cpi2010;

/* assigning specific value of income */
proc sql;
create table hh_8 as select x.yr_id as yr,x.mgra as mgra13,x.ct,x.jur,x.hh_id/*,x.inc16*/
,round(y.inc / &cpi2010,1) as inc_2010 format=6./*,x.acs_yr */
from hh_7 as x
left join hh_05b as y on x.yr_id=y.yr_id and x.ct=y.ct and x.inc16=y.inc16 and x.i=y.i;

create table hh_8a as select x.*,y.*
from (select count(hh_id) as n1 from hh_6) as x
cross join (select count(inc_2010) as n2 from hh_8) as y;
quit;


proc sql;
create table hh_10 as select x.*
,y.income_group_id as income_group_id_2010
from hh_8 as x
left join income_group_id as y on y.lower_bound <= x.inc_2010 <= y.upper_bound;
quit;

/* fetching household income */
proc sql;
create table hh_inc_0 as select yr,mgra13,jur,inc_2010,income_group_id_2010 as income_group_id
from hh_10;

create table hh_inc_1 as select yr,mgra13,jur,income_group_id,count(*) as hh_inc
from hh_inc_0 group by yr,mgra13,jur,income_group_id;

create table hh_inc_1a as select yr,sum(hh_inc) as hh_inc from hh_inc_1 group by yr;
quit;


proc sql;
create table hp_0 as select yr,hp_id,hh_id,age,sex,role
,case
when age <= 4 then "C0004"
when age <= 9 then "C0509"
when age <= 12 then "C1012"
when age <= 16 then "C1316"
when age <= 19 then "C1719"
when age <= 24 then sex||"2024"
when age <= 29 then sex||"2529"
when age <= 39 then sex||"3039"
when age <= 49 then sex||"4049"
when age <= 59 then sex||"5059"
when age <= 64 then sex||"6064"
when age <= 74 then sex||"6574"
when age <= 84 then sex||"7584"
else sex||"8599" end as hp_type
from sql_xpef.household_population
where yr in (&list1);
quit;


proc sql;
create table hh_0 as select yr length=3 format=4.,mgra length=4 format=5.,jur length=3 format=2.,hh_id,size length=3 format=2.
from sql_xpef.households
where yr in (&list1);

create table hh_0a as select yr,count(hh_id) as hh
from hh_0 group by yr;
quit;

/*
Tables sh.sd_pums_h_1 and sh.sd_pums_p_1
were generated in "T:\socioec\Current_Projects\Synthetic Households\PUMS\Preparing PUMS (San Diego).sas"
*/

proc sql;
create table sd_pums_inc_0 as select x.yr,x.serialno,x.hh_size,x.income_group_id format=2.
,y.hp_type
from sh.sd_pums_h_1 as x
inner join sh.sd_pums_p_1 as y on x.yr=y.yr and x.serialno=y.serialno
where substr(y.hp_type,1,1)^="C" and y.role="H" and x.hh_size<=10 and x.income_group_id^=.;

create table sd_pums_inc_1 as
select hp_type,hh_size,income_group_id,count(*) as n
from sd_pums_inc_0 where hh_size<=4 group by hp_type,hh_size,income_group_id
	union all
select x.hp_type,y.hh_size,x.income_group_id,x.n
from (select hp_type,income_group_id,count(*) as n
from sd_pums_inc_0 where hh_size>=5 group by hp_type,income_group_id) as x
cross join (select distinct hh_size from sd_pums_inc_0 where hh_size>=5) as y
order by hp_type,hh_size,income_group_id;

create table sd_pums_inc_2 as select x.*,y.*,z.*,coalesce(w.n,0.01) as n
from (select distinct hp_type from sd_pums_inc_1) as x
cross join (select distinct hh_size from sd_pums_inc_1) as y
cross join (select distinct income_group_id from sd_pums_inc_1) as z
left join sd_pums_inc_1 as w on x.hp_type=w.hp_type and y.hh_size=w.hh_size and z.income_group_id=w.income_group_id;
quit;

proc sql;
create table max_inc_slots as select max(hh_inc) as max_hh_inc from hh_inc_1;
quit;

proc sql;
create table ztest_001 as select x.*,y.hh
from (select yr,sum(hh_inc) as slots from hh_inc_1 group by yr) as x
left join (select yr,count(hh_id) as hh from hh_0 group by yr) as y
on x.yr=y.yr;
quit;


%macro inc1(mx=);

proc datasets library=work nolist; delete hh_done;quit;

/* Fill in missing income_group_ids with yearly jurisdiction avearge (rounded) */
proc sql;
create table hh_inc_fill as select 
yr,jur,round(mean(income_group_id),1) as income_group_id
from hh_inc_1
group by yr,jur;

create table hh_inc_2 as
select a.yr, a.mgra13, a.jur,
coalesce(a.income_group_id, b.income_group_id) as income_group_id, a.hh_inc
from hh_inc_1 as a
inner join hh_inc_fill as b on a.yr=b.yr and a.jur=b.jur;
quit;

data income_slots_1(drop=hh_inc);set hh_inc_2(rename=(mgra13=mgra));length inc_slot 3;
do inc_slot=1 to hh_inc;output;end;
run;

proc sql noprint;
create table hh_1 as select x.yr,x.mgra,x.jur,x.hh_id,x.size
,case when y.age<=19 then y.sex||"2024" else y.hp_type end as hp_type
from hh_0 as x
inner join hp_0 as y on x.yr=y.yr and x.hh_id=y.hh_id
where y.role="H"
order by yr,mgra,jur;

update hh_1 set size=10 where size>10;

quit;

%let t=0; /* t is a counter */

%do %while(&t<=&mx);

%let t=%eval(&t+1);

/* create a prob distribution */
proc sql;
create table prob_1 as select x.*,y.income_group_id,z.n
from (select distinct yr,mgra,jur,hp_type,size from hh_1) as x
inner join (select distinct yr,mgra,jur,income_group_id length=3 from income_slots_1) as y on x.yr=y.yr and x.mgra=y.mgra and x.jur=y.jur
inner join sd_pums_inc_2 as z on x.hp_type=z.hp_type and x.size=z.hh_size and y.income_group_id=z.income_group_id;

create table prob_2 as select *,n/sum(n) as p
from prob_1 group by yr,mgra,jur,hp_type,size
order by yr,mgra,jur,hp_type,size,ranuni(&t);
quit;

data prob_3;set prob_2;by yr mgra jur hp_type size;retain b;
if first.size then do;a=0;b=p;end;
else do;a=b;b=b+p;end;
run;

proc sql;
/* assign an income group */
create table hh_2 as select x.*,y.income_group_id
from (select *,ranuni(&t+1) as rn from hh_1) as x
left join prob_3 as y
on x.yr=y.yr and x.mgra=y.mgra and x.jur=y.jur and x.hp_type=y.hp_type and x.size=y.size and y.a <= x.rn <= y.b
order by yr,mgra,jur,income_group_id,rn;
quit;

data hh_2;set hh_2;by yr mgra jur income_group_id;retain i;length i 4;
if first.income_group_id then i=1;else i=i+1;
run;

proc sort data=income_slots_1;by yr mgra jur income_group_id inc_slot;run;

data income_slots_2;set income_slots_1;by yr mgra jur income_group_id;retain i;length i 4;
if first.income_group_id then i=1;else i=i+1;
run;

proc sql noprint;
create table hh_3 as select x.yr,x.mgra,x.jur,x.hh_id,x.income_group_id,y.inc_slot
from hh_2 as x
inner join income_slots_2 as y on x.yr=y.yr and x.mgra=y.mgra and x.jur=y.jur and x.income_group_id=y.income_group_id and x.i=y.i;

select count(*) into :h1 from hh_3;
quit;

%put &h1 households are assigned at cycle = &t;

%if &h1>0 %then %do;
	proc append base=hh_done data=hh_3(drop=inc_slot mgra);run;
%end;

proc sql noprint;
/* selecting unused income slots */
create table income_slots_1 as select x.yr,x.mgra,x.jur,x.income_group_id,x.inc_slot
from income_slots_2 as x
left join hh_3 as y on x.yr=y.yr and x.mgra=y.mgra and x.jur=y.jur and x.income_group_id=y.income_group_id and x.inc_slot=y.inc_slot
where y.inc_slot=.;

/* selecting unassigned households */
create table hh_1 as select x.yr,x.mgra,x.jur,x.hh_id,x.size,x.hp_type
from hh_2 as x
left join hh_3 as y on x.yr=y.yr and x.hh_id=y.hh_id
where y.hh_id=.;

select count(*) into :h from hh_1;
quit;

%if &h=0 %then %do;
	%put All households are assigned at cycle = &t;
	%goto exit1;
%end;

%if &t=&mx %then %do;
	%put Max iterations (&mx) reached, &h households are left unassigned;
	%goto exit1;
%end;

%end;

%exit1:

%mend inc1;

%inc1(mx=10);
/* mx sets the max iterations */


proc sql;
create table hh_done_1 as select x.*,y.mgra as mgra13
from hh_done as x
inner join hh_0 as y on x.yr=y.yr and x.hh_id=y.hh_id
order by yr,mgra13,jur,income_group_id,ranuni(1);
quit;

data hh_done_1;set hh_done_1;by yr mgra13 jur income_group_id;retain i;
if first.income_group_id then i=1;else i=i+1;
run;

proc sql;
create table hh_inc_01 as select yr,mgra13,jur,income_group_id,inc_2010
from hh_inc_0 order by yr,mgra13,jur,income_group_id,ranuni(2);
quit;

data hh_inc_01;set hh_inc_01;by yr mgra13 jur income_group_id;retain i;
if first.income_group_id then i=1;else i=i+1;
run;

proc sql;
create table hh_done_2 as select x.yr format=4.
,x.hh_id
,x.income_group_id as income_group_id_2010 format=2.
,y.inc_2010 length=4 format=6.
from hh_done_1 as x
left join hh_inc_01 as y on x.yr=y.yr and x.mgra13=y.mgra13 and x.jur=y.jur and x.income_group_id=y.income_group_id
and x.i=y.i;

create table hh_done_2a as select * from hh_done_2 where inc_2010=.;
quit;


proc sql;
create table hh_income_base as select yr length=3 format=4.
,hh_id
,income_group_id_2010 length=3 format=2.
,inc_2010 length=4 format=6.
from sql_est.household_income where yr=&by1;
quit;


proc sql;
create table hh_inc_01a as select yr,count(i) as n from hh_inc_01 group by yr;
quit;


data hh_done_3;set hh_done_2 hh_income_base;run;


proc means data=hh_done_3 noprint;
class yr;
ways 1;
var inc_2010;
output out=hh_done_2_med median(inc_2010)=median_inc2010;
run;


proc sql;
CONNECT TO ODBC(noprompt="driver=SQL Server; server=sql2014a8; database=isam; DBCOMMIT=10000; Trusted_Connection=yes;") ;

EXECUTE ( drop table if exists isam.&xver..household_income; ) BY ODBC ;

EXECUTE
(
CREATE TABLE isam.&xver..household_income(
yr smallint
,hh_id int
,income_group_id_2010 tinyint
,inc_2010 int
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;

DISCONNECT FROM ODBC ;
quit;

options notes;

proc sql;
insert into sql_xpef.household_income(bulkload=yes bl_options=TABLOCK) select * from hh_done_3;
quit;

options nonotes;
