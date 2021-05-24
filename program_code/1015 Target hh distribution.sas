
libname mr "T:\socioec\socioec_data_test\Census_2010\Modified_Race";

libname sf1_2000 "T:\socioec\socioec_data_test\Census_2000\SF1";
libname sf1_2010 "T:\socioec\socioec_data_test\Census_2010\SF1";

/*
age of householder by tenure by race (block)
2000 H16
2010 H17
*/

/*
race of householder by hisp (block)
2000 H7
2010 H7
*/

proc sql;
create table mr_1 as
select r_code,hisp,r7,sum(tp_2010) as p
from mr.census_mr_crosswalk group by r_code,hisp,r7;

create table mr_2 as select hisp,r_code,r7,p,p/sum(p) as s
from mr_1 group by hisp,r_code;
quit;


proc sql;
create table h7_2000_0 as select y.tract,y.block,x.tblid,x.lineid as i,x.value
from sf1_2000.sf1_2000_values as x
inner join sf1_2000.sf1_2000_geo as y on x.id=y.id
where substr(x.tblid,1,4)="H007" and y.sumlev="101" and y.county="073"
order by tract,block;
quit;

data h7_2010_00;set sf1_2010.sandag_101(keep=tract block h007:);run;
proc sort data=h7_2010_00;by tract block;run;
proc transpose data=h7_2010_00 out=h7_2010_0;by tract block;run;

proc sql;
create table h7_2000_1 as select tract as ct_2000,tract||block as blk_2000
,case
when i > 10 then "H" else "NH" end as hisp
,case 
when i = 3 then "R10"
when i = 11 then "R11"
when i in (4,12) then "R02"
when i in (5,13) then "R03"
when i in (6,14) then "R04"
when i in (7,15) then "R05"
when i in (8,16) then "R06"
when i in (9,17) then "R07"
end as r_code
,sum(value) as hh
from h7_2000_0 where i in (3:9,11:17)
group by ct_2000,blk_2000,hisp,r_code;

create table h7_2010_1 as select tract as ct_2010,tract||block as blk_2010
,case
when input(substr(_name_,5,4),4.) > 10 then "H" else "NH" end as hisp
,case 
when input(substr(_name_,5,4),4.) = 3 then "R10"
when input(substr(_name_,5,4),4.) = 11 then "R11"
when input(substr(_name_,5,4),4.) in (4,12) then "R02"
when input(substr(_name_,5,4),4.) in (5,13) then "R03"
when input(substr(_name_,5,4),4.) in (6,14) then "R04"
when input(substr(_name_,5,4),4.) in (7,15) then "R05"
when input(substr(_name_,5,4),4.) in (8,16) then "R06"
when input(substr(_name_,5,4),4.) in (9,17) then "R07"
end as r_code

,sum(col1) as hh
from h7_2010_0 where input(substr(_name_,5,4),4.) in (3:9,11:17)
group by ct_2010,blk_2010,hisp,r_code;
quit;


proc sql;
create table h7_2000_1a as select x.ct_2000,x.blk_2000,x.hisp,x.r_code,x.hh, y.s
,y.r7, round(x.hh * y.s,1) as p0
from h7_2000_1 as x
left join mr_2 as y on x.hisp=y.hisp and x.r_code=y.r_code
order by ct_2000,blk_2000,hisp,r_code,p0;

create table h7_2010_1a as select x.ct_2010,x.blk_2010,x.hisp,x.r_code,x.hh, y.s
,y.r7, round(x.hh * y.s,1) as p0
from h7_2010_1 as x
left join mr_2 as y on x.hisp=y.hisp and x.r_code=y.r_code
order by ct_2010,blk_2010,hisp,r_code,p0;
quit;

data h7_2000_1b;set h7_2000_1a;by ct_2000 blk_2000 hisp r_code; retain cp;
if first.r_code then do; p1 = p0; cp = p1; end;
else if last.r_code then do; p1 = hh - cp; cp = cp + p1; end;
else do; p1 = min(p0, hh - cp); cp = cp + p1; end;
run;

data h7_2010_1b;set h7_2010_1a;by ct_2010 blk_2010 hisp r_code; retain cp;
if first.r_code then do; p1 = p0; cp = p1; end;
else if last.r_code then do; p1 = hh - cp; cp = cp + p1; end;
else do; p1 = min(p0, hh - cp); cp = cp + p1; end;
run;

proc sql;
create table h7_2000_1c as select ct_2000,blk_2000,r7,sum(p1) as hh
from h7_2000_1b group by ct_2000,blk_2000,r7;

create table h7_2010_1c as select ct_2010,blk_2010,r7,sum(p1) as hh
from h7_2010_1b group by ct_2010,blk_2010,r7;
quit;

%include "T:\socioec\socioec_data_test\Work\GIS\block_relationship_files\import blk 2000-2010 relationship.sas";

proc sql;
create table h7_2000_2 as select x.*,put(y.tract_2010,z6.) as ct_2010
from H7_2000_1c as x
left join blk_2000_4 as y on x.blk_2000=put(y.blk_2000,z10.);
quit;

proc sql;
create table hh_ct_1 as select x.*,coalesce(y.hh_2000,0) as hh_2000
from (select ct_2010,r7,sum(hh) as hh_2010 from h7_2010_1c group by ct_2010,r7) as x
left join (select ct_2010,r7,sum(hh) as hh_2000 from h7_2000_2 group by ct_2010,r7) as y
on x.ct_2010=y.ct_2010 and x.r7=y.r7;

create table hh_ct_1a as select ct_2010,sum(hh_2000) as hh_2000,sum(hh_2010) as hh_2010
from hh_ct_1 group by ct_2010;

create table hh_ct_1b as select r7,sum(hh_2000) as hh_2000,sum(hh_2010) as hh_2010
from hh_ct_1 group by r7;

create table hh_ct_1c as select sum(hh_2000) as hh_2000,sum(hh_2010) as hh_2010
from hh_ct_1;
quit;


proc sql;
create table hh_ct_2 as select *,hh_2010/sum(hh_2010) as s_2010 format=percent7.
from (select *,hh_2000/sum(hh_2000) as s_2000 format=percent7.
from hh_ct_1 where ct_2010^="990100" group by ct_2010)
group by ct_2010;

create table hh_ct_3 as select *,round(s_2000,0.05) as s_2000_5 format=percent7.,round(s_2010,0.05) as s_2010_5 format=percent7.
,s_2010 - s_2000 as d format=percent7.
,round(s_2010 - s_2000,0.05) as d_5 format=percent7.
from hh_ct_2;

create table hh_ct_3_test as select * 
from hh_ct_3 where s_2000_5 = . or s_2010_5 = . or d_5 = .;
quit;

proc sql;
create table hh_ct_3a as select r7,s_2010_5,d_5,count(*) as n
from hh_ct_3 where s_2010_5^=. and d_5^=. group by r7,s_2010_5,d_5
order by r7,s_2010_5,d_5;

create table hh_ct_3b as select * from hh_ct_3a where d_5>0;

create table hh_ct_3b as select r7,max(s_2010_5) as max_s_2010_5 format=percent7.
from hh_ct_3 group by r7;
quit;


proc sql;
create table est_hh_0 as select x.yr,y.ct,x.r7,count(x.hh_id) as hh
from sql_est.household_population as x
inner join sql_est.housing_units as y on x.hh_id=y.hh_id and x.yr=y.yr
where x.role="H"
group by x.yr,y.ct,x.r7
order by ct,r7,yr;

create table est_hh_2 as select *,hh/sum(hh) as s format=percent7.
from est_hh_0 group by yr,ct;
quit;


proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table fut_hh_0 as select yr, ct, r as r_code,hisp,hh
from connection to odbc
(select x.yr,y.ct,x.r,x.hisp,count(x.hh_id) as hh
FROM [isam].[xpef09].[household_population] as x
inner join [isam].[xpef09].[housing_units] as y on x.hh_id=y.hh_id and x.yr=y.yr
where x.role='H' and x.yr=2051
group by x.yr,y.ct,x.r,x.hisp)
order by yr,ct,r_code,hisp
;

disconnect from odbc;
quit;

proc sql;
create table fut_hh_0a as select x.yr, x.ct ,x.hisp ,x.r_code, x.hh, y.s
,y.r7, round(x.hh * y.s,1) as p0
from fut_hh_0 as x
left join mr_2 as y on x.hisp=y.hisp and x.r_code=y.r_code
order by yr,ct,hisp,r_code,p0;
quit;

data fut_hh_0b;set fut_hh_0a;by yr ct hisp r_code; retain cp;
if first.r_code then do; p1 = p0; cp = p1; end;
else if last.r_code then do; p1 = hh - cp; cp = cp + p1; end;
else do; p1 = min(p0, hh - cp); cp = cp + p1; end;
run;

proc sql;
create table fut_hh_0c as select yr,ct,r7,sum(p1) as hh
from fut_hh_0b group by yr,ct,r7;
quit;


proc sql;
create table fut_hh_1a as select yr,ct,sum(hh) as hh
from fut_hh_0c group by yr,ct;

create table fut_hh_1b as select yr,r7,sum(hh) as hh
from fut_hh_0c group by yr,r7;
quit;


proc sql;
create table test_01 as select x.*
from (select * from est_hh_0 where yr = &by1) as x
left join hh_ct_3 as y on x.ct=y.ct_2010 and x.r7=y.r7
where y.hh_2010=.;
quit;

proc sql;
create table hh_ct_4 as select x.ct_2010,x.r7,x.hh_2000,x.hh_2010,x.s_2000,x.s_2010
,coalesce(y.hh,0) as hh_&by1,coalesce(y.s,0) as s_&by1 format=percent7.
,case when x.d_5>0 then coalesce(y.s,0) + 0.005 * (2051 - &by1) /* 5% per decade*/ else coalesce(y.s,0) end as s_2051a format=percent7.
,case when x.d>0 then coalesce(y.s,0) + x.d/10 * (2051 - &by1) else coalesce(y.s,0) end as s_2051b format=percent7.
from hh_ct_3 as x
left join (select * from est_hh_2 where yr = &by1) as y on x.ct_2010=y.ct and x.r7=y.r7;

create table hh_ct_5 as select ct_2010,r7,hh_2000,hh_2010,hh_&by1,s_2000,s_2010,s_&by1
,s_2051a / sum(s_2051a) as s_2051a format=percent7.
,s_2051b / sum(s_2051b) as s_2051b format=percent7.
from hh_ct_4 group by ct_2010;
quit;


proc sql;
create table hh_ct_6 as select x.ct_2010,x.r7
,round(coalesce(x.s_2051a,0) * coalesce(y.hh,0), 1) as hh
from hh_ct_5 as x
left join (select * from fut_hh_1a where yr=2051) as y on x.ct_2010=y.ct
order by hh;
quit;


/*------------IPF SECTION---------------------------------------*/

/*
i: version (1 and 2)
A: ct
B: race/age3
The purpose is to create a joint distribution of A and B
*/


proc sql;
create table inp_a_00 as
select "1" as i,ct as a,hh as h from fut_hh_1a where yr=2051
	union all
select "2" as i,ct as a,hh as h from fut_hh_1a where yr=2051
order by i,a;

create table inp_b_00 as
select "1" as i,r7 as b,hh as h from fut_hh_1b where yr=2051
	union all
select "2" as i,r7 as b,hh as h from fut_hh_1b where yr=2051
order by i,b;

create table inp_ab_m_0 as
select "1" as i, ct_2010 as a, r7 as b, hh as h from hh_ct_6
	union all
select "2" as i, ct_2010 as a, r7 as b, hh as h from hh_ct_6
order by i,a,b;

create table inp_a_0 as select i,a,h from inp_a_00 order by i,a;
create table inp_b_0 as select i,b,h from inp_b_00 order by i,b;
quit;


/* these tables should have zero records */
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

%ipf1(mx=700); /* sets the number of iterations */


proc sql;
create table inp_final_1 as select i,a,b,c,int(c) as i1
from inp_final_0;

create table inp_final_1a as select x.*,y.c,y.i1,x.h - y.i1 as d_a
from inp_a_0 as x
left join (select i,a,sum(c) as c,sum(i1) as i1 from inp_final_1 group by i,a) as y
on x.i=y.i and x.a=y.a;

create table inp_final_1b as select x.*,y.c,y.i1,x.h - y.i1 as d_b
from inp_b_0 as x
left join (select i,b,sum(c) as c,sum(i1) as i1 from inp_final_1 group by i,b) as y
on x.i=y.i and x.b=y.b;

create table inp_final_1ab as select x.i,x.d_a,y.d_b
from (select i,sum(d_a) as d_a from inp_final_1a group by i) as x
inner join (select i,sum(d_b) as d_b from inp_final_1b group by i) as y on x.i=y.i;
quit;


data slots_a_1(drop=j d_a);set inp_final_1a(keep=i a d_a);
do j=1 to d_a;
	output;
end;
run;

data slots_b_1(drop=j d_b);set inp_final_1b(keep=i b d_b);
do j=1 to d_b;
	output;
end;
run;


proc sql;
create table slots_a_2 as select i,a as ct from slots_a_1 order by i,ranuni(2051);
create table slots_b_2 as select i,b as r7 from slots_b_1 order by i,ranuni(2051);

create table slots_ab_1 as select x.i,x.ct,y.r7
from (select *,monotonic() as j from slots_a_2) as x
inner join (select *,monotonic() as j from slots_b_2) as y
on x.j=y.j and x.i=y.i;
quit;

data inp_final_2(drop=j i1);set inp_final_1(drop=c rename=(a=ct));
do j=1 to i1;
	output;
end;
run;

proc sql;
create table inp_final_3 as
select * from inp_final_2
	union all
select * from slots_ab_1;
quit;

proc sql;
create table hh_target_1 as select i,ct,b as r7,count(*) as hh_target
from inp_final_3 group by i,ct,r7;

create table hh_target_2 as select *,hh_target/sum(hh_target) as s_target format=percent7.0
from hh_target_1 group by i,ct;
quit;

proc sql;
create table test_02 as select x.ct_2010,x.r7,x.hh_2000,x.hh_2010,x.hh_&by1
,coalesce(y.hh_target,0) as hh_2051a,coalesce(z.hh_target,0) as hh_2051b
,x.s_2000,x.s_2010,x.s_&by1
,coalesce(y.s_target,0) as s_2051a format=percent7., coalesce(z.s_target,0) as s_2051b format=percent7.
,(coalesce(y.s_target,0) - x.s_&by1)/(2051-&by1) as ac /* annual change */
from hh_ct_5 as x
left join (select * from hh_target_2 where i="1") as y on x.ct_2010=y.ct and x.r7=y.r7
left join (select * from hh_target_2 where i="2") as z on x.ct_2010=z.ct and x.r7=z.r7
order by ct_2010,r7;
quit;

proc sql;
/* reversal */
create table test_02a as select *
from test_02 where (s_2010 - s_2000)>0.1 and (s_2051a-s_&by1)<-0.1;

/* continue */
create table test_02b as select *
from test_02 where (s_2010 - s_2000)>0.05 and (s_2051a-s_&by1)>0.05;
quit;

data yr2051;length yr 3;
do yr=&by1 to 2051;
	output;
end;
run;

proc sql;
create table hh_target_3 as select x.yr,y.ct_2010 as ct,y.r7, y.s_&by1 + y.ac * (x.yr - &by1) as s
from yr2051 as x
cross join test_02 as y
order by ct,r7,yr;

create table test_03 as select * from hh_target_3 where s = .;

create table hh_target_4 as select yr,ct,r7,s/sum(s) as hh_target_s format=percent8.1
from hh_target_3 where ct not in ("003800","005500","006200","006300","009902")group by yr,ct
order by ct,r7,yr;

create table hh_target_4a as select * from hh_target_4 where hh_target_s = .;
quit;

proc sql;
create table e1.ct_hh_target_dist as select * from hh_target_4
order by yr,ct,r7;
quit;
