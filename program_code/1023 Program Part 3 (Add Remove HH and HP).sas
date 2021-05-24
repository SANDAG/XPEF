
proc sql;
create table hp_cest_1 as select r7 length=1, age as age101,sex,hp as hp_cest
from e1.future_hp where yr=&yrn;
quit;


/*-----GQ SECTION---------------------*/
proc sql;
/* updating GQ records; dob is incremented by one year */
create table gq_next_1 as select
gq_id, gq_type,jur,ct,cpa,mgra
,age,r, hisp, sex, intnx('year', dob, 1, 'same') as dob format=mmddyy10.
,case when age<=100 then age else 100 end as age101
,r7
from gq_base;

create table gq_next_1a as select age101,sex,r7,count(gq_id) as gq
from gq_next_1 group by age101,sex,r7;
quit;

/* for now, GQ assumed to be non-declining; however, you might want to build in an option 
to manage declining targets in GQ */

data gq_next_2; set gq_next_1;run;

proc sql noprint;
select sum(gq) into :gqcheck from e1.gq_dev where yr=&yr;
quit;

%macro addgq;
%if &gqcheck >0 %then
%do;

data gq_add_1(drop=yr gq i);set e1.gq_dev;where yr=&yr;
do i=1 to gq;
	output;
end;
run;

data gq_add_1;set gq_add_1;
i+1;
run;

proc sort data=gq_add_1;by gq_type;run;

data gq_add_1;set gq_add_1;by gq_type;retain j;
if first.gq_type then j=1;else j=j+1;
run;

proc sql;
create table gq_next_2a as select * from gq_next_2
order by gq_type,ranuni(&yr+12);
quit;

data gq_next_2b;set gq_next_2a;by gq_type;retain j;
if first.gq_type then j=1;else j=j+1;
run;

proc sql;
create table gq_add_2 as select
x.i + (select max(gq_id) from gq_base) as gq_id length=5 format=8.
,x.gq_type
,x.jur,x.ct,x.cpa,x.mgra
,y.age,y.r,y.r7,y.hisp,y.sex,y.dob

from gq_add_1 as x
inner join gq_next_2b as y on x.gq_type=y.gq_type and x.j=y.j;
quit;

data gq_next_3;set gq_next_2(drop=/*r7*/ age101) gq_add_2;run;
%put ------------------- GQ additions for year &yr;
%end;


%else %do;

data gq_next_3;set gq_next_2(drop=/*r7*/ age101);run;
data gq_add_2;set gq_next_3(obs=0);run;

%put ------------------- NO GQ additions for year &yr;
%end;

%mend addgq;

%addgq;

%let t=%sysfunc(time(),time8.0);
%put Finished GQ Section for year &yr at &t;

/*------------------------------------*/

/* dof_3 is based on dof_jur_update */
proc sql;
create table dof_4 as select sum(hp_dof) as hp_dof,sum(hh_dof) as hh_dof
from dof_3 where est_yr=&yrn;
quit;


proc sql;
create table dof_5c as select r7,age101,sex,hp_cest as hp_dof
from hp_cest_1;
quit;


proc sql;
create table sandag_hp_0 as select
r7
,case 
when age<=100 then age else 100 end as age101 length=3
,sex
,count(x.hp_id) as hp
from hp_base as x inner join hh_base as y on x.hh_id=y.hh_id
where x.age>14 
group by r7,age101,sex;

create table sandag_hh_0 as select
r7
,case 
when age<=100 then age else 100 end as age101 length=3
,sex
,case when size>10 then 10 else size end as size
,count(x.hp_id) as hh
from hp_base as x inner join hh_base as y on x.hh_id=y.hh_id
where x.role="H" and x.age>14
group by r7,age101,sex,calculated size;

create table sandag_hh_1 as select r7,age101,sex,sum(hh) as hh from sandag_hh_0
group by r7,age101,sex;

create table sandag_hh_1s as select r7,age101,sex,size,sum(hh) as hh from sandag_hh_0
group by r7,age101,sex,size;
quit;

data size10;do size=1 to 10;length size 3;output;end;run;

proc sql;
create table test_hh_01 as select sex,age101,sum(hh) as hh from sandag_hh_0
where age101<=20 group by sex,age101;
quit;


proc sql;
create table sandag_hh_2 as select x.*,z.size,coalesce(y.hh,0) as hh
,coalesce(y.hh,0)/x.hp as hhr /* household headship rate */
from sandag_hp_0 as x
cross join size10 as z
left join sandag_hh_0 as y on x.r7=y.r7 and x.age101=y.age101 and x.sex=y.sex and y.size=z.size
where x.age101>19 
order by r7,age101,sex,size;

create table sandag_hh_2a as select * from sandag_hh_2 where age101>99;

/* removing households headed by persons 100 years and older */
update sandag_hh_2 set hh=0,hhr=0 where hh>0 and age101>99;
quit;

proc sql;
create table dof_6 as select x.r7,x.age101,x.sex,x.hp_dof
,y.size,coalesce(y.hhr,0) as hhr
,round(x.hp_dof * coalesce(y.hhr,0),1) as hh0
from dof_5c as x
inner join sandag_hh_2 as y
on x.r7=y.r7 and x.age101=y.age101 and x.sex=y.sex
order by r7,age101,sex,size;

create table dof_7 as select x.r7,x.age101,x.sex,x.size,x.hp_dof,y.hh_dof,x.hh0
,int(x.s * y.hh_dof) as hh1
from (select *,hh0/sum(hh0) as s,sum(hh0) as sum_hh0 from dof_6) as x, dof_4 as y
order by hh1;
quit;

data dof_8;set dof_7;hhc+hh1;run;
proc sort data=dof_8;by descending hh1;run;
data dof_8;set dof_8;i+1;run;

proc sql;
create table dof_8a as select max(hh_dof)-max(hhc) as d from dof_8;

/* hh2 matches the number of DOF's households; however, hp2 may not */
create table dof_9 as select x.r7,x.age101,x.sex,x.size,x.i,x.hh1
,case
when (y.d - int(y.d/2)*2) > 0 and x.i = 1 then x.hh1+3
when (y.d - int(y.d/2)*2) > 0 and x.i <= int(y.d/2) then x.hh1+2
when (y.d - int(y.d/2)*2) = 0 and x.i <= int(y.d/2) then x.hh1+2
else x.hh1 end as hh2

,calculated hh2 * x.size as hp2
from dof_8 as x
cross join dof_8a as y
order by i;

create table dof_10 as select size,sum(hh2) as hh2,sum(hp2) as hp2
from dof_9 group by size;

/* if hpd>0 then more hp is needed (increase the size of some households)
if hpd<0 then less hp is needed (decrease the size of some households) */

create table dof_10a as select x.*,y.hp_dof,y.hp_dof-x.hp2 as hpd
from (select sum(hp2) as hp2 from dof_10) as x
cross join dof_4 as y;
quit;

%macro sizeadj;
proc sql noprint;
select hpd into :a from dof_10a;
quit;

proc sort data=dof_9;by r7 age101 sex size;run;

data dof_11(drop=hh2 i);set dof_9(drop=hp2);by r7 age101 sex size;do i=1 to hh2;output;end;run;

%if &a>=0 %then
	%do;
		proc sql;
			create table dof_12 as select *, case when size<6 then ranuni(&yr) else 2 end as rn
			from dof_11 order by rn;

			create table dof_12a as select *,monotonic() as i from dof_12;

			create table dof_13 as select r7,age101,sex,case when i<=&a then size+1 else size end as size from dof_12a;
		quit;
	%end;

%else
	%do;
		proc sql;
			create table dof_12 as select *, case when size>2 then ranuni(&yr) else 2 end as rn
			from dof_11 order by rn;

			create table dof_12a as select *,monotonic() as i from dof_12;

			create table dof_13 as select r7,age101,sex,case when i<=abs(&a) then size-1 else size end as size from dof_12a;
		quit;
	%end;

proc sql;
create table dof_14 as select x.r7,x.age101,x.sex,y.size,coalesce(z.hh,0) as hh
from (select distinct r7,age101,sex from dof_13) as x
cross join size10 as y
left join (select r7,age101,sex,size,count(*) as hh from dof_13 group by r7,age101,sex,size) as z
on x.r7=z.r7 and x.age101=z.age101 and x.sex=z.sex and y.size=z.size;

create table dof_14a as select x.*,y.*,z.*
from (select sum(hh) as hh,sum(size*hh) as hp from dof_14) as x
cross join dof_4 as y
cross join (select sum(hp_dof) as hp_dof2 from dof_5c) as z;
quit;

%mend sizeadj;

%sizeadj;

proc sql;
create table ztest_1 as select x.*,y.*,x.hp1 - y.hp2 as hp_d
from (select sum(hp_dof) as hp1 from dof_5c) as x
cross join (select sum(size*hh) as hp2,sum(hh) as hh from dof_14) as y;

/* this table should have zero records */
create table ztest_1a as select * from ztest_1 where hp_d^=0;

create table ztest_2 as select x.r7,x.age101,x.sex,x.hp_dof as hp,y.hh
from dof_5c as x
left join (select r7,age101,sex,sum(hh) as hh from dof_14 group by r7,age101,sex) as y
on x.r7=y.r7 and x.age101=y.age101 and x.sex=y.sex;

/* this table should have zero records */
create table ztest_3 as select * from ztest_2 where hh>hp;
quit;

/*
dof_5c contain the target number of hp by cohort (race/age)
dof_14 contains the target number of households (by size) headed by people (by race/age cohort)
*/

proc sql;
create table sandag_hp_1 as select *
,case 
when age<=100 then age else 100 end as age101 length=3
from hp_next_4;

create table sandag_hp_2 as select x.*,coalesce(y.hh,0) as hh
from (select r7,age101,sex,count(hp_id) as hp from sandag_hp_1 group by r7,age101,sex) as x
left join (select r7,age101,sex,count(hh_id) as hh from sandag_hp_1 where role="H" group by r7,age101,sex) as y
on x.r7=y.r7 and x.age101=y.age101 and x.sex=y.sex;

create table sandag_hp_2a as select x.*
,coalesce(y.hp_dof,0) as hp_dof
,coalesce(z.hh_dof,0) as hh_dof
,coalesce(y.hp_dof,0) - x.hp as hp_d1
,coalesce(z.hh_dof,0) - x.hh as hh_d1
,case
when calculated hp_d1<0 then max(abs(calculated hp_d1),abs(calculated hh_d1))
when calculated hh_d1>0 and calculated hh_d1 > calculated hp_d1 then calculated hh_d1 - calculated hp_d1
else 0 end as hp_d

from sandag_hp_2 as x
left join dof_5c as y on x.r7=y.r7 and x.age101=y.age101 and x.sex=y.sex
left join (select r7,age101,sex,sum(hh) as hh_dof from dof_14 group by r7,age101,sex) as z
on x.r7=z.r7 and x.age101=z.age101 and x.sex=z.sex;

create table sandag_hp_2b as select *
from sandag_hp_2a where hp_d1 < hh_d1 and hh_d1 > 0;

create table sandag_hp_2c as select * from sandag_hp_2a where hp_d > 0;

create table sandag_hp_3 as select x.*,y.hp_d
from sandag_hp_1 as x left join sandag_hp_2a as y on x.r7=y.r7 and x.age101=y.age101 and x.sex=y.sex
order by r7,age101,sex,ranuni(&yr+1);
quit;

data sandag_hp_3;set sandag_hp_3;by r7 age101 sex;retain i;
if first.sex then i=1;else i=i+1;run;



proc sql;
/* 1st round of removals; removing excess people (and other people in their households) */
create table hh_rem_1 as select distinct hh_id from sandag_hp_3 where i<=hp_d;

create table hp_rem_1 as select x.*
from sandag_hp_3(drop=hp_d i) as x inner join hh_rem_1 as y on x.hh_id=y.hh_id;

create table hp_new_1 as select x.*
from sandag_hp_3(drop=hp_d i) as x left join hh_rem_1 as y on x.hh_id=y.hh_id
where y.hh_id=.;

create table hh_new_1 as select x.*,y.size
from (select hh_id,hp_id,r,hisp,sex,age,dob,role,r7,age101 from hp_new_1 where role="H") as x
left join (select hh_id,count(hp_id) as size from hp_new_1 group by hh_id) as y
on x.hh_id=y.hh_id
order by r7,age101,size;

create table hh_new_1a as select r7,age101,sex,size,count(hh_id) as hh
from hh_new_1 group by r7,age101,sex,size;

create table hh_new_1b as select x.*,coalesce(y.hh,0) as hh_dof
,case when x.hh>coalesce(y.hh,0) then x.hh-coalesce(y.hh,0) else 0 end as hh_d
from hh_new_1a as x left join dof_14 as y on x.r7=y.r7 and x.age101=y.age101 and x.sex=y.sex and x.size=y.size;

create table hh_new_1c as select x.*,y.hh_d
from hh_new_1 as x left join hh_new_1b as y on x.r7=y.r7 and x.age101=y.age101 and x.sex=y.sex and x.size=y.size
order by r7,age101,sex,size,ranuni(&yr+2);
quit;

data hh_new_1c;set hh_new_1c;by r7 age101 sex size;retain i;
if first.size then i=1;else i=i+1;run;

proc sql;
/* 2nd round of removals; removing excess households (and people in them) */
create table hh_rem_2 as select hh_id from hh_new_1c where i<=hh_d;

create table hp_rem_2 as select x.*
from hp_new_1 as x inner join hh_rem_2 as y on x.hh_id=y.hh_id;

create table hp_new_2 as select x.*
from hp_new_1 as x left join hh_rem_2 as y on x.hh_id=y.hh_id
where y.hh_id=.;

create table hh_new_2 as select x.*
from hh_new_1 as x left join hh_rem_2 as y on x.hh_id=y.hh_id
where y.hh_id=.;
quit;


proc sql;
create table sandag_hp_3a as select x.*
,coalesce(y.hh_dof,0) as hh_dof
,coalesce(z.hp,0) as hp
,coalesce(w.hh,0) as hh
,x.hp_dof - calculated hp as hp_d
,calculated hh_dof - calculated hh as hh_d
,case when calculated hh_d > calculated hp_d then calculated hh_d - calculated hp_d else 0 end as d
from dof_5c as x
left join (select r7,age101,sex,sum(hh) as hh_dof from dof_14 group by r7,age101,sex) as y
	on x.r7=y.r7 and x.age101=y.age101 and x.sex=y.sex
left join (select r7,age101,sex,count(hp_id) as hp from hp_new_2 group by r7,age101,sex) as z
	on x.r7=z.r7 and x.age101=z.age101 and x.sex=z.sex
left join (select r7,age101,sex,count(hh_id) as hh from hh_new_2 group by r7,age101,sex) as w
	on x.r7=w.r7 and x.age101=w.age101 and x.sex=w.sex;

create table sandag_hp_3b as select * from sandag_hp_3a where d>0;
/* this identifies instances when the need for additional heads (hh_d) is greater than the need for additional hp (hp_d)
d is the number of people to be removed; however, none of the people should be household heads*/
quit;


proc sql;
/* selecting prohibited heads */
create table hh_list_1 as select x.hh_id
from hp_new_2 as x
inner join sandag_hp_3b as y on x.r7=y.r7 and x.age101=y.age101 and x.sex=y.sex
where x.role="H";

create table hp_list_1 as select x.*,z.d
from hp_new_2 as x
left join hh_list_1 as y on x.hh_id=y.hh_id
inner join sandag_hp_3b as z on x.r7=z.r7 and x.age101=z.age101 and x.sex=z.sex
where y.hh_id=.
order by r7,age101,sex,ranuni(&yr+3);
quit;

data hp_list_1;set hp_list_1;by r7 age101 sex;retain i;
if first.sex then i=1;else i=i+1;run;

proc sql;
create table hh_rem_3 as select distinct hh_id from hp_list_1 where i<=d;

create table hp_rem_3 as select x.*
from hp_new_2 as x inner join hh_rem_3 as y on x.hh_id=y.hh_id;

create table hp_new_3 as select x.*
from hp_new_2 as x left join hh_rem_3 as y on x.hh_id=y.hh_id
where y.hh_id=.
order by r7,age101,sex,ranuni(&yr+4);

create table hh_new_3 as select x.*
from hh_new_2 as x left join hh_rem_3 as y on x.hh_id=y.hh_id
where y.hh_id=.
order by r7,age101,sex,ranuni(&yr+5);
quit;

data hp_new_3;set hp_new_3;by r7 age101 sex;retain i;
if first.sex then i=1; else i=i+1;
run;

data hp_rem_4;set hp_rem_1 hp_rem_2 hp_rem_3;rn=ranuni(&yr+6);run;
proc sort data=hp_rem_4;by r7 age101 sex rn;run;

data hp_rem_4;set hp_rem_4;by r7 age101 sex;retain i;
if first.sex then i=1; else i=i+1;
run;


proc sql;
create table dof_15 as select x.*
,coalesce(y.hh_dof,0) as hh_dof
,coalesce(z.hp_new,0) as hp_old
,coalesce(w.hh_new,0) as hh_old
,coalesce(u.hp_avl,0) as hp_avl
,x.hp_dof - coalesce(z.hp_new,0) as hp_need
,coalesce(y.hh_dof,0) - coalesce(w.hh_new,0) as hh_need
,case
when calculated hp_need > coalesce(u.hp_avl,0) then coalesce(u.hp_avl,0)
else calculated hp_need end as use_avl
,case
when calculated hp_need > coalesce(u.hp_avl,0) then calculated hp_need - coalesce(u.hp_avl,0)
else 0 end as hp_clone
from dof_5c as x
left join (select r7,age101,sex,sum(hh) as hh_dof from dof_14 group by r7,age101,sex) as y
	on x.r7=y.r7 and x.age101=y.age101 and x.sex=y.sex
left join (select r7,age101,sex,count(hp_id) as hp_new from hp_new_3 group by r7,age101,sex) as z
	on x.r7=z.r7 and x.age101=z.age101 and x.sex=z.sex
left join (select r7,age101,sex,count(hh_id) as hh_new from hp_new_3 where role="H" group by r7,age101,sex) as w
	on x.r7=w.r7 and x.age101=w.age101 and x.sex=w.sex
left join (select r7,age101,sex,count(hp_id) as hp_avl from hp_rem_4 group by r7,age101,sex) as u
	on x.r7=u.r7 and x.age101=u.age101 and x.sex=u.sex;

/* this table should have zero records */
create table dof_15a as select * from dof_15 where hh_need>hp_need;

create table dof_15_sum as select
sum(hp_dof) as hp_dof
,sum(hh_dof) as hh_dof
,sum(hh_old) as hh_old
,sum(hh_need) as hh_need
,sum(hp_old) as hp_old
,sum(use_avl) as use_avl
,sum(hp_clone) as hp_clone
,sum(use_avl) + sum(hp_clone) as hp_added
,sum(hp_dof) - sum(hp_old) - sum(use_avl) - sum(hp_clone) as t
from dof_15;
quit;


proc sql;
create table hp_clone_1 as select r7,age101,sex,hp_clone,hp_old as mx from dof_15 where hp_clone>0;

create table hp_clone_1a as select * from hp_clone_1 where hp_clone>0 and mx=0;
quit;



data hp_clone_2;set hp_clone_1;
do i=1 to hp_clone;
	id = ceil(mx * ranuni(i));
	output;
end;
run;

proc sql;
create table hp_clone_3 as select x.r7,x.age101,x.sex
,y.r,y.hisp,y.age,y.dob
from hp_clone_2 as x
left join hp_new_3 as y on x.r7=y.r7 and x.age101=y.age101 and x.sex=y.sex and x.i=y.i;

create table hp_clone_3a as select * from hp_clone_3 where age=.;

update hp_clone_3 set age=age101,r="R02",hisp="NH",dob=mdy(12,10,&yr-age101) where age=. and r7="B";
update hp_clone_3 set age=age101,r="R03",hisp="NH",dob=mdy(12,11,&yr-age101) where age=. and r7="I";
update hp_clone_3 set age=age101,r="R07",hisp="NH",dob=mdy(12,12,&yr-age101) where age=. and r7="M";
update hp_clone_3 set age=age101,r="R05",hisp="NH",dob=mdy(12,13,&yr-age101) where age=. and r7="P";
update hp_clone_3 set age=age101,r="R04",hisp="NH",dob=mdy(12,14,&yr-age101) where age=. and r7="S";
update hp_clone_3 set age=age101,r="R10",hisp="NH",dob=mdy(12,15,&yr-age101) where age=. and r7="W";
update hp_clone_3 set age=age101,r="R11",hisp="H",dob=mdy(12,16,&yr-age101) where age=. and r7="H";

/* this table should have zero records */
create table hp_clone_3b as select * from hp_clone_3 where age=.;
quit;

/* not all records from hp_rem_4 are needed; take only as much as needed (use_avl in dof_15) */
proc sql;
create table hp_avl_0 as select x.*
from hp_rem_4 as x inner join dof_15 as y
on x.r7=y.r7 and x.age101=y.age101 and x.sex=y.sex and x.i<=y.use_avl;

create table hp_pool_0 as 
select * from hp_clone_3
	union all
select r7,age101,sex,r,hisp,age,dob from hp_avl_0;

create table hp_pool_0_ as select r7,age101,sex,r,hisp,age length=3,dob length=4
from hp_pool_0;

create table hp_pool_0 as select * from hp_pool_0_
order by r7,age101,sex,ranuni(&yr+7);

drop table hp_pool_0_;
quit;


data hp_pool_0;set hp_pool_0;by r7 age101 sex;length pid 5;retain i;
pid+1;
if first.sex then i=1;else i=i+1;
run;


proc sql;
create table dof_14b as select x.*,coalesce(y.hh_old,0) as hh_old,x.hh - coalesce(y.hh_old,0) as hh_new
from dof_14 as x
left join (select r7,age101,sex,size,count(hh_id) as hh_old from hh_new_3 group by r7,age101,sex,size) as y
on x.r7=y.r7 and x.age101=y.age101 and x.sex=y.sex and x.size=y.size;

create table dof_14b_ as select * from dof_14b where hh_new<0;

create table dof_14_sum as select sum(hh_new) as hh_new,sum(hh_new*size) as hp_new
,sum(hh_new*size) - sum(hh_new) as hm_new
from dof_14b;
quit;


data dof_14c;set dof_14b;by r7 age101 sex size;retain a b;where hh_new>0;
if first.sex then do;a=1;b=hh_new;end;
else do;a=b+1;b=a+hh_new-1;end;
run;

proc sql;
create table ztest_01 as select x.*,y.pid
from (select r7,age101,sex,sum(hh_new) as hh from dof_14c group by r7,age101,sex) as x
left join (select r7,age101,sex,count(pid) as pid from hp_pool_0 group by r7,age101,sex) as y
on x.r7=y.r7 and x.age101=y.age101 and x.sex=y.sex;

/* this table should have zero records */
create table ztest_02 as select * from ztest_01 where hh>pid;

create table hp_pool_1 as select x.r7,x.age101,x.r,x.hisp,x.sex,x.age,x.dob,x.pid
,case when y.size^=. then "H" else "M" end as role
,y.size
,case
when x.age101<=4 then "00_04"
when x.age101<=9 then "05_09"
when x.age101<=14 then "10_14"
when x.age101<=19 then "15_19"
when x.age101<=24 then "20_24"
when x.age101<=29 then "25_29"
when x.age101<=34 then "30_34"
when x.age101<=39 then "35_39"
when x.age101<=44 then "40_44"
when x.age101<=49 then "45_49"
when x.age101<=54 then "50_54"
when x.age101<=59 then "55_59"
when x.age101<=64 then "60_64"
when x.age101<=69 then "65_69"
when x.age101<=74 then "70_74"
when x.age101<=79 then "75_79"
when x.age101<=84 then "80_84" else "85_99" end as age18 length=5
from hp_pool_0 as x left join dof_14c as y on x.r7=y.r7 and x.age101=y.age101 and x.sex=y.sex and y.a<=x.i<=y.b
order by pid;

create table hp_pool_1a as select size,count(*) as hh,sum(size) as hp,sum(size-1) as hm
from hp_pool_1 where role="H" group by size;

create table hp_pool_1b as select x.*,y.*
from (select sum(hh) as hh, sum(hp) as hp,sum(hm) as hm1 from hp_pool_1a) as x
cross join (select count(*) as hm2 from hp_pool_1 where role="M") as y;

/* this table should have zero records */
create table hp_pool_1c as select * from hp_pool_1 where role="";

create table dof_16 as select x.*
,coalesce(y.hh_dof,0) as hh_dof
,coalesce(z.hp_new,0) as hp_new
,coalesce(w.hh_new,0) as hh_new
,x.hp_dof - coalesce(z.hp_new,0) as hp_need
,coalesce(u.hp_cloned,0) as hp_cloned
,coalesce(y.hh_dof,0) - coalesce(w.hh_new,0) as hh_need
,coalesce(v.hh_cloned,0) as hh_cloned
from dof_5c as x
left join (select r7,age101,sum(hh) as hh_dof from dof_14 group by r7,age101) as y on x.r7=y.r7 and x.age101=y.age101
left join (select r7,age101,count(hp_id) as hp_new from hp_new_3 group by r7,age101) as z on x.r7=z.r7 and x.age101=z.age101
left join (select r7,age101,count(hh_id) as hh_new from hp_new_3 where role="H" group by r7,age101) as w on x.r7=w.r7 and x.age101=w.age101
left join (select r7,age101,count(*) as hp_cloned from hp_pool_1 group by r7,age101) as u on x.r7=u.r7 and x.age101=u.age101
left join (select r7,age101,count(*) as hh_cloned from hp_pool_1 where role="H" group by r7,age101) as v on x.r7=v.r7 and x.age101=v.age101;
quit;

/*
create configuration for the members (given householder's sex/age18/size, distribution of members
into 16 classes

set size=1 aside
*/


proc sql;
create table config_0 as select x.hh_id,x.hp_id
,r7
,case 
when x.age<=4 then "00_04"
when x.age<=9 then "05_09"
when x.age<=14 then "10_14"
when x.age<=19 then "15_19"
when x.age<=24 then "20_24"
when x.age<=29 then "25_29"
when x.age<=34 then "30_34"
when x.age<=39 then "35_39"
when x.age<=44 then "40_44"
when x.age<=49 then "45_49"
when x.age<=54 then "50_54"
when x.age<=59 then "55_59"
when x.age<=64 then "60_64"
when x.age<=69 then "65_69"
when x.age<=74 then "70_74"
when x.age<=79 then "75_79"
when x.age<=84 then "80_84" else "85_99" end as age18 length=5
,case 
when x.age<=9 then "00_09"
when x.age<=19 then "10_19"
when x.age<=29 then "20_29"
when x.age<=39 then "30_39"
when x.age<=49 then "40_49"
when x.age<=59 then "50_59"
when x.age<=69 then "60_69"
when x.age<=79 then "70_79" else "80_99" end as age9 length=5
,x.sex
,x.role
,y.size
from hp_base as x inner join hh_base as y on x.hh_id=y.hh_id
where (x.role="M" or x.age>19) and y.size<=10;

create table config_1 as select x.hh_id,y.hp_id,x.r7 as hh_r7,x.age18 as hh_age18,x.sex as hh_sex,x.size
,case when y.age9 in ("00_09","10_19") then "C"||y.age9 else y.sex||y.age9 end as hm_cat
from config_0 as x inner join config_0 as y
on x.hh_id=y.hh_id
where x.size>1 and x.role="H" and y.role="M"
order by hh_id,hp_id;

create table config_2 as select hh_id,hh_r7,hh_age18,hh_sex,size,hm_cat
,count(hp_id) as p
from config_1 group by hh_id,hh_r7,hh_age18,hh_sex,size,hm_cat;
quit;

proc transpose data=config_2 out=config_2a(drop=_name_);by hh_id hh_r7 hh_age18 hh_sex size;var p;id hm_cat;run;
proc transpose data=config_2a out=config_2b;by hh_id hh_r7 hh_age18 hh_sex size;run;
data config_2b;set config_2b;if col1=. then col1=0;run;
proc transpose data=config_2b out=config_3(drop=_name_);by hh_id hh_r7 hh_age18 hh_sex size;var col1;id _name_;run;

proc sql;
create table config_4_rsa as select hh_r7,hh_age18,hh_sex,size
,C00_09,C10_19
,M20_29,M30_39,M40_49,M50_59,M60_69,M70_79,M80_99
,F20_29,F30_39,F40_49,F50_59,F60_69,F70_79,F80_99
,count(hh_id) as n
from config_3 group by hh_r7,hh_age18,hh_sex,size
,C00_09,C10_19
,M20_29,M30_39,M40_49,M50_59,M60_69,M70_79,M80_99
,F20_29,F30_39,F40_49,F50_59,F60_69,F70_79,F80_99
order by hh_r7,hh_age18,hh_sex,size,n desc;

create table config_4_sa as select hh_age18,hh_sex,size
,C00_09,C10_19
,M20_29,M30_39,M40_49,M50_59,M60_69,M70_79,M80_99
,F20_29,F30_39,F40_49,F50_59,F60_69,F70_79,F80_99
,count(hh_id) as n
from config_3 group by hh_age18,hh_sex,size
,C00_09,C10_19
,M20_29,M30_39,M40_49,M50_59,M60_69,M70_79,M80_99
,F20_29,F30_39,F40_49,F50_59,F60_69,F70_79,F80_99
order by hh_age18,hh_sex,size,n desc;

create table config_4_a as select hh_age18,size
,C00_09,C10_19
,M20_29,M30_39,M40_49,M50_59,M60_69,M70_79,M80_99
,F20_29,F30_39,F40_49,F50_59,F60_69,F70_79,F80_99
,count(hh_id) as n
from config_3 group by hh_age18,size
,C00_09,C10_19
,M20_29,M30_39,M40_49,M50_59,M60_69,M70_79,M80_99
,F20_29,F30_39,F40_49,F50_59,F60_69,F70_79,F80_99
order by hh_age18,size,n desc;

create table config_id_1 as select x.*,monotonic() as c_id
from (select distinct C00_09,C10_19
,M20_29,M30_39,M40_49,M50_59,M60_69,M70_79,M80_99
,F20_29,F30_39,F40_49,F50_59,F60_69,F70_79,F80_99 from config_4_rsa) as x;
quit;

proc transpose data=config_id_1 out=config_id_1a;by c_id;run;

proc sql;
create table config_id_2_orig as select c_id,_name_ as hm_cat,col1 as p from config_id_1a where col1>0;

create table config_5_rsa as select x.*,y.c_id,x.n/sum(x.n) as p
from config_4_rsa as x
inner join config_id_1 as y on 
x.C00_09 = y.C00_09
and x.C10_19 = y.C10_19
and x.M20_29 = y.M20_29
and x.M30_39 = y.M30_39
and x.M40_49 = y.M40_49
and x.M50_59 = y.M50_59
and x.M60_69 = y.M60_69
and x.M70_79 = y.M70_79
and x.M80_99 = y.M80_99
and x.F20_29 = y.F20_29
and x.F30_39 = y.F30_39
and x.F40_49 = y.F40_49
and x.F50_59 = y.F50_59
and x.F60_69 = y.F60_69
and x.F70_79 = y.F70_79
and x.F80_99 = y.F80_99
group by hh_r7,hh_age18,hh_sex,size
order by hh_r7,hh_age18,hh_sex,size,n desc;

create table config_5_sa as select x.*,y.c_id,x.n/sum(x.n) as p
from config_4_sa as x inner join config_id_1 as y on 
x.C00_09 = y.C00_09
and x.C10_19 = y.C10_19
and x.M20_29 = y.M20_29
and x.M30_39 = y.M30_39
and x.M40_49 = y.M40_49
and x.M50_59 = y.M50_59
and x.M60_69 = y.M60_69
and x.M70_79 = y.M70_79
and x.M80_99 = y.M80_99
and x.F20_29 = y.F20_29
and x.F30_39 = y.F30_39
and x.F40_49 = y.F40_49
and x.F50_59 = y.F50_59
and x.F60_69 = y.F60_69
and x.F70_79 = y.F70_79
and x.F80_99 = y.F80_99
group by hh_age18,hh_sex,size
order by hh_age18,hh_sex,size,n desc;

create table config_5_a as select x.*,y.c_id,x.n/sum(x.n) as p
from config_4_a as x inner join config_id_1 as y on 
x.C00_09 = y.C00_09
and x.C10_19 = y.C10_19
and x.M20_29 = y.M20_29
and x.M30_39 = y.M30_39
and x.M40_49 = y.M40_49
and x.M50_59 = y.M50_59
and x.M60_69 = y.M60_69
and x.M70_79 = y.M70_79
and x.M80_99 = y.M80_99
and x.F20_29 = y.F20_29
and x.F30_39 = y.F30_39
and x.F40_49 = y.F40_49
and x.F50_59 = y.F50_59
and x.F60_69 = y.F60_69
and x.F70_79 = y.F70_79
and x.F80_99 = y.F80_99
group by hh_age18,size
order by hh_age18,size,n desc;
quit;


/* a: lower bound, b: upper bound */
data config_6_rsa;set config_5_rsa(keep= hh_r7 hh_age18 hh_sex size c_id p);by hh_r7 hh_age18 hh_sex size;retain b;
if first.size then do;a=0;b=p;end;
else do;a=b;b=b+p;end;
run;

data config_6_sa;set config_5_sa(keep= hh_age18 hh_sex size c_id p);by hh_age18 hh_sex size;retain b;
if first.size then do;a=0;b=p;end;
else do;a=b;b=b+p;end;
run;

data config_6_a;set config_5_a(keep= hh_age18 size c_id p);by hh_age18 size;retain b;
if first.size then do;a=0;b=p;end;
else do;a=b;b=b+p;end;
run;


proc sql;
create table hm_pool_0_orig as select *
,case
when age<=9 then "C00_09"
when age<=19 then "C10_19"
when age<=29 then sex||"20_29"
when age<=39 then sex||"30_39"
when age<=49 then sex||"40_49"
when age<=59 then sex||"50_59"
when age<=69 then sex||"60_69"
when age<=79 then sex||"70_79" else sex||"80_99" end as hm_cat length=6
from hp_pool_1(drop=size) where role="M"
order by r7,hm_cat;

create table hh_pool_0_orig as select *,monotonic() as hid length=5 from hp_pool_1 where role="H";

create table hh_done_size_1 as select * from hh_pool_0_orig where size=1;

delete from hh_pool_0_orig where size=1;
quit;

%macro conf1 (m=);

%let ts=%sysfunc(time(),time8.0);

proc sql noprint;
create table hh_pool_0 as select * from hh_pool_0_orig;
create table hm_pool_0 as select * from hm_pool_0_orig;
create table config_id_2 as select * from config_id_2_orig;
quit;

proc datasets library=work nolist; delete hh_done_1 hm_done_1 hh_noconfig_0;quit;

%let t=0; /* t is a counter */

%do %while(&t<=&m);

%let t=%eval(&t+1);

data hm_pool_1;set hm_pool_0;by r7 hm_cat;retain i1;
if first.hm_cat then i1=1;else i1=i1+1;run;

proc sort data=hm_pool_1;by hm_cat;run;

data hm_pool_1;set hm_pool_1;by hm_cat;retain i2;
if first.hm_cat then i2=1;else i2=i2+1;run;

/* i1 is indexing by r7 and hm_cat; i2 is indexing just by hm_cat (irrespective of race) */

proc sql noprint;
select count(*) into :hh1 from hh_pool_0; 

create table hm_cat_i1 as select r7,hm_cat,max(i1) as maxi1 from hm_pool_1 group by r7,hm_cat;
create table hm_cat_i2 as select hm_cat,max(i2) as maxi2 from hm_pool_1 group by hm_cat;

create table config_id_3 as select x.*
,case
when y.hm_cat="" then 0
when x.p>y.maxi2 then 0 else 1 end as flag1
from config_id_2 as x
left join hm_cat_i2 as y on x.hm_cat=y.hm_cat;

create table config_id_3a as select c_id,count(c_id) as n1,sum(flag1) as n2
from config_id_3 group by c_id having calculated n1=calculated n2;
/* this ensures that the selected configuration 
1. have cohort that exists (this is accomplished by x.hm_cat=y.hm_cat)
2. exclude configurations that require more people of a specific cohort than what's available (this is accomplished by x.p<=y.maxi2)
*/

create table config_id_4 as select x.*
from config_id_3(drop=flag1) as x
inner join config_id_3a as y on x.c_id=y.c_id;

create table config_06_a as select x.hh_age18,x.size,x.c_id,x.p
from config_6_a as x
inner join (select distinct c_id from config_id_4) as y on x.c_id=y.c_id
order by hh_age18,size,p desc;

create table config_06_sa as select x.hh_age18,x.hh_sex,x.size,x.c_id,x.p
from config_6_sa as x
inner join (select distinct c_id from config_id_4) as y on x.c_id=y.c_id
order by hh_age18,hh_sex,size,p desc;

create table config_06_rsa as select x.hh_r7,x.hh_age18,x.hh_sex,x.size,x.c_id,x.p
from config_6_rsa as x
inner join (select distinct c_id from config_id_4) as y on x.c_id=y.c_id
order by hh_r7,hh_age18,hh_sex,size,p desc;
quit;

data config_06_a;set config_06_a;by hh_age18 size;retain i;
if first.size then i=1;else i=i+1;run;

data config_06_sa;set config_06_sa;by hh_age18 hh_sex size;retain i;
if first.size then i=1;else i=i+1;run;

data config_06_rsa;set config_06_rsa;by hh_r7 hh_age18 hh_sex size;retain i;
if first.size then i=1;else i=i+1;run;

proc sql noprint;
create table config_7_a as select hh_age18,size,c_id,p/sum(p) as p
from config_06_a where i<=20 /* selecting top 20 most frequent configuration */
group by hh_age18,size;

create table config_7_sa as select hh_age18,hh_sex,size,c_id,p/sum(p) as p
from config_06_sa where i<=20 /* selecting top 20 most frequent configuration */
group by hh_age18,hh_sex,size;

create table config_7_rsa as select hh_r7,hh_age18,hh_sex,size,c_id,p/sum(p) as p
from config_06_rsa where i<=20 /* selecting top 20 most frequent configuration */
group by hh_r7,hh_age18,hh_sex,size;
quit;

data config_8_a;set config_7_a;by hh_age18 size;retain b;
if first.size then do;a=0;b=p;end;
else do;a=b;b=b+p;end;
run;

data config_8_sa;set config_7_sa;by hh_age18 hh_sex size;retain b;
if first.size then do;a=0;b=p;end;
else do;a=b;b=b+p;end;
run;

data config_8_rsa;set config_7_rsa;by hh_r7 hh_age18 hh_sex size;retain b;
if first.size then do;a=0;b=p;end;
else do;a=b;b=b+p;end;
run;

data hh_pool_1;set hh_pool_0(keep=r7 age18 sex hid size);
rn=ranuni(&t);
hid2=ceil(1000000 * rn);
run;

proc sql noprint;
/* get configuration */
create table hh_pool_2(drop=rn) as select x.*, coalesce(y.c_id,v.c_id,u.c_id,0) as c_id
from hh_pool_1 as x
left join config_8_rsa as y
on x.r7=y.hh_r7 and x.age18=y.hh_age18 and x.sex=y.hh_sex and x.size=y.size and y.a <= x.rn <= y.b
left join config_8_sa as v
on x.age18=v.hh_age18 and x.sex=v.hh_sex and x.size=v.size and v.a <= x.rn <= v.b
left join config_8_a as u
on x.age18=u.hh_age18 and x.size=u.size and u.a <= x.rn <= u.b

order by c_id;

select count(*) into :c1 from hh_pool_2 where c_id=0;

create table hh_pool_2a as select r7,age18,sex,size,hid,&t as cycle
from hh_pool_2 where c_id=0;

delete from hh_pool_2 where c_id=0;

/* get composition */
create table hh_pool_3 as select x.*,z.hm_cat,z.p
from hh_pool_2 as x
left join config_id_3 as z on x.c_id=z.c_id
order by hid,hm_cat;
quit;

%if &c1>0 %then %do;
	proc append base=hh_noconfig_0 data=hh_pool_2a;run;
%end;

/* j is slot inside a household (for a hm_cat)*/
data hh_pool_4(drop=p j);set hh_pool_3;do j=1 to p;output;end;run;

proc sql noprint;
/* get index for members */
create table hh_pool_5 as select x.*
,ceil(y.maxi1 * ranuni(&yr+8)) as i1
,ceil(z.maxi2 * ranuni(&yr+9)) as i2
from hh_pool_4 as x
left join hm_cat_i1 as y on x.r7=y.r7 and x.hm_cat=y.hm_cat
left join hm_cat_i2 as z on x.hm_cat=z.hm_cat;

/* get pid for specific members */
create table hh_pool_6 as select x.*,coalesce(y.pid,z.pid) as pid
from hh_pool_5 as x
left join hm_pool_1 as y on x.hm_cat=y.hm_cat and x.i1=y.i1 and x.r7=y.r7
left join hm_pool_1 as z on x.hm_cat=z.hm_cat and x.i2=z.i2
order by pid,hid2;
quit;

data hh_pool_6a;set hh_pool_6;by pid;
if first.pid then pid2=pid;
run;

proc sql noprint;
create table hid_test_3 as select hid,size,count(pid2) as n
from hh_pool_6a group by hid,size having calculated n=size-1;

create table hid_test_4 as select x.hid,x.pid
from hh_pool_6 as x inner join hid_test_3 as y on x.hid=y.hid
order by hid,pid;

create table pid_done as select x.*,y.hid
from hm_pool_0 as x inner join hid_test_4 as y
on x.pid=y.pid;

select count(*) into :pd from pid_done;
quit;

%if &pd>0 %then
%do;

proc sort data=hh_pool_4;by hm_cat hid2;run;

data hh_pool_4;set hh_pool_4;by hm_cat;retain i2;
if first.hm_cat then i2=1;else i2=i2+1;run;

proc sort data=hh_pool_4;by hid2 i2;run;

proc sql;
/* get pid for specific members */
create table hh_pool_6 as select x.*,coalesce(z.pid) as pid length=5
from hh_pool_4 as x
left join hm_pool_1 as z on x.hm_cat=z.hm_cat and x.i2=z.i2
order by pid;
quit;

data hh_pool_6a;set hh_pool_6;by pid;
if first.pid then pid2=pid;
run;

proc sql;
create table hid_test_3 as select hid,size,count(pid2) as n
from hh_pool_6a group by hid,size having calculated n=size-1;

create table hid_test_4 as select x.hid,x.pid
from hh_pool_6 as x inner join hid_test_3 as y on x.hid=y.hid
order by hid,pid;

create table pid_done as select x.*,y.hid
from hm_pool_0 as x inner join hid_test_4 as y
on x.pid=y.pid;
quit;
%end;


proc append base=hh_done_1 data=hid_test_4;run;
proc append base=hm_done_1 data=pid_done;run;

proc sql noprint;
create table hh_pool_01 as select x.*
from hh_pool_0 as x
left join hid_test_4 as y on x.hid=y.hid
left join hh_pool_2a as z on x.hid=z.hid
where y.hid=. and z.hid=.;

create table hm_pool_01 as select x.*
from hm_pool_0 as x
left join hid_test_4 as y on x.pid=y.pid
where y.pid=.
order by r7, hm_cat;

create table hh_pool_0 as select * from hh_pool_01;
create table hm_pool_0 as select * from hm_pool_01;

create table config_id_2 as select * from config_id_4;

select count(*) into :hhl from hh_pool_0; 
select count(*) into :hml from hm_pool_0; 

quit;

%if &hh1=&hhl %then %do;
	%put Failure (no households were processed) at cycle = &t (hh left = &hhl, hm left = &hml);
	%let tf=%sysfunc(time(),time8.0);
    %put Started at &ts, Finished at &tf;
	%goto exit1;
%end;

%if &hhl=0 or &hml=0 %then %do;
	%put Graceful exit at cycle = &t (hh left = &hhl, hm left = &hml);
	%let tf=%sysfunc(time(),time8.0);
    %put Started at &ts, Finished at &tf;
	%goto exit1;
%end;

%end;

%put Exit after &m cycles (hh left = &hhl, hm left = &hml);
%let tf=%sysfunc(time(),time8.0);
%put Started at &ts, Finished at &tf;

%exit1:

%mend conf1;

%conf1 (m=200);
/* m controls maximum iterations */


%macro conf2 (m=);

proc sql noprint;
create table config_prob_1 as select 
hh_age18,hh_sex,_name_ as hm_cat,sum(col1) as n
from config_2b group by hh_age18,hh_sex,hm_cat;

create table config_prob_2 as select *,n/sum(n) as p
from config_prob_1 group by hm_cat
order by hm_cat,p desc;
quit;

%if %sysfunc(exist(hh_noconfig_0)) %then
%do;
proc sql noprint;
create table hh_noconfig_0a as
select r7,age18,sex,size,hid from hh_noconfig_0
	union all
select r7,age18,sex,size,hid from hh_pool_0;
quit;
%end;

%else 
%do;
proc sql noprint;
create table hh_noconfig_0a as
select r7,age18,sex,size,hid from hh_pool_0;
quit;
%end;

proc sql noprint;
select count(*) into :hh from hh_noconfig_0a;

create table hm_pool_0a as select *,ranuni(&yr+10) as rn from hm_pool_0;
quit;

proc datasets library=work nolist; delete hh_noconfig_done_1;quit;

%if &hh=0 %then 
%do;
	%put Macro conf2 skipped;
	%goto exit1;
%end;

data hh_noconfig_1(drop=size);set hh_noconfig_0a;
length slot pid 5;
do slot=1 to size-1;pid=.;output;end;run;

%let t=0; /* t is a counter */

%do %while(&t<=&m);

%let t=%eval(&t+1);

proc sql noprint;
create table hh_noconfig_1a as select r7,age18,sex,hid length=5,slot from hh_noconfig_1 where pid=.
order by age18,sex;

create table hh_noconfig_2 as select age18,sex,count(hid) as hh
from hh_noconfig_1a group by age18,sex;

create table hm_pool_1 as select hm_cat,count(pid) as hm from hm_pool_0a group by hm_cat;

create table config_prob_3 as select x.hh_age18,x.hh_sex,x.hm_cat,x.p
from config_prob_2 as x
inner join hm_pool_1 as y on x.hm_cat=y.hm_cat
inner join hh_noconfig_2 as z on x.hh_age18=z.age18 and x.hh_sex=z.sex
order by hm_cat;

create table config_prob_4 as select hh_age18,hh_sex,hm_cat,p/sum(p) as p
from config_prob_3 group by hm_cat;
quit;

data config_prob_4;set config_prob_4;by hm_cat;retain b;
if first.hm_cat then do;a=0;b=p;end;
else do;a=b;b=b+p;end;
run;

proc sql noprint;
create table hm_pool_2 as select x.*,y.hh_age18,y.hh_sex
from hm_pool_0a as x
left join config_prob_4 as y on x.hm_cat=y.hm_cat and y.a<=x.rn<=y.b
order by hh_age18,hh_sex;
quit;

data hm_pool_2;set hm_pool_2;by hh_age18 hh_sex;retain i;
if first.hh_sex then i=1;else i=i+1;
run;

data hh_noconfig_3; set hh_noconfig_1a; by age18 sex;retain i;
if first.sex then i=1;else i=i+1;
run;

proc sql noprint;
create table hh_noconfig_4 as select x.*,y.hm_cat,y.pid
from hh_noconfig_3 as x
left join hm_pool_2 as y on x.age18=y.hh_age18 and x.sex=y.hh_sex and x.i=y.i;

create table hh_noconfig_done as select hid,slot,pid from hh_noconfig_4 where pid^=.;
quit;

proc append base=hh_noconfig_done_1 data=hh_noconfig_done;run;

proc sql noprint;
create table hh_noconfig_5 as select x.r7,x.age18,x.sex,x.hid,x.slot,coalesce(x.pid,y.pid) as pid
from hh_noconfig_1 as x
left join hh_noconfig_done as y on x.hid=y.hid and x.slot=y.slot;

create table hh_noconfig_1 as select * from hh_noconfig_5;

create table hm_pool_0b as select x.*
from hm_pool_0a as x
left join hh_noconfig_done as y on x.pid=y.pid
where y.pid=.;

create table hm_pool_0a as select * from hm_pool_0b;

select count(*) into :hh from hh_noconfig_1 where pid=.;
quit;

%if &hh=0 %then 
%do;
	%put Macro conf2 finished after &t cycles;
	%goto exit2;
%end;

%end;

%do;
	%put Macro conf2 did not converge after &t cycles;
%end;

%exit1:
%exit2:

%mend conf2;

%conf2 (m=50);
/* m controls maximum iterations */


%macro conf3;

%if %sysfunc(exist(hh_noconfig_done_1)) %then

%do;
	data hh_done_2;set hh_done_1 hh_noconfig_done_1(drop=slot);run;
%end;

%else %do;
	data hh_done_2;set hh_done_1;run;
%end;

proc sql noprint;
select count(*) into :hmerr from hm_pool_0a;
quit;

%if &hmerr > 0 %then 
%do;
	%put ERROR ----- records remain in hm_pool_0a in year = &yrn;
%end;

%mend conf3;

%conf3;


proc sql;
/* heads of households of size 2+ */
create table hp_add_1a as select
x.hid + z.max_hh_id as hh_id length=5
,x.pid + w.max_hp_id as hp_id length=5
,x.r,x.r7,x.hisp,x.sex,x.age,x.dob length=4,x.role
from hh_pool_0_orig as x
inner join (select distinct hid from hh_done_2) as y on x.hid=y.hid
cross join max_hh_id as z
cross join max_hp_id as w;

/* heads of single person households */
create table hp_add_1b as select
x.hid + z.max_hh_id as hh_id length=5
,x.pid + w.max_hp_id as hp_id length=5
,x.r,x.r7,x.hisp,x.sex,x.age,x.dob length=4,x.role
from hh_done_size_1 as x
cross join max_hh_id as z
cross join max_hp_id as w;

/* members of households */
create table hp_add_1c as select
y.hid + z.max_hh_id as hh_id length=5
,y.pid + w.max_hp_id as hp_id length=5
,x.r,x.r7,x.hisp,x.sex,x.age,x.dob length=4,x.role
from hm_pool_0_orig as x
inner join hh_done_2 as y on x.pid=y.pid
cross join max_hh_id as z
cross join max_hp_id as w;
quit;

data hp_add_2;set hp_add_1a hp_add_1b hp_add_1c;run;

proc sql;
create table hh_add_2 as select hh_id,. as hu_id length=5,count(hp_id) as size length=3
from hp_add_2 group by hh_id;

create table hh_rem_4 as select hh_id,count(hp_id) as size 
from hp_rem_4 group by hh_id;

create table hu_next_1a as select x.hu_id,x.du_type,x.mgra,x.ct /*,x.place,x.zip5*/ ,x.sto_flag,x.jur,x.cpa
,case when y.hh_id^=. then . else x.hh_id end as hh_id length=5
,case when y.hh_id^=. then . else x.size end as size length=3
from hu_next_1 as x
left join hh_rem_4 as y on x.hh_id=y.hh_id; 

create table hp_next_4a as select x.*
from hp_next_4(drop=mothers_hp_id) as x
left join hh_rem_4 as y on x.hh_id=y.hh_id
where y.hh_id=.;

create table hh_next_4a as select x.*
from hh_next_4 as x left join
hh_rem_4 as y on x.hh_id=y.hh_id
where y.hh_id=.;
quit;

/* reshuffling */
proc sql;
create table test_base_1 as select x.hh_id,x.hp_id,x.role,y.size
,r7
,case 
when x.age <= 4 then 00.04
when x.age <= 9 then 05.09
when x.age <= 14 then 10.14
when x.age <= 17 then 15.17
when x.age <= 19 then 18.19
when x.age <= 24 then 20.24
when x.age <= 29 then 25.29
when x.age <= 34 then 30.34
when x.age <= 44 then 35.44
when x.age <= 54 then 45.54
when x.age <= 64 then 55.64
when x.age <= 74 then 65.74
when x.age <= 84 then 75.84
when x.age >= 85 then 85.99
end as age14
,x.sex
from hp_add_2 as x
inner join hh_add_2 as y on x.hh_id=y.hh_id;

create table test_base_2 as select x.*
,y.r7 as r7_hh,y.age14 as age14_hh,y.sex as sex_hh
,case when x.age14 < 18 then "C"||put(x.age14,z5.2) else x.sex||put(x.age14,z5.2) end as age14_sex
from test_base_1 as x
inner join test_base_1 as y on x.hh_id=y.hh_id
where y.role="H";
quit;


proc sql;
create table test_bp_1 as select hh_id,r7,age14,sex,hp_id,age14_sex
from test_base_2 where role="M" order by r7,age14_sex,ranuni(&yr + 1);

create table test_bs_1 as select hh_id,r7_hh,age14_hh,sex_hh,age14,sex,hp_id,age14_sex
from test_base_2 where role="M" order by r7_hh,age14_sex,ranuni(&yr + 2);
quit;

data test_bp_2;set test_bp_1;by r7 age14_sex;retain i;
if first.age14_sex then i=1;else i=i+1;
run;

data test_bs_2;set test_bs_1;by r7_hh age14_sex;retain i;
if first.age14_sex then i=1;else i=i+1;
run;

proc sql;
create table test_bs_3 as select x.hh_id,x.hp_id as hp_id_old,x.r7_hh,x.age14_hh,x.sex_hh
,x.age14,x.sex,x.age14_sex
,y.hp_id as hp_id_new
from test_bs_2 as x
left join test_bp_2 as y on x.r7_hh=y.r7 and x.age14_sex=y.age14_sex and x.i=y.i;

/* slots not filled */
create table test_bs_3a as select * from test_bs_3 where hp_id_new=.
order by /*ct,jur,*/age14_sex,ranuni(&yr + 3);

/* hp not used */
create table test_bp_2a as select x.*
from test_bp_2(drop=i) as x
left join test_bs_3 as y on x.hp_id=y.hp_id_new
where y.hp_id_new = .
order by age14_sex,ranuni(&yr + 4);
quit;

data test_bs_3b;set test_bs_3a;by age14_sex;retain i;
if first.age14_sex then i=1;else i=i+1;
run;

data test_bp_2b;set test_bp_2a;by age14_sex;retain i;
if first.age14_sex then i=1;else i=i+1;
run;

proc sql;
create table test_bs_4 as select x.hh_id,x.hp_id_old,x.r7_hh,x.age14_hh,x.sex_hh
,x.age14,x.sex,x.age14_sex
,y.hp_id as hp_id_new
from test_bs_3b as x
left join test_bp_2b as y on x.age14_sex=y.age14_sex and x.i=y.i;

/* slots not filled */
create table test_bs_4a as select * from test_bs_4 where hp_id_new=.
order by age14_sex;

/* hp not used */
create table test_bp_2c as select x.*
from test_bp_2b(drop=i) as x
left join test_bs_4 as y on x.hp_id=y.hp_id_new
where y.hp_id_new = .
order by age14_sex;
quit;

proc sql;
create table test_bs_5 as 
select hh_id, r7_hh, age14_hh, sex_hh, hp_id_old, hp_id_new from test_bs_3 where hp_id_new ^= .
	union all
select hh_id, r7_hh, age14_hh, sex_hh, hp_id_old, hp_id_new from test_bs_4 where hp_id_new ^= .;

create table test_bs_6 as select x.*
, y.r7, y.age14, y.age14_sex
from test_bs_5 as x
inner join test_base_2 as y on x.hp_id_new = y.hp_id
order by hh_id;
quit;

proc sql;
create table hp_add_2a as
select * from hp_add_2 where role="H"
	union all
select y.hh_id, x.hp_id, x.r, x.r7, x.hisp, x.sex, x.age, x.dob, x.role 
from (select * from hp_add_2 where role="M") as x
left join test_bs_6 as y on x.hp_id = y.hp_id_new
order by hh_id;
quit;

data hp_add_2a;set hp_add_2a;
length hh_id 5 hp_id 5 r $3. r7 $1. hisp $2. sex $1. age 3 dob 4 role $1.;
run;


data hp_next_4b;set hp_next_4a hp_add_2a;run;

data hh_next_4b;set hh_next_4a hh_add_2;run;


proc sql;
create table hh_rem_5 as select x.*,y.hu_id
from hh_rem_4 as x inner join hu_next_1 as y on x.hh_id=y.hh_id;

create table hp_rem_5 as select * from hp_rem_4(drop=rn i r7 age101);
quit;


/* final step */
data hu_next_2;set hu_next_1a;run;
data hp_next_5;set hp_next_4b;run;
data hh_next_5;set hh_next_4b;run;

%let t=%sysfunc(time(),time8.0);
%put ========== Finished Adding and Removing Households for year &yr at &t;
