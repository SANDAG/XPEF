/* these are households by size that need to be added */
proc sql;
create table jur_hh_next_1 as select jur,size,count(*) as hh from jur_slots_4
group by jur,size;

create table jur_hh_next_1a as select size,sum(hh) as hh from jur_hh_next_1
group by size;

create table jur_hh_next_1_sum as select jur,sum(hh) as hh from jur_hh_next_1
group by jur;
quit;

/* base vacancy and occupancy rates */
proc sql;
create table hu_new_0 as
select x.jur,x.ct
,case when x.hu_id ^= . then 1 else 0 end as hu_id1
,case when y.hu_id ^= . then 1 else 0 end as hu_id2
,case when x.hh_id ^= . then 1 else 0 end as hh_id1
,case when y.hh_id ^= . then 1 else 0 end as hh_id2
,count(x.hu_id) as hu
from (select * from hu_next_2 where sto_flag=0) as x
left join hu_base as y on x.hu_id=y.hu_id
group by x.jur,x.ct,hu_id1,hu_id2,hh_id1,hh_id2;

create table hu_new_0a as select distinct hu_id1,hu_id2,hh_id1,hh_id2
from hu_new_0;

create table hu_new_0b as select hu_id1,hu_id2,hh_id1,hh_id2,sum(hu) as hu
from hu_new_0 group by hu_id1,hu_id2,hh_id1,hh_id2;
quit;

proc sql;
create table hu_new_1 as select x0.*
,coalesce(x1.hu_new,0) as hu_new
,coalesce(x2.hu_remain,0) as hu_remain
,coalesce(x3.hu_reocc,0) as hu_reocc
,coalesce(x4.hu_vac,0) as hu_vac
,coalesce(x1.hu_new,0) + coalesce(x2.hu_remain,0) + coalesce(x3.hu_reocc,0) + coalesce(x4.hu_vac,0) as hu_all
from (select distinct jur,ct from hu_new_0) as x0

left join (select jur,ct,hu as hu_new from hu_new_0 where hu_id2=0) as x1 /* new construction */
	on x0.jur=x1.jur and x0.ct=x1.ct

left join (select jur,ct,hu as hu_remain from hu_new_0 where hh_id1=1) as x2 /* hh remaining in place */
	on x0.jur=x2.jur and x0.ct=x2.ct

left join (select jur,ct,hu as hu_reocc from hu_new_0 where hh_id1=0 and hh_id2=1) as x3 /* hu needs to be reoccupied  */
	on x0.jur=x3.jur and x0.ct=x3.ct

left join (select jur,ct,hu as hu_vac from hu_new_0 where hh_id1=0 and hh_id2=0 and hu_id2=1) as x4 /* hu remains vacant */
	on x0.jur=x4.jur and x0.ct=x4.ct;

create table hu_new_1a as select jur
,sum(hu_all) as hu_all
,sum(hu_new) as hu_new
,sum(hu_remain) as hu_remain
,sum(hu_reocc) as hu_reocc
,sum(hu_vac) as hu_vac
from hu_new_1 group by jur;

create table hu_new_1b as select x.*,y.hh as hh_dof
,y.hh - (x.hu_remain + x.hu_reocc) as hu_occ
,case
when calculated hu_occ < 0 then hu_reocc + calculated hu_occ else hu_reocc end as hu_reocc2 /* hus to be occupied */
,y.hh - (x.hu_remain + calculated hu_reocc2) as hu_occ2 /* new units to be occupied; essentially vacant available*/
,y.hh / x.hu_all as or_jur format=percent8.1 /* average occupancy level in a jurisdiction in the next year */

from hu_new_1a as x
left join (select jur,hh_dof as hh from dof_3 where est_yr=&yrn) as y
on x.jur=y.jur;
quit;


proc sql;
create table hu_reocc_1 as select x.hu_id,x.jur,x.ct
from hu_next_2 as x
inner join hu_base as y on x.hu_id=y.hu_id
where x.sto_flag=0 and x.hh_id=. and y.hh_id^=.
order by jur,ranuni(&yrn + 20);
quit;

data hu_reocc_1;set hu_reocc_1;by jur;retain i;
if first.jur then i=1;else i=i+1;
run;

proc sql;
create table hu_reocc_2 as select x.jur,x.ct,x.hu_id
from hu_reocc_1 as x
inner join hu_new_1b as y on x.jur=y.jur
where x.i <= y.hu_reocc2;

create table hu_reocc_2a as select jur,count(hu_id) as hu
from hu_reocc_2 group by jur;
quit;


proc sql;
create table base_vr_jurct_1 as select x.jur,x.ct
,x.hu_new + x.hu_vac as hu_avl
,x.hu_remain + x.hu_reocc as hu_must_occ
,coalesce(y.or,0) as or
,coalesce(y.vr,0) as vr
,z.or_jur
,round((calculated hu_avl + calculated hu_must_occ) * z.or_jur,1) as hh01 /* applying jur's occupancy rate to all units */
,case when calculated hh01 < calculated hu_must_occ then 0 else calculated hh01 - calculated hu_must_occ end as hh02
from hu_new_1 as x

left join (select jur,ct
,count(hh_id) / count(hu_id) as or format=percent8.1
,1 - count(hh_id) / count(hu_id) as vr format=percent8.1
from hu_base where sto_flag=0 group by ct,jur) as y
	on x.ct=y.ct and x.jur=y.jur

left join hu_new_1b as z
	on x.jur=z.jur;

create table base_vr_ct_1 as select ct
,count(hh_id) / count(hu_id) as or format=percent8.1
,1 - count(hh_id) / count(hu_id) as vr format=percent8.1
from hu_base where sto_flag=0 group by ct;

create table base_vr_jurct_1a as select x.*
,coalesce(y.or,0) as or_ct
,coalesce(y.vr,1) as vr_ct
from base_vr_jurct_1 as x
left join base_vr_ct_1 as y on x.ct=y.ct
order by jur,ct;

create table base_vr_jur_1 as select jur,sum(hu_avl) as hu_avl
from base_vr_jurct_1 group by jur;
quit;

proc sql;
create table base_vr_jurct_2 as select jur,ct
,hu_avl
,case
when or > 0.98 then 0.98 /* setting maximum occupancy to 98% */
when or < 0.9 and or_ct < 0.9 then 0.9 /* setting minimum occupancy to 90% */
when or < or_ct then or_ct
else or end as or format=percent7.1
,case
when vr < 0.02 then 0.02 /* setting minimum vacancy to 2% */
when vr > 0.1  and or_ct > 0.1 then 0.1 /* setting maximum vacancy to 10% */
when vr > vr_ct then vr_ct
else vr end as vr format=percent7.1
,round(hu_avl * calculated or,1) as hh0
/* initial estimate of occupied units (from the universe of vacant units) */
,hh02
from base_vr_jurct_1a
order by jur,ct,hu_avl;
quit;

proc sql;
create table base_vr_jurct_3 as select x.*
,y.hu_occ2
,case
when hu_avl < round(x.s * (y.hu_occ2),1) then hu_avl else round(x.s * (y.hu_occ2),1) end as hh1
,case
when hu_avl < round(x.s0 * (y.hu_occ2),1) then hu_avl else round(x.s0 * (y.hu_occ2),1) end as hh10
/* initial estimate of new households; the lesser of the 2 (hu_avl or computed number)*/
from (select *,coalesce(hh02/sum(hh02),0) as s,coalesce(hh0/sum(hh0),0) as s0 from base_vr_jurct_2 group by jur) as x
inner join hu_new_1b as y on x.jur=y.jur
order by jur,hh1,hu_avl;
quit;

/* hh2 = final estimate of new households */
data base_vr_jurct_4;set base_vr_jurct_3;by jur;retain hhc;
if first.jur then do; hh2 = hh1; hhc = hh2; end;
else if last.jur then do; hh2 = hu_occ2 - hhc; hhc = hh2 + hhc; end;
else do; hh2 = min(hh1, hu_occ2 - hhc); hhc = hh2 + hhc; end;
run;

proc sql noprint;
select count(*) into :err1 from base_vr_jurct_4 where hh2 > hu_avl;
quit;

/* Back-propogate hh to another ct if hh2 > hu_avl */
%macro err1;
%if &err1 > 0 %then %do;
	%put ERROR with base_vr_jurct_4 (hh2 > hu_avl) for year &yr;
	%put Attempting to correct;

proc sql;
create table base_vr_jurct_4a as select * from base_vr_jurct_4 
where jur in (select jur from base_vr_jurct_4 where hh2 > hu_avl) and hu_avl > 0
order by jur desc,hh1 desc,hu_avl desc,hh2 desc;
quit;

data base_vr_jurct_4b;set base_vr_jurct_4a;by jur;retain hhc2;
if first.jur then do; hh2n = hu_avl; hhc2 = hh2 - hu_avl; end;
else do; hh2n = min(hh2 + hhc2, hu_avl); hhc2 = max(hh2 + hhc2 - hu_avl, 0); end;
run;

proc sql;
update base_vr_jurct_4 as x
	set hh2=(select hh2n from base_vr_jurct_4b as y
		where x.jur=y.jur and x.ct=y.ct)
	where x.jur in (select jur from base_vr_jurct_4b)
	and x.ct in (select ct from base_vr_jurct_4b);
quit;

%end;
%mend err1;

%err1;

proc sql;
create table base_vr_jur_4 as select jur,sum(hu_avl) as hu_avl,sum(hh2) as hh2
from base_vr_jurct_4 group by jur;
quit;

/* create a pool of vacant (new units plus old vacant) */
proc sql;
create table hu_vacfil_1 as
select x.hu_id,x.jur,x.ct
from hu_next_2 as x
inner join hu_base as y on x.hu_id=y.hu_id
where x.sto_flag=0 and x.hh_id =. and y.hh_id =.
	union all
select x.hu_id,x.jur,x.ct
from (select * from hu_next_2 where sto_flag=0 and hh_id=.) as x
left join hu_base as y on x.hu_id=y.hu_id
where y.hu_id =.
order by jur,ct,ranuni(&yrn + 30);

create table hu_vacfil_1a as select jur,count(hu_id) as hu
from hu_vacfil_1 group by jur;
quit;

data hu_vacfil_1;set hu_vacfil_1;by jur ct;retain i;
if first.ct then i=1;else i=i+1;
run;

proc sql;
create table hu_vacfil_2 as select x.jur,x.ct,x.hu_id
from hu_vacfil_1 as x
inner join base_vr_jurct_4 as y on x.jur=y.jur and x.ct=y.ct
where x.i <= y.hh2;
quit;

/* renamed from hu_next_slots_1 */
proc sql;
create table hu_next_slots_0 as 
select * from hu_reocc_2
	union all
select * from hu_vacfil_2;
quit;

proc sql;
create table hu_next_slots_2 as select ct,jur,count(hu_id) as slots
from hu_next_slots_0 group by ct,jur;

create table hu_next_slots_2a as select jur,sum(slots) as slots
from hu_next_slots_2 group by jur;

/* ct/jur/size distribution */
create table base_ct_jur_size_1 as select ct,jur,size,count(hh_id) as hh
from hu_base where size in (1:10) group by ct,jur,size;

create table base_ct_jur_size_2 as select x.*,y.*,coalesce(z.hh,0) as hh
from (select distinct ct,jur from hu_next_slots_2) as x
cross join (select distinct size from base_ct_jur_size_1) as y
left join base_ct_jur_size_1 as z on x.ct=z.ct and x.jur=z.jur and y.size=z.size
order by ct,jur,size;

create table base_ct_size_2 as select ct,size,sum(hh) as hh
from base_ct_jur_size_2 group by ct,size;

create table base_jur_size_2 as select jur,size,sum(hh) as hh
from base_ct_jur_size_2 group by jur,size;

create table base_ct_jur_size_3 as select *,hh/sum(hh) as f
from base_ct_jur_size_2 group by jur,ct;

create table base_ct_size_3 as select *,hh/sum(hh) as f
from base_ct_size_2 group by ct;

create table base_jur_size_3 as select *,hh/sum(hh) as f
from base_jur_size_2 group by jur;

create table base_ct_jur_size_4 as select x.ct,x.jur,x.size,coalesce(x.f,y.f,z.f) as f
from base_ct_jur_size_3 as x 
left join base_ct_size_3 as y on x.ct=y.ct and x.size=y.size
left join base_jur_size_3 as z on x.jur=z.jur and x.size=z.size
order by ct,jur,size;

create table hu_next_slots_3 as select x.*,y.size,y.f,ceil(x.slots * y.f) as s
from hu_next_slots_2 as x
inner join base_ct_jur_size_4 as y on x.ct=y.ct and x.jur=y.jur
order by ct,jur,f;

/* this table should have zero records */
create table hu_next_slots_3a as select x.*
from (select distinct ct,jur from base_ct_jur_size_4) as x
left join hu_next_slots_2 as y on x.ct=y.ct and x.jur=y.jur
where y.ct="";
quit;


data hu_next_slots_4;set hu_next_slots_3;by ct jur;retain c;
if first.jur then do;s1=s;c=s1;end;
else if last.jur then do;s1=slots-c;c=s1+c;end;
else do;s1=min(slots-c,s);c=s1+c;end;
run;


proc sql;
create table hu_next_slots_4a as select 
coalesce(x.jur,y.jur) as jur
,coalesce(x.size,y.size) as size
,coalesce(y.hh,0) as hh
,coalesce(x.slots,0) as slots
,max(calculated slots - calculated hh,0) as vacant_slots
,max(calculated hh - calculated slots,0) as unplaced_hh
,min(calculated hh, calculated slots) as placed_hh
from (select jur,size,sum(s1) as slots from hu_next_slots_4 group by jur,size) as x
full join jur_hh_next_1 as y on x.jur=y.jur and x.size=y.size;

create table hu_next_slots_4b as select jur
,sum(hh) as hh,sum(placed_hh) as placed_hh
,sum(vacant_slots) as vacant_slots,sum(unplaced_hh) as unplaced_hh
from hu_next_slots_4a group by jur;

create table hu_next_slots_4c as select * from hu_next_slots_4b where unplaced_hh > vacant_slots;
quit;


data s_1(drop=i s1 f);set hu_next_slots_4(keep=ct jur size s1 f);
do i=1 to s1;output;end;run;
data s_1;set s_1;slot_id+1;run;

proc sql;
create table s_2 as select * from s_1 order by jur,size,ranuni(&yr + 10);
quit;

data s_3;set s_2;by jur size;retain i;
if first.size then i=1;else i=i+1;
run;

data h_3(drop=hh);set jur_hh_next_1;
do i=1 to hh;output;end;run;
data h_3;set h_3;h_id+1;run;


proc sql;
create table s_4 as select x.ct,x.jur,x.size,x.slot_id,y.h_id from s_3 as x left join h_3 as y
on x.jur=y.jur and x.size=y.size and x.i=y.i;

create table s_4a as select x.*,coalesce(y.filled,0) as filled,coalesce(z.vacant,0) as vacant
from (select jur,size,count(*) as tot from s_4 group by jur,size) as x
left join (select jur,size,count(*) as filled from s_4 where h_id^=. group by jur,size) as y on x.jur=y.jur and x.size=y.size
left join (select jur,size,count(*) as vacant from s_4 where h_id=. group by jur,size) as z on x.jur=z.jur and x.size=z.size;

create table s_4b as select x.*,coalesce(y.filled,0) as filled,coalesce(z.vacant,0) as vacant
from (select ct,jur,size,count(*) as tot from s_4 group by ct,jur,size) as x
left join (select ct,jur,size,count(*) as filled from s_4 where h_id^=. group by ct,jur,size) as y
	on x.ct=y.ct and x.jur=y.jur and x.size=y.size
left join (select ct,jur,size,count(*) as vacant from s_4 where h_id=. group by ct,jur,size) as z
	on x.ct=z.ct and x.jur=z.jur and x.size=z.size;

/* f_filled: share of each tract */
create table s_4c as select *,filled/sum(filled) as f_filled
from s_4b group by jur,size;

create table h_4 as select x.jur,x.size,x.h_id,y.slot_id from h_3 as x left join s_4 as y 
on x.h_id=y.h_id;

create table h_4a as select x.*,y.placed,coalesce(z.not_placed,0) as not_placed
from (select jur,size,count(*) as tot from h_4 group by jur,size) as x
left join (select jur,size,count(*) as placed from h_4 where slot_id^=. group by jur,size) as y on x.jur=y.jur and x.size=y.size
left join (select jur,size,count(*) as not_placed from h_4 where slot_id=. group by jur,size) as z on x.jur=z.jur and x.size=z.size;

create table s_5 as select ct,jur,slot_id from s_4 where h_id=. order by jur,ranuni(2060);

create table h_5 as select jur,size,h_id from h_4 where slot_id=. order by jur,ranuni(2070);
quit;

data s_5;set s_5;by jur;retain i; if first.jur then i=1;else i=i+1;run;
data h_5;set h_5;by jur;retain i; if first.jur then i=1;else i=i+1;run;

proc sql;
create table s_6 as select x.ct,x.jur,y.size,x.slot_id,y.h_id from s_5 as x left join h_5 as y
on x.jur=y.jur and x.i=y.i;

create table h_6 as select x.jur,x.size,x.h_id,y.slot_id from h_5 as x left join s_6 as y 
on x.h_id=y.h_id;

/* this table should have zero records */
create table h_6a as select * from h_6 where slot_id=.;
quit;

/* s_7 has slots (for each ct/jur) of households by size that need to be added */

proc sql;
create table s_7 as
select * from s_4 where h_id^=.
	union all
select * from s_6 where h_id^=.
order by ct,jur,size;

create table s_8 as select ct,size,count(h_id) as hh,count(h_id)*size as hp
from s_7 group by ct,size;

create table s_8a as select jur,size,count(h_id) as hh,count(h_id)*size as hp
from s_7 group by jur,size;

create table s_8b as select ct,count(size) as hh,sum(size) as hp
from s_7 group by ct;

create table s_8c as select size,count(h_id) as hh,count(h_id)*size as hp
from s_7 group by size;

create table s_8d as select count(size) as hh,sum(size) as hp
from s_7;
quit;

proc sql;
create table all_hh_0 as select r7,count(hh_id) as hh
from
(
select x.hh_id
,r7
from hp_next_5 as x
inner join hh_next_5 as z on x.hh_id=z.hh_id
where x.role="H"
)
group by r7;
quit;


proc sql;
create table old_hh_0 as select 
x.hh_id
,x.r7
,z.size
,y.ct
from hp_next_5 as x
inner join hh_next_5 as z on x.hh_id=z.hh_id
inner join hu_next_2 as y on x.hh_id=y.hh_id
where x.role="H" and z.hu_id ^=.
order by ct,r7, ranuni(&yr + 40);

create table old_hh_1 as select ct,r7,count(hh_id) as hh
from old_hh_0
group by ct, r7;

/* old households plus future households */
create table old_hh_2 as select ct,sum(hu) as hut
from
(
select ct,sum(hh) as hu from old_hh_1 group by ct
	union all
select ct,count(h_id) as hu from s_7 group by ct
)
group by ct;
quit;

/*
all_hh_0: count by r7
old_hh_2: count by ct
need to adjust targets
*/

/* Fill in missing census tracts using regional average race distribution */
proc sql;
create table ct_fill as
select o.ct
from old_hh_2 as o
full join e1.ct_hh_target_dist as t
	on o.ct = t.ct
where t.ct is null;

create table r7_region as
select r7, hh/sum(hh) as hh_target_s format percent8.1
from all_hh_0;

create table ct_r7_fill as
select &yrn as yr, c.*, r.*
from ct_fill as c
cross join r7_region as r;
quit;

/* Add missing ct(s) to distribution table */
proc sql;
create table ct_hh_target_dist as select * from (
select * from e1.ct_hh_target_dist where yr=&yrn
	union all
select * from ct_r7_fill);
quit;

proc sql;
create table hh_target_0 as select x.ct,y.r7, round(x.hut * y.hh_target_s,1) as hh
from old_hh_2 as x
left join ct_hh_target_dist as y on x.ct=y.ct;
quit;


/*------------IPF SECTION---------------------------------------*/

/*
i: 1
A: ct
B: race
The purpose is to create a joint distribution of A and B
*/

proc sql;
create table inp_a_00 as
select "0" as i,ct as a,hut as h
from old_hh_2 order by i,a;

create table inp_b_00 as
select "0" as i,r7 as b,hh as h
from all_hh_0 order by i,b;

create table inp_ab_m_0 as select "0" as i, ct as a, r7 as b, hh as h
from hh_target_0
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

proc sql;
select count(distinct a) into :ct1 from inp_a_0;
select count(distinct a) into :ct2 from inp_ab_m_0;
select count(distinct b) into :r71 from inp_b_0;
select count(distinct b) into :r72 from inp_ab_m_0;
quit;

%if &ct1^=&ct2 %then %do;
%put ERROR: Input tract count (&ct1) does not match target tract count (&ct2);
%end;

%if &r71^=&r72 %then %do;
%put ERROR: Input race group count (&r71) does not match target race group count (&r72);
%end;

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
create table slots_a_2 as select a as ct from slots_a_1 order by ranuni(&yr);
create table slots_b_2 as select b as r7 from slots_b_1 order by ranuni(&yrn);

create table slots_ab_1 as select x.ct,y.r7
from (select *,monotonic() as i from slots_a_2) as x
inner join (select *,monotonic() as i from slots_b_2) as y
on x.i=y.i;
quit;

data inp_final_2(drop=i i1);set inp_final_1(drop=i c rename=(a=ct b=r7));
do i=1 to i1;
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
create table hh_target_1 as select ct,r7,count(*) as hh_target
from inp_final_3 group by ct,r7;
quit;

proc sql;
create table hh_target_2 as select x1.ct,x2.r7
,x1.hut
,coalesce(y.hh_target,0) as hh_target
,coalesce(z.hh,0) as hh_old
from old_hh_2 as x1
cross join (select distinct r7 from old_hh_1) as x2
left join hh_target_1 as y on x1.ct = y.ct and x2.r7 = y.r7
left join old_hh_1 as z on x1.ct = z.ct and x2.r7 = z.r7
order by ct,r7,calculated hh_target,ranuni(&yr + 50);
quit;

data hh_target_2a(drop=hhc hh_target);set hh_target_2;by ct; retain hhc;
if first.ct then do; hh1 = hh_target; hhc = hh1; end;
else if last.ct then do; hh1 = hut - hhc; hhc = hh1 + hhc; end;
else do; hh1 = min(hut-hhc, hh_target); hhc = hh1 + hhc; end;
hh2 = hh1 - hh_old;
if hh2 >= 0 then nc=hh_old;
else nc = hh_old + hh2;  
run;


proc sql;
create table hh_target_2b as select x.ct
,coalesce(y.no_change,0) as no_change
,coalesce(z.add,0) as add
,coalesce(v.drop,0) as drop
from (select distinct ct from hh_target_2a) as x
left join (select ct,sum(nc) as no_change from hh_target_2a group by ct) as y on x.ct=y.ct
left join (select ct,sum(hh2) as add from hh_target_2a where hh2>0 group by ct) as z on x.ct=z.ct
left join (select ct,sum(hh2)*-1 as drop from hh_target_2a where hh2<0 group by ct) as v on x.ct=v.ct;

create table hh_target_2c as select sum(no_change) as no_change, sum(add) as add, sum(drop) as drop
from hh_target_2b;
quit;

/* select hh that need to be relocated (when target is exceeded) */
data old_hh_01;set old_hh_0;by ct r7;retain i;
if first.r7 then i=1;else i=i+1;
run;

proc sql;
create table old_hh_02 as select x.*,z.jur
from old_hh_01 as x
inner join hh_target_2a as y on x.ct=y.ct and x.r7=y.r7
inner join hu_next_2 as z on x.hh_id = z.hh_id
where y.hh2 < 0 and x.i <= y.hh2 * -1;

create table old_hh_02a as select ct,count(hh_id) as hh
from old_hh_02 group by ct;

create table old_hh_02b as select count(hh_id) as hh
from old_hh_02;
quit;

proc sql;
create table old_hh_03 as select x.*
from hu_next_2 as x
inner join old_hh_02 as y on x.hh_id=y.hh_id;

update old_hh_03 set hh_id=., size=.;
quit;


/* update households and units */
proc sql;
create table hu_next_2a as select hu_id, du_type, mgra, ct, sto_flag, jur,cpa
,case when f=0 then hh_id else . end as hh_id length=5
,case when f=0 then size else . end as size length=3
from
(
select x.*, case when y.hh_id ^=. then 1 else 0 end as f
from hu_next_2 as x
left join old_hh_02 as y on x.hh_id = y.hh_id
);
quit;

proc sql;
create table hh_next_5a as select hh_id, case when f = 0 then hu_id else . end as hu_id length=5, size
from
(
select x.*, case when y.hh_id ^=. then 1 else 0 end as f
from hh_next_5 as x
left join old_hh_02 as y on x.hh_id = y.hh_id
);
quit;


proc sql;
create table s_7a as
select ct,jur,size from s_7
	union all
select ct,jur,size from old_hh_02;
quit;


data hh_target_3(drop=i hh2);set hh_target_2a(drop=hut hh_old hh1 nc); where hh2>0;
do i=1 to hh2;
	output;
end;
run;



proc sql;
create table new_hh_0 as select x.hh_id
,x.r7
,z.size
from hp_next_5 as x
inner join hh_next_5a as z on x.hh_id=z.hh_id
where x.role="H" and z.hu_id=.;

create table new_hh_1 as select r7,size,count(hh_id) as hh
from new_hh_0 group by r7,size;
quit;


%macro slots2 (m=);

proc sql;
create table s_9 as select *, monotonic() as slot_id length=5,. as hh_id length=5, "" as match_type length=8
from (select ct,jur,size from s_7a);

create table new_hh_2 as select *,. as slot_id length=5 from new_hh_0;

create table hh_target_4 as select *, monotonic() as target_id length=5, . as hh_id length=5 from hh_target_3;
quit;

%let c=0; /* c is a counter */

%put Matching on r7 only;

%do %while(&c<=&m);

%let c=%eval(&c+1);


proc sql noprint;
/* selecting available slots */
create table s_9a as select * from s_9 where hh_id=.
order by ct,size;

/* selecting unassigned households */
create table new_hh_2c as select hh_id,r7/*,age14*/,size 
from new_hh_2 where slot_id = .
order by r7/*,age14*/,size, ranuni(&c + 10);

/* computing probability of size conditioned on r7 */
create table new_hh_2a as select *,hh/sum(hh) as p
from (select r7/*,age14*/,size,count(hh_id) as hh from new_hh_2 where slot_id = . group by r7/*, age14*/, size) as x
group by r7/*,age14*/ order by r7/*,age14*/,ranuni(&c + 20);
quit;

/* creating probability brackets (between a and b) */
data new_hh_2b;set new_hh_2a;by r7 /*age14*/;retain b;
if first.r7/*age14*/ then do; a = 0; b = p ; end;
else if last.r7/*age14*/ then do; a = b; b = 1; end;
else do; a = b; b = b + p; end;
run;

/* numbering slots within ct/size */ 
data s_9b;set s_9a;by ct size;retain i;
if first.size then i=1;else i=i+1;
run;

/* imputing size for each target record */
proc sql noprint;
create table hh_target_4a as select x.ct, x.r7/*, x.age14*/, x.target_id
,y.size
from (select *,ranuni(&c + 30) as rn from hh_target_4 where hh_id = .) as x
left join new_hh_2b as y on x.r7=y.r7 /*and x.age14=y.age14*/
where y.a <= x.rn <= y.b
order by ct,size,rn;
quit;

/* numbering target records within ct/size */ 
data hh_target_4b;set hh_target_4a;by ct size;retain i;
if first.size then i=1;else i=i+1;
run;

/* assigning slot to each target */
proc sql noprint;
create table hh_target_4c as select x.ct,x.r7/*,x.age14*/,x.size,x.target_id,y.slot_id
from hh_target_4b as x
inner join s_9b as y on x.ct=y.ct and x.size=y.size and x.i=y.i
order by r7/*,age14*/,size,ranuni(&c + 40);
quit;

/* numbering target records within r7/size */ 
data hh_target_4d;set hh_target_4c;by r7 size;retain i;
if first.size then i=1;else i=i+1;
run;

/* numbering households within r7/size */ 
data new_hh_2d; set new_hh_2c;by r7 size;retain i;
if first.size then i=1;else i=i+1;
run;

/* assigning household to each target */
proc sql noprint;
create table hh_target_4e as select x.ct,x.r7,x.size,x.target_id,x.slot_id,y.hh_id, "r7" as match_type length=8
from hh_target_4d as x
inner join new_hh_2d as y on x.r7=y.r7 and x.size=y.size and x.i=y.i;

select count(*) into :n from hh_target_4e;
quit;

%if &n=0 %then
	%do;
		%put Cycle &c: no matches made, &h households, and &s units, and &t targets left;
		%goto exit1;
	%end;


/* update records */
proc sql noprint;
create table s_9_ as select x.ct,x.jur,x.size,x.slot_id,coalesce(x.hh_id, y.hh_id) as hh_id length=5
,coalesce(x.match_type, y.match_type) as match_type length=8
from s_9 as x left join hh_target_4e as y on x.slot_id=y.slot_id;

create table new_hh_2_ as select x.hh_id,x.r7/*,x.age14*/,x.size,coalesce(x.slot_id,y.slot_id) as slot_id length=5
from new_hh_2 as x left join hh_target_4e as y on x.hh_id=y.hh_id;

create table hh_target_4_ as select x.ct,x.r7/*,x.age14*/,x.target_id,coalesce(x.hh_id,y.hh_id) as hh_id length=5
from hh_target_4 as x left join hh_target_4e as y on x.target_id=y.target_id;

create table s_9 as select * from s_9_;
create table new_hh_2 as select * from new_hh_2_;
create table hh_target_4 as select * from hh_target_4_;

/* check if any slots or hh or targets left */
select count(*) into :h from new_hh_2 where slot_id = .;
select count(*) into :s from s_9 where hh_id = .;
select count(*) into :t from hh_target_4 where hh_id = .;
quit;

%put Cycle &c: &n hh-slot pairs matched, &h households, &s units, and &t targets left;

%if &s=0 %then
	%do;
		%put No more units, &h households left;
		%goto exit0;
	%end;

%if &h=0 %then
	%do;
		%put No more households, &s units left;
		%goto exit0;
	%end;

%exit1:
%end;


%put No conversion after &m cycles, &h households and &s units left;

%put Proceeding to forced conversion;

proc sql noprint;
create table s_9a as select size,slot_id
from s_9 where hh_id=. order by size,ranuni(&c + 50);

create table new_hh_3 as select size,hh_id
from new_hh_2 where slot_id=. order by size,ranuni(&c + 60);
quit;

data s_9a;set s_9a;by size;retain i;if first.size then i=1;else i=i+1;run;

data new_hh_3;set new_hh_3;by size;retain i;if first.size then i=1;else i=i+1;run;

proc sql noprint;
create table new_hh_5 as select x.*,y.slot_id, "random" as match_type length=8
from new_hh_3 as x inner join s_9a as y on x.size=y.size and x.i=y.i;

create table s_9_ as select x.ct,x.jur,x.size,x.slot_id,coalesce(x.hh_id, y.hh_id) as hh_id length=5
,coalesce(x.match_type, y.match_type) as match_type length=8
from s_9 as x
left join new_hh_5 as y on x.slot_id=y.slot_id;

create table new_hh_2_ as select x.hh_id,x.r7/*,x.age14*/,x.size,coalesce(x.slot_id,y.slot_id) as slot_id length=5
from new_hh_2 as x left join new_hh_5 as y on x.hh_id=y.hh_id;

create table s_9 as select * from s_9_;
create table new_hh_2 as select * from new_hh_2_;
quit;


%exit0:

%mend slots2;


%slots2 (m=100); /* maximum iterations control */

proc sql;
/* select vacant and reoccupiable units */
create table hu_next_slots_5 as
select ct,jur,hu_id from hu_next_slots_0
	union all
/* units vacated to match targets */
select ct,jur,hu_id from old_hh_03

order by ct,jur,ranuni(&yr + 60);

create table s_10 as select ct,jur,hh_id,size
from s_9 order by ct,jur,ranuni(&yr + 70);
quit;

data hu_next_slots_5;set hu_next_slots_5;by ct jur;retain i;
if first.jur then i=1;else i=i+1;
run;

data s_10;set s_10;by ct jur;retain i;
if first.jur then i=1;else i=i+1;
run;

proc sql;
create table s_11 as select x.hh_id,x.size,y.hu_id
from s_10 as x inner join hu_next_slots_5 as y on x.ct=y.ct and x.jur=y.jur and x.i=y.i;
quit;

proc sql;
create table s_12 as select x.*,y.jur,y.ct,z.hu_id as hu_id_prev
from s_11 as x
left join hu_next_2a as y on x.hu_id=y.hu_id
left join hu_base as z on x.hu_id=z.hu_id;
quit;

proc sql;
create table s_12a as select jur,ct,count(hu_id) as hu
from s_12 where hu_id_prev = .
group by jur,ct;

create table s_12b as select ct,sum(hu) as hu
from s_12a group by ct;

create table s_12c as select jur,sum(hu) as hu
from s_12a group by jur;

create table s_12c as select sum(hu) as hu
from s_12a;
quit;

proc sql;
create table hu_next_3 as select x.hu_id,x.du_type,x.mgra,x.ct,x.sto_flag,x.jur,x.cpa
,coalesce(y.hh_id,x.hh_id) as hh_id length=5
,coalesce(y.size,x.size) as size length=3
from hu_next_2a as x
left join s_11 as y on x.hu_id=y.hu_id;

create table hh_next_6 as select x.hh_id,coalesce(y.hu_id,x.hu_id) as hu_id length=5,x.size
from hh_next_5a as x
left join s_11 as y on x.hh_id=y.hh_id;
quit;


%let t=%sysfunc(time(),time8.0);
%put Finished Housing Unit Assignment for year &yr at &t;

PROC DATASETS LIB=SD nolist; delete
summary_&yrn
newborns_&yr
hp_removed_death_&yr
gq_removed_death_&yr
gq_removed_adj_&yr

hh_removed_size_&yr
hp_removed_size_&yr
hu_removed_demo_&yr
hp_removed_demo_&yr
hh_removed_config_&yr
hp_removed_config_&yr
hh_added_&yr
hp_added_&yr
gq_added_&yr

hh_relocated_&yr
hu_slots_&yr
hu_slots2_&yr
hh_targets_&yr

hu_&yrn
hp_&yrn
hp_&yrn
gq_&yrn
;
RUN; QUIT;

proc sql;
create table sd.summary_&yrn as select x.*,y.*,z1.*,z2.*,z3.*
from (select count(hh_id) as hh_next,sum(size) as hp_next from hh_next_5) as x
cross join (select count(hh_id) as hh_base,sum(size) as hp_base from hh_base) as y

cross join (select count(hh_id) as hh_add from hh_add_2) as z1
cross join (select count(hp_id) as hp_add from hp_add_2) as z2
cross join (select count(hh_id) as hh_rem from hh_rem_5) as z3
;

quit;

/*
hh new (hh_add_2)
hp new (hp_add_2)
hh tba--N/A
hp tba--N/A
hh drop (hh_rem_5)
hh dof--N/A
hp dof--NA
*/

data sd.newborns_&yr;set newborns_0;run;
data sd.hp_removed_death_&yr;set hp_removed_death;run;
data sd.gq_removed_death_&yr;set gq_removed_death;run;

data sd.hh_removed_size_&yr;set hh_removed_size;run;
data sd.hp_removed_size_&yr;set hp_removed_size;run;

data sd.hu_removed_demo_&yr;set hu_removed_demo;run;
data sd.hp_removed_demo_&yr;set hp_removed_demo;run;

data sd.hh_removed_config_&yr;set hh_rem_5;run;
data sd.hp_removed_config_&yr;set hp_rem_5;run;

data sd.hh_added_&yr;set s_11 ;run;
data sd.hp_added_&yr;set hp_add_2;run;
data sd.gq_added_&yr;set gq_add_2;run;

data sd.hh_relocated_&yr; set old_hh_02(keep=hh_id);run;
data sd.hu_slots_&yr; set s_7;run;
data sd.hu_slots2_&yr; set s_9;run;

data sd.hh_targets_&yr(rename=(hh2=target));set hh_target_2a(drop=hut hh_old hh1 nc); where hh2>0;run;

data sd.hu_&yrn;set hu_next_3;run;
data sd.hp_&yrn;set hp_next_5;run;
data sd.hh_&yrn;set hh_next_6;run;
data sd.gq_&yrn;set gq_next_3;run;

%let t=%sysfunc(time(),time8.0);
%put ========== Finished Closing Section for year &yr at &t;
