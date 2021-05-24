%macro jurswap (m=);

/* compare with DOF estimates */

proc sql;
create table jur_next_1 as select x.jur, x.hp_dof, x.hh_dof
,x.hh_dof as hu_dof
,y.hp, y.hh, y.sto_hu
,z.hu_avl

,x.hh_dof - y.hh as hh_d
,min(calculated hh_d, hu_avl) as hh1 /* households to be added */
,calculated hh_d - calculated hh1 as hh2 /* households that cannot be added (no units) */
,calculated hh1 + y.hh as hh3 /* total number of new households */

,x.hp_dof as hp3 /* total number of new household population */

,x.hp_dof - y.hp as hp1 /* household people to be added */

,calculated hp1 / calculated hh1 as avgs format=8.2

from (select * from dof_3 where est_yr = &yrn) as x
left join (select jur, sum(size) as hp, count(hh_id) as hh, sum(sto_flag) as sto_hu from hu_next_2 group by jur) as y on x.jur = y.jur
left join (select jur, count(hu_id) as hu_avl from hu_next_2 where hh_id= . and sto_flag = 0 group by jur) as z on x.jur = z.jur
left join (select jur, count(gq_id) as gq from gq_base group by jur) as u on z.jur = u.jur;
quit;

proc sql;
/* this table should have zero records */
create table jur_slots_0 as select * from jur_next_1 where hh2>0;

create table jur_slots_1 as select jur,hh1 as hh,hp1 as hp from jur_next_1;
quit;

data jur_slots_2(drop=hh);set jur_slots_1(drop=hp);do slot=1 to hh;output;end;run;

proc sql;
create table hh_add_3 as select hh_id,size from hh_add_2
order by ranuni(2050);

create table jur_slots_3 as select *,monotonic() as i from jur_slots_2;
quit;

data hh_add_3;set hh_add_3;i+1;run;

proc sql;
create table jur_slots_4(drop=i) as select x.*,y.hh_id,y.size
from jur_slots_3 as x inner join hh_add_3 as y on x.i=y.i;
quit;

%let t=0; /* t is a counter */

%do %while(&t<=&m);

%let t=%eval(&t+1);

proc sql noprint;
create table jur_slots_4a as select jur,count(*) as hh,sum(size) as hp
from jur_slots_4 group by jur;

/* when hp_d>0 swap for larger size; when hp_d<0 swap for smaller size */
create table jur_slots_compare as select x.*,y.hh as hh_target,y.hp as hp_target
,y.hp - x.hp as hp_d
from jur_slots_4a as x inner join jur_slots_1 as y on x.jur=y.jur;

select count(*) into :d from jur_slots_compare where hp_d^=0;
quit;

%if &d=0 %then
%do;
	%put Convergence after %eval(&t-1) cycles;
	%goto exit1;
%end;

proc sql noprint;
create table js_neg_1 as select x.*
from jur_slots_4 as x inner join jur_slots_compare as y on x.jur=y.jur
where y.hp_d<0 and x.size>1 order by size,ranuni(&t);

create table js_pos_1 as select x.*
from jur_slots_4 as x inner join jur_slots_compare as y on x.jur=y.jur
where y.hp_d>0 and x.size<10 order by size,ranuni(&t+1);
quit;

data js_neg_1;set js_neg_1;by size;retain i;
if first.size then i=1;else i=i+1;run;

data js_pos_1;set js_pos_1;by size;retain i;
if first.size then i=1;else i=i+1;run;

proc sql noprint;
create table js_neg_pos_1 as select
x.jur as jur_1,x.slot as slot_1,x.hh_id as hh_id_1,x.size as size_1
,y.jur as jur_2,y.slot as slot_2,y.hh_id as hh_id_2,y.size as size_2
from js_neg_1 as x inner join js_pos_1 as y on x.size=y.size+1 and x.i=y.i
order by jur_1,ranuni(&t+2);
quit;

data js_neg_pos_1;set js_neg_pos_1;by jur_1;retain i_1;
if first.jur_1 then i_1=1;else i_1=i_1+1;run;

proc sql noprint;
create table js_neg_pos_2 as select x.*
from js_neg_pos_1 as x inner join jur_slots_compare as y
on x.jur_1=y.jur
where x.i_1 <= (y.hp_d * -1)
order by jur_2,ranuni(&t+3);
quit;

data js_neg_pos_2;set js_neg_pos_2;by jur_2;retain i_2;
if first.jur_2 then i_2=1;else i_2=i_2+1;run;

proc sql noprint;
create table js_neg_pos_3 as select x.*
from js_neg_pos_2 as x inner join jur_slots_compare as y
on x.jur_2=y.jur
where x.i_2 <= y.hp_d;

create table jur_slots_5 as select x.jur,x.slot
,coalesce(y.hh_id_2,z.hh_id_1,x.hh_id) as hh_id length=5
,coalesce(y.size_2,z.size_1,x.size) as size length=3
from jur_slots_4 as x
left join js_neg_pos_3 as y on x.jur=y.jur_1 and x.slot=y.slot_1
left join js_neg_pos_3 as z on x.jur=z.jur_2 and x.slot=z.slot_2;

create table jur_slots_4 as select * from jur_slots_5;

quit;

%end;

%put No convergence after &m cycles;

%exit1:

%let t=%sysfunc(time(),time8.0);
%put ========== Finished macro jurswap for year &yr at &t;

%mend jurswap;
