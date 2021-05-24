proc sql;
create table base_hh_0 as select x.hh_id,x.hp_id
,case 
when x.hisp="H" then "H"
when x.r="R10" then "W"
when x.r="R02" then "B"
when x.r="R03" then "I" 
when x.r="R04" then "S"
when x.r="R05" then "P" 
when x.r="R06" then "M" 
when x.r="R07" then "M" 
end as r7 length=1
,case 
when x.age<=9 then "00_09"
when x.age<=19 then "10_19"
when x.age<=29 then "20_29"
when x.age<=39 then "30_39"
when x.age<=49 then "40_49"
when x.age<=59 then "50_59"
when x.age<=69 then "60_69"
when x.age<=79 then "70_79" else "80_99" end as age9 length=5
,y.ct
,y.jur
,z.size
from hp_base as x
inner join hu_base as y on x.hh_id=y.hh_id
inner join hh_base as z on x.hh_id=z.hh_id
where (x.role="H" and x.age>19) and z.size<=10;

create table base_hh_1 as select r7,age9,size,ct,count(hh_id) as hh
from base_hh_0 group by r7,age9,size,ct;

create table new_hh_0 as select x.hh_id
,case 
when x.hisp="H" then "H"
when x.r="R10" then "W"
when x.r="R02" then "B"
when x.r="R03" then "I"
when x.r="R04" then "S"
when x.r="R05" then "P"
when x.r="R06" then "M"
when x.r="R07" then "M"
end as r7 length=1
,case 
when x.age<=9 then "00_09"
when x.age<=19 then "10_19"
when x.age<=29 then "20_29"
when x.age<=39 then "30_39"
when x.age<=49 then "40_49"
when x.age<=59 then "50_59"
when x.age<=69 then "60_69"
when x.age<=79 then "70_79" else "80_99" end as age9 length=5
,z.size
from hp_next_5 as x
inner join hh_next_5 as z on x.hh_id=z.hh_id
where x.role="H" and z.hu_id=.;

create table new_hh_1 as select r7,age9,size,count(hh_id) as hh
from new_hh_0 group by r7,age9,size;
quit;


%macro slots (m=);

proc sql;
create table s_9 as select *,monotonic() as slot_id,. as hh_id
from (select ct,jur,size from s_7);

create table new_hh_2 as select *,. as slot_id from new_hh_0;
quit;

%let t=0; /* t is a counter */

%do %while(&t<=&m);

%let t=%eval(&t+1);

proc sql noprint;
create table s_9a as select * from s_9 where hh_id=. order by ct,size;
create table s_9b as select ct,count(*) as c from s_9a group by ct;

create table base_hh_2 as select x.ct,y.r7,y.age9,coalesce(y.hh,0) as base_hh
from s_9b as x
left join (select ct,r7,age9,sum(hh) as hh from base_hh_1 group by ct,r7,age9) as y
on x.ct=y.ct;

/* this table should have zero records */
create table base_hh_2_test as select * from base_hh_2 where base_hh=0;

/* p is a probability that a household of r7/age9 resides in a particular ct */
create table base_hh_3 as select *,base_hh/sum(base_hh) as p
from base_hh_2 group by r7,age9
order by r7,age9,p desc;
quit;

data base_hh_3;set base_hh_3;by r7 age9;retain b;
if first.age9 then do;a=0;b=p;end;
else do;a=b;b=b+p;end;
run;

proc sql;
create table new_hh_3 as select hh_id,r7,age9,size,ranuni(&t) as rn from new_hh_2 where slot_id=.;

/* selecting a ct */
create table new_hh_4 as select x.*,y.ct
from new_hh_3 as x
left join base_hh_3 as y on x.r7 = y.r7 and x.age9 = y.age9 and y.a <= x.rn <= y.b
order by ct,size;
quit;

data new_hh_4;set new_hh_4;by ct size;retain i;
if first.size then i=1;else i=i+1;
run;

data s_9a;set s_9a;by ct size;retain i;
if first.size then i=1;else i=i+1;
run;

proc sql noprint;
create table new_hh_5 as select x.*,y.slot_id
from new_hh_4 as x inner join s_9a as y on x.ct=y.ct and x.size=y.size and x.i=y.i;

select count(*) into :n from new_hh_5;
quit;

%if &n=0 %then
	%do;
		%put Cycle &t: no matches made, &h households and &s units left;
		%goto exit0;
	%end;

proc sql noprint;
create table s_9_ as select x.ct,x.jur,x.size,x.slot_id,coalesce(x.hh_id,y.hh_id) as hh_id length=5
from s_9 as x left join new_hh_5 as y on x.slot_id=y.slot_id;

create table new_hh_2_ as select x.hh_id,x.r7,x.age9,x.size,coalesce(x.slot_id,y.slot_id) as slot_id length=5
from new_hh_2 as x left join new_hh_5 as y on x.hh_id=y.hh_id;

create table s_9 as select * from s_9_;
create table new_hh_2 as select * from new_hh_2_;

/* check if any slots or hh left */
select count(*) into :s from s_9 where hh_id=.;
select count(*) into :h from new_hh_2 where slot_id=.;
quit;

%put Cycle &t: &n hh-slot pairs matched, &h households left, &s units left;

%if &s=0 %then
	%do;
		%put No more units, &h households left;
		%goto exit1;
	%end;

%if &h=0 %then
	%do;
		%put No more households, &s units left;
		%goto exit1;
	%end;

%exit0:

%end;

%put No conversion after &m cycles, &h households and &s units left;
%put Proceeding to forced conversion;
/* this is executed only if no convergence happened */
proc sql noprint;
create table s_9a as select size,slot_id
from s_9 where hh_id=. order by size,ranuni(&t);

create table new_hh_3 as select size,hh_id
from new_hh_2 where slot_id=. order by size,ranuni(&t);
quit;

data s_9a;set s_9a;by size;retain i;if first.size then i=1;else i=i+1;run;
data new_hh_3;set new_hh_3;by size;retain i;if first.size then i=1;else i=i+1;run;

proc sql noprint;
create table new_hh_5 as select x.*,y.slot_id
from new_hh_3 as x inner join s_9a as y on x.size=y.size and x.i=y.i;

create table s_9_ as select x.ct,x.jur,x.size,x.slot_id,coalesce(x.hh_id,y.hh_id) as hh_id length=5
from s_9 as x left join new_hh_5 as y on x.slot_id=y.slot_id;

create table new_hh_2_ as select x.hh_id,x.r7,x.age9,x.size,coalesce(x.slot_id,y.slot_id) as slot_id length=5
from new_hh_2 as x left join new_hh_5 as y on x.hh_id=y.hh_id;

create table s_9 as select * from s_9_;
create table new_hh_2 as select * from new_hh_2_;
quit;

%exit1:

%mend slots;

%slots (m=100); /* m controls maximum iterations */


/* new_hh_2 s_9 */

proc sql;
/* select vacant units */
create table hu_next_slots_5 as select ct,jur,hu_id
from hu_next_slots_1 order by ct,jur,ranuni(&yr+10);

create table s_10 as select ct,jur,hh_id,size
from s_9 order by ct,jur,ranuni(&yr+11);
quit;

data hu_next_slots_5;set hu_next_slots_5;by ct jur;retain i;if first.jur then i=1;else i=i+1;run;
data s_10;set s_10;by ct jur;retain i;if first.jur then i=1;else i=i+1;run;

proc sql;
create table s_11 as select x.hh_id,x.size,y.hu_id
from s_10 as x inner join hu_next_slots_5 as y on x.ct=y.ct and x.jur=y.jur and x.i=y.i;
quit;

proc sql;
create table hu_next_3 as select x.hu_id,x.du_type,x.mgra,x.ct /*,x.place,x.zip5 */,x.sto_flag,x.jur,x.cpa
,coalesce(y.hh_id,x.hh_id) as hh_id length=5
,coalesce(y.size,x.size) as size length=3
from hu_next_2 as x
left join s_11 as y on x.hu_id=y.hu_id;

create table hh_next_6 as select x.hh_id,coalesce(y.hu_id,x.hu_id) as hu_id length=5,x.size
from hh_next_5 as x
left join s_11 as y on x.hh_id=y.hh_id;
quit;
