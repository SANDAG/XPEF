options notes;

%let r = 2021;

proc sql;
create table test_hh_1 as select y.jur,count(distinct x.hh_id) as hh
from sd.hh_&r as x
inner join sd.hu_&r as y on x.hh_id=y.hh_id
group by y.jur;

create table test_hp_1 as select z.jur,count(x.hp_id) as hp
from sd.hp_&r as x
inner join sd.hh_&r as y on x.hh_id=y.hh_id
inner join sd.hu_&r as z on y.hh_id=z.hh_id
group by z.jur;

create table test_hp_2 as select x.*,y.hp
from test_hh_1 as x
left join test_hp_1 as y on x.jur=y.jur
order by jur;
quit;

proc sql;
create table test_hp_3 as select x.*,y.hh as hh1,y.hp as hp1
from test_hp_2 as x
inner join e1.dof_update_jur as y on x.jur=y.jur
where y.yr = &r;

create table test_hp_4 as select * from test_hp_3 where hh^=hh1 or hp^=hp1;
quit;


proc sql;
create table ztest_est_0 as
select "HP" as type
,case when age<=101 then age else 101 end as age102
,sex
,case
when hisp="H" then "H"
when r="R10" then "W"
when r="R02" then "B"
when r="R03" then "I"
when r="R04" then "S"
when r="R05" then "P"
else "M" end as r7
from sd.hp_&r
	union all
select "GQ" as type
,case when age<=101 then age else 101 end as age102
,sex
,case
when hisp="H" then "H"
when r="R10" then "W"
when r="R02" then "B"
when r="R03" then "I"
when r="R04" then "S"
when r="R05" then "P"
else "M" end as r7
from sd.gq_&r;

create table ztest_gq_2017 as
select "GQ" as type
,case when age<=101 then age else 101 end as age102
,sex
,case
when hisp="H" then "H"
when r="R10" then "W"
when r="R02" then "B"
when r="R03" then "I"
when r="R04" then "S"
when r="R05" then "P"
else "M" end as r7
from sd.gq_2017;

create table ztest_est_1 as select age102,sex,r7,type,count(*) as p
from ztest_est_0 group by age102,sex,r7,type;
quit;

proc sql;
create table ztest_gq_2017_1 as select age102,sex,r7,count(*) as gq_2017
from ztest_gq_2017 group by age102,sex,r7;
quit;


proc transpose data=ztest_est_1 out=ztest_est_2;by age102 sex r7;var p;id type;run;

proc sql;
create table ztest_dof_0 as select yr,age102,sex,r7,p_cest as tp_dof
from e1.dof_pop_proj_r7_age102 where yr in (&r);
quit;

proc sql;
create table ztest_dof_1 as select x.*
,coalesce(y.hp,0) as hp, coalesce(y.gq,0) as gq
,coalesce(z.gq_2017,0) as gq_2017
,coalesce(y.gq,0) - coalesce(z.gq_2017,0) as gq_new
,tp_dof - coalesce(y.hp,0) - coalesce(z.gq_2017,0) as d
from ztest_dof_0 as x
left join ztest_est_2 as y on x.age102=y.age102 and x.sex=y.sex and x.r7=y.r7
left join ztest_gq_2017_1 as z on x.age102=z.age102 and x.sex=z.sex and x.r7=z.r7;
quit;

proc sql;
create table ztest_dof_2 as select * from ztest_dof_1 where tp_dof ^= (hp + gq_2017);
create table ztest_dof_3 as select * from ztest_dof_1 where gq_new > 0;
quit;

proc sql;
create table ytest_01 as select x.*,y.hp_jur,x.hp_fut - y.hp_jur as d
from (select yr,sum(hp) as hp_fut from e1.future_hp group by yr) as x
inner join (select yr,sum(hp) as hp_jur from e1.dof_update_jur group by yr) as y on x.yr=y.yr
order by yr;

create table ytest_02 as select * from ytest_01 where d ^= 0;
quit;





/*
proc sql;
create table ytest_01 as select count(hp_id) as hp
from hp_next_5 where sex="F" and age=20 and r="R10";
quit;

proc sql;
create table ytest_02 as select *
from dof_5c where sex="F" and age102=20 and r7="W";
quit;
*/
