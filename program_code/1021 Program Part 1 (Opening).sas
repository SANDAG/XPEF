%let t=%sysfunc(time(),time8.0);
%put ========== Begin Simulation for year &yr at &t;


%macro gq2018;

%if &yr = 2018 %then %do;
proc sql;
create table gq_base_a as select jur,ct,cpa,mgra,r,r7,hisp,sex,age,dob length=4,gq_type,gq_id from sd.gq_&yr where jur <> 3;
create table gq_base_b as select jur,ct,cpa,mgra,r,r7,hisp,sex,age,dob length=4,gq_type,gq_id from sd.gq_2017 where jur = 3;
update gq_base_b set dob = intnx('year', dob, 1, 'same');
quit;

data gq_base; set gq_base_a gq_base_b;run;
%end;

%else %do;
proc sql;
create table gq_base as select jur,ct,cpa,mgra,r,r7,hisp,sex,age,dob length=4,gq_type,gq_id from sd.gq_&yr;
quit;
%end;

%mend gq2018;

%gq2018;


proc sql;
create table hu_base as select hu_id,du_type,sto_flag,mgra,ct,jur,cpa,hh_id,size from sd.hu_&yr;

create table hp_base as select hh_id,hp_id,r,r7,hisp,sex,age,dob length=4,role from sd.hp_&yr order by hh_id;

create table hh_base as select hh_id,hu_id,size from sd.hh_&yr;

create table hu_ludu as select yr,jur,count(hu_id) as hu
from sd.ludu
where sto_flag=0
group by yr,jur;

create table tp_base_jur as select a.*,b.hp,coalesce(c.gq,0) as gq
from (select jur,count(hh_id) as hh,count(hu_id) as hu from hu_base group by jur) as a

left join (select y.jur,count(x.hp_id) as hp
from hp_base as x inner join hu_base as y on x.hh_id=y.hh_id group by jur) as b
on a.jur=b.jur

left join (select jur,count(*) as gq from gq_base group by jur) as c
on a.jur=c.jur
order by jur;
quit;


proc sql;

create table dof_3 as select yr as est_yr,jur,hp as hp_dof,hh as hh_dof
from e1.dof_update_jur
order by est_yr,jur;
quit;

%let t=%sysfunc(time(),time8.0);
%put ========== Finished Program Part 1 for year &yr at &t;
