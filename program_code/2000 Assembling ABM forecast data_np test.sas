/*%let xver=xpef05;*/

/*
%let by1=2018; 
*/

/* datasource id */
/*%let ds=14;*/

/*
libname sql_xpef odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;
Trusted_Connection=yes" schema=&xver;

libname e1 "T:\socioec\Current_Projects\&xver\input_data";
*/

/*libname dw "T:\socioec\Current_Projects\Data_for_Wu_2018";*/

%let by0=%eval(&by1 - 1);

%macro abm1;

%let l = %sysfunc(countw(&listabm));
proc datasets library=work nolist; delete abm_lu_0;quit;

/* iteration over years */
%do k=1 %to &l;
	%let yrr=%scan(&listabm,&k);

PROC IMPORT OUT=abm_lu_00 DATAFILE="T:\ABM\release\ABM\version_13_3_2\input\&yrr.\mgra13_based_input&yrr..csv"
DBMS=CSV REPLACE; GETNAMES=YES; DATAROW=2;
RUN; 

data abm_lu_00;set abm_lu_00; yr = &yrr; run;

proc append base=abm_lu_0 data=abm_lu_00;run;

proc datasets library=work nolist; delete abm_lu_00;quit;

%end;

/* resetting the year to match the new base year */
/*
data abm_lu_0;set abm_lu_0;
if yr = 2015 then yr = 2016;
run;
*/

%mend abm1;
/* Must include base year (2016) in this list */
%let listabm=2016 2018 2020 2023 2025 2026 2029 2030 2032 2035 2040 2045 2050;
%abm1;

proc sql;
create table old_abm_yrs as select distinct yr from abm_lu_0;
quit;

proc sql;
create table abm_lu_0a as select * from abm_lu_0(obs=1);
quit;
proc transpose data=abm_lu_0a out=abm_lu_0b;run;

/*
proc sql;
create table abm_lu_0a as select * from abm_lu_0(obs=1);
quit;
proc transpose data=abm_lu_0a out=abm_lu_0b;run;

proc sql;
create table es_sd as select distinct ech_dist from abm_lu_0;
create table hs_sd as select distinct hch_dist from abm_lu_0;
quit;
*/



/* assembling part 1 */


proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table mgra_taz as select *
from connection to odbc
(
select mgra_13 as mgra,taz_13 as taz,luz_13 as luz,sra_1990 as sra FROM [data_cafe].[ref].[vi_xref_geography_mgra_13]
)
order by mgra
;

disconnect from odbc;
quit;


proc sql;
create table hh_0 as select x.yr,y.mgra,"i"||strip(put(x.income_group_id_2010-10,2.0)) as l
,count(x.hh_id) as hh
from sql_xpef.household_income_upgraded as x
inner join sql_xpef.households as y
on x.yr=y.yr and x.hh_id=y.hh_id
group by x.yr,y.mgra,l;
quit;

proc sql;
create table hh_1a as select a.yr,x.*,z.*,coalesce(y.hh,0) as hh
from (select distinct yr from hh_0) as a
cross join (select distinct mgra,taz from mgra_taz) as x
cross join (select distinct l from hh_0) as z
left join hh_0 as y on a.yr=y.yr and x.mgra=y.mgra and z.l=y.l
order by yr,mgra,taz,l;
quit;

proc transpose data=hh_1a out=hh_1b(drop=_name_);by yr mgra taz;var hh;id l;run;

proc sql;
create table hh_2 as select yr,mgra,taz,i1,i2,i3,i4,i5,i6,i7,i8,i9,i10
from hh_1b order by yr,mgra;
quit;


proc sql;
create table gq_1 as select x.yr,x.mgra,x.gq_type,count(x.gq_id) as gq
from sql_xpef.gq_population as x
inner join (select distinct yr from hh_2) as y on x.yr=y.yr
group by x.yr,x.mgra,x.gq_type;

create table gq_2 as select x0.*,x.*,coalesce(y.gq_civ,0) as gq_civ,coalesce(z.gq_mil,0) as gq_mil
from (select distinct yr from gq_1) as x0
cross join mgra_taz as x
left join (select yr,mgra,sum(gq) as gq_civ from gq_1 where gq_type^="MIL" group by yr,mgra) as y
	on x0.yr=y.yr and x.mgra=y.mgra
left join (select yr,mgra,gq as gq_mil from gq_1 where gq_type="MIL") as z
	on x0.yr=z.yr and x.mgra=z.mgra;
quit;

proc sql;
create table hp_1 as select x.yr,y.mgra,count(x.hp_id) as hhp
from sql_xpef.household_population as x
inner join sql_xpef.households as y on x.yr=y.yr and x.hh_id=y.hh_id
inner join (select distinct yr from hh_2) as z on x.yr=z.yr
group by x.yr,y.mgra;

create table hs_1 as select x.yr,x.mgra,x.du_type,count(x.hu_id) as du,count(x.hh_id) as hh
from sql_xpef.housing_units as x
inner join (select distinct yr from hh_2) as y on x.yr=y.yr
group by x.yr,x.mgra,x.du_type;
quit;

proc sql;
create table du_type as select distinct du_type from hs_1;
quit;


proc sql;
create table hs_1a as select y.yr,x.mgra,x.taz
from mgra_taz as x
cross join (select distinct yr from hs_1) as y;

create table hs_2 as select x.*
,coalesce(y.du,0) as hs_sf,coalesce(y.hh,0) as hh_sf
,coalesce(z.du,0) as hs_mf,coalesce(z.hh,0) as hh_mf
,coalesce(v.du,0) as hs_mh,coalesce(v.hh,0) as hh_mh
,coalesce(u.hhp,0) as hhp
from hs_1a as x
left join (select yr,mgra,sum(du) as du,sum(hh) as hh from hs_1 where du_type in ("SFA","SFD") group by yr,mgra) as y
	on x.yr=y.yr and x.mgra=y.mgra
left join (select * from hs_1 where du_type="MF") as z on x.yr=z.yr and x.mgra=z.mgra
left join (select * from hs_1 where du_type="MH") as v on x.yr=v.yr and x.mgra=v.mgra
left join hp_1 as u on x.yr=u.yr and x.mgra=u.mgra;

create table hs_3 as select *
,hs_sf+hs_mf+hs_mh as hs
,hh_sf+hh_mf+hh_mh as hh
,case when hhp>0 then round(hhp/calculated hh,0.001) else 0 end as hhs
from hs_2;

create table hs_3a as select * from hs_3 where hhp>0 and hh=0;
quit;

/*
proc sql;
create table test_01 as select * from hs_3 where hhp>0 and hh=0;
quit;
*/

proc sql;
create table part_1 as select x.yr,x.mgra,x.taz
,x.hs
,x.hs_sf
,x.hs_mf
,x.hs_mh
,x.hh
,x.hh_sf
,x.hh_mf
,x.hh_mh
,y.gq_civ
,y.gq_mil
,z.i1,z.i2,z.i3,z.i4,z.i5,z.i6,z.i7,z.i8,z.i9,z.i10
,x.hhs
,x.hhp + y.gq_civ + y.gq_mil as pop
,x.hhp
from hs_3 as x
inner join gq_2 as y on x.yr=y.yr and x.mgra=y.mgra
inner join hh_2 as z on x.yr=z.yr and x.mgra=z.mgra;
quit;

proc sql;
create table part_1_test as select distinct yr from part_1;
quit;

/*
proc sql;
create table gq_mil_1 as select mgra,yr,count(gq_id) as gqmil
from sh.gq_0 where gq_type="MIL" group by mgra,yr;
create table gq_mil_1a as select yr,sum(gqmil) as gqmil
from gq_mil_1 group by yr;
quit;

proc transpose data=gq_mil_1 out=gq_mil_2(drop=_name_);by mgra; var gqmil;id yr;run;
*/

/* Assembling Part 2 */

/* rates of private household employment per household by household income */
proc import out=privhj_1 datafile="T:\socioec\Current_Projects\Popsyn_Related\Private Household Employment (IMPLAN).xlsx"
replace dbms=excelcs;sheet="Data";run;

/*
proc sql;
create table privhj_2 as select x.yr,x.hh_id,x.mgra,y.jobs_per_hh
from sql_xpef.syn_households as x
inner join (select distinct yr from hh_0) as z on x.yr=z.yr
cross join privhj_1 as y 
where x.hh_id < 30000000
*/
/* this excludes college and military gq */
/*
and y.inc_1<=x.hinc<=y.inc_2;

create table privhj_2a as select yr,hh_id,count(hh_id) as n
from privhj_2 group by yr,hh_id having calculated n>1;
quit;
*/

proc sql;
create table privhj_2 as select x.yr,x.hh_id,u.mgra,u.jur as jur_id
,case when int(u.cpa/100) in (14,19) then u.cpa else 0 end as cpa_id
,y.jobs_per_hh
from sql_xpef.household_income_upgraded as x
inner join (select distinct yr from hh_0) as z on x.yr=z.yr
inner join sql_xpef.housing_units as u on x.hh_id=u.hh_id and x.yr=u.yr
cross join privhj_1 as y 
where y.inc_1 <= x.inc_2010 <= y.inc_2;

create table privhj_2a as select yr,hh_id,count(hh_id) as n
from privhj_2 group by yr,hh_id having calculated n>1;
quit;

/*THIS WAS CHANGED TO OUTPUT THE 2016 SELF EMPLOYED JOBS*/
/*create 2016 private jobs data*/
proc sql;
create table privhj_2_2016 as select x.yr,x.hh_id,u.mgra,u.jur as jur_id
,case when int(u.cpa/100) in (14,19) then u.cpa else 0 end as cpa_id
,y.jobs_per_hh
from sql_xpef.household_income_upgraded as x
inner join sql_est.housing_units as u on x.hh_id=u.hh_id and x.yr=u.yr
cross join privhj_1 as y 
where y.inc_1 <= x.inc_2010 <= y.inc_2 and x.yr = 2016;

create table privhj_2a_2016 as select yr,hh_id,count(hh_id) as n
from privhj_2_2016 group by yr,hh_id having calculated n>1;
quit;

/*private household jobs for 2016*/
proc sql;
create table privhj_3_2016 as select yr,mgra,jur_id,cpa_id,count(hh_id) as hh,round(sum(jobs_per_hh),1) as emp_pvt_hh
from privhj_2_2016 group by yr,mgra,jur_id,cpa_id;

create table privhj_3a_2016 as select yr,sum(emp_pvt_hh) as j
from privhj_3_2016 group by yr;

/*create table emp_pvt_hh_old_2016 as select yr,sum(emp_pvt_hh) as j from abm_lu_0 group by yr;*/
quit;

proc sql;
create table privhj_3 as select yr,mgra,jur_id,cpa_id,count(hh_id) as hh,round(sum(jobs_per_hh),1) as emp_pvt_hh
from privhj_2 group by yr,mgra,jur_id,cpa_id;

create table privhj_3a as select yr,sum(emp_pvt_hh) as j
from privhj_3 group by yr;

create table emp_pvt_hh_old as select yr,sum(emp_pvt_hh) as j from abm_lu_0 group by yr;

insert into privhj_3 select * from privhj_3_2016; 
quit;

proc sql; 
create table test_inc_2 as 
select yr, sum(emp_pvt_hh) as emp_pvt_hh 
from privhj_3
group by yr
order by yr; 
quit; 

proc import out=emp_bridge datafile="T:\socioec\Current_Projects\Popsyn_Related\SANDAG sectors to ABM sectors.xlsx"
replace dbms=excelcs;sheet="emp_bridge";run;

proc sql;
create table abm_emp_1 as select x.*,coalesce(y.abm_name_2,x.abm_name_1) as abm_name_2
,y.sandag_industry_id,y.sandag_industry_name
from (select _name_ as abm_name_1 from abm_lu_0b where substr(_name_,1,4)="emp_" and _name_^="emp_total") as x
left join emp_bridge as y on x.abm_name_1=y.abm_name_1;

update abm_emp_1 set abm_name_2=scan(abm_name_1,1,"_")||"_"||scan(abm_name_1,2,"_")
where scan(abm_name_1,2,"_") in ("const","utilities","mfg");

update abm_emp_1 set abm_name_2=scan(abm_name_1,1,"_")||"_"||scan(abm_name_1,2,"_")||"_"||scan(abm_name_1,3,"_")
where scan(abm_name_1,2,"_") in ("pvt");

update abm_emp_1 set abm_name_2=scan(abm_name_1,1,"_")||"_"||scan(abm_name_1,2,"_")||"_"||scan(abm_name_1,3,"_")||"_"||scan(abm_name_1,4,"_")
where scan(abm_name_1,4,"_") in ("svcs","gov");
quit;


proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table emp_type_id as select sandag_industry_id,employment_type_id
from connection to odbc
( select * FROM [socioec_data].[ca_edd].[sandag_industry] );

disconnect from odbc;
quit;


proc sql;
create table usj_3a as
select x.yr,x.mgra,x.jur_id,x.cpa_id,x.sandag_industry_id,x.type,y.employment_type_id, 1 as source_id, x.j_base as j 
from (select * from e1.jobs_all where j_base>0) as x
left join emp_type_id as y on x.sandag_industry_id = y.sandag_industry_id
	union all
select x.yr,x.mgra,x.jur_id,x.cpa_id,x.sandag_industry_id,x.type,y.employment_type_id, 2 as source_id, x.j_col_mil + x.j_dev_events as j 
from (select * from e1.jobs_all where j_col_mil>0 or j_dev_events>0) as x
left join emp_type_id as y on x.sandag_industry_id = y.sandag_industry_id
	union all
select x.yr,x.mgra,x.jur_id,x.cpa_id,x.sandag_industry_id,x.type,y.employment_type_id, 3 as source_id, x.j_capacity as j 
from (select * from e1.jobs_all where j_capacity>0) as x
left join emp_type_id as y on x.sandag_industry_id = y.sandag_industry_id
	union all

/* adding private households jobs */
select yr-1 as yr,mgra,jur_id,cpa_id,29 as sandag_industry_id, 5 as type, 15 as employment_type_id, 4 as source_id, emp_pvt_hh as j
from privhj_3 where yr > &by1;

insert into usj_3a select yr as yr,mgra,jur_id,cpa_id,29 as sandag_industry_id, 5 as type, 15 as employment_type_id, 4 as source_id, emp_pvt_hh as j
from privhj_3 where yr = 2016; 

create table usj_3b as select yr,mgra,jur_id,cpa_id,employment_type_id,sum(j) as j
from usj_3a group by yr,mgra,jur_id,cpa_id,employment_type_id;
quit;


proc sql;
create table usj_3b_test_1 as select yr,employment_type_id,sum(j) as j
from usj_3b group by yr,employment_type_id;

create table usj_3a_test_2 as select yr,sum(j) as j
from usj_3a group by yr;

create table usj_3b_test_2 as select yr,sum(j) as j
from usj_3b group by yr;

create table usj_3b_test_3 as select yr,sum(j) as j
from usj_3b where employment_type_id not in (15,1) group by yr;

create table usj_3b_test_4 as select * from usj_3b
where mgra in (0,.) or jur_id in (0,.) or cpa_id in (.) or employment_type_id in (0.,) or j in (.);
quit;

proc sql;
create table test_01 as select x.*
from (select distinct mgra,jur_id,cpa_id from usj_3b) as x
left join sql_xpef.mgra_id_new as y
on x.mgra=y.mgra and x.jur_id=y.jurisdiction_id and x.cpa_id=y.cpa_id
where y.mgra_id=.;

create table test_02 as select x.*, y.jurisdiction_id, cpa_id, mgra_id
from (select distinct mgra from test_01) as x
left join sql_xpef.mgra_id_new as y
on x.mgra=y.mgra
order by mgra_id;
quit;



proc sql;
create table jobs_out_1 as select x.yr as yr_id, y.mgra_id
,x.employment_type_id,x.j as jobs
from usj_3b as x
left join sql_xpef.mgra_id_new as y on x.mgra=y.mgra and x.jur_id=y.jurisdiction_id and x.cpa_id=y.cpa_id;

create table jobs_all_1 as select x.yr as yr_id, y.mgra_id
,x.sandag_industry_id,x.type,x.employment_type_id,x.source_id,x.j as jobs
from usj_3a as x
left join sql_xpef.mgra_id_new as y on x.mgra=y.mgra and x.jur_id=y.jurisdiction_id and x.cpa_id=y.cpa_id;

create table jobs_out_1a as select max(jobs) as max_jobs from jobs_out_1;
create table jobs_all_1a as select max(jobs) as max_jobs from jobs_all_1;

create table scale_1 as select x.*,y.*,z.*
from (select distinct yr_id length=3 format=4. from jobs_out_1) as x
cross join (select distinct mgra_id length=6 format=10. from sql_xpef.mgra_id_new) as y
cross join (select distinct employment_type_id length=3 format=2. from jobs_out_1) as z;

create table scale_2 as select x.*,y.*,z.*
from (select distinct yr_id length=3 format=4. from jobs_all_1) as x
cross join (select distinct mgra_id length=6 format=10. from sql_xpef.mgra_id_new) as y
cross join (select distinct sandag_industry_id length=3 format=2.,type as type_id length=3 format=1.,employment_type_id length=3 format=2.
,source_id length=3 format=1. from jobs_all_1) as z;

create table jobs_out_2 as select
x.*
,coalesce(y.jobs,0) as jobs format=6.
from scale_1 as x
left join jobs_out_1 as y on x.yr_id = y.yr_id and x.mgra_id=y.mgra_id and x.employment_type_id = y.employment_type_id
order by yr_id,mgra_id,employment_type_id;

create table jobs_all_2 as select
x.*
,coalesce(y.jobs,0) as jobs format=6.
from scale_2 as x
left join jobs_all_1 as y on x.yr_id = y.yr_id and x.mgra_id=y.mgra_id and x.sandag_industry_id = y.sandag_industry_id
and x.type_id = y.type and x.employment_type_id = y.employment_type_id and x.source_id = y.source_id
order by yr_id,mgra_id,sandag_industry_id,employment_type_id,source_id;
quit;

proc sql;
create table jobs_out_2a as select yr_id,mgra_id,employment_type_id,count(*) as n
from jobs_out_2 group by yr_id,mgra_id,employment_type_id
having calculated n>1;

create table jobs_all_2a as select yr_id,mgra_id,sandag_industry_id,type_id,employment_type_id,source_id,count(*) as n
from jobs_all_2 group by yr_id,mgra_id,sandag_industry_id,type_id,employment_type_id,source_id
having calculated n>1;

create table jobs_out_2b as select yr_id,employment_type_id,sum(jobs) as jobs
from jobs_out_2 group by yr_id,employment_type_id;

create table jobs_all_2b as select yr_id,sandag_industry_id,type_id,employment_type_id,sum(jobs) as jobs
from jobs_all_2 group by yr_id,sandag_industry_id,type_id,employment_type_id;

create table jobs_out_2c as select yr_id,sum(jobs) as jobs
from jobs_out_2 group by yr_id;

create table jobs_all_2c as select yr_id,sum(jobs) as jobs
from jobs_all_2 group by yr_id;
quit;


proc sql;
create table usj_4 as
select yr,mgra
,case 
when sandag_industry_id in (1,2) then 91
when sandag_industry_id in (9,10,11,12,13,14) then 92
/*
when sandag_industry_id in (24,26) then 93
when sandag_industry_id in (23,25) then 94
when sandag_industry_id in (22,27) then 95
*/
when sandag_industry_id in (22,27) then 95
when sandag_industry_id in (23,25) then 94

when sandag_industry_id = 28 and type in (3,4) then 93
when sandag_industry_id = 28 and type = 2 then 21

else sandag_industry_id end as sector
,sum(j) as j
from usj_3a group by yr,mgra,sector;
quit;

proc sql;
create table usj_4_test_1 as select yr,sum(j) as j
from usj_4 group by yr;
quit;


data abm_emp_2;set abm_lu_0(keep= yr mgra taz emp_:);run;

proc sql;
create table abm_emp_2a as select x.*,y.luz,y.sra
from abm_emp_2 as x
inner join mgra_taz as y on x.mgra=y.mgra
order by yr, mgra, taz, luz, sra;
quit;

proc transpose data=abm_emp_2a out=abm_emp_2b;by yr mgra taz luz sra;run;

proc sql;
create table abm_emp_3 as select x.yr, x.mgra,x.taz,x.luz,x.sra,x._name_ as abm_name_1
,y.abm_name_2,y.sandag_industry_id
,coalesce(x.col1,0) as old_j
from abm_emp_2b as x

left join abm_emp_1 as y on x._name_=y.abm_name_1
where x._name_ not in ("emp_total");

create table abm_emp_3a as select *,old_j/sum(old_j) as s1
from abm_emp_3 group by yr,mgra,taz,abm_name_2;

create table abm_emp_4 as select yr,taz,abm_name_1,abm_name_2,sandag_industry_id,sum(old_j) as old_j
from abm_emp_3 group by yr,taz,abm_name_1,abm_name_2,sandag_industry_id;

create table abm_emp_4a as select *,old_j/sum(old_j) as s2
from abm_emp_4 group by yr,taz,abm_name_2;

create table abm_emp_5 as select yr,luz,abm_name_1,abm_name_2,sandag_industry_id,sum(old_j) as old_j
from abm_emp_3 group by yr,luz,abm_name_1,abm_name_2,sandag_industry_id;

create table abm_emp_5a as select *,old_j/sum(old_j) as s3
from abm_emp_5 group by yr,luz,abm_name_2;

create table abm_emp_6 as select yr,sra,abm_name_1,abm_name_2,sandag_industry_id,sum(old_j) as old_j
from abm_emp_3 group by yr,sra,abm_name_1,abm_name_2,sandag_industry_id;

create table abm_emp_6a as select *,old_j/sum(old_j) as s4
from abm_emp_6 group by yr,sra,abm_name_2;

create table abm_emp_7 as select yr,abm_name_1,abm_name_2,sandag_industry_id,sum(old_j) as old_j
from abm_emp_3 group by yr,abm_name_1,abm_name_2,sandag_industry_id;

create table abm_emp_7a as select *,old_j/sum(old_j) as s5
from abm_emp_7 group by yr,abm_name_2;
quit;

proc sql;
create table abm_emp_8 as select 
x.yr,x.mgra,x.taz,x.abm_name_1,x.abm_name_2,x.sandag_industry_id,x.old_j,x.s1
,y.s2,z.s3,u.s4,v.s5
,coalesce(x.s1,y.s2,z.s3,u.s4,v.s5,1) as s
from abm_emp_3a as x
inner join abm_emp_4a as y
on x.yr=y.yr and x.taz=y.taz and x.abm_name_1=y.abm_name_1 and x.abm_name_2=y.abm_name_2 and x.sandag_industry_id=y.sandag_industry_id

inner join abm_emp_5a as z
on x.yr=z.yr and x.luz=z.luz and x.abm_name_1=z.abm_name_1 and x.abm_name_2=z.abm_name_2 and x.sandag_industry_id=z.sandag_industry_id

inner join abm_emp_6a as u
on x.yr=u.yr and x.sra=u.sra and x.abm_name_1=u.abm_name_1 and x.abm_name_2=u.abm_name_2 and x.sandag_industry_id=u.sandag_industry_id

inner join abm_emp_7a as v
on x.yr=v.yr and x.abm_name_1=v.abm_name_1 and x.abm_name_2=v.abm_name_2 and x.sandag_industry_id=v.sandag_industry_id;
quit;

proc sql;
create table test_03 as select distinct yr from abm_emp_8;
quit;

proc sql;
create table abm_emp_9 as 
select yr,mgra,taz,abm_name_1,abm_name_2,sandag_industry_id,s,old_j from abm_emp_8
	union all
select 2018 as yr,mgra,taz,abm_name_1,abm_name_2,sandag_industry_id,s,old_j from abm_emp_8 where yr=2020;
/*
	union all
select 2030 as yr,mgra,taz,abm_name_1,abm_name_2,sandag_industry_id,s,old_j from abm_emp_8 where yr=2025
	union all
select 2045 as yr,mgra,taz,abm_name_1,abm_name_2,sandag_industry_id,s,old_j from abm_emp_8 where yr=2040;
*/
quit;

proc sql;
create table usj_5_test_1 as select x.*,y.*
from (select distinct sector from usj_4) as x
left join (select distinct sandag_industry_id from abm_emp_9) as y on x.sector = y.sandag_industry_id
where y.sandag_industry_id = .;
quit;



proc sql;
create table usj_5 as select x.yr,x.mgra,x.taz,x.abm_name_1,x.abm_name_2,x.sandag_industry_id,x.s,x.old_j
,coalesce(y.j,0) as j_us
,coalesce(y.j*x.s,0) as j1
,round(coalesce(y.j*x.s,0),1) as j2
from abm_emp_9 as x
left join usj_4 as y
on x.yr=y.yr and x.mgra=y.mgra and x.sandag_industry_id=y.sector
order by yr,mgra,abm_name_2,s;
quit;

data usj_6;set usj_5;by yr mgra abm_name_2;retain jc;
if first.abm_name_2 then do; j3 = j2; jc = j3; end;
else if last.abm_name_2 then do; j3 = j_us - jc; jc = jc + j3; end;
else do; j3 = min(j2, max(j_us - jc,0)); jc = jc + j3; end;
run;

/* this table should have zero records */
proc sql;
create table usj_6a as select yr,mgra,abm_name_2,j_us,sum(j3) as j4
from usj_6 group by yr,mgra,abm_name_2,j_us
having j_us^=j4;
quit;

/*
data usj_6b; set usj_6;where yr=2016 and mgra=19093 and abm_name_2="emp_pvt_hh";
run;
*/

proc sql;
create table usj_7 as select x.yr,x.mgra,x.taz
,y.luz,y.sra
,x.abm_name_1,x.abm_name_2,x.sandag_industry_id,x.old_j,x.j3 as new_j
from usj_6 as x
inner join mgra_taz as y on x.mgra=y.mgra
order by yr,mgra;
quit;


proc sql;
create table usj_7a as select yr,sum(old_j) as old_j,sum(new_j) as new_j
,sum(new_j)-sum(old_j) as d
from usj_7 group by yr;

create table usj_7b as select yr,abm_name_2,sum(old_j) as old_j,sum(new_j) as new_j
,sum(new_j)-sum(old_j) as d
from usj_7 group by yr,abm_name_2
order by abs(d) desc;

create table usj_7c as select yr,mgra,taz,sum(old_j) as old_j,sum(new_j) as new_j
,abs(sum(new_j)-sum(old_j)) as d
from usj_7 group by yr,mgra,taz
order by d desc;
quit;


proc transpose data=usj_7 out=usj_8(drop=_name_); by yr mgra;var new_j;id abm_name_1;run;

proc sql; 
create table test_usj8 as 
select yr, sum(emp_pvt_hh) as emp_pvt_hh 
from usj_8 
group by yr
order by yr; 
quit; 

data ready_jobs; retain yr mgra
emp_ag
emp_const_non_bldg_prod
emp_const_non_bldg_office
emp_utilities_prod
emp_utilities_office
emp_const_bldg_prod
emp_const_bldg_office
emp_mfg_prod
emp_mfg_office
emp_whsle_whs
emp_trans
emp_retail
emp_prof_bus_svcs
emp_prof_bus_svcs_bldg_maint
emp_pvt_ed_k12
emp_pvt_ed_post_k12_oth
emp_health
emp_personal_svcs_office
emp_amusement
emp_hotel
emp_restaurant_bar
emp_personal_svcs_retail
emp_religious
emp_pvt_hh
emp_state_local_gov_ent
emp_fed_non_mil
emp_fed_mil
emp_state_local_gov_blue
emp_state_local_gov_white
emp_public_ed
emp_own_occ_dwell_mgmt
emp_fed_gov_accts
emp_st_lcl_gov_accts
emp_cap_accts;
set usj_8;
emp_total = emp_ag + emp_const_non_bldg_prod + emp_const_non_bldg_office + emp_utilities_prod
+ emp_utilities_office + emp_const_bldg_prod + emp_const_bldg_office + emp_mfg_prod
+ emp_mfg_office + emp_whsle_whs + emp_trans + emp_retail + emp_prof_bus_svcs
+ emp_prof_bus_svcs_bldg_maint + emp_pvt_ed_k12 + emp_pvt_ed_post_k12_oth + emp_health
+ emp_personal_svcs_office + emp_amusement + emp_hotel + emp_restaurant_bar
+ emp_personal_svcs_retail + emp_religious + emp_pvt_hh + emp_state_local_gov_ent
+ emp_fed_non_mil + emp_fed_mil + emp_state_local_gov_blue + emp_state_local_gov_white
+ emp_public_ed + emp_own_occ_dwell_mgmt + emp_fed_gov_accts + emp_st_lcl_gov_accts + emp_cap_accts;
run;

proc sql;
create table part_3a as
select yr,mgra
,adultschenrl, ech_dist, hch_dist, pseudomsa
/*,parkarea, hstallsoth, hstallssam, hparkcost, numfreehrs, dstallsoth
,dstallssam, dparkcost, mstallsoth, mstallssam, mparkcost*/

/*CHANGED HERE TO COMMENT OUT THE VARIABLES THAT WU DOESNT WANT ANYMORE*/
/*, totint
,duden, empden, popden, retempden, totintbin, empdenbin, dudenbin
,zip09, parkactive, openspaceparkpreserve, beachactive, budgetroom
,economyroom, luxuryroom, midpriceroom, upscaleroom, hotelroomtotal
,luz_id, truckregiontype, district27, milestocoast */
from abm_lu_0
	union all
select 2018 as yr,mgra
,adultschenrl, ech_dist, hch_dist, pseudomsa
/*,parkarea, hstallsoth, hstallssam, hparkcost, numfreehrs, dstallsoth
,dstallssam, dparkcost, mstallsoth, mstallssam, mparkcost*/

/*, totint
,duden, empden, popden, retempden, totintbin, empdenbin, dudenbin
,zip09, parkactive, openspaceparkpreserve, beachactive, budgetroom
,economyroom, luxuryroom, midpriceroom, upscaleroom, hotelroomtotal
,luz_id, truckregiontype, district27, milestocoast */
from abm_lu_0 where yr=2020;
quit;

proc sql;
create table part_3b as
select yr,mgra
/*,adultschenrl, ech_dist, hch_dist, pseudomsa */

/*,parkarea, hstallsoth, hstallssam, hparkcost, numfreehrs, dstallsoth
,dstallssam, dparkcost, mstallsoth, mstallssam, mparkcost*/
, totint
,duden, empden, popden, retempden, totintbin, empdenbin, dudenbin
,zip09, parkactive, openspaceparkpreserve, beachactive, budgetroom
,economyroom, luxuryroom, midpriceroom, upscaleroom, hotelroomtotal
,luz_id, truckregiontype, district27, milestocoast
from abm_lu_0
	union all
select 2018 as yr,mgra
/*,adultschenrl, ech_dist, hch_dist, pseudomsa*/
/*,parkarea, hstallsoth, hstallssam, hparkcost, numfreehrs, dstallsoth
,dstallssam, dparkcost, mstallsoth, mstallssam, mparkcost*/
, totint
,duden, empden, popden, retempden, totintbin, empdenbin, dudenbin
,zip09, parkactive, openspaceparkpreserve, beachactive, budgetroom
,economyroom, luxuryroom, midpriceroom, upscaleroom, hotelroomtotal
,luz_id, truckregiontype, district27, milestocoast
from abm_lu_0 where yr=2020;
quit;




/* 2015 refers to 2015-2016 academic year */


proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table vi_school_enrollment_mgra13 as select *
from connection to odbc
(select * from isam.dbo.vi_school_enrollment_mgra13);

disconnect from odbc;

quit;



proc sql;
create table school_enr_1 as select * from vi_school_enrollment_mgra13
where yr in (2016) order by mgra;

create table school_enr_2 as select * from vi_school_enrollment_mgra13
where yr in (2016) and mgra^=. order by mgra;

create table school_enr_3 as select yr,mgra,sum(enrollgradekto8) as enrollgradekto8,sum(enrollgrade9to12) as enrollgrade9to12
from school_enr_2 group by yr,mgra;

create table school_enr_3a as select yr,sum(enrollgradekto8) as gk8 ,sum(enrollgrade9to12) as g912
from school_enr_2 group by yr;
quit;

proc import out=col_enr datafile="T:\socioec\Current_Projects\College_Enrollment\University Emp_Enroll.xlsx"
dbms=xlsx replace; sheet="IPEDS";run;

proc import out=col_mgra datafile="T:\socioec\Current_Projects\College_Enrollment\University Emp_Enroll.xlsx"
dbms=xlsx replace; sheet="MGRA";run;

proc sql;
create table col_enr_1 as select id,school,major,enrollment_headcount as enr
from col_enr where enrollment_period="Academic Year 2015-2016";

create table col_mgra_1 as select id,school,mgra,share/sum(share) as s
from col_mgra group by id,school;

create table col_mgra_1a as select distinct id,school from col_mgra_1;

create table col_mgra_1b as select x.*
from col_mgra as x
inner join (select mgra,count(mgra) as n from col_mgra_1 group by mgra having calculated n>1) as y
on x.mgra=y.mgra;
quit;

proc sql;
create table col_mgra_2 as select x.id,x.major,x.enr as t,round(x.enr * y.s,1) as e,y.mgra
from col_enr_1 as x
left join col_mgra_1 as y on x.id=y.id
order by id,e;

create table col_mgra_3 as select mgra,major,sum(e) as enr
from col_mgra_2 group by mgra,major;

create table col_mgra_4 as select x.*
,coalesce(y.enr,0) as collegeenroll
,coalesce(z.enr,0) as othercollegeenroll
from (select distinct mgra from col_mgra_3) as x
left join (select * from col_mgra_3 where major=1) as y on x.mgra=y.mgra
left join (select * from col_mgra_3 where major=0) as z on x.mgra=z.mgra
order by mgra;

create table col_mgra_5 as
/*select 2014 as yr,* from col_mgra_4
	union all
select 2015 as yr,* from col_mgra_4
	union all*/
select 2016 as yr,* from col_mgra_4;
quit;

proc sql;
create table college_test_1 as select x.*
from (select distinct mgra from col_mgra_2 where id in (1,2,3,4)) as x
left join (select distinct mgra from col_mgra_2 where id not in (1,2,3,4)) as y on x.mgra = y.mgra
where y.mgra ^= .;

create table college_test_2 as select mgra,count(id) as n
from col_mgra_2 where id in (1,2,3,4) group by mgra
having calculated n>1;

create table college_test_3 as select * from col_mgra_2 where id in (1,2,3,4)
order by id,e desc;

create table college_test_4 as select *
,case
when col_name = "University of California San Diego" then 1
when col_name = "San Diego State University" then 2
when col_name = "California State University San Marcos" then 3
when col_name = "University of San Diego" then 4
end as id
from (select distinct col_name, mgra from e1.col_enroll)
order by id;
quit;

proc sql;
create table sch_col_mgra as select distinct yr,mgra
from (select distinct yr,mgra from school_enr_3 union all select distinct yr,mgra from col_mgra_5);

create table sch_col_enr_1 as select x.*
,coalesce(y.enrollgradekto8,0) as enrollgradekto8
,coalesce(y.enrollgrade9to12,0) as enrollgrade9to12
,coalesce(z.collegeenroll,0) as collegeenroll
,coalesce(z.othercollegeenroll,0) as othercollegeenroll
from sch_col_mgra as x
left join school_enr_3 as y on x.yr=y.yr and x.mgra=y.mgra
left join col_mgra_5 as z on x.yr=z.yr and x.mgra=z.mgra;

create table sch_col_enr_2 as select x.yr,y.mgra
,coalesce(z.enrollgradekto8,0) as enrollgradekto8
,coalesce(z.enrollgrade9to12,0) as enrollgrade9to12
,coalesce(z.collegeenroll,0) as collegeenroll
,coalesce(z.othercollegeenroll,0) as othercollegeenroll
from (select distinct yr from sch_col_enr_1) as x
cross join (select distinct mgra from part_1) as y
left join sch_col_enr_1 as z on z.mgra=y.mgra;
quit;


proc sql;
create table test_04 as select distinct yr from part_1;
quit;

proc sql;
create table ready_part_1 as select * from part_1;

update ready_part_1 set yr=yr-1;
quit;

proc sql;
create table sch_col_enr_3 as
select * from sch_col_enr_2
	union all
select 2018 as yr, mgra, enrollgradekto8, enrollgrade9to12, collegeenroll, othercollegeenroll from sch_col_enr_2 where yr=2016
	union all
select 2020 as yr, mgra, enrollgradekto8, enrollgrade9to12, collegeenroll, othercollegeenroll from sch_col_enr_2 where yr=2016
	union all
select 2025 as yr, mgra, enrollgradekto8, enrollgrade9to12, collegeenroll, othercollegeenroll from sch_col_enr_2 where yr=2016
	union all
select 2030 as yr, mgra, enrollgradekto8, enrollgrade9to12, collegeenroll, othercollegeenroll from sch_col_enr_2 where yr=2016
	union all
select 2035 as yr, mgra, enrollgradekto8, enrollgrade9to12, collegeenroll, othercollegeenroll from sch_col_enr_2 where yr=2016
	union all
select 2040 as yr, mgra, enrollgradekto8, enrollgrade9to12, collegeenroll, othercollegeenroll from sch_col_enr_2 where yr=2016
	union all
select 2045 as yr, mgra, enrollgradekto8, enrollgrade9to12, collegeenroll, othercollegeenroll from sch_col_enr_2 where yr=2016
	union all
select 2050 as yr, mgra, enrollgradekto8, enrollgrade9to12, collegeenroll, othercollegeenroll from sch_col_enr_2 where yr=2016
	union all

select 2018 as yr,mgra, 0  as enrollgradekto8, 0 as enrollgrade9to12, sum(collegeenroll) as collegeenroll, 0 as  othercollegeenroll
from e1.col_enroll where yr <= 2018 group by mgra
	union all
select 2020 as yr,mgra, 0  as enrollgradekto8, 0 as enrollgrade9to12, sum(collegeenroll) as collegeenroll, 0 as  othercollegeenroll
from e1.col_enroll where yr <= 2020 group by mgra
	union all
select 2025 as yr,mgra, 0  as enrollgradekto8, 0 as enrollgrade9to12, sum(collegeenroll) as collegeenroll, 0 as  othercollegeenroll
from e1.col_enroll where yr <= 2025 group by mgra
	union all
select 2030 as yr,mgra, 0  as enrollgradekto8, 0 as enrollgrade9to12, sum(collegeenroll) as collegeenroll, 0 as  othercollegeenroll
from e1.col_enroll where yr <= 2030 group by mgra
	union all
select 2035 as yr,mgra, 0  as enrollgradekto8, 0 as enrollgrade9to12, sum(collegeenroll) as collegeenroll, 0 as  othercollegeenroll
from e1.col_enroll where yr <= 2035 group by mgra
	union all
select 2040 as yr,mgra, 0  as enrollgradekto8, 0 as enrollgrade9to12, sum(collegeenroll) as collegeenroll, 0 as  othercollegeenroll
from e1.col_enroll where yr <= 2040 group by mgra
	union all
select 2045 as yr,mgra, 0  as enrollgradekto8, 0 as enrollgrade9to12, sum(collegeenroll) as collegeenroll, 0 as  othercollegeenroll
from e1.col_enroll where yr <= 2045 group by mgra
	union all
select 2050 as yr,mgra, 0  as enrollgradekto8, 0 as enrollgrade9to12, sum(collegeenroll) as collegeenroll, 0 as  othercollegeenroll
from e1.col_enroll where yr <= 2050 group by mgra;

create table ready_enrollment as select yr,mgra
,sum(enrollgradekto8) as enrollgradekto8
,sum(enrollgrade9to12) as enrollgrade9to12
,sum(collegeenroll) as  collegeenroll
,sum(othercollegeenroll) as othercollegeenroll
from sch_col_enr_3 group by yr,mgra;
quit;

/*
proc sql;
create table ready_enrollment as

select yr,mgra
,enrollgradekto8
,enrollgrade9to12
,collegeenroll
,othercollegeenroll
from sch_col_enr_2

	union all

select yr,mgra
,enrollgradekto8
,enrollgrade9to12
,collegeenroll
,othercollegeenroll
from abm_lu_0 where yr > &by0

	union all

select 2018 as yr,mgra
,enrollgradekto8
,enrollgrade9to12
,collegeenroll
,othercollegeenroll
from abm_lu_0 where yr=2020

	union all

select 2030 as yr,x.mgra
,round( (x.enrollgradekto8 + y.enrollgradekto8) / 2, 1) as enrollgradekto8
,round( (x.enrollgrade9to12 + y.enrollgrade9to12) / 2, 1) as enrollgrade9to12
,round( (x.collegeenroll + y.collegeenroll) / 2, 1) as collegeenroll
,round( (x.othercollegeenroll + y.othercollegeenroll) / 2, 1) as othercollegeenroll
from abm_lu_0 as x inner join abm_lu_0 as y on x.mgra=y.mgra
where x.yr=2025 and y.yr=2035

	union all

select 2045 as yr,x.mgra
,round( (x.enrollgradekto8 + y.enrollgradekto8) / 2, 1) as enrollgradekto8
,round( (x.enrollgrade9to12 + y.enrollgrade9to12) / 2, 1) as enrollgrade9to12
,round( (x.collegeenroll + y.collegeenroll) / 2, 1) as collegeenroll
,round( (x.othercollegeenroll + y.othercollegeenroll) / 2, 1) as othercollegeenroll
from abm_lu_0 as x inner join abm_lu_0 as y on x.mgra=y.mgra
where x.yr=2040 and y.yr=2050

;
quit;
*/

proc import out=from_WU datafile="T:\socioec\Current_Projects\XPEF06\input_data\from_WU.xlsx"
dbms=xlsx replace; sheet="Data";run;

/*import file that Wu indicated has new info for autonomous vehicles and charging stations*/
proc import out=from_WU_2 datafile="T:\ABM\release_test\ABM\version_14_2_0\input\2016\mgra13_based_input2016.csv"
dbms=csv replace; run;


proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;");

create table parking_ratio_1 as select mgra,parking_ratio
from connection to odbc
(select * from [spacecore].[input].[vi_costar_parking_ratio_mgra] where parking_ratio is not NULL);

/*
create table abm_1 as select *
from connection to odbc
(select * from [isam].[&xver].[abm_mgra13_based_input]);
*/

disconnect from odbc;
quit;

/* the filename may change !!!! */
PROC IMPORT OUT=new_parking_1 DATAFILE="T:\socioec\pecas\data\parking2016\mgra13_based_input2016_updated_103118.csv"
DBMS=CSV REPLACE; GETNAMES=YES; DATAROW=2;
RUN; 


proc sql;
create table new_parking_1a as select 2016 as yr,mgra
,sum(hstallsoth) as h_o
,sum(hstallssam) as h_s
,sum(dstallsoth) as d_o
,sum(dstallssam) as d_s
,sum(mstallsoth) as m_o
,sum(mstallssam) as m_s
from new_parking_1 /*abm_1*/
group by yr,mgra;

create table new_parking_1b as select * from new_parking_1a where m_o> 0 or m_s > 0;

create table new_parking_1c as select yr
,sum(m_o) as m_o,sum(m_s) as m_s
,sum(d_o) as d_o,sum(d_s) as d_s
,sum(h_o) as h_o,sum(h_s) as h_s
from new_parking_1a group by yr;
quit;


proc sql;
create table jfc_00 as select parcel_id,mgra,yr,count(*) as j
from e1.jobs_from_capacities group by parcel_id,mgra,yr;

create table jfc_01 as select yr,count(*) as j
from e1.jobs_from_capacities group by yr;
quit;

proc sql;
create table jfc_0 as select parcel_id,yr,count(*) as j
from e1.jobs_from_capacities group by parcel_id,yr;

create table jfc_0a as select yr,sum(j) as j
from jfc_0 group by yr;

create table jfc_0b as select parcel_id, sum(j) as j, min(yr) as min_yr, max(yr) as max_yr, count(yr) as n_yr
from jfc_0 group by parcel_id;
quit;

proc sql;
create table slots_by_source_1 as select parcel_id,mgra
,sum(slots_capacity + slots_events) as slots
from e1.job_slots_by_source
where slots_capacity > 0 or slots_events > 0
group by parcel_id,mgra;

create table jfc_1 as select parcel_id,yr,count(*) as j
from e1.jobs_from_capacities group by parcel_id,yr;

create table jfc_2 as select x.*,y.mgra
from jfc_1 as x
inner join slots_by_source_1 as y on x.parcel_id=y.parcel_id;

create table jfc_3 as select mgra,yr,sum(j) as j
from jfc_2 group by mgra,yr;

create table jfc_4 as
select 2018 as yr,mgra,sum(j) as j from jfc_3 where yr<=2018 group by mgra
	union all
select 2020 as yr,mgra,sum(j) as j from jfc_3 where yr<=2020 group by mgra
	union all
select 2025 as yr,mgra,sum(j) as j from jfc_3 where yr<=2025 group by mgra
	union all
select 2030 as yr,mgra,sum(j) as j from jfc_3 where yr<=2030 group by mgra
	union all
select 2035 as yr,mgra,sum(j) as j from jfc_3 where yr<=2035 group by mgra
	union all
select 2040 as yr,mgra,sum(j) as j from jfc_3 where yr<=2040 group by mgra
	union all
select 2045 as yr,mgra,sum(j) as j from jfc_3 where yr<=2045 group by mgra
	union all
select 2050 as yr,mgra,sum(j) as j from jfc_3 where yr<=2050 group by mgra
order by mgra,yr;

create table jfc_4a as select * from jfc_4
where yr=2050 order by j desc;
quit;


proc sql;
create table jfc_5 as select x.*,y.parking_ratio
from jfc_4 as x
left join parking_ratio_1 as y on x.mgra=y.mgra;

create table jfc_5a as select * from jfc_5 where yr=2050 and parking_ratio=.
order by j desc;

create table jfc_6 as select *
,round((j * 200)/1000 * parking_ratio,1) as p
from jfc_5 where parking_ratio ^= .
order by mgra,yr;

create table jfc_6a as select yr,sum(p) as p
from jfc_6 group by yr;
quit;


proc sql;
create table p_jobs_all_1 as select mgra,yr,sum(j) as j
from e1.jobs_all group by mgra,yr;

create table p_jobs_all_2 as select yr,sum(j) as j
from p_jobs_all_1 group by yr;
quit;

proc sql;
create table new_parking_2 as
select 2016 as yr,mgra
,parkarea as parkarea
,hstallsoth as hstallsoth
,hstallssam as hstallssam
,hparkcost as hparkcost
,numfreehrs as numfreehrs
,dstallsoth as dstallsoth
,dstallssam as dstallssam
,dparkcost as dparkcost
,mstallsoth as mstallsoth
,mstallssam as mstallssam
,mparkcost as mparkcost
from new_parking_1
	union all
select z.yr,x.mgra
,x.parkarea as parkarea
,x.hstallsoth + coalesce(y.p,0) as hstallsoth
,x.hstallssam + coalesce(y.p,0) as hstallssam
,x.hparkcost as hparkcost
,x.numfreehrs as numfreehrs
,x.dstallsoth + coalesce(y.p,0) as dstallsoth
,x.dstallssam + coalesce(y.p,0) as dstallssam
,x.dparkcost as dparkcost
,x.mstallsoth + coalesce(y.p,0) as mstallsoth
,x.mstallssam + coalesce(y.p,0) as mstallssam
,x.mparkcost as mparkcost
from new_parking_1 as x
cross join (select distinct yr from jfc_6) as z
left join jfc_6 as y on x.mgra=y.mgra and z.yr=y.yr
order by yr,mgra;
quit;


proc sql;
create table final_01 as select x.*,x1.*,x2.*,x3.*,x4.*,x5.*,x6.*,x7.MicroAccessTime,x7.remoteAVParking,x7.refueling_stations
from ready_part_1 as x
inner join ready_jobs as x1 on x.yr=x1.yr and x.mgra=x1.mgra
inner join ready_enrollment as x2 on x.yr=x2.yr and x.mgra=x2.mgra
inner join part_3a as x3 on x.yr=x3.yr and x.mgra=x3.mgra

inner join new_parking_2 as x4 on x.yr=x4.yr and x.mgra=x4.mgra

inner join part_3b as x5 on x.yr=x5.yr and x.mgra=x5.mgra

inner join from_wu as x6 on x.mgra=x6.mgra

/*added in join for the new data from Wu*/
inner join from_wu_2 as x7 on x.mgra=x7.mgra
order by mgra,taz,yr;
quit;

proc sql; 
create table test_final_01 as
select yr, count(mgra) as mgra 
from final_01 
group by yr
order by yr; 
quit; 

/* This section interpolates the data in the non-standard years */
/* First, Recreate final_01 using only the standard years (except for data from ready_part_1) */
proc sql;
create table zz_final_01 as select x.*,x1.*,x2.*,x3.*,x4.*,x5.*,x6.*,x7.MicroAccessTime,x7.remoteAVParking,x7.refueling_stations
from ready_part_1 as x
left join (select * from ready_jobs where yr in (2016,2018,2020,2025,2030,2035,2040,2045,2050)) as x1 on x.yr=x1.yr and x.mgra=x1.mgra
left join (select * from ready_enrollment where yr in (2016,2018,2020,2025,2030,2035,2040,2045,2050)) as x2 on x.yr=x2.yr and x.mgra=x2.mgra
left join (select * from part_3a where yr in (2016,2018,2020,2025,2030,2035,2040,2045,2050)) as x3 on x.yr=x3.yr and x.mgra=x3.mgra
left join (select * from new_parking_2 where yr in (2016,2018,2020,2025,2030,2035,2040,2045,2050)) as x4 on x.yr=x4.yr and x.mgra=x4.mgra
left join (select yr, mgra, zip09, parkactive,openspaceparkpreserve,beachactive,budgetroom,economyroom,luxuryroom, 
                  midpriceroom,upscaleroom,hotelroomtotal,luz_id,truckregiontype,district27,milestocoast 
from part_3b where yr in (2016,2018,2020,2025,2030,2035,2040,2045,2050)) as x5 on x.yr=x5.yr and x.mgra=x5.mgra
left join from_wu as x6 on x.mgra=x6.mgra

/*CHANGED added in to add data from wu*/
left join from_wu_2 as x7 on x.mgra=x7.mgra 
order by mgra, yr;
quit;
/**/
/*proc sql; */
/*create table test_ready_jobs as */
/*select yr, sum(emp_pvt_hh) as emp_pvt_hh */
/*from ready_jobs */
/*group by yr*/
/*order by yr; */
/*quit; */

/*test to see if the private hh jobs are maintained*/
proc sql; 
create table test_zz_final_01 as 
select yr, sum(emp_pvt_hh) as emp_pvt_hh 
from zz_final_01 
group by yr
order by yr; 
quit; 

/*CHANGE--commented out the variables Wu doesnt want*/
/* Use linear interpolation to fill the data for non-standard years */
proc expand data=zz_final_01 out=zz_final_02;
	by mgra;
	id yr;
	convert emp_ag emp_const_non_bldg_prod emp_const_non_bldg_office emp_utilities_prod emp_utilities_office emp_const_bldg_prod 
		emp_const_bldg_office emp_mfg_prod emp_mfg_office emp_whsle_whs emp_trans emp_retail emp_prof_bus_svcs emp_prof_bus_svcs_bldg_maint 
		emp_pvt_ed_k12 emp_pvt_ed_post_k12_oth emp_health emp_personal_svcs_office emp_amusement emp_hotel emp_restaurant_bar emp_personal_svcs_retail 
		emp_religious emp_pvt_hh emp_state_local_gov_ent emp_fed_non_mil emp_fed_mil emp_state_local_gov_blue emp_state_local_gov_white emp_public_ed 
		emp_own_occ_dwell_mgmt emp_fed_gov_accts emp_st_lcl_gov_accts emp_cap_accts emp_total enrollgradekto8 enrollgrade9to12 collegeenroll 
		othercollegeenroll adultschenrl ech_dist hch_dist pseudomsa parkarea hstallsoth hstallssam hparkcost numfreehrs dstallsoth dstallssam dparkcost 
		mstallsoth mstallssam mparkcost /*totint duden empden popden retempden totintbin empdenbin dudenbin*/ zip09 parkactive openspaceparkpreserve 
		beachactive budgetroom economyroom luxuryroom midpriceroom upscaleroom hotelroomtotal luz_id truckregiontype district27 milestocoast / method=join;
run;

proc sql; 
create table test_zz_final_02 as 
select yr, sum(emp_pvt_hh) as emp_pvt_hh 
from zz_final_02
group by yr
order by yr; 
quit; 

/* Round the interpolated variables back to integers where needed */
proc sql;
create table zz_final_03 as select
yr ,mgra ,taz ,hs ,hs_sf ,hs_mf ,hs_mh ,hh ,hh_sf ,hh_mf ,hh_mh ,gq_civ ,gq_mil ,i1 ,i2 ,i3 ,i4 ,i5 ,i6 ,i7 ,i8 ,i9 ,i10 ,hhs ,pop ,hhp 
/* the variable above come from the simulation data and did not need interpolation */
,round(emp_ag,1) as emp_ag
,round(emp_const_non_bldg_prod,1) as emp_const_non_bldg_prod 
,round(emp_const_non_bldg_office,1) as emp_const_non_bldg_office 
,round(emp_utilities_prod,1) as emp_utilities_prod 
,round(emp_utilities_office,1) as emp_utilities_office 
,round(emp_const_bldg_prod,1) as emp_const_bldg_prod 
,round(emp_const_bldg_office,1) as emp_const_bldg_office 
,round(emp_mfg_prod,1) as emp_mfg_prod 
,round(emp_mfg_office,1) as emp_mfg_office 
,round(emp_whsle_whs,1) as emp_whsle_whs 
,round(emp_trans,1) as emp_trans 
,round(emp_retail,1) as emp_retail 
,round(emp_prof_bus_svcs,1) as emp_prof_bus_svcs 
,round(emp_prof_bus_svcs_bldg_maint,1) as emp_prof_bus_svcs_bldg_maint 
,round(emp_pvt_ed_k12,1) as emp_pvt_ed_k12 
,round(emp_pvt_ed_post_k12_oth,1) as emp_pvt_ed_post_k12_oth 
,round(emp_health,1) as emp_health 
,round(emp_personal_svcs_office,1) as emp_personal_svcs_office 
,round(emp_amusement,1) as emp_amusement 
,round(emp_hotel,1) as emp_hotel 
,round(emp_restaurant_bar,1) as emp_restaurant_bar 
,round(emp_personal_svcs_retail,1) as emp_personal_svcs_retail 
,round(emp_religious,1) as emp_religious 
,round(emp_pvt_hh,1) as emp_pvt_hh 
,round(emp_state_local_gov_ent,1) as emp_state_local_gov_ent 
,round(emp_fed_non_mil,1) as emp_fed_non_mil 
,round(emp_fed_mil,1) as emp_fed_mil 
,round(emp_state_local_gov_blue,1) as emp_state_local_gov_blue 
,round(emp_state_local_gov_white,1) as emp_state_local_gov_white 
,round(emp_public_ed,1) as emp_public_ed 
,round(emp_own_occ_dwell_mgmt,1) as emp_own_occ_dwell_mgmt 
,round(emp_fed_gov_accts,1) as emp_fed_gov_accts 
,round(emp_st_lcl_gov_accts,1) as emp_st_lcl_gov_accts 
,round(emp_cap_accts,1) as emp_cap_accts 
,round(emp_total,1) as emp_total 
,round(enrollgradekto8,1) as enrollgradekto8 
,round(enrollgrade9to12,1) as enrollgrade9to12 
,round(collegeenroll,1) as collegeenroll 
,round(othercollegeenroll,1) as othercollegeenroll 
,round(adultschenrl,1) as adultschenrl 
,round(ech_dist,1) as ech_dist 
,round(hch_dist,1) as hch_dist 
,round(pseudomsa,1) as pseudomsa 
,round(parkarea,1) as parkarea 
,round(hstallsoth,1) as hstallsoth 
,round(hstallssam,1) as hstallssam 
,round(hparkcost,1) as hparkcost 
,round(numfreehrs,1) as numfreehrs 
,round(dstallsoth,1) as dstallsoth 
,round(dstallssam,1) as dstallssam 
,round(dparkcost,1) as dparkcost 
,round(mstallsoth,1) as mstallsoth 
,round(mstallssam,1) as mstallssam 
,round(mparkcost,1) as mparkcost 
,. as totint
,. as duden
,. as empden
,. as popden
,. as retempden
,. as totintbin
,. as empdenbin
,. as dudenbin
/*,round(totint,1) as totint */
/*,duden */
/*,empden */
/*,popden */
/*,retempden */
/*,round(totintbin,1) as totintbin */
/*,round(empdenbin,1) as empdenbin */
/*,round(dudenbin,1) as dudenbin */
,round(zip09,1) as zip09 
,parkactive
,openspaceparkpreserve
,beachactive
,round(budgetroom,1) as budgetroom 
,round(economyroom,1) as economyroom 
,round(luxuryroom,1) as luxuryroom 
,round(midpriceroom,1) as midpriceroom 
,round(upscaleroom,1) as upscaleroom 
,round(hotelroomtotal,1) as hotelroomtotal 
,round(luz_id,1) as luz_id 
,round(truckregiontype,1) as truckregiontype 
,round(district27,1) as district27 
,milestocoast
,acres
,effective_acres
,land_acres
,MicroAccessTime
,remoteAVParking
,refueling_stations
from zz_final_02;
quit;



/* Update parking */
proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;");

create table mgra_parking_update as select *
from connection to odbc
(select *
from ws.dbo.noz_parking_mgra_info);

disconnect from odbc;

create table zz_final_04 as select a.*
	,b.pricehr2025,b.pricehr2035,b.pricehr2050
	,b.priceday2025,b.priceday2035,b.priceday2050
	,b.pricemon2025,b.pricemon2035,b.pricemon2050
	,b.baseline_req_pricing,b.annual_chg_2036_2050
from zz_final_03 a
left join mgra_parking_update b
	on a.mgra = b.mgra
order by mgra,yr;
quit;

data zz_final_05;set zz_final_04;by mgra;retain empt empb hstallsothb hstallssamb dstallsothb dstallssamb mstallsothb mstallssamb;
if first.mgra then do;empc=0;empt=0;empb=emp_total;
	hstallsoth2=hstallsoth;hstallssam2=hstallssam;dstallsoth2=dstallsoth;dstallssam2=dstallssam;mstallsoth2=mstallsoth;mstallssam2=mstallssam;
	hstallsothb=hstallsoth;hstallssamb=hstallssam;dstallsothb=dstallsoth;dstallssamb=dstallssam;mstallsothb=mstallsoth;mstallssamb=mstallssam;
	end;
if baseline_req_pricing=. then do;empc=.;empt=.;empb=.;
	hstallsoth2=hstallsoth;hstallssam2=hstallssam;dstallsoth2=dstallsoth;dstallssam2=dstallssam;mstallsoth2=mstallsoth;mstallssam2=mstallssam;
	hstallsothb=hstallsoth;hstallssamb=hstallssam;dstallsothb=dstallsoth;dstallssamb=dstallssam;mstallsothb=mstallsoth;mstallssamb=mstallssam;
	end;
if (yr=2035 and baseline_req_pricing^=.) then do;empc=max(empt,emp_total-empb,0);empt=empc;empb=emp_total;
		hstallsothb=coalesce(hstallsothb+round(empc*(300/baseline_req_pricing),1),hstallsothb);
		hstallssamb=coalesce(hstallssamb+round(empc*(300/baseline_req_pricing),1),hstallssamb);
		dstallsothb=coalesce(dstallsothb+round(empc*(300/baseline_req_pricing),1),dstallsothb);
		dstallssamb=coalesce(dstallssamb+round(empc*(300/baseline_req_pricing),1),dstallssamb);
		mstallsothb=coalesce(mstallsothb+round(empc*(300/baseline_req_pricing),1),mstallsothb);
		mstallssamb=coalesce(mstallssamb+round(empc*(300/baseline_req_pricing),1),mstallssamb);
		hstallsoth2=hstallsothb;hstallssam2=hstallssamb;dstallsoth2=dstallsothb;dstallssam2=dstallssamb;mstallsoth2=mstallsothb;mstallssam2=mstallssamb;
		end;
if (yr>2035 and baseline_req_pricing^=.) then do;empc=min(empt,emp_total-empb,0);empt=empc;empb=empb;
		hstallsothb=hstallsothb;hstallssamb=hstallssamb;dstallsothb=dstallsothb;dstallssamb=dstallssamb;mstallsothb=mstallsothb;mstallssamb=mstallssamb;
		hstallsoth2=max(round(coalesce(hstallsothb+round(empc*(300/baseline_req_pricing),1),hstallsothb)*((1+coalesce(annual_chg_2036_2050,0))**(yr-2035)),1),0);
		hstallssam2=max(round(coalesce(hstallssamb+round(empc*(300/baseline_req_pricing),1),hstallssamb)*((1+coalesce(annual_chg_2036_2050,0))**(yr-2035)),1),0);
		dstallsoth2=max(round(coalesce(dstallsothb+round(empc*(300/baseline_req_pricing),1),dstallsothb)*((1+coalesce(annual_chg_2036_2050,0))**(yr-2035)),1),0);
		dstallssam2=max(round(coalesce(dstallssamb+round(empc*(300/baseline_req_pricing),1),dstallssamb)*((1+coalesce(annual_chg_2036_2050,0))**(yr-2035)),1),0);
		mstallsoth2=max(round(coalesce(mstallsothb+round(empc*(300/baseline_req_pricing),1),mstallsothb)*((1+coalesce(annual_chg_2036_2050,0))**(yr-2035)),1),0);
		mstallssam2=max(round(coalesce(mstallssamb+round(empc*(300/baseline_req_pricing),1),mstallssamb)*((1+coalesce(annual_chg_2036_2050,0))**(yr-2035)),1),0);
		end;
if (yr<2035 and baseline_req_pricing^=.) then do;empc=max(empt,emp_total-empb,0);empt=empc;empb=empb;
		hstallsothb=hstallsothb;hstallssamb=hstallssamb;dstallsothb=dstallsothb;dstallssamb=dstallssamb;mstallsothb=mstallsothb;mstallssamb=mstallssamb;
		hstallsoth2=coalesce(hstallsothb+round(empc*(300/baseline_req_pricing),1),hstallsothb);
		hstallssam2=coalesce(hstallssamb+round(empc*(300/baseline_req_pricing),1),hstallssamb);
		dstallsoth2=coalesce(dstallsothb+round(empc*(300/baseline_req_pricing),1),dstallsothb);
		dstallssam2=coalesce(dstallssamb+round(empc*(300/baseline_req_pricing),1),dstallssamb);
		mstallsoth2=coalesce(mstallsothb+round(empc*(300/baseline_req_pricing),1),mstallsothb);
		mstallssam2=coalesce(mstallssamb+round(empc*(300/baseline_req_pricing),1),mstallssamb);end;
run;

proc sql;
create table zz_final_06 as select
yr ,mgra ,taz ,hs ,hs_sf ,hs_mf ,hs_mh ,hh ,hh_sf ,hh_mf ,hh_mh ,gq_civ ,gq_mil ,i1 ,i2 ,i3 ,i4 ,i5 ,i6 ,i7 ,i8 ,i9 ,i10 ,hhs ,pop ,hhp 
,emp_ag ,emp_const_non_bldg_prod ,emp_const_non_bldg_office ,emp_utilities_prod ,emp_utilities_office ,emp_const_bldg_prod ,emp_const_bldg_office 
,emp_mfg_prod ,emp_mfg_office ,emp_whsle_whs ,emp_trans ,emp_retail ,emp_prof_bus_svcs ,emp_prof_bus_svcs_bldg_maint ,emp_pvt_ed_k12 ,emp_pvt_ed_post_k12_oth 
,emp_health ,emp_personal_svcs_office ,emp_amusement ,emp_hotel ,emp_restaurant_bar ,emp_personal_svcs_retail ,emp_religious ,emp_pvt_hh ,emp_state_local_gov_ent 
,emp_fed_non_mil ,emp_fed_mil ,emp_state_local_gov_blue ,emp_state_local_gov_white ,emp_public_ed ,emp_own_occ_dwell_mgmt ,emp_fed_gov_accts ,emp_st_lcl_gov_accts 
,emp_cap_accts ,emp_total ,enrollgradekto8 ,enrollgrade9to12 ,collegeenroll ,othercollegeenroll ,adultschenrl ,ech_dist ,hch_dist ,pseudomsa ,parkarea 

,COALESCE(hstallsoth2,hstallsoth) as hstallsoth
,COALESCE(hstallssam2,hstallssam) as hstallssam 
,COALESCE(CASE WHEN yr = 2050 THEN pricehr2050
	WHEN yr >= 2035 THEN pricehr2035
	WHEN yr >= 2025 THEN pricehr2025
	END, hparkcost) as hparkcost
,numfreehrs 
,COALESCE(dstallsoth2,dstallsoth) as dstallsoth 
,COALESCE(dstallssam2,dstallssam) as dstallssam 
,COALESCE(CASE WHEN yr = 2050 THEN priceday2050
	WHEN yr >= 2035 THEN priceday2035
	WHEN yr >= 2025 THEN priceday2025
	END, dparkcost) as dparkcost 
,COALESCE(mstallsoth2,mstallsoth) as mstallsoth 
,COALESCE(mstallssam2,mstallssam) as mstallssam 
,COALESCE(CASE WHEN yr = 2050 THEN pricemon2050
	WHEN yr >= 2035 THEN pricemon2035
	WHEN yr >= 2025 THEN pricemon2025
	END, mparkcost) as mparkcost 

,totint ,duden ,empden ,popden ,retempden ,totintbin ,empdenbin ,dudenbin
,zip09 ,parkactive ,openspaceparkpreserve ,beachactive ,budgetroom ,economyroom ,luxuryroom ,midpriceroom ,upscaleroom ,hotelroomtotal ,luz_id ,truckregiontype 
,district27 ,milestocoast ,acres ,effective_acres ,land_acres ,MicroAccessTime ,remoteAVParking ,refueling_stations
from zz_final_05
order by mgra,yr;
quit;


/* Overwrite final_01 with the new version */
/* _03 is old parking, _06 is new parking */
proc sql;
create table final_01 as select * from zz_final_03;
create table final_01_np as select * from zz_final_06;
quit;
