
%let dw1=2017,2019,2021,2026,2031,2036,2041,2046,2051;

/*
ethnicity_id
1 hisp
2 white
3 black
4 american indian
5 asian
6 pacific islander
7 other
8 2+

sex_id: 1 - Female, 2 - Male

housing_type_id
1 - hh
2 - gq mil
3 - gq col
4 - gq ins + gq oth
*/

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table mgra_id as select mgra_id,mgra,jurisdiction_id as jur,cpa_id as cpa
from connection to odbc
(select * FROM [demographic_warehouse].[dim].[mgra_denormalize] where series=14);

disconnect from odbc;
quit;


proc sql;
create table pop_est_2 as
select
x.yr-1 as yr_id
,1 as housing_type_id
,case
when x.age > 100 then 20 else v.age_group_id end as age_group_id
,case
when x.sex="F" then 1 else 2 end as sex_id
,case
when x.hisp="H" then 1
when x.r="R10" then 2
when x.r="R02" then 3
when x.r="R03" then 4
when x.r="R04" then 5
when x.r="R05" then 6
when x.r="R06" then 7
when x.r="R07" then 8 end as ethnicity_id
,u.mgra_id

from (select * from sql_xpef.household_population where yr in (&dw1)) as x
inner join sql_xpef.households as y on x.hh_id=y.hh_id and x.yr=y.yr
inner join sql_xpef.housing_units as z on y.hh_id=z.hh_id and y.yr=z.yr
left join mgra_id as u on z.mgra=u.mgra and z.jur=u.jur and z.cpa=u.cpa
left join sql_dim.age_group as v on v.lower_bound<=x.age<=v.upper_bound

union all

/*
housing_type_id
1 - hh
2 - gq mil
3 - gq col
4 - gq ins + gq oth
*/

select
x.yr - 1 as yr_id
,case
when x.gq_type="MIL" then 2
when x.gq_type="COL" then 3
else 4 end as housing_type_id
,case
when x.age > 100 then 20 else v.age_group_id end as age_group_id
,case when x.sex="F" then 1 else 2 end as sex_id
,case
when x.hisp="H" then 1
when x.r="R10" then 2
when x.r="R02" then 3
when x.r="R03" then 4
when x.r="R04" then 5
when x.r="R05" then 6
when x.r="R06" then 7
when x.r="R07" then 8 end as ethnicity_id
,u.mgra_id
from (select * from sql_xpef.gq_population where yr in (&dw1)) as x
left join mgra_id as u
on x.mgra=u.mgra and x.jur=u.jur and x.cpa=u.cpa
left join sql_dim.age_group as v on v.lower_bound<=x.age<=v.upper_bound;

quit;

proc sql;
create table hh_est_01 as
select x.yr-1 as yr_id,u.mgra_id,x.household_size_id,count(x.hh_id) as households
from (select yr,hh_id,case when size>7 then 7 else size end as household_size_id from sql_xpef.households where yr in (&dw1)) as x
left join sql_xpef.housing_units as y on x.hh_id=y.hh_id and x.yr=y.yr
left join mgra_id as u
on y.mgra=u.mgra and y.jur=u.jur and y.cpa=u.cpa
group by yr_id,mgra_id,household_size_id;
quit;



proc sql;
create table hu_est_01 as
select x.yr-1 as yr_id,u.mgra_id,x.du_type as du_type length=3 format=$3.,x.sto_flag
,count(x.hu_id) as hu_all
from (select * from sql_xpef.housing_units where yr in (&dw1)) as x
left join mgra_id as u
on x.mgra=u.mgra and x.jur=u.jur and x.cpa=u.cpa
group by yr_id,mgra_id,du_type,sto_flag;

create table hu_est_02 as
select x.yr-1 as yr_id,u.mgra_id,x.du_type as du_type length=3 format=$3.,x.sto_flag
,count(x.hu_id) as hu_occ
from (select * from sql_xpef.housing_units where hh_id^=. and yr in (&dw1)) as x
left join mgra_id as u
on x.mgra=u.mgra and x.jur=u.jur and x.cpa=u.cpa
group by yr_id,mgra_id,du_type,sto_flag;
quit;


proc sql;
create table hu_est_2a as select x.yr_id,x.mgra_id,x.du_type,x.hu_all,coalesce(y.hu_occ,0) as hu_occ,coalesce(z.hu_all,0) as hu_unocc
from hu_est_01 as x
left join hu_est_02 as y
	on x.yr_id=y.yr_id and x.mgra_id=y.mgra_id and x.du_type=y.du_type and x.sto_flag=y.sto_flag
left join (select * from hu_est_01 where sto_flag=1) as z
	on x.yr_id=z.yr_id and x.mgra_id=z.mgra_id and x.du_type=z.du_type and x.sto_flag=z.sto_flag;

create table hu_est_3 as select yr_id,mgra_id,du_type
,sum(hu_all) as hu_all,sum(hu_occ) as hu_occ,sum(hu_all-hu_occ) as hu_vac,sum(hu_unocc) as hu_unocc
from hu_est_2a group by yr_id,mgra_id,du_type;
quit;

proc sql;
create table test_01 as select yr_id,sum(hu_unocc) as hu_unocc
from hu_est_3 group by yr_id;
quit;

/*
structure_type_id
1 - sf
2 - sfmu
3 - mf
4 - mh
*/

proc sql;
create table hu_est_4 as 
select yr_id,mgra_id
,case
when du_type="SFD" then 1
when du_type="SFA" then 2
when du_type="MF" then 3
when du_type="MH" then 4
end as structure_type_id
,hu_all as units
,hu_unocc as unoccupiable
,hu_occ as occupied
,hu_vac as vacancy
from hu_est_3;
quit;


proc sql;
create table hu_est_4a as select max(units) as max_units from hu_est_4;
create table hu_est_4b as select x.* from hu_est_4 as x 
inner join hu_est_4a as y on x.units=y.max_units; 

create table hu_est_4c as select yr_id
,sum(units) as units
,sum(unoccupiable) as unoccupiable
,sum(occupied) as occupied
,sum(vacancy) as vacancy
from hu_est_4 group by yr_id;
quit;

proc sql;
create table max_hh as select max(households) as max_hh from hh_est_01;
quit;


proc sql;
create table scale_yr as select distinct yr_id length=3 format=4. from hu_est_4;

/* create table scale_mgra as select distinct mgra_id length=5 format=7. from mgra_sra;*/

create table scale_mgra as select mgra_id length=6 format=10.
from mgra_id
order by mgra_id;

create table scale_mgra_yr as select x.*,y.* from scale_mgra as x cross join scale_yr as y;

create table scale_households as select distinct household_size_id length=3 format=1. from hh_est_01/*sql_dim.housing_type*/;

create table scale_housing as select distinct housing_type_id length=3 format=1. from sql_dim.housing_type;
create table scale_age as select distinct age_group_id length=3 format=2. from sql_dim.age_group;
create table scale_sex as select distinct sex_id length=3 format=1. from sql_dim.sex;
create table scale_ethnicity as select distinct ethnicity_id length=3 format=1. from sql_dim.ethnicity;
create table scale_structure as select distinct structure_type_id length=3 format=1. from sql_dim.structure_type;
quit;


proc sql;
create table dw_population_0 as select 
yr_id,mgra_id,housing_type_id,count(*) as population
from pop_est_2 group by yr_id,mgra_id,housing_type_id;

create table dw_age_0 as select 
yr_id,mgra_id,age_group_id,count(*) as population
from pop_est_2 group by yr_id,mgra_id,age_group_id;

create table dw_sex_0 as select 
yr_id,mgra_id,sex_id,count(*) as population
from pop_est_2 group by yr_id,mgra_id,sex_id;

create table dw_ethnicity_0 as select 
yr_id,mgra_id,ethnicity_id,count(*) as population
from pop_est_2 group by yr_id,mgra_id,ethnicity_id;

create table dw_age_sex_ethnicity_0 as select 
yr_id,mgra_id,age_group_id,sex_id,ethnicity_id,count(*) as population
from pop_est_2 group by yr_id,mgra_id,age_group_id,sex_id,ethnicity_id;
quit;


proc sql;
create table dw_housing_0 as select
yr_id length=3 format=4.
,mgra_id length=6 format=10.
,structure_type_id length=3 format=1.
,units length=4 format=5.
,unoccupiable length=4 format=5.
,occupied length=4 format=5.
,vacancy length=4 format=5.
from hu_est_4;
quit;

proc sql;
create table dw_households_0 as select
yr_id length=3 format=4.
,mgra_id length=6 format=10.
,household_size_id length=3 format=1.
,households length=4 format=5.
from hh_est_01;
quit;


proc sql;
create table dw_housing as select
x1.* ,y.*
,coalesce(z.units,0) as units length=4 format=5.
,coalesce(z.unoccupiable,0) as unoccupiable length=4 format=5.
,coalesce(z.occupied,0) as occupied length=4 format=5.
,coalesce(z.vacancy,0) as vacancy length=4 format=5.
from scale_mgra_yr as x1
cross join scale_structure as y
left join dw_housing_0 as z
on x1.yr_id=z.yr_id and x1.mgra_id=z.mgra_id and y.structure_type_id=z.structure_type_id;
quit;

proc sql;
create table dw_households as select
x1.* ,y.*, coalesce(z.households,0) as households length=5 format=7.
from scale_mgra_yr as x1
cross join scale_households as y
left join dw_households_0 as z
on x1.yr_id=z.yr_id and x1.mgra_id=z.mgra_id and y.household_size_id=z.household_size_id;

create table dw_population as select
x1.* ,y.*, coalesce(z.population,0) as population length=5 format=7.
from scale_mgra_yr as x1
cross join scale_housing as y
left join dw_population_0 as z
on x1.yr_id=z.yr_id and x1.mgra_id=z.mgra_id and y.housing_type_id=z.housing_type_id;

create table dw_age as select 
x1.* ,y.*, coalesce(z.population,0) as population length=5 format=7.
from scale_mgra_yr as x1
cross join scale_age as y
left join dw_age_0 as z
on x1.yr_id=z.yr_id and x1.mgra_id=z.mgra_id and y.age_group_id=z.age_group_id;

create table dw_sex as select
x1.* ,y.*, coalesce(z.population,0) as population length=5 format=7.
from scale_mgra_yr as x1
cross join scale_sex as y
left join dw_sex_0 as z
on x1.yr_id=z.yr_id and x1.mgra_id=z.mgra_id and y.sex_id=z.sex_id;

create table dw_ethnicity as select
x1.* ,y.*, coalesce(z.population,0) as population length=5 format=7.
from scale_mgra_yr as x1
cross join scale_ethnicity as y
left join dw_ethnicity_0 as z
on x1.yr_id=z.yr_id and x1.mgra_id=z.mgra_id and y.ethnicity_id=z.ethnicity_id;

create table dw_age_sex_ethnicity as select
x1.* ,y1.*,y2.*,y3.*,coalesce(z.population,0) as population length=5 format=7.
from scale_mgra_yr as x1
cross join scale_age as y1
cross join scale_sex as y2
cross join scale_ethnicity as y3
left join dw_age_sex_ethnicity_0 as z
on x1.yr_id=z.yr_id and x1.mgra_id=z.mgra_id
and y1.age_group_id=z.age_group_id
and y2.sex_id=z.sex_id
and y3.ethnicity_id=z.ethnicity_id;
quit; 

proc sql;
create table hh_9 as
select x.yr - 1 as yr,x.hh_id,x.income_group_id_2010,x.inc_2010
,u.mgra_id length=6 format=10.
from (select * from sql_xpef.household_income_upgraded where yr in (&dw1)) as x
inner join sql_xpef.housing_units as z
	on x.hh_id=z.hh_id and x.yr=z.yr
left join mgra_id as u
on z.mgra=u.mgra and z.jur=u.jur and z.cpa=u.cpa;
quit;


proc sql;
create table scale_inc as select distinct income_group_id_2010 as income_group_id format=2.
from hh_9;

create table scale_3a as select y.*,z.*
from scale_mgra_yr as y
cross join scale_inc as z;
quit;

proc sql;
create table hh_9_2010_0 as select
yr as yr_id length=3 format=4.
,mgra_id length=6 format=10.
,income_group_id_2010 as income_group_id length=3 format=2.
,count(*) as households length=4 format=5.
from hh_9 group by yr_id,mgra_id,income_group_id;

create table hh_9_2010 as select 
x1.* ,coalesce(y.households,0) as households length=4 format=5.
from scale_3a as x1
left join hh_9_2010_0 as y
on x1.yr_id=y.yr_id and x1.mgra_id=y.mgra_id and x1.income_group_id=y.income_group_id
order by yr_id,mgra_id,income_group_id;
quit;

options notes;

proc sql;
CONNECT TO ODBC(noprompt="driver=SQL Server; server=sql2014a8; database=isam; DBCOMMIT=10000; Trusted_Connection=yes;") ;

EXECUTE ( drop table if exists isam.&xver..dw_household_income; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;
EXECUTE ( drop table if exists isam.&xver..dw_population; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;
EXECUTE ( drop table if exists isam.&xver..dw_age; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;
EXECUTE ( drop table if exists isam.&xver..dw_sex; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;
EXECUTE ( drop table if exists isam.&xver..dw_ethnicity; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;
EXECUTE ( drop table if exists isam.&xver..dw_age_sex_ethnicity; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;
EXECUTE ( drop table if exists isam.&xver..dw_housing; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;
EXECUTE ( drop table if exists isam.&xver..dw_households; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE ( drop table if exists isam.&xver..dw_jobs; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;
EXECUTE ( drop table if exists isam.&xver..dw_jobs_2; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE ( drop table if exists isam.&xver..dw_jobs_3; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;
EXECUTE ( drop table if exists isam.&xver..dw_jobs_4; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE ( drop table if exists isam.&xver..dw_jobs_base; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;


EXECUTE
(CREATE TABLE isam.&xver..dw_household_income(
mgra_id int
,yr_id smallint
,income_group_id tinyint
,households int
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE
(CREATE TABLE isam.&xver..dw_population(
mgra_id int
,yr_id smallint
,housing_type_id tinyint
,population int
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE
(CREATE TABLE isam.&xver..dw_age(
mgra_id int
,yr_id smallint
,age_group_id tinyint
,population int
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE
(CREATE TABLE isam.&xver..dw_sex(
mgra_id int
,yr_id smallint
,sex_id tinyint
,population int
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE
(CREATE TABLE isam.&xver..dw_ethnicity(
mgra_id int
,yr_id smallint
,ethnicity_id tinyint
,population int
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE
(CREATE TABLE isam.&xver..dw_age_sex_ethnicity(
mgra_id int
,yr_id smallint
,age_group_id tinyint
,sex_id tinyint
,ethnicity_id tinyint
,population int
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE
(CREATE TABLE isam.&xver..dw_housing(
mgra_id int
,yr_id smallint
,structure_type_id tinyint
,units int
,unoccupiable int
,occupied int
,vacancy int
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE
(CREATE TABLE isam.&xver..dw_households(
mgra_id int
,yr_id smallint
,household_size_id tinyint
,households int
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE
(CREATE TABLE isam.&xver..dw_jobs(
yr_id smallint
,mgra_id int
,employment_type_id tinyint
,jobs int
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE
(CREATE TABLE isam.&xver..dw_jobs_2(
yr_id smallint
,mgra_id int
,sandag_industry_id tinyint
,type_id tinyint
,employment_type_id tinyint
,source_id tinyint
,jobs int
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE
(CREATE TABLE isam.&xver..dw_jobs_3(
parcel_id int
,sandag_industry_id_1 tinyint
,mgra int
,jur_id tinyint
,cpa_id smallint
,slots_capacity int
,slots_vacancy int
,slots_cloned int
,slots_events int
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE
(CREATE TABLE isam.&xver..dw_jobs_4(
parcel_id int
,mgra int
,jur_id tinyint
,cpa_id smallint
,sandag_industry_id tinyint
,type tinyint
,yr smallint
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE
(CREATE TABLE isam.&xver..dw_jobs_base(
parcel_id int
,job_id int
,sector_id tinyint
,type tinyint
,sandag_industry_id tinyint
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;


DISCONNECT FROM ODBC ;
quit;

proc sql;
insert into sql_xpef.dw_household_income(bulkload=yes bl_options=TABLOCK) select * from hh_9_2010;
insert into sql_xpef.dw_population(bulkload=yes bl_options=TABLOCK) select * from dw_population;
insert into sql_xpef.dw_age(bulkload=yes bl_options=TABLOCK) select * from dw_age;
insert into sql_xpef.dw_sex(bulkload=yes bl_options=TABLOCK) select * from dw_sex;
insert into sql_xpef.dw_ethnicity(bulkload=yes bl_options=TABLOCK) select * from dw_ethnicity;
insert into sql_xpef.dw_age_sex_ethnicity(bulkload=yes bl_options=TABLOCK) select * from dw_age_sex_ethnicity;
insert into sql_xpef.dw_housing(bulkload=yes bl_options=TABLOCK) select * from dw_housing;

insert into sql_xpef.dw_households(bulkload=yes bl_options=TABLOCK) select * from dw_households;

insert into sql_xpef.dw_jobs(bulkload=yes bl_options=TABLOCK) select * from e1.dw_jobs;

insert into sql_xpef.dw_jobs_2(bulkload=yes bl_options=TABLOCK) select * from e1.dw_jobs_2;

insert into sql_xpef.dw_jobs_3(bulkload=yes bl_options=TABLOCK) select * from e1.job_slots_by_source;

insert into sql_xpef.dw_jobs_4(bulkload=yes bl_options=TABLOCK) select * from e1.jobs_from_capacities;

insert into sql_xpef.dw_jobs_base(bulkload=yes bl_options=TABLOCK) select * from e1.jobs_base;

quit;

options nonotes;
