proc sql;
create table births_1(drop=max_date min_date rng) as select *
,mdy(1,1,&yr) as min_date format=mmddyy10.
,mdy(12,31,&yr) as max_date format=mmddyy10.
,calculated max_date - calculated min_date +1 as rng
,calculated min_date + int(ranuni(300) * calculated rng) as newborn_dob length=4 format=mmddyy10.
,case when ranuni(400)<.51 then "M" else "F" end as newborn_sex length=1
,monotonic() as newborn_hp_id length=5 format=7.
from hp_fem where b_id^=.;
quit;


proc sql;
create table max_hp_id as select max(hp_id) as max_hp_id from hp_base;

create table max_hh_id as select max(hh_id) as max_hh_id from hh_base;

update births_1 as x set newborn_hp_id = newborn_hp_id + (select max_hp_id from max_hp_id);  

create table newborns_0 as
select x.hh_id,x.newborn_hp_id as hp_id,y.r,y.r7,y.hisp,x.newborn_sex as sex
,0 as age length=3 format=3.,x.newborn_dob as dob format=mmddyy10.,"M" as role
,x.hp_id as mothers_hp_id length=5 format=7.
/* race and hispanism are inherited from the mother */
from births_1 as x
inner join hp_base as y on x.hp_id=y.hp_id;
quit;

/*----------END OF BIRTHS SECTION-------------*/


/*-----------DEATHS----------------------*/

/* assembling population for death assignment */
/* females with newborns are included in death assignment */
/* College and military group quarters are excluded from death assignment */
proc sql;
create table pop_1 as
select x.hh_id,x.hp_id,. as gq_id,"" as gq_type,x.sex,x.age,x.dob format=mmddyy10.
,case
when hisp="H" then "H"
when r="R10" then "W"
when r="R02" then "B"
when r="R04" then "S"
else "O" end as race
,ranuni(&yr + 200) as rn
,coalesce(y.b_id,0) as b_id
from hp_base as x left join births_1 as y on x.hp_id=y.hp_id

	union all
select . as hh_id,. as hp_id,gq_id,gq_type,sex,age,dob format=mmddyy10.
,case
when hisp="H" then "H"
when r="R10" then "W"
when r="R02" then "B"
when r="R04" then "S"
else "O" end as race
,ranuni(&yr + 300) as rn
,0 as b_id
from gq_base where gq_type not in ("COL","MIL")

	union all
select hh_id,hp_id,. as gq_id,"" as gq_type,sex,age,dob format=mmddyy10.
,case
when hisp="H" then "H"
when r="R10" then "W"
when r="R02" then "B"
when r="R04" then "S"
else "O" end as race
,ranuni(&yr + 400) as rn
,-1 as b_id
from newborns_0;
quit;

/* using proq sql with union resets the lengths of numeric vars back to 8
this restores the original length (to save space) */
/* data pop_1;length hh_id hp_id gq_id 5 b_id dob 4 age 3;set pop_1;run; */

proc sql;
create table pop_1_ as select hh_id,hp_id length=5
,gq_id length=5,b_id length=4,dob length=4,age length=3,gq_type,sex,race,rn
from pop_1;

create table pop_1 as select * from pop_1_;

drop table pop_1_;
quit;


proc sql;
create table hp_age_test as select age,count(*) as p from hp_base group by age;
create table gq_age_test as select age,count(*) as p from gq_base group by age;
quit;


data hp_fem_0;set hp_fem_0;length i 4;
i+1;
run;

proc sql;
create table hp_fem(drop=i) as select x.*,monotonic() as b_id length=4
from hp_fem_0 as x
cross join (select * from sql_dof.&coc where county_name = "San Diego" and calendar_yr = &yr) as y
where x.i <= y.births;
quit;


proc sql;
create table deaths_01 as select x.*
,case
when x.age>=100 then 0 else ranuni(&yr + 402) end as rn2
from pop_1 as x
inner join (select * from pdsr.death_rates where yr = &yr and death_rate_id = &dr) as y
on x.age = y.age and x.race = y.race and x.sex=y.sex
where x.age>=100 or x.rn <= (y.death_rate * 2) /* this is needed to oversample */
order by rn2;
quit;

data deaths_01;set deaths_01(drop=rn rn2);length i 4;
i+1;
run;


proc sql;
create table deaths_1(drop=max_date min_date rng i) as select x.*
,mdy(1,1,&yr) as min_date format=mmddyy10.
,mdy(12,31,&yr) as max_date format=mmddyy10.
,calculated max_date - calculated min_date +1 as rng
,calculated min_date + int(ranuni(900) * calculated rng) as dod length=4 format=mmddyy10.
from deaths_01 as x
cross join (select * from sql_dof.&coc where county_name = "San Diego" and calendar_yr = &yr) as y
where x.i<=y.deaths;
quit;



proc sql;
create table deaths_1a as select 
case when gq_type="" then "HP" else gq_type end as type1
,case when b_id=-1 then "Newborn" when b_id>0 then "Mother" when b_id=0 then "Other" end as type2
,count(*) as d
from deaths_1 group by type1,type2;
quit;

proc sql;
create table hp_removed_death as select y.hh_id,z.hp_id,x.age,y.size,z.role,x.dod
from deaths_1 as x
inner join hh_base as y on x.hh_id=y.hh_id
inner join hp_base as z on x.hp_id=z.hp_id;

create table gq_removed_death as select *
from deaths_1(drop=hh_id hp_id b_id) where gq_type^="";
quit;


/* update household population by adding newborns */
data hp_next_0;set hp_base newborns_0;run;

proc sql;
/* update max_hp_id */
create table max_hp_id as select max(hp_id) as max_hp_id from hp_next_0;

/* update household population by removing entire households where the householder is dead */
create table hp_next_1 as select x.*
from hp_next_0 as x
left join (select distinct hh_id from hp_removed_death where role="H") as y on x.hh_id=y.hh_id
where y.hh_id=.;

/* update household population by removing dead household members*/
create table hp_next_2 as select x.*
from hp_next_1 as x
left join (select hp_id from hp_removed_death where role="M") as y
on x.hp_id=y.hp_id
where y.hp_id=.;

/* update households by removing households with a dead householder */
create table hh_next_1 as select x.*
from hh_base as x
left join (select distinct hh_id from hp_removed_death where role="H") as y
on x.hh_id=y.hh_id
where y.hh_id=.;

/* update households by updating household size (changes due to births and deaths)*/
create table hh_next_2 as select x.hh_id,x.hu_id,y.size
from hh_next_1 as x
inner join (select hh_id,count(*) as size length=3 format=2. from hp_next_2 group by hh_id) as y
on x.hh_id=y.hh_id;
quit;


proc sql;
/* demolished units (includes unoccupied) */
create table hu_removed_demo as select x.hu_id,x.hh_id
from hu_base as x
left join (select hu_id from sd.ludu where yr=&yrn) as y
on x.hu_id = y.hu_id
where y.hu_id = .;

/* households with a demolished unit */
create table hh_removed_demo as select x.hh_id
from hh_next_2 as x
inner join hu_removed_demo as y on x.hu_id=y.hu_id;

/* household population with a demolished unit */
create table hp_removed_demo as select x.hp_id
from hp_next_2 as x
inner join hu_removed_demo as y on x.hh_id=y.hh_id;

/* updating households by removing households with a demolished unit */
create table hh_next_3 as select x.*
from hh_next_2 as x
left join hh_removed_demo as y
on x.hh_id=y.hh_id
where y.hh_id=.;

/* updating household population by removing households with a demolished unit */
create table hp_next_3 as select x.*
from hp_next_2 as x
left join hh_removed_demo as y
on x.hh_id=y.hh_id
where y.hh_id=.;

/* households removed due to excessive size */
create table hh_removed_size as select * from hh_next_3 where size>10;

/* household population removed due to excessive size */
create table hp_removed_size as select x.*
from hp_next_3 as x
inner join hh_removed_size as y on x.hh_id=y.hh_id;


/* updating household population by removing households with excessive size */
create table hp_next_4 as select x.*
from hp_next_3 as x
left join hh_removed_size as y
on x.hh_id=y.hh_id
where y.hh_id=.;

/* updating households by removing households with excessive size */
create table hh_next_4 as select *
from hh_next_3 where size<=10;
quit;

proc sql;
/* updating next year's units */
create table hu_next_1 as select x.hu_id,x.du_type,x.mgra,x.ct,x.sto_flag,x.jur,x.cpa
,y.hh_id,y.size
from (select * from sd.ludu where yr = &yrn) as x
left join hh_next_4 as y on x.hu_id=y.hu_id;
quit;


proc sql;
/* setting age relative to 1/1/2011 */
update hp_next_4 set age=intck('year', dob, mdy(1, 1, &yrn), "C") ;
quit;


proc sql;
/* household pop from households with a dead householder */
create table hp_dead_hh_1 as select x.hh_id,x.hp_id,x.role
from hp_next_0 as x
inner join (select distinct hh_id from hp_removed_death where role="H") as y on x.hh_id=y.hh_id
left join hp_removed_death as z on x.hp_id=z.hp_id
where z.hp_id=.;

create table hp_dead_hm_2 as select x.*,z.ct,z.jur
from hp_next_0 as x
inner join hp_dead_hh_1 as y on x.hp_id=y.hp_id
inner join hu_base as z on x.hh_id=z.hh_id;
quit;


%let t=%sysfunc(time(),time8.0);
%put ========== Finished Births-Deaths-Demolitions for year &yr at &t;
