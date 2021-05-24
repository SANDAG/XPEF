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
select x.hh_id,x.newborn_hp_id as hp_id,y.r,y.hisp,x.newborn_sex as sex
,0 as age length=3 format=3.,x.newborn_dob as dob format=mmddyy10.,"M" as role
,x.hp_id as mothers_hp_id length=5 format=7.
/* race and hispanism are inherited from the mother */
from births_1 as x
inner join hp_base as y on x.hp_id=y.hp_id;
quit;

/*----------END OF BIRTHS SECTION-------------*/

/* %let t=%sysfunc(time(),time8.0);
%put Finished Births Section for year &yr at &t; */


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

/*
proc sql;
create table test_01 as select * from pop_1 where age>100 order by age desc;

create table test_02 as select * from pdsr.death_rates where death_rate_id = &dr and yr=2017 and age>=100;

create table test_03 as select * from pop_1 where age=.;
quit;
*/


proc sql;
create table hp_age_test as select age,count(*) as p from hp_base group by age;
create table gq_age_test as select age,count(*) as p from gq_base group by age;
quit;



/*
proc sql;
create table d_3 as select * from e.d_1 where yr=&yr;
quit;
*/
/* splitting deaths in the category 85+ into 85-99 and 100;
assuming that nobody survives into age 101 */
/*
proc sql;
create table pop_100 as select sex,r8,count(*) as p from pop_1 where age>=100
group by sex,r8;

create table d_4 as
select yr,sex,r8,a11,d from d_3 where a11^=85.101
	union all
select x.yr,x.sex,x.r8,85.99 as a11,x.d-coalesce(y.p,0) as d
from (select * from d_3 where a11=85.101) as x
inner join pop_100 as y on x.sex=y.sex and x.r8=y.r8
	union all
select x.yr,x.sex,x.r8,100 as a11,y.p as d
from (select * from d_3 where a11=85.101) as x
inner join pop_100 as y on x.sex=y.sex and x.r8=y.r8
order by yr,sex,a11,r8;
quit;
*/

/*
proc sql;
create table d_4 as select yr,sex,r8,a11,d from d_3;
create table d_4a as select * from d_4 where d<0;
create table d_4b as select sum(d) as d from d_4;
quit;

proc sql;
create table dr_2 as select age,sex,r8
,case
when age=0 then 0
when age<=4 then 1.04
when age<=14 then 5.14
when age<=24 then 15.24
when age<=34 then 25.34
when age<=44 then 35.44
when age<=54 then 45.54
when age<=64 then 55.64
when age<=74 then 65.74
when age<=84 then 75.84
else 85.101
end as a11
,p as original_prob
from e.dr_0 where yr=&yr and age<=101;

create table dr_3 as select x.*,coalesce(y.pop,0) as pop,calculated pop * x.original_prob as expected_d
from dr_2 as x
left join (select age,sex,r8,count(*) as pop from pop_1 group by age,sex,r8) as y
on x.age=y.age and x.sex=y.sex and x.r8=y.r8

order by sex,r8,age;
quit;

proc sql;
create table dr_4 as select *,coalesce(expected_d/sum(expected_d),0) as f
from dr_3 group by sex,r8,a11;

create table dr_5 as select x.*,coalesce(y.d,0) as actual_d, coalesce(y.d,0) * x.f as estimated_d
,coalesce(calculated estimated_d / pop, 0) as adjusted_prob
from dr_4 as x
left join d_4 as y on x.sex=y.sex and x.r8=y.r8 and x.a11=y.a11
order by sex,r8,age;

update dr_5 set adjusted_prob=1 where age=101 and adjusted_prob^=1;

create table dr_5a as select * from dr_5 where age>=100;
quit;

proc sql;
create table pop_2 as select x.*,y.adjusted_prob as p,ranuni(800) as rn
from pop_1 as x left join dr_5 as y on x.age=y.age and x.sex=y.sex and x.r8=y.r8;

create table pop_2a as select * from pop_2 where p=.;

create table pop_3 as select * from pop_2 where b_id=0 and rn < (p * 20)
order by sex,r8,a11,rn;

create table pop_3a as select sex,r8,a11,count(*) as d1 from pop_3 group by sex,r8,a11;

create table pop_3b as select x.*,coalesce(y.d1,0) as d1
from d_4 as x
left join pop_3a as y 
on x.sex=y.sex and x.r8=y.r8 and x.a11=y.a11;

create table pop_3c as select * from pop_3b where d>d1;
*/
/* this table should return zero records; if it doesn't, increase the multiplication factor in pop_3 */




data hp_fem_0;set hp_fem_0;length i 4;
i+1;
run;

proc sql;
create table hp_fem(drop=i) as select x.*,monotonic() as b_id length=4
from hp_fem_0 as x
cross join (select * from sql_de.coc_calendar_projections_2017 where county_name = "San Diego" and calendar_yr = &yr) as y
where x.i <= y.births;
quit;


proc sql;
create table deaths_01 as select x.*
,case
when x.age>=101 then 0 else ranuni(&yr + 402) end as rn2
from pop_1 as x
inner join (select * from pdsr.death_rates where yr = &yr and death_rate_id = &dr) as y
on x.age = y.age and x.race = y.race and x.sex=y.sex
where x.age>=101 or x.rn <= (y.death_rate * 2) /* this is needed to oversample */
order by rn2;
quit;

data deaths_01;set deaths_01(drop=rn rn2);length i 4;
i+1;
run;

/*
proc sql;
create table deaths_02(drop=i) as select x.*
from deaths_01 as x
cross join (select * from sql_de.coc_calendar_projections_2017 where county_name = "San Diego" and calendar_yr = &yr) as y
where x.i <= y.deaths;
quit;
*/


proc sql;
create table deaths_1(drop=max_date min_date rng i) as select x.*
,mdy(1,1,&yr) as min_date format=mmddyy10.
,mdy(12,31,&yr) as max_date format=mmddyy10.
,calculated max_date - calculated min_date +1 as rng
,calculated min_date + int(ranuni(900) * calculated rng) as dod length=4 format=mmddyy10.
from deaths_01 as x
cross join (select * from sql_de.coc_calendar_projections_2017 where county_name = "San Diego" and calendar_yr = &yr) as y
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



/* %let t=%sysfunc(time(),time8.0);
%put Finished Deaths Section for year &yr at &t; */


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

/*
proc sql;
create table ztest_1 as select * from hu_next_1 where jur^=jur_prev;
quit;
*/


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


/*
proc sql;
create table hp_leftover_1 as
select * from hp_dead_hm_2
	union all
select x.*,y.ct,y.jur from hp_removed_size as x inner join hu_base as y on x.hh_id=y.hh_id
	union all
select z.*,y.ct,y.jur from hp_removed_demo as x
inner join hp_next_0 as z on x.hp_id=z.hp_id
inner join hu_base as y on z.hh_id=y.hh_id;

update hp_leftover_1 set age=intck('year', dob, mdy(1, 1, &yrn), "C") ;
quit;

proc sql;
create table hp_leftover_1a as select *,intck('year', dob, mdy(1, 1, &yrn), "C") as age2
from hp_leftover_1;
quit;
*/


%let t=%sysfunc(time(),time8.0);
%put ========== Finished Births-Deaths-Demolitions for year &yr at &t;
