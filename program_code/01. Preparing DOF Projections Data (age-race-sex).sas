/* References the 'Variables and Libaries' file */
%let a=%sysget(SAS_EXECFILEPATH);
%let b=%sysget(SAS_EXECFILENAME);
%let valib=%sysfunc(tranwrd(&a,&b,_ Variables and Libraries.sas));
%include "&valib";

/* Pull the DOF's ASE population projections from the vintage year from the sql server */
proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table dof_proj_1 as select fiscal_yr
,case
when race_code=1 then "W"
when race_code=2 then "B"
when race_code=3 then "I"
when race_code=4 then "S"
when race_code=5 then "P" 
when race_code=6 then "M"
when race_code=7 then "H"
end as r7
,case
when age <= 100 then age
else 100 end as age101,sex
,sum(population) as p
from connection to odbc
(
select * FROM [socioec_data].[ca_dof].[population_proj_&dofver]
where county_fips_code = 6073 and fiscal_yr >= &by1 and fiscal_yr <= 2051
)
group by fiscal_yr,r7,age101,sex;

DISCONNECT FROM odbc;
quit;

/* Sum population by fiscal year */
proc sql;
create table dof_proj_2 as select fiscal_yr,sum(p) as p
from dof_proj_1 group by fiscal_yr;

/* Average two fiscal years to estimate calendar year population */
create table dof_proj_3 as select x.fiscal_yr as yr
,round((x.p+y.p)/2,1) as p
from dof_proj_2 as x
inner join dof_proj_2 as y on x.fiscal_yr=y.fiscal_yr+1
order by yr;
quit;

/* Copy year and pop values */
proc sql;
create table dof_proj_t as
select yr,p as tp from dof_proj_3
order by yr;
quit;

/* Average two fiscal years to estimate calendar year population from full ASE projections */
proc sql;
create table dof_proj_4 as select 
coalesce(x.fiscal_yr,y.fiscal_yr+1) as yr
,coalesce(x.age101,y.age101) as age101
,coalesce(x.sex,y.sex) as sex
,coalesce(x.r7,y.r7) as r7
,round((coalesce(x.p,0) + coalesce(y.p,0))/2,1) as p
from dof_proj_1 as x
full join dof_proj_1 as y on x.r7=y.r7 and x.age101=y.age101 and x.sex=y.sex and x.fiscal_yr=y.fiscal_yr+1
order by yr,r7,age101,sex;

/* Calculate each ASE share of the year's population sum */
create table dof_proj_4a as select *,p/sum(p) as s
from dof_proj_4 group by yr
order by yr;

/* Multiply shares by total pop from first step, compare to ASE estimated pop */
create table dof_proj_5 as select x.yr,y.age101,y.sex,y.r7,x.tp,round(x.tp * y.s,1) as p1,y.p as p0
from dof_proj_t as x
inner join dof_proj_4a as y on x.yr=y.yr
order by yr,p;
quit;

/* Count the population by year, but force an exact match last row of the year */
/* This seems to always be less than ~30 people, often less than 10 */
data dof_proj_6;set dof_proj_5; by yr; retain cp;
if first.yr then do;p2=p1;cp=p2;end;
else if last.yr then do;p2=tp - cp;cp=p2+cp;end;
else do;p2=min(p1,(tp - cp));cp=p2+cp;end;run;
run;

/* Use p2 as the estimate of population for ASE group by year */
proc sql;
create table dof_proj_7 as select yr,age101,sex,r7,p2 as p_cest
from dof_proj_6 order by yr,age101,sex,r7;
quit;

/* This step fills in any unobserved combinations with 0 */
/* Create a fully enummerated list of ASE+year combinations */
proc sql;
create table dof_proj_7a as select x1.*,x2.*,x3.*,x4.*
from (select distinct yr from dof_proj_7) as x1
cross join (select distinct age101 from dof_proj_7) as x2
cross join (select distinct sex from dof_proj_7) as x3
cross join (select distinct r7 from dof_proj_7) as x4;

/* Join the ASE population to the fully enumerated list */
create table dof_proj_8 as select x.*,coalesce(y.p_cest,0) as p_cest
from dof_proj_7a as x
left join dof_proj_7 as y on
x.yr=y.yr and x.age101=y.age101 and x.sex=y.sex and x.r7=y.r7
order by yr,age101,sex,r7;
quit;

/* Saves this file to the inputs folder in the main XPEF directory */
data e1.dof_pop_proj_r7_age101;set dof_proj_8;run;
