options nonotes;

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

/* parcel-level jobs from 2016 */
create table j_2016_0 as select job_id,sector_id
,case
when job_id > 200000000 then 5 /* self-employed */
when job_id > 50000000 then 2 /* fed government owned */
when job_id > 40000000 then 3 /* state government owned */
when job_id > 30000000 then 4 /* local government owned */
else 1 /* private */
end as type
,case
when sector_id in (21,24,26) then 28 /* public administration */
when sector_id in (23,25) then 15 /* education */
else sector_id 
end as sandag_industry_id
from connection to odbc
(select job_id,sector_id FROM [urbansim].[urbansim].[job] as x)
;

/* get sectoral targets for future jobs*/

create table jf_1 as select *
from connection to odbc
(
select x.yr,y.sandag_industry_id,x.jobs
FROM (select * from [isam].[economic_output].[sectors] where economic_simulation_id = &ecver) as x
left join [socioec_data].[ca_edd].[xref_sandag_industry_edd_sector] as y on x.sandag_sector=y.sandag_sector
) 
order by yr,sandag_industry_id
;

disconnect from odbc;

create table j_2016_1 as select sandag_industry_id,type,sector_id,count(job_id) as j
from j_2016_0 group by sandag_industry_id,type,sector_id;

create table jf_2 as select * from jf_1 where yr>2016;
quit;

proc sql;
create table jf_test_1 as select * from jf_1 order by sandag_industry_id,yr;

create table jf_test_2 as select x.*,y.jobs as jobs_prev, x.jobs - y.jobs as d
from jf_1 as x
inner join jf_1 as y on x.sandag_industry_id = y.sandag_industry_id and x.yr = y.yr + 1
order by sandag_industry_id,yr;
quit;


/* splitting government into sectors */
proc sql;
create table g_1 as select type,sandag_industry_id,j,j/sum(j) as s
from j_2016_1 where type in (2,3,4) and sandag_industry_id not in (27,22,15)
/* excludes military,civilian DOD and state&local education */
group by type
order by type,j;

create table g_2 as select *
,case
when type = 2 then 21
when type = 3 then 24
when type = 4 then 26
end as gov_sector
from g_1;

create table g_3 as select x.type, y.yr, x.sandag_industry_id, y.jobs as jt, round(x.s * y.jobs,1) as j0
from g_2 as x
inner join jf_2 as y on x.gov_sector = y.sandag_industry_id
order by type,yr,j0;
quit;

data g_4;set g_3; by type yr; retain jc;
if first.yr then do; j1 = j0; jc = j1; end;
else if last.yr then do; j1 = jt - jc; jc = j1 + jc; end;
else do; j1 = j0; jc = j1 + jc; end;
run;

/* get growth in self employed */

/* self employed will grow in proportion to private w&s jobs */

proc sql;
create table se_1 as select x.sandag_industry_id,x.wsj,y.sej, y.sej / x.wsj as s
from (select sandag_industry_id,sum(j) as wsj from j_2016_1 where type in (1) group by sandag_industry_id)  as x
inner join (select sandag_industry_id,sum(j) as sej from j_2016_1 where type in (5) group by sandag_industry_id) as y
on x.sandag_industry_id=y.sandag_industry_id
order by s;
quit;

proc sql;
create table se_2 as select x.yr,x.sandag_industry_id,x.jobs as wsj,y.wsj as wsj_2016,round(x.jobs * y.s,1) as sej,y.sej as sej_2016
from jf_2 as x
inner join se_1 as y on x.sandag_industry_id=y.sandag_industry_id
order by yr,sandag_industry_id;

create table se_2a as select * from se_2 where sej < sej_2016;

create table se_2b as select * from se_2 where wsj < wsj_2016;
quit;

proc sql;
create table employment_controls as
select yr,sandag_industry_id,2 as type,jobs as j from jf_2 where sandag_industry_id = 22
	union all
select yr,15 as sandag_industry_id,3 as type,jobs as j from jf_2 where sandag_industry_id = 23
	union all
select yr,15 as sandag_industry_id,4 as type,jobs as j from jf_2 where sandag_industry_id = 25
	union all

select yr,sandag_industry_id,type, j1 as j from g_4
	union all
select yr,sandag_industry_id,5 as type, sej as j from se_2
	union all
select yr,sandag_industry_id,1 as type,jobs as j from jf_2 where sandag_industry_id not in (21:28)

order by yr,sandag_industry_id,type;
quit;


proc sql;
create table test_2 as select coalesce(x.yr,y.yr) as yr
,x.old_sector,y.sandag_industry_id,x.j,y.jobs, x.j - y.jobs as d
from (select yr
,case 
when type = 2 and sandag_industry_id = 22 then 22
when type = 2 then 21
when type = 3 and sandag_industry_id = 15 then 23
when type = 3 then 24
when type = 4 and sandag_industry_id = 15 then 25
when type = 4 then 26
else sandag_industry_id end as old_sector
,sum(j) as j
from employment_controls
where type < 5 group by yr,old_sector) as x

full join jf_2 as y
on x.yr=y.yr and x.old_sector = y.sandag_industry_id;

create table test_2a as select * from test_2 where d^=0;
quit;

data e1.employment_controls; set employment_controls;run;
