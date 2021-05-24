%let xver=xpef04;

libname sql_xpef odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;
Trusted_Connection=yes" schema=&xver;

/* mgra-jur-cpa jobs by sector */

proc sql;
create table j1_mjc_1 as select y.mgra,y.jurisdiction_id,y.cpa_id,x.employment_type_id,x.yr_id
,sum(jobs) as j
from sql_xpef.dw_jobs as x
left join sql_xpef.mgra_id_new as y on x.mgra_id=y.mgra_id
group by y.mgra,y.jurisdiction_id,y.cpa_id,x.employment_type_id,x.yr_id
order by mgra,jurisdiction_id,cpa_id,employment_type_id,yr_id;

create table j1_mjc_2 as select y.mgra,y.jurisdiction_id,y.cpa_id/*,x.employment_type_id*/,x.yr_id
,sum(jobs) as j
from sql_xpef.dw_jobs as x
left join sql_xpef.mgra_id_new as y on x.mgra_id=y.mgra_id
group by y.mgra,y.jurisdiction_id,y.cpa_id/*,x.employment_type_id*/,x.yr_id
order by mgra,jurisdiction_id,cpa_id/*,employment_type_id*/,yr_id;
quit;

data j1_mjc_1;set j1_mjc_1;by mgra jurisdiction_id cpa_id employment_type_id;retain i;
if first.employment_type_id then i=1;else i=i+1;
run;

data j1_mjc_2;set j1_mjc_2;by mgra jurisdiction_id cpa_id;retain i;
if first.cpa_id then i=1;else i=i+1;
run;


proc sql;
create table j1_mjc_1a as select x.*,y.j as j_prev,x.j - y.j as jc
from j1_mjc_1 as x
inner join j1_mjc_1 as y on x.mgra=y.mgra and x.jurisdiction_id=y.jurisdiction_id and x.cpa_id=y.cpa_id
and x.employment_type_id=y.employment_type_id and x.i=y.i+1
where x.j<y.j /*and employment_type_id^=15*/
order by jc,mgra,jurisdiction_id,cpa_id,employment_type_id,yr_id;
quit;


/*regional jobs*/

/*regional jobs by sector*/

/* jobs by juri
