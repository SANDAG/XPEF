/* References the 'Variables and Libaries' file */
%let a=%sysget(SAS_EXECFILEPATH);
%let b=%sysget(SAS_EXECFILENAME);
%let valib=%sysfunc(tranwrd(&a,&b,_ Variables and Libraries.sas));
%include "&valib";

/* The below tables are moved from other locations into the local input folder */
/* This table has estimates of population by income group (16-level) from the ACS 2009-2017 by Census Tract */
data e1.acs_inc_ct_1 ;set irmi.acs_ct_inc16_2017_adj;run;

/* This table has annual County median income and margin of error from ACS 2005-2017 */
data e1.acs_medinc_cnt_1y; set irmi.acs_medinc_cnt_1y;run;

/* 10-group income crosswalk from median income to share of households in that income group */
data e1.medinc1k_inc10_sd; set irmi.medinc1k_inc10_sd;run; 

/* Census place codes for the 19 jurisdictions associated with jurisdiction_id and Name */
data e1.sf1_place;set old_e.sf1_place;run;

/* These 2 tables have data on GQ pop compiled internally, see spreadsheet */
proc import out=gq_dev_mil
datafile="M:\RES\estimates & forecast\SR14 Forecast\Other Land Use Inputs\group quarters information.xlsx"
replace dbms=excelcs; sheet="mil";
run;

proc import out=gq_dev_col
datafile="M:\RES\estimates & forecast\SR14 Forecast\Other Land Use Inputs\group quarters information.xlsx"
replace dbms=excelcs; sheet="col";
run;

/* The below series of commands builds the estimated GQ pop by type, mgra and year */
proc sql;
create table gq_dev_0 as
select yr_id as yr length=3 format=4.,mgra length=4 format=5.,"COL" as gq_type format=$3.,gq_col as gq from gq_dev_col where gq_col>0
	union all
select yr_id as yr length=3 format=4.,mgra length=4 format=5.,"MIL" as gq_type format=$3.,gq_mil as gq from gq_dev_mil where gq_mil>0
order by yr,gq_type,mgra;

create table gq_sum as select sum(gq) as gq from gq_dev_0;
quit;

/* This section applys sector_id to the GQ employment */
proc sql;
create table emp_colmil_0 as
select yr_id as yr length=3 format=4.,mgra_2 as mgra length=4 format=5.,23 as sector_id format=2.,col_emp as j
from gq_dev_col where col_emp>0 and strip(col_name) ^= "University of San Diego"

	union all

select yr_id as yr length=3 format=4.,mgra_2 as mgra length=4 format=5.,15 as sector_id format=2.,col_emp as j
from gq_dev_col where col_emp>0 and strip(col_name) = "University of San Diego"

	union all

select yr_id as yr length=3 format=4.,mgra length=4 format=5.,27 as sector_id format=2.,mil_total as j
from gq_dev_mil where mil_total>0
order by yr,sector_id,mgra;
quit;

proc sql;
create table col_enroll_0 as
select col_name,yr_id as yr length=3 format=4.,mgra_2 as mgra length=4 format=5.,col_enrollment as collegeenroll
from gq_dev_col where col_enrollment>0;
quit;

/* Uses the mgra table in the XPEF schema and maps GQ population to these mgras */
proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;

create table mgra_ct as select mgra_13 as mgra length=4 format=5.,put(tract_2010,z6.0) as ct format=$6.
from connection to odbc
(SELECT mgra_13 as mgra_13,tract_2010 FROM [data_cafe].[ref].[vi_xref_geography_mgra_13])
order by mgra;

disconnect from odbc;

create table mgra_01 as select mgra,mgra_id,input(substr(put(mgra_id,10.),9,2),2.0) as i
from sql_est.mgra_id;

create table mgra_02 as select x.mgra, x.mgra_id
from mgra_01 as x
inner join (select mgra,max(i) as max_i from mgra_01 group by mgra) as y
on x.mgra=y.mgra and x.i = y.max_i;


create table mgra_1 as select x.mgra length=4 format=5.,x.jurisdiction_id as jur length=3 format=3.,cpa_id as cpa length=3 format=4.
from sql_xpef.mgra_id_new as x
inner join
(
select distinct mgra
from (select distinct mgra from gq_dev_0 union all select distinct mgra from emp_colmil_0)
) as y on x.mgra=y.mgra

inner join mgra_02 as z on x.mgra_id=z.mgra_id;

create table mgra_2 as select x.*,y.ct
from mgra_1 as x
left join mgra_ct as y on x.mgra=y.mgra;

create table gq_dev_1 as select x.*,y.jur,y.cpa,y.ct
from gq_dev_0 as x
left join mgra_2 as y on x.mgra=y.mgra;

create table emp_colmil_1 as select x.*,y.jur as jur_id,y.cpa as cpa_id,y.ct
from emp_colmil_0 as x
left join mgra_2 as y on x.mgra=y.mgra;

create table col_enroll_1 as select x.*,y.jur,y.cpa,y.ct
from col_enroll_0 as x
left join mgra_2 as y on x.mgra=y.mgra;
quit;

/* Places these tables in the input folder */
data e1.gq_dev;set gq_dev_1;run;

data e1.emp_colmil;set emp_colmil_1;run;

data e1.col_enroll;set col_enroll_1;run;
