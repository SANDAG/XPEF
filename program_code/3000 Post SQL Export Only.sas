options nonotes;
options set=SAS_HADOOP_RESTFUL 1;

%let t01=%sysfunc(time(),time8.0);

/* setting version */
%let xver=xpef05;

/*%put %Unquote(%bquote(')&xver%bquote('));*/

libname sql_xpef odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;
Trusted_Connection=yes" schema=&xver;

/* needed in " WHERE???.sas" */
libname sql_est odbc noprompt="driver=SQL Server; server=sql2014a8; database=estimates;
Trusted_Connection=yes" schema=est_2017_03;

libname sql_de odbc noprompt="driver=SQL Server; server=sql2014a8; database=socioec_data;
Trusted_Connection=yes" schema=ca_dof;

libname sql_dbo odbc noprompt="driver=SQL Server; server=sql2014a8; database=ws;
Trusted_Connection=yes" schema=dbo;

libname sql_dim odbc noprompt="driver=SQL Server; server=sql2014a8; database=demographic_warehouse;
Trusted_Connection=yes" schema=dim;

/* folders related to the estimates */
/*libname e0 "T:\socioec\Current_Projects\estimates\input_data";*/

libname e1 "T:\socioec\Current_Projects\&xver\input_data";

libname sd0 "T:\socioec\Current_Projects\estimates\2017_03\output_data";

/* folders related to the forecast */
/*libname e "T:\socioec\Current_Projects\XPEF\input_data";*/
libname sd "T:\socioec\Current_Projects\&xver\simulation_data\";

/* setting a library to access birth and death rates */
libname pdsr odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;
Trusted_Connection=yes" schema=demographic_rates;

/* Synthetic households */
libname sh "T:\socioec\Current_Projects\Synthetic Households";

/* Other pums data */
libname shp "T:\socioec\Current_Projects\Synthetic Households\pums";

/*%let yr1=2016;*/
/* setting the year for the latest ACS data */
%let acs_yr=2016;

/*
The starting point of the simulation is 1/1/2017
Accordingly, the first year of the simulation is 2017: it will result in population for 1/1/2018
The last year of the simulation is 2050: it will result in population for 1/1/2051

For the economic forecast, we need the year-end population, so population from 1/1/2051
will be used as "2050" population
*/

%let by1=2017; /* setting the base year */
%let by2=%eval(&by1 + 1);

/*
proc sql;
create table sd.hu_&by1 as select x.mgra length=4 format=5.
,x.jur length=3 format=2.,x.cpa length=3 format=4., x.ct
,coalesce(y.du_type2,x.du_type) as du_type
,x.sto_flag length=3 format=1.
,x.hh_id length=5 format=8.,x.hu_id length=5 format=8.
,z.size length=3 format=2.
from sql_est.housing_units as x
left join sql_est.housing_units_sf as y on x.hu_id=y.hu_id
left join sql_est.households as z on x.hh_id=z.hh_id and x.yr=z.yr
where x.yr=&by1;

create table sd.hh_&by1 as select x.mgra length=4 format=5.
,x.jur length=3 format=2.,x.ct
,x.size length=3 format=2.
,x.hh_id length=5 format=8.
,y.hu_id length=5 format=8.
from sql_est.households as x
inner join sql_est.housing_units as y on x.hh_id=y.hh_id and x.yr=y.yr
where x.yr=&by1 and y.yr=&by1;

create table sd.hp_&by1 as select hp_id length=5 format=8.
,hh_id length=5 format=8.,age length=3 format=3.,r,hisp,sex
,datepart(dob) as dob length=4 format=MMDDYY10.
,role
from sql_est.household_population
where yr=&by1;

create table sd.gq_&by1 as select gq_id length=5 format=8.,gq_type
,jur length=3 format=2.
,ct
,cpa length=3 format=4.
,mgra length=4 format=5.
,age length=3 format=3.
,r,hisp,sex
,datepart(dob) as dob length=4 format=MMDDYY10.
from sql_est.gq_population
where yr=&by1;
quit;

data sd.hu_&by1;set sd.hu_&by1;
informat 
mgra jur cpa ct du_type sto_flag hh_id hu_id size;
run;

data sd.hh_&by1;set sd.hh_&by1;
informat 
mgra jur ct size hh_id hu_id;
run;

data sd.hp_&by1;set sd.hp_&by1;
informat 
hp_id hh_id age r hisp sex dob role;
run;

data sd.gq_&by1;set sd.gq_&by1;
informat 
gq_id gq_type jur ct cpa mgra age r hisp sex dob;
run;
*/

/* setting a version of urbanim outputs */
/*%let usver=232;*/

/* creates sd.dof_update and sd.ludu */
/*%include "T:\socioec\Current_Projects\&xver\program_code\1011 Scenario (hu from urbansim).sas";*/

/* %let vyr=2017; */ /* setting vintage_yr for the DOF's estimates */
/* %let byr=2016; */ /* setting last year for the birth data */

/*%let xprev = xpef04;*/
/* specifying a version of xpef without the HP controls for jurisdictions */
/* %include "T:\socioec\Current_Projects\&xver\program_code\1012 Jurisdiction HP controls.sas"; */

/*%let br = 102;*/ /* setting id for birth rates */
/*%let dr = 102;*/ /* setting id for death rates */


/*
%include "T:\socioec\Current_Projects\&xver\program_code\1020 Annual cycle.sas";
*/
/* contains macro forecast */


%let yy2=2050; /* setting the last year of the simulation; this will create population for 1/1/&yy2+1 */

%let yy3=%eval(&yy2 + 1);


/*
%macro xpef;
%do yr=&by1 %to &yy2;
	%forecast (yr=&yr);
%end;
%mend xpef;

%xpef;
*/



/* !!! specifying years that will be used in the ABM !!! */
%global list1;
%let list1=2019,2021,2026,2031,2036,2041,2046,2051;
%put &list1;


/*
%include "T:\socioec\Current_Projects\&xver\program_code\1041 Export microdata to SQL.sas";
*/


%include "T:\socioec\Current_Projects\&xver\program_code\1043 Income Imputation and Assignment.sas";

%include "T:\socioec\Current_Projects\&xver\program_code\1050 Income Upgrading.sas";

%include "T:\socioec\Current_Projects\&xver\program_code\1055 Synthetic Households.sas";

%include "T:\socioec\Current_Projects\&xver\program_code\1057 Jobs capacity.sas";

%include "T:\socioec\Current_Projects\&xver\program_code\2000 Assembling ABM forecast data.sas";


%let t02=%sysfunc(time(),time8.0);
%put Program Started at &t01;
%put Program Ended at &t02;
