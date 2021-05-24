/* Log did not work as intended, prevents in-window output */
/* would need to set xver here for the below log method to work:
%let xver = xpef## */
/*
proc printto log="T:\socioec\Current_Projects\&xver\log\&xver_log_started_%sysfunc(compress(%sysfunc(today(),date9.)_%sysfunc(time(),time6.0),%str( :))).txt";
run;
*/

/* References the 'Variables and Libaries' file */
%let a=%sysget(SAS_EXECFILEPATH);
%let b=%sysget(SAS_EXECFILENAME);
%let valib=%sysfunc(tranwrd(&a,&b,_ Variables and Libraries.sas));
%include "&valib";

options nonotes;
options set=SAS_HADOOP_RESTFUL 1;

%let t01=%sysfunc(time(),time8.0);

/* setting version of the components of change projections */
%let coc = coc_calendar_proj_2020_1_10;

libname sql_dof odbc noprompt="driver=SQL Server; server=sql2014a8; database=socioec_data;
Trusted_Connection=yes" schema=ca_dof;

libname sql_dim odbc noprompt="driver=SQL Server; server=sql2014a8; database=demographic_warehouse;
Trusted_Connection=yes" schema=dim;

/* folders related to the forecast */
libname sd "T:\socioec\Current_Projects\&xver\simulation_data\";

/* Synthetic households */
libname sh "T:\socioec\Current_Projects\Synthetic Households";

/* Other pums data */
libname shp "T:\socioec\Current_Projects\Synthetic Households\pums";

/* setting the year for the latest ACS data used */
%let acs_yr=2017;

/*
The starting point of the simulation is 1/1/2017
Accordingly, the first year of the simulation is 2017: it will result in population for 1/1/2018
The last year of the simulation is 2050: it will result in population for 1/1/2051

For the economic forecast, we need the year-end population, so population from 1/1/2051
will be used as "2050" population
*/

/* The base year is set in the variables file */
/* %let by1=2018; */ /* setting the base year */
%let by2=%eval(&by1 + 1);
%let by0=%eval(&by1 - 1);

%let yy2=2050; /* setting the last year of the simulation; this will create population for 1/1/&yy2+1 */
%let yy3=%eval(&yy2 + 1);

%let br = 102; /* setting id for birth rates */
%let dr = 102; /* setting id for death rates */

/* version of the economic simulation */
%let ecver=1209;

/* !!! specifying years that will be used in the ABM !!! */
%global list1;
%let list1=2017,2019,2021,2024,2026,2027,2030,2031,2033,2036,2041,2046,2051;
%put &list1;

%let abmpath=T:\socioec\Current_Projects\&xver\abm_csv;
%let list4=2016 2018 2020 2023 2025 2026 2029 2030 2032 2035 2040 2045 2050;
%let abmv=01;

/*************************************************************************************************************/
/*************************************************************************************************************/
/********************************* Check the above settings before running!! *********************************/
/******** If a new economic scenario or other abm years are needed, they may need to be modified some ********/
/*************************************************************************************************************/
/*************************************************************************************************************/

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
,hh_id length=5 format=8.,age length=3 format=3.,r,r7,hisp,sex
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
,r,r7,hisp,sex
,datepart(dob) as dob length=4 format=MMDDYY10.
from sql_est.gq_population
where yr=&by1;
quit;

proc sql;
create table sd.hu_&by0 as select x.mgra length=4 format=5.
,x.jur length=3 format=2.,x.cpa length=3 format=4., x.ct
,coalesce(y.du_type2,x.du_type) as du_type
,x.sto_flag length=3 format=1.
,x.hh_id length=5 format=8.,x.hu_id length=5 format=8.
,z.size length=3 format=2.
from sql_est.housing_units as x
left join sql_est.housing_units_sf as y on x.hu_id=y.hu_id
left join sql_est.households as z on x.hh_id=z.hh_id and x.yr=z.yr
where x.yr=&by0;

create table sd.hh_&by0 as select x.mgra length=4 format=5.
,x.jur length=3 format=2.,x.ct
,x.size length=3 format=2.
,x.hh_id length=5 format=8.
,y.hu_id length=5 format=8.
from sql_est.households as x
inner join sql_est.housing_units as y on x.hh_id=y.hh_id and x.yr=y.yr
where x.yr=&by0 and y.yr=&by0;

create table sd.hp_&by0 as select hp_id length=5 format=8.
,hh_id length=5 format=8.,age length=3 format=3.,r,r7,hisp,sex
,datepart(dob) as dob length=4 format=MMDDYY10.
,role
from sql_est.household_population
where yr=&by0;

create table sd.gq_&by0 as select gq_id length=5 format=8.,gq_type
,jur length=3 format=2.
,ct
,cpa length=3 format=4.
,mgra length=4 format=5.
,age length=3 format=3.
,r,r7,hisp,sex
,datepart(dob) as dob length=4 format=MMDDYY10.
from sql_est.gq_population
where yr=&by0;
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
hp_id hh_id age r r7 hisp sex dob role;
run;

data sd.gq_&by1;set sd.gq_&by1;
informat 
gq_id gq_type jur ct cpa mgra age r r7 hisp sex dob;
run;


data sd.hu_&by0;set sd.hu_&by0;
informat 
mgra jur cpa ct du_type sto_flag hh_id hu_id size;
run;

data sd.hh_&by0;set sd.hh_&by0;
informat 
mgra jur ct size hh_id hu_id;
run;

data sd.hp_&by0;set sd.hp_&by0;
informat 
hp_id hh_id age r r7 hisp sex dob role;
run;

data sd.gq_&by0;set sd.gq_&by0;
informat 
gq_id gq_type jur ct cpa mgra age r r7 hisp sex dob;
run;

/* creates sd.dof_update and sd.ludu */
%include "T:\socioec\Current_Projects\&xver\qa\1013 Scenario (hu from urbansim).sas";
/*
%include "T:\socioec\Current_Projects\&xver\program_code\1014 Jurisdiction HP controls.sas";

%include "T:\socioec\Current_Projects\&xver\program_code\1015 Target hh distribution.sas";

%include "T:\socioec\Current_Projects\&xver\program_code\1020 Annual cycle.sas"; *//* contains macro forecast */
/*
%macro xpef;
%do yr=&by1 %to &yy2;
	%forecast (yr=&yr);
%end;
%mend xpef;

%xpef;

%include "T:\socioec\Current_Projects\&xver\program_code\1041 Export microdata to SQL.sas";

%include "T:\socioec\Current_Projects\&xver\program_code\1043 Income Imputation and Assignment.sas";

%include "T:\socioec\Current_Projects\&xver\program_code\1050 Income Upgrading.sas";

%include "T:\socioec\Current_Projects\&xver\program_code\1055 Synthetic Households.sas";

%include "T:\socioec\Current_Projects\&xver\program_code\1056 Employment Controls.sas";

%include "T:\socioec\Current_Projects\&xver\program_code\1057 Jobs capacity.sas";

%include "T:\socioec\Current_Projects\&xver\program_code\1058 Incorporating employment events.sas";

%include "T:\socioec\Current_Projects\&xver\program_code\2000 Assembling ABM forecast data.sas";
*/
options notes;

%let t02=%sysfunc(time(),time8.0);
%put Program Started at &t01;
%put Program Ended at &t02;

/* proc printto;run; */
