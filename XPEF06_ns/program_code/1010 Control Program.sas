options nonotes;
options set=SAS_HADOOP_RESTFUL 1;

%let t01=%sysfunc(time(),time8.0);

/* setting forecast version */
%let xver=xpef06_ns;

/* setting estimates version */
%let estver=est_2017_04;


/*%put %Unquote(%bquote(')&xver%bquote('));*/

libname sql_xpef odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;
Trusted_Connection=yes" schema=&xver;

libname sql_est odbc noprompt="driver=SQL Server; server=sql2014a8; database=estimates;
Trusted_Connection=yes" schema=&estver;

libname sql_de odbc noprompt="driver=SQL Server; server=sql2014a8; database=socioec_data;
Trusted_Connection=yes" schema=ca_dof;

libname sql_dbo odbc noprompt="driver=SQL Server; server=sql2014a8; database=ws;
Trusted_Connection=yes" schema=dbo;

libname sql_dim odbc noprompt="driver=SQL Server; server=sql2014a8; database=demographic_warehouse;
Trusted_Connection=yes" schema=dim;

/* folders related to the estimates */
/*libname e0 "T:\socioec\Current_Projects\estimates\input_data";*/

libname e1 "T:\socioec\Current_Projects\&xver\input_data";

/*libname sd0 "T:\socioec\Current_Projects\estimates\2017_03\output_data";*/

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

%let yy2=2050; /* setting the last year of the simulation; this will create population for 1/1/&yy2+1 */
%let yy3=%eval(&yy2 + 1);

%let br = 102; /* setting id for birth rates */
%let dr = 102; /* setting id for death rates */

/* setting a version of urbanim outputs */
%let usver=390;

/* version of the economic simulation */
%let ecver=1192;

/* !!! specifying years that will be used in the ABM !!! */
%global list1;
%let list1=2019,2021,2024,2026,2027,2030,2031,2033,2036,2041,2046,2051;
%put &list1;

%let abmpath=T:\socioec\Current_Projects\&xver\abm_csv;
%let list4=2018 2020 2023 2025 2026 2029 2030 2032 2035 2040 2045 2050;
%let abmv=03;
/*
%include "T:\socioec\Current_Projects\&xver\program_code\1041 Export microdata to SQL.sas";

%include "T:\socioec\Current_Projects\&xver\program_code\1043 Income Imputation and Assignment.sas";

%include "T:\socioec\Current_Projects\&xver\program_code\1050 Income Upgrading.sas";
*/
%include "T:\socioec\Current_Projects\&xver\program_code\1055 Synthetic Households.sas";

%include "T:\socioec\Current_Projects\&xver\program_code\1057 Jobs capacity.sas";
/*
%include "T:\socioec\Current_Projects\&xver\program_code\1058 Incorporating employment events.sas";
*/
%include "T:\socioec\Current_Projects\&xver\program_code\2000 Assembling ABM forecast data.sas";
/*
%include "T:\socioec\Current_Projects\&xver\program_code\2000 NEW Assembling ABM forecast data.sas";

%include "T:\socioec\Current_Projects\&xver\program_code\4000 Post model change (parking).sas";
*/
options notes;

%let t02=%sysfunc(time(),time8.0);
%put Program Started at &t01;
%put Program Ended at &t02;
