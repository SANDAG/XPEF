/* BEFORE RUNNING THIS FILE, MAKE SURE TO UPDATE THE VARIABLE thuver IN THE VARIABLES FILE */

/* References the 'Variables and Libaries' file */
%let a=%sysget(SAS_EXECFILEPATH);
%let b=%sysget(SAS_EXECFILENAME);
%let valib=%sysfunc(tranwrd(&a,&b,_ Variables and Libraries.sas));
%include "&valib";

/* Changes from original HU Construction worksheet to updated one */
/*
proc import out=hu_2
datafile="T:\socioec\Current_Projects\&xver\input_data\HU Construction and PPH projections.xlsx"
replace dbms=excelcs; RANGE='Data2$k1:l36'n;
run;
*/

proc import out=hu_2
datafile="T:\socioec\Current_Projects\&xver\input_data\HU Construction and PPH projections_Feb2020.xlsx"
replace dbms=excelcs; RANGE='Sheet1$r1:s36'n;
run;

/* Creates a table in the database with the spreadsheet annual targets */
/* Note: This should not change without new targets, however Dmitry felt that all version_ids in this process should advance together */
proc sql;
create table hu_urb_1 as select &thuver as version_id,yr_built_during as yr /* this is BUILD DURING YR */
,round(hu_g,1) as housing_units_add format=6.
from hu_2 where yr_built_during in (&by1:2050) order by yr;
quit;

proc sql;
delete from urb.urbansim_target_housing_units where version_id=&thuver;

insert into urb.urbansim_target_housing_units select * from hu_urb_1;
quit;
