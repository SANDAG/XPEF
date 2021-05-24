/* References the 'Variables and Libaries' file */
%let a=%sysget(SAS_EXECFILEPATH);
%let b=%sysget(SAS_EXECFILENAME);
%let valib=%sysfunc(tranwrd(&a,&b,_ Variables and Libraries.sas));
%include "&valib";

proc sql;
drop table sql_xpef.abm_mgra13_based_input;
drop table sql_xpef.abm_syn_households;
drop table sql_xpef.abm_syn_persons;
drop table sql_xpef.dw_age;
drop table sql_xpef.dw_age_sex_ethnicity;
drop table sql_xpef.dw_ethnicity;
drop table sql_xpef.dw_household_income;
drop table sql_xpef.dw_households;
drop table sql_xpef.dw_housing;
drop table sql_xpef.dw_jobs;
drop table sql_xpef.dw_jobs_2;
drop table sql_xpef.dw_jobs_3;
drop table sql_xpef.dw_jobs_4;
drop table sql_xpef.dw_jobs_base;
drop table sql_xpef.dw_population;
drop table sql_xpef.dw_sex;
drop table sql_xpef.gq_population;
drop table sql_xpef.hh_aggregated;
drop table sql_xpef.household_income;
drop table sql_xpef.household_income_upgraded;
drop table sql_xpef.household_population;
drop table sql_xpef.households;
drop table sql_xpef.housing_units;
drop table sql_xpef.hp_aggregated;
drop table sql_xpef.mgra_id_new;
drop table sql_xpef.parcel_du_xref_post2017;
/*drop schema &xver;*/
quit;
