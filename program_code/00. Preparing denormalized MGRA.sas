%let xver=xpef06;

proc sql;
CONNECT TO ODBC(noprompt="driver=SQL Server; server=sql2014a8; database=isam; DBCOMMIT=10000; Trusted_Connection=yes;") ;
EXECUTE(
IF NOT EXISTS (
SELECT  schema_name
FROM    information_schema.schemata
WHERE   schema_name = %Unquote(%bquote(')&xver%bquote(')) ) 

BEGIN
EXEC sp_executesql N%Unquote(%bquote(')CREATE SCHEMA &xver%bquote('))
END
) BY ODBC ;
  %PUT &SQLXRC. &SQLXMSG.;
DISCONNECT FROM ODBC ;
quit;


libname sql_xpef odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;
Trusted_Connection=yes" schema=&xver;

libname xpef_p odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;
Trusted_Connection=yes" schema=xpef04;

/* copying mgra_id_new from a previous version */

proc sql;
drop table sql_xpef.mgra_id_new;

create table sql_xpef.mgra_id_new(bulkload=yes bl_options=TABLOCK) as select * from xpef_p.mgra_id_new;
quit;

/*

proc sql;
drop table sql_xpef.hh_aggregated;
drop table sql_xpef.hp_aggregated;
quit;



proc sql;
create table sql_xpef.abm_syn_households(bulkload=yes bl_options=TABLOCK) as select * from xpef_p.abm_syn_households;

create table sql_xpef.abm_syn_persons(bulkload=yes bl_options=TABLOCK) as select * from xpef_p.abm_syn_persons;

create table sql_xpef.gq_population(bulkload=yes bl_options=TABLOCK) as select * from xpef_p.gq_population;

create table sql_xpef.hh_aggregated(bulkload=yes bl_options=TABLOCK) as select * from xpef_p.hh_aggregated;

create table sql_xpef.household_income(bulkload=yes bl_options=TABLOCK) as select * from xpef_p.household_income;

create table sql_xpef.household_income_upgraded(bulkload=yes bl_options=TABLOCK) as select * from xpef_p.household_income_upgraded;

create table sql_xpef.household_population(bulkload=yes bl_options=TABLOCK) as select * from xpef_p.household_population;

create table sql_xpef.households(bulkload=yes bl_options=TABLOCK) as select * from xpef_p.households;

create table sql_xpef.housing_units(bulkload=yes bl_options=TABLOCK) as select * from xpef_p.housing_units;

create table sql_xpef.hp_aggregated(bulkload=yes bl_options=TABLOCK) as select * from xpef_p.hp_aggregated;
quit;

*/
