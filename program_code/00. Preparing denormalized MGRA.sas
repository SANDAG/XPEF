/* References the 'Variables and Libaries' file */
%let a=%sysget(SAS_EXECFILEPATH);
%let b=%sysget(SAS_EXECFILENAME);
%let valib=%sysfunc(tranwrd(&a,&b,_ Variables and Libraries.sas));
%include "&valib";

/* Creates a schema in the isam database named after the current folder */
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

/* Maps a copy of the mgra_id table from the current estimates version */
proc sql;
CONNECT TO ODBC(noprompt="driver=SQL Server; server=sql2014a8; DBCOMMIT=10000; Trusted_Connection=yes;") ;

create table mgra_id_new as select *
from connection to odbc 
(
SELECT mgra_id, mgra, jurisdiction_id, cpa_id, jurisdiction, cpa
FROM [estimates].[&estver].[mgra_id]
);

disconnect from odbc;
quit;

/* Place the mgra_id table into the new schema in isam */
proc sql;
drop table sql_xpef.mgra_id_new;

create table sql_xpef.mgra_id_new(bulkload=yes bl_options=TABLOCK) as select * from mgra_id_new;
quit;


