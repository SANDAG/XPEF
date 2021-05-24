/* Variables to update when needed */

%let xver=xpef32; /* This must match the name of the XPEF folder being used */

%let estver=est_2018_03; /* Used to set the sql_est library below, only updated with new estimates */

%let dofver=2020_1_10; /* Set the DOF release version to reference */

%let by1=2018; /* First year of the Forecast, only updated with new estimates */
/* This is both the 'last year of estimates' and 'first year of projections' from DOF */




/* Library references - should not need to edit these */
/* Updated server to ddamwsql16 from sql2014a8 4-Feb-2021 */
/* Database Connections*/
libname sql_xpef odbc noprompt="driver=SQL Server; server=ddamwsql16; database=isam;
Trusted_Connection=yes" schema=&xver;

libname sql_est odbc noprompt="driver=SQL Server; server=ddamwsql16; database=estimates;
Trusted_Connection=yes" schema=&estver;

libname urb odbc noprompt="driver=SQL Server; server=ddamwsql16; database=urbansim;
Trusted_Connection=yes" schema=urbansim;

libname pdsr odbc noprompt="driver=SQL Server; server=ddamwsql16; database=isam;
Trusted_Connection=yes" schema=demographic_rates;

/* Filepath Connections */
libname e1 "T:\socioec\Current_Projects\&xver\input_data";

libname old_e "T:\socioec\Current_Projects\estimates\input_data";

libname irmi "T:\socioec\Income_Reconciliation_Model\Inputs";




/* These variables are used in creating and setting reduced capacity for urbansim */
/* THESE MUST BE UPDATED before running the 035, 040 and 045 scripts as they are
used to DELETE and then add a new version of the related tables. */

/* These are for creating new capacity targets and reduced capacity */
/* 10, 122 and 510 are used in XPEF28 and XPEF29 */
/* rcver unused, 123, 511, scsver = 1 in XPEF 32 */
/* Also used in XPEF31 but urbansim_lite logic was modified (scheduled development update) */
%let rcver = 10; /* reduced capacity version id */ 
%let thuver = 123; /* target housing units version id */
%let scver = 511; /* subregional control version id - deletes old and sets new*/

%let scsver = 1; /* scs scenario version */

/* The below variable determines which run_id from urbansim_lite HU module is used in the forecast */
/* This will need to be updated after 030, 035, 040 and 045 are run, if used */
/* It will need to be updated if any new urbansim run is performed as well, such as scenario testing */
/* It is not called until 1010 has begun running the full XPEF process */
%let usver=477;
/* Urbansim_lite run_id 477 is used in XPEF32 */

/* No other variables or libnames should need modification in the main scripts (unless you change something like the estimates vintage or DOF forecast...) */
/* Feb 11, 2020: will try to consolidate more of these potentially updateable things here when time permits */
