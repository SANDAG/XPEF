/* Variables to update when needed */

%let xver=xpef24; /* This must match the name of the XPEF folder being used */

%let estver=est_2018_03; /* Used to set the sql_est library below, only updated with new estimates */

%let by1=2018; /* First year of the Forecast, only updated with new estimates */


/* Common library references - should not need to edit these */

libname sql_xpef odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;
Trusted_Connection=yes" schema=&xver;

libname e1 "T:\socioec\Current_Projects\&xver\input_data";

libname sql_est odbc noprompt="driver=SQL Server; server=sql2014a8; database=estimates;
Trusted_Connection=yes" schema=&estver;

libname urb odbc noprompt="driver=SQL Server; server=sql2014a8; database=urbansim;
Trusted_Connection=yes" schema=urbansim;

libname pdsr odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;
Trusted_Connection=yes" schema=demographic_rates;


/* These variables are used in creating and setting reduced capacity for urbansim */
/* THESE MUST BE UPDATED before running the 035, 040 and 045 scripts as they are
used to DELETE and then add a new version of the related tables. */

/* These are for creating new capacity targets and reduced capacity */
/*6, 118 and 506 are used in XPEF24 */
%let rcver = 6; /* reduced capacity version id */ 
%let thuver = 118; /* target housing units version id */
%let scver = 506; /* subregional control version id - deletes old and sets new*/

/* The below variable determines which run_id from urbansim_lite HU module is used in the forecast */
/* This will need to be updated after 030, 035, 040 and 045 are run, if used */
/* It will need to be updated if any new urbansim run is performed as well, such as scenario testing */
/* It is not called until 1010 has begun running the full XPEF process */
%let usver=460;


/* No other variables or libnames should need modification in the main scripts */
