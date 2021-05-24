%let xver=xpef05;

libname sd "T:\socioec\Current_Projects\&xver\simulation_data\";

libname sql_xpef odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;
Trusted_Connection=yes" schema=&xver;

%let by1=2017;
%let yy2=2050; /* setting the last year of the simulation; this will create population for 1/1/&yy2+1 */
%let yy3=%eval(&yy2 + 1);


%macro m1;

proc datasets library=work nolist; delete hp_0 hh_0 hu_0 gq_0;quit;

%do yr=&by1 %to &yy3;

proc sql;
create table hh as select &yr as yr length=3 format=4.
/*,mgra format=5.,jur format=2.,ct format=$6.*/
,size format=2.
,hh_id format=8.
,hu_id format=8.
from sd.hu_&yr where hh_id^=.;

create table hp as select &yr as yr length=3 format=4.
,hh_id format=8.
,hp_id format=8.
/*,age length=3 format=3.
,r format=$3.,hisp format=$2.,sex format=$1.
,dob length=4 format=mmddyy10.,role format=$1.*/
from sd.hp_&yr;

create table hu as select &yr as yr length=3 format=4.
,mgra format=5.,jur format=2.,cpa format=4.,ct format=$6.
,du_type length=3 format=$3.,sto_flag format=1.
,hh_id format=8.
,hu_id format=8.
from sd.hu_&yr;

/*
create table gq as select &yr as yr length=3 format=4.
,gq_id format=8.
,gq_type format=$3.
,age length=3 format=3.
,r format=$3.,hisp format=$2.,sex format=$1.
,dob length=4 format=mmddyy10.
,mgra format=5.,jur format=2.,cpa format=4.,ct format=$6.
from sd.gq_&yr;
*/
quit;

proc append base=hu_0 data=hu;run;
proc append base=hh_0 data=hh;run;
proc append base=hp_0 data=hp;run;
/*proc append base=gq_0 data=gq;run;*/

%end;
%mend m1;

%m1;

proc sql;
create table test_01 as select * from hh_0 where hu_id=.;
quit;

proc sql;
create table ztest_01 as select x.*,y.hu_id
from sql_xpef.households as x
left join sql_xpef.housing_units as y on x.yr=y.yr and x.hh_id=y.hh_id;

create table ztest_01a as select * from ztest_01 where hu_id=.;
quit;
