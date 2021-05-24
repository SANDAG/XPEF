
%macro m1;

proc datasets library=work nolist; delete hp_0 hh_0 hu_0 gq_0;quit;

%do yr=&by0 %to &yy3;

proc sql;
create table hh as select &yr as yr length=3 format=4.
,mgra format=5.,jur format=2.,ct format=$6.,size format=2.
,hh_id format=8.
from sd.hu_&yr where hh_id^=.;

create table hp as select &yr as yr length=3 format=4.
,hh_id format=8.
,hp_id format=8.
,age length=3 format=3.
,r format=$3.,hisp format=$2.,sex format=$1.
,dhms(dob,0,0,0) as dob format=datetime20.
,role format=$1.
from sd.hp_&yr;

create table hu as select &yr as yr length=3 format=4.
,mgra format=5.,jur format=2.,cpa format=4.,ct format=$6.
,du_type length=3 format=$3.,sto_flag format=1.,hh_id format=7.
,hu_id format=8.
from sd.hu_&yr;

create table gq as select &yr as yr length=3 format=4.
,gq_id format=8.
,gq_type format=$3.
,age length=3 format=3.
,r format=$3.,hisp format=$2.,sex format=$1.
,dhms(dob,0,0,0) as dob format=datetime20.
,mgra format=5.,jur format=2.,cpa format=4.,ct format=$6.
from sd.gq_&yr;

quit;

proc append base=hu_0 data=hu;run;
proc append base=hh_0 data=hh;run;
proc append base=hp_0 data=hp;run;
proc append base=gq_0 data=gq;run;

%end;
%mend m1;

%m1;

proc sql;
create table hp_aggregated as select yr length=3 format=4.,age length=3 format=3.,r length=3 format=$3.,hisp length=2 format=$2.,
sex length=1 format=$1.
,count(hp_id) as hp
from hp_0
group by yr,age,r,hisp,sex;

create table hh_aggregated as select yr length=3 format=4.
,count(hh_id) as households
from hh_0
group by yr;
quit;

proc sql;
CONNECT TO ODBC(noprompt="driver=SQL Server; server=sql2014a8; database=isam; DBCOMMIT=10000; Trusted_Connection=yes;") ;

EXECUTE ( drop table if exists isam.&xver..households; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;
EXECUTE ( drop table if exists isam.&xver..housing_units; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;
EXECUTE ( drop table if exists isam.&xver..household_population; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;
EXECUTE ( drop table if exists isam.&xver..gq_population; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE ( drop table if exists isam.&xver..hp_aggregated; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;
EXECUTE ( drop table if exists isam.&xver..hh_aggregated; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;



EXECUTE
(
CREATE TABLE isam.&xver..households(
yr smallint
,mgra smallint
,jur tinyint
,ct varchar(6)
,size tinyint
,hh_id int
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE
(
CREATE TABLE isam.&xver..housing_units(
yr smallint
,mgra smallint
,jur tinyint
,cpa smallint
,ct varchar(6)
,du_type varchar(3)
,sto_flag tinyint
,hh_id int
,hu_id int
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE
(
CREATE TABLE isam.&xver..household_population(
yr smallint
,hh_id int
,hp_id int
,age tinyint
,r varchar(3)
,hisp varchar(2)
,sex varchar(1)
,dob datetime
,role varchar(1)
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE
(
CREATE TABLE isam.&xver..gq_population(
yr smallint
,gq_id int
,gq_type varchar(3)
,age tinyint
,r varchar(3)
,hisp varchar(2)
,sex varchar(1)
,dob datetime
,mgra smallint
,jur tinyint
,cpa smallint
,ct varchar(6)
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE
(
CREATE TABLE isam.&xver..hp_aggregated(
yr smallint
,age tinyint
,r varchar(3)
,hisp varchar(2)
,sex varchar(1)
,hp int
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE
(
CREATE TABLE isam.&xver..hh_aggregated(
yr smallint
,households int
) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;


DISCONNECT FROM ODBC ;
quit;


options notes;

proc sql;

insert into sql_xpef.households(bulkload=yes bl_options=TABLOCK) select * from hh_0;
insert into sql_xpef.housing_units(bulkload=yes bl_options=TABLOCK) select * from hu_0;
insert into sql_xpef.household_population(bulkload=yes bl_options=TABLOCK) select * from hp_0;
insert into sql_xpef.gq_population(bulkload=yes bl_options=TABLOCK) select * from gq_0;

insert into sql_xpef.hp_aggregated(bulkload=yes bl_options=TABLOCK) select * from hp_aggregated;
insert into sql_xpef.hh_aggregated(bulkload=yes bl_options=TABLOCK) select * from hh_aggregated;
quit;

options nonotes;
