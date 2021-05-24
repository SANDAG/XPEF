 
    data WORK.NON_RES_SCHED_DEV    ;
    %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
    infile 'M:\RES\estimates & forecast\SR14 Forecast\Scheduled Development\Non-residential Scheduled Development\non residential sched dev for the SCS forecast employment 05142020.csv' delimiter = '
,' MISSOVER DSD lrecl=13106 firstobs=2 ;
       informat siteid best32. ;
       informat sitename $57. ;
       informat _totalsqft best32. ;
       informat _civemp best32. ;
       informat milemp best32. ;
       informat sfu best32. ;
       informat mfu best32. ;
       informat mhu best32. ;
       informat civgq best32. ;
       informat milgq best32. ;
       informat Link $175. ;
       informat source $46. ;
       informat infodate mmddyy10. ;
       informat spacesqft best32. ;
       informat startdate best32. ;
       informat compdate mmddyy10. ;
       informat ressqft best32. ;
       informat nressqft best32. ;
       informat created_us $7. ;
       informat created_da mmddyy10. ;
       informat devtypeid best32. ;
       informat city $8. ;
       informat old_siteid best32. ;
       informat status $13. ;
       informat comment $68. ;
       format siteid best12. ;
       format sitename $57. ;
       format _totalsqft best12. ;
       format _civemp best12. ;
       format milemp best12. ;
       format sfu best12. ;
       format mfu best12. ;
       format mhu best12. ;
       format civgq best12. ;
       format milgq best12. ;
       format Link $175. ;
       format source $46. ;
       format infodate mmddyy10. ;
       format spacesqft best12. ;
       format startdate best12. ;
       format compdate mmddyy10. ;
       format ressqft best12. ;
       format nressqft best12. ;
       format created_us $7. ;
       format created_da mmddyy10. ;
       format devtypeid best12. ;
       format city $8. ;
       format old_siteid best12. ;
       format status $13. ;
       format comment $68. ;
    input
                siteid
                sitename  $
                _totalsqft 
                _civemp  
                milemp
                sfu
                mfu
                mhu
                civgq
                milgq
                Link  $
                source  $
                infodate
                spacesqft
                startdate  
                compdate  
                ressqft
                nressqft
                created_us  $
                created_da
                devtypeid
                city  $
                old_siteid
                status  $
                comment  $
    ;
    if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
    run;
proc sort data = non_res_sched_dev; by siteid; run; 

proc sql; 
create table non_res_sched_dev_2 as 
select distinct siteid  
from non_res_sched_dev 
where (year(compdate) >= 2016 and year(compdate) <= 2025) and (_civemp >0 or milemp >0); 
quit; 

proc sql; 
create table non_res_sched_dev_3 as
select siteid, sitename, _totalsqft as totalsqft, _civemp as civemp, milemp, sfu, mfu, civgq, milgq, link, source, year(infodate) as infodate, spacesqft,
startdate, year(compdate) as compdate, ressqft, created_us, year(created_da) as created_da, old_siteid, status, comment
from non_res_sched_dev
where (year(compdate) >= 2016 and year(compdate) <=2025) and (_civemp >=0 or milemp >0); 
quit;  

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014b8;Trusted_Connection=yes;") ;
create table sched_dev_parcel as select *
from connection to odbc
(
select *
from rm.dbo.urbansim_scheddevparcel_SCS_0514202
);
disconnect from odbc;
quit; 

proc sql; 
create table sched_dev_parcel_2 as 
select distinct site_id from sched_dev_parcel 
where civemp_imputed >0; 
quit; 



proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014b8;Trusted_Connection=yes;") ;
create table sched_dev_sites as select a.objectid, a.siteid, a.sitename, 
a.totalsqft, a.empden, a.civemp, a.milemp, a.sfu, a.mfu, a.mhu, a.civgq, a.milgq, a.source, 
year(DATEPART(a.infodate)) as infodate, 
a.spacesqft, year(DATEPART(a.startdate)) as startdate, year(DATEPART(a.compdate)) as compdate, a.created_us, year(DATEPART(a.created_da)) as created_da, a.last_edite, 
year(DATEPART(a.last_edi_1)) as last_edi_1, 
a.devtypeid, a.city, a.old_siteid, a.check_, a.status, a.shape
from connection to odbc
(
select objectid, siteid, sitename, totalsqft, empden, civemp, milemp, sfu, mfu, mhu, civgq, milgq, source, 
CAST(infodate as datetime) as infodate, 
spacesqft, CAST(startdate as datetime) as startdate , CAST(compdate as datetime) as compdate, created_us, CAST(created_da as datetime) as created_da, last_edite, 
CAST(last_edi_1 as datetime) as last_edi_1,
devtypeid, city, old_siteid, check_, status, shape
from rm.dbo.SCHEDDEV_URBANSIM_SCS_05142020
) as a
order by siteid;
disconnect from odbc;
quit; 

proc sql; 
create table sched_dev_sites_2 as 
select distinct siteid from sched_dev_sites 
where civemp >0; 
quit; 

/*update the sched dev sites table for the SCS forecast with the updated sheet--some will just need updated fields, some will need to be added*/
proc sql; 
create table sched_dev_sites_edited as 
select coalesce(a.siteid, b.siteid) as siteid,
coalesce(a.sitename, b.sitename) as sitename,
coalesce(a.totalsqft, b.totalsqft) as totalsqft,
a.empden,
coalesce(b.civemp,a.civemp) as civemp, 
coalesce(a.milemp,b.milemp) as milemp,  
coalesce(a.sfu, b.sfu) as sfu, 
coalesce(a.mfu, b.mfu) as mfu, 
a.mhu, 
coalesce(a.civgq, b.civgq) as civgq, 
coalesce(a.milgq, b.milgq) as milgq, 
coalesce(a.source,b.source) as source, 
coalesce(a.infodate,b.infodate) as infodate, 
coalesce(a.spacesqft, b.spacesqft) as spacesqft, 
coalesce(b.startdate,a.startdate) as startdate, 
coalesce(b.compdate,a.compdate) as compdate, 
a.created_us,
coalesce(a.created_da,b.created_da) as created_da, 
a.last_edite, 
last_edi_1,
a.devtypeid, 
a.city, 
coalesce(a.old_siteid, b.old_siteid) as old_siteid, 
a.check_, 
coalesce(a.status,b.status) as status,
a.shape
from sched_dev_sites as a 
full join non_res_sched_dev_3 as b on a.siteid = b.siteid 
order by siteid; 
quit; 

/*make sure none of the sched dev sites that were reviewed get dropped in the above step*/
proc sql; 
create table site_test as 
select * from sched_dev_sites_edited 
where siteid not in(select * from non_res_sched_dev_2)
order by siteid; 
quit; 

proc sql; 
create table sdunit_count as 
select sum(civemp) as civemp
from sched_dev_sites_edited; 
quit; 

proc sql; 
create table non_res_sched_dev_3_count as 
select sum(civemp) as civemp
from non_res_sched_dev_3; 
quit; 

/*update the civemp fields for navwar*/
proc sql; 
update sched_dev_sites_edited set civemp = 3271 where siteid = 19020; 
quit; 

/*should be 10824*/
proc sql; 
create table sdunit_count as 
select sum(civemp) as civemp
from sched_dev_sites_edited; 
quit; 
/*take the old parcel-level non res sched dev jobs and allocate them to the new sched dev parcel files*/

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;") ;
create table sched_dev_parcel_old as select *
from connection to odbc
(
select *
from urbansim.urbansim.scheduled_development_parcel
);
disconnect from odbc;
quit; 


/*these are the events that are already allocated to the parcel*/
proc sql; 
create table sched_dev_parcel_3 as 
select * from sched_dev_parcel
where site_id in(select siteid from non_res_sched_dev)
order by site_id; 
quit; 

/*these are the events that need to be allocated to the parcel*/
proc sql; 
create table sched_dev_need as
select * from non_res_sched_dev_3 
where siteid not in(select site_id from sched_dev_parcel_3)
order by siteid; 
quit; 

/*take the parcel level information from the sites in the above table and add it to the sched_dev_parcel table*/
proc sql; 
create table needed as 
select * from sched_dev_parcel_old 
where site_id in(select siteid from sched_dev_need)
order by site_id; 
quit; 

proc sql; 
create table non_res_sched_dev_complete as 
select site_id, parcel_id, capacity_3, sfu_effective_adj, mfu_effective_adj, notes, editor, shape, civgq, civemp_imputed, sector_id, civemp_notes, pid from sched_dev_parcel 
union all 
select site_id, parcel_id, capacity_3, sfu_effective_adj, mfu_effective_adj, notes, editor, shape, civgq, civemp_imputed, sector_id, civemp_notes, p_id as pid from needed 
order by site_id; 
quit; 

/*update with NAVWAR info*/
proc sql; 
insert into non_res_sched_dev_complete set site_id = 19020, parcel_id = 5124836, civemp_imputed = 117,  sector_id = 7,  
			civemp_notes = 'estimates per timeline/phasing info'; 
insert into non_res_sched_dev_complete set site_id = 19020, parcel_id = 5124836, civemp_imputed = 2250, sector_id = 13, 
			civemp_notes = 'estimates per timeline/phasing info'; 
insert into non_res_sched_dev_complete set site_id = 19020, parcel_id = 5124836, civemp_imputed = 360,  sector_id = 12, 
			civemp_notes = 'estimates per timeline/phasing info'; 
insert into non_res_sched_dev_complete set site_id = 19020, parcel_id = 5124836, civemp_imputed = 544,  sector_id = 6,  
			civemp_notes = 'estimates per timeline/phasing info'; 
quit; 

/*this should equal the number of sites in non_res_sched_dev_2*/
proc sql; 
create table test_final as 
select distinct site_id from non_res_sched_dev_complete
where civemp_imputed >0
order by site_id; 
quit; 
/*14088 and 15005 are added back in b/c they are from the SCS file. 14088 has a phase date of 2028 and 15005 has a phase date of 2025. Might want to leave 15005
but can change select statements in the 1057 program so will leave them both in this table*/

libname sql_urb odbc noprompt="driver=SQL Server; server=sql2014a8; database=urbansim;
Trusted_Connection=yes" schema=urbansim;

proc sql; 
drop table sql_urb.non_res_sched_dev_parcel_scs; 

create table sql_urb.non_res_sched_dev_parcel_scs as select site_id, parcel_id, capacity_3, sfu_effective_adj, mfu_effective_adj, notes, editor, civgq, 
civemp_imputed, sector_id, civemp_notes, pid from non_res_sched_dev_complete; 
quit; 

libname sql_ref odbc noprompt="driver=SQL Server; server=sql2014a8; database=urbansim;
Trusted_Connection=yes" schema=ref;

proc sql; 
drop table sql_ref.non_res_sched_dev_sites_scs; 

create table sql_ref.non_res_sched_dev_sites_scs as select * from sched_dev_sites_edited; 
quit;
