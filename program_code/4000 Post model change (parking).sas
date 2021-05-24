/* setting forecast version */
%let xver=xpef06;

libname sql_xpef odbc noprompt="driver=SQL Server; server=sql2014a8; database=isam;
Trusted_Connection=yes" schema=&xver;

libname e1 "T:\socioec\Current_Projects\&xver\input_data";



proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;");

create table parking_ratio_1 as select mgra,parking_ratio
from connection to odbc
(select * from [spacecore].[input].[vi_costar_parking_ratio_mgra] where parking_ratio is not NULL);

create table abm_1 as select *
from connection to odbc
(select * from [isam].[&xver].[abm_mgra13_based_input]);

disconnect from odbc;
quit;


/* the filename may change !!!! */
PROC IMPORT OUT=new_parking_1 DATAFILE="T:\socioec\pecas\data\parking2016\mgra13_based_input2016_updated_103118.csv"
DBMS=CSV REPLACE; GETNAMES=YES; DATAROW=2;
RUN; 


proc sql;
create table new_parking_1a as select 2016 as yr,mgra
,sum(hstallsoth) as h_o
,sum(hstallssam) as h_s
,sum(dstallsoth) as d_o
,sum(dstallssam) as d_s
,sum(mstallsoth) as m_o
,sum(mstallssam) as m_s
from new_parking_1 /*abm_1*/
group by yr,mgra;

create table new_parking_1b as select * from new_parking_1a where m_o> 0 or m_s > 0;

create table new_parking_1c as select yr
,sum(m_o) as m_o,sum(m_s) as m_s
,sum(d_o) as d_o,sum(d_s) as d_s
,sum(h_o) as h_o,sum(h_s) as h_s
from new_parking_1a group by yr;
quit;


proc sql;
create table jfc_00 as select parcel_id,mgra,yr,count(*) as j
from e1.jobs_from_capacities group by parcel_id,mgra,yr;

create table jfc_01 as select yr,count(*) as j
from e1.jobs_from_capacities group by yr;
quit;

proc sql;
create table jfc_0 as select parcel_id,yr,count(*) as j
from e1.jobs_from_capacities group by parcel_id,yr;

create table jfc_0a as select yr,sum(j) as j
from jfc_0 group by yr;

create table jfc_0b as select parcel_id, sum(j) as j, min(yr) as min_yr, max(yr) as max_yr, count(yr) as n_yr
from jfc_0 group by parcel_id;
quit;




/*
proc sql;
create table slots_by_source_1 as select parcel_id,mgra
,sum(slots_capacity + slots_events) as slots
from e1.job_slots_by_source
where slots_capacity > 0 or slots_events > 0
group by parcel_id,mgra;

create table jfc_1 as select parcel_id,min(yr) as yr
from e1.jobs_from_capacities group by parcel_id;

create table slots_by_source_2 as select x.*,y.yr
from slots_by_source_1 as x
left join jfc_1 as y on x.parcel_id=y.parcel_id;

create table slots_by_source_3 as select mgra,yr,sum(slots) as slots
from slots_by_source_2 where yr ^= . group by mgra,yr;

create table slots_by_source_4 as select x.*,y.parking_ratio
from slots_by_source_3 as x
left join parking_ratio_1 as y on x.mgra=y.mgra;

create table slots_by_source_5 as select *
,round((slots * 400)/1000 * parking_ratio,1) as p
from slots_by_source_4 where parking_ratio ^= .
order by mgra,yr;

create table slots_by_source_5a as select yr,sum(p) as p
from slots_by_source_5 group by yr;
quit;
*/

proc sql;
create table slots_by_source_1 as select parcel_id,mgra
,sum(slots_capacity + slots_events) as slots
from e1.job_slots_by_source
where slots_capacity > 0 or slots_events > 0
group by parcel_id,mgra;

create table jfc_1 as select parcel_id,yr,count(*) as j
from e1.jobs_from_capacities group by parcel_id,yr;

create table jfc_2 as select x.*,y.mgra
from jfc_1 as x
inner join slots_by_source_1 as y on x.parcel_id=y.parcel_id;

create table jfc_3 as select mgra,yr,sum(j) as j
from jfc_2 group by mgra,yr;

create table jfc_4 as
select 2018 as yr,mgra,sum(j) as j from jfc_3 where yr<=2018 group by mgra
	union all
select 2020 as yr,mgra,sum(j) as j from jfc_3 where yr<=2020 group by mgra
	union all
select 2025 as yr,mgra,sum(j) as j from jfc_3 where yr<=2025 group by mgra
	union all
select 2030 as yr,mgra,sum(j) as j from jfc_3 where yr<=2030 group by mgra
	union all
select 2035 as yr,mgra,sum(j) as j from jfc_3 where yr<=2035 group by mgra
	union all
select 2040 as yr,mgra,sum(j) as j from jfc_3 where yr<=2040 group by mgra
	union all
select 2045 as yr,mgra,sum(j) as j from jfc_3 where yr<=2045 group by mgra
	union all
select 2050 as yr,mgra,sum(j) as j from jfc_3 where yr<=2050 group by mgra
order by mgra,yr;

create table jfc_4a as select * from jfc_4
where yr=2050 order by j desc;
quit;



proc sql;
create table jfc_5 as select x.*,y.parking_ratio
from jfc_4 as x
left join parking_ratio_1 as y on x.mgra=y.mgra;

create table jfc_5a as select * from jfc_5 where yr=2050 and parking_ratio=.
order by j desc;

create table jfc_6 as select *
,round((j * 200)/1000 * parking_ratio,1) as p
from jfc_5 where parking_ratio ^= .
order by mgra,yr;

create table jfc_6a as select yr,sum(p) as p
from jfc_6 group by yr;
quit;


proc sql;
create table jobs_all_1 as select mgra,yr,sum(j) as j
from e1.jobs_all group by mgra,yr;

create table jobs_all_2 as select yr,sum(j) as j
from jobs_all_1 group by yr;
quit;

/*
proc sql;
create table jobs_all_3 as select x.*
,y.m_s, y.d_s, y.h_s, y.m_o, y.d_o, y.h_o
,coalesce(z.p,0) as new_slots
,calculated new_slots + m_s as new_m_s
,calculated new_m_s / j as new_m_s_j format=6.2
from jobs_all_2 as x
cross join abm_3a as y
left join jfc_6a as z on x.yr=z.yr
order by yr;
quit;
*/

proc sql;
create table new_slots_1 as
select 2016 as yr,mgra
,parkarea as parkarea_
,hstallsoth as hstallsoth_
,hstallssam as hstallssam_
,dstallsoth as dstallsoth_
,dstallssam as dstallssam_
,mstallsoth as mstallsoth_
,mstallssam as mstallssam_
,hparkcost as hparkcost_
,dparkcost as dparkcost_
,mparkcost as mparkcost_
,numfreehrs as numfreehrs_
from new_parking_1
	union all
select z.yr,x.mgra
,x.parkarea as parkarea_
,x.hstallsoth + coalesce(y.p,0) as hstallsoth_
,x.hstallssam + coalesce(y.p,0) as hstallssam_
,x.dstallsoth + coalesce(y.p,0) as dstallsoth_
,x.dstallssam + coalesce(y.p,0) as dstallssam_
,x.mstallsoth + coalesce(y.p,0) as mstallsoth_
,x.mstallssam + coalesce(y.p,0) as mstallssam_
,x.hparkcost as hparkcost_
,x.dparkcost as dparkcost_
,x.mparkcost as mparkcost_
,x.numfreehrs as numfreehrs_
from new_parking_1 as x
cross join (select distinct yr from jfc_6) as z
left join jfc_6 as y on x.mgra=y.mgra and z.yr=y.yr
order by yr,mgra;
quit;

/*
proc sql;
create table test01 as select mgra
,parkarea
,hstallsoth
,hstallssam
,dstallsoth
,dstallssam
,mstallsoth
,mstallssam
,hparkcost
,dparkcost
,mparkcost
,numfreehrs
from new_parking_1
order by mgra;
quit;
*/



/*
%macro abm3;

%let l = %sysfunc(countw(&listabm));
proc datasets library=work nolist; delete abm_lu_0;quit;

%do k=1 %to &l;
	%let yrr=%scan(&listabm,&k);

PROC IMPORT OUT=abm_00 DATAFILE="T:\socioec\Current_Projects\&xver\abm_csv\mgra13_based_input&yrr._01.csv"
DBMS=CSV REPLACE; GETNAMES=YES; DATAROW=2;
RUN; 

data abm_00;set abm_00; yr = &yrr; run;

proc append base=abm_0 data=abm_00;run;

proc datasets library=work nolist; delete abm_00;quit;

%end;

%mend abm3;

%let listabm=2016 2018 2020 2025 2030 2035 2040 2045 2050;
%abm3;
*/


proc sql;
create table abm_2 as select x.*,y.*
from abm_1 as x
left join new_slots_1 as y on x.yr=y.yr and x.mgra=y.mgra
order by yr,mgra;

create table abm_2a as select mgra,parkarea,parkarea_
from abm_2 where parkarea <> parkarea_;

update abm_2 set hstallsoth = hstallsoth_;
update abm_2 set hstallssam = hstallssam_;
update abm_2 set dstallsoth = dstallsoth_;
update abm_2 set dstallssam = dstallssam_;
update abm_2 set mstallsoth = mstallsoth_;
update abm_2 set mstallssam = mstallssam_;
update abm_2 set hparkcost = hparkcost_;
update abm_2 set dparkcost = dparkcost_;
update abm_2 set mparkcost = mparkcost_;
update abm_2 set numfreehrs = numfreehrs_;
update abm_2 set parkarea = parkarea_;

create table abm_3 as select * 
from abm_2(drop=hstallsoth_ hstallssam_ dstallsoth_ dstallssam_ mstallsoth_ mstallssam_ hparkcost_ dparkcost_ mparkcost_ numfreehrs_ parkarea_);
quit;


%let abmv=02;

proc sql;
CONNECT TO ODBC(noprompt="driver=SQL Server; server=sql2014a8; DBCOMMIT=10000; Trusted_Connection=yes;") ;

EXECUTE ( drop table if exists isam.&xver..abm_mgra13_based_input_&abmv; ) BY ODBC ; %PUT &SQLXRC. &SQLXMSG.;

EXECUTE
(
CREATE TABLE isam.&xver..abm_mgra13_based_input_&abmv(
yr smallint
,mgra smallint
,taz smallint
,hs smallint
,hs_sf smallint
,hs_mf smallint
,hs_mh smallint
,hh smallint
,hh_sf smallint
,hh_mf smallint
,hh_mh smallint
,gq_civ smallint
,gq_mil smallint
,i1 smallint
,i2 smallint
,i3 smallint
,i4 smallint
,i5 smallint
,i6 smallint
,i7 smallint
,i8 smallint
,i9 smallint
,i10 smallint
,hhs tinyint
,pop int
,hhp int
,emp_ag smallint
,emp_const_non_bldg_prod smallint
,emp_const_non_bldg_office smallint
,emp_utilities_prod smallint
,emp_utilities_office smallint
,emp_const_bldg_prod smallint
,emp_const_bldg_office smallint
,emp_mfg_prod smallint
,emp_mfg_office smallint
,emp_whsle_whs smallint
,emp_trans smallint
,emp_retail smallint
,emp_prof_bus_svcs smallint
,emp_prof_bus_svcs_bldg_maint smallint
,emp_pvt_ed_k12 smallint
,emp_pvt_ed_post_k12_oth smallint
,emp_health int
,emp_personal_svcs_office smallint
,emp_amusement smallint
,emp_hotel smallint
,emp_restaurant_bar smallint
,emp_personal_svcs_retail smallint
,emp_religious smallint
,emp_pvt_hh smallint
,emp_state_local_gov_ent smallint
,emp_fed_non_mil smallint
,emp_fed_mil int
,emp_state_local_gov_blue smallint
,emp_state_local_gov_white smallint
,emp_public_ed smallint
,emp_own_occ_dwell_mgmt smallint
,emp_fed_gov_accts smallint
,emp_st_lcl_gov_accts smallint
,emp_cap_accts smallint
,emp_total int
,enrollgradekto8 smallint
,enrollgrade9to12 smallint
,collegeenroll int
,othercollegeenroll int
,adultschenrl smallint
,ech_dist int
,hch_dist int
,pseudomsa tinyint
,parkarea tinyint
,hstallsoth smallint
,hstallssam int
,hparkcost tinyint
,numfreehrs tinyint
,dstallsoth int
,dstallssam int
,dparkcost tinyint
,mstallsoth int
,mstallssam int
,mparkcost tinyint
,totint smallint
,duden numeric(6,2)
,empden numeric(6,2)
,popden numeric(6,2)
,retempden numeric(6,2)
,totintbin tinyint
,empdenbin tinyint
,dudenbin tinyint
,zip09 int
,parkactive numeric(6,2)
,openspaceparkpreserve numeric(8,2)
,beachactive numeric(6,2)
,budgetroom smallint
,economyroom smallint
,luxuryroom smallint
,midpriceroom smallint
,upscaleroom smallint
,hotelroomtotal smallint
,luz_id smallint
,truckregiontype tinyint
,district27 tinyint
,milestocoast numeric(6,2)
,acres numeric(8,2)
,effective_acres numeric(8,2)
,land_acres numeric(8,2)

) WITH (DATA_COMPRESSION = PAGE)
) BY ODBC; %PUT &SQLXRC. &SQLXMSG.;


insert into sql_xpef.abm_mgra13_based_input_&abmv(bulkload=yes bl_options=TABLOCK) select * from abm_3;
quit;


%macro abm2;

%let l = %sysfunc(countw(&list4));

/* iteration over years */
%do k=1 %to &l;
	%let yrr=%scan(&list4,&k);

data out_l_&yrr(drop=yr);set abm_3;where yr=&yrr;run;

PROC export data=out_l_&yrr outfile="&abmpath\mgra13_based_input&yrr._&abmv..csv" DBMS=CSV REPLACE;
RUN; 

%end;

%mend abm2;

%let abmpath=T:\socioec\Current_Projects\&xver\abm_csv;
%let list4=2016 2018 2020 2025 2030 2035 2040 2045 2050;

%abm2;
