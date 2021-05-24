libname e201704 "T:\socioec\Current_Projects\estimates\2017_04\input_data";

/*
libname e1 "T:\socioec\Current_Projects\XPEF06\input_data";
*/

data yr2051;length yr 3;
do yr=2018 to 2051;
	output;
end;
run;

proc sql;
create table e1.ct_hh_target_dist as select y.yr,x.ct,x.r7,x.hh_target_s,x.hp_target_s
from (select * from e201704.ct_hh_target_dist where yr=2017) as x
cross join yr2051 as y
order by yr,ct,r7;
quit;


/*
libname sql_est odbc noprompt="driver=SQL Server; server=sql2014a8; database=estimates;
Trusted_Connection=yes" schema=est_2017_04;

proc sql;
create table est_hh_0 as select x.yr,y.ct
,case
when x.age <= 4 then 00.04
when x.age <= 9 then 05.09
when x.age <= 14 then 10.14
when x.age <= 17 then 15.17
when x.age <= 19 then 18.19
when x.age <= 24 then 20.24
when x.age <= 29 then 25.29
when x.age <= 34 then 30.34
when x.age <= 44 then 35.44
when x.age <= 54 then 45.54
when x.age <= 64 then 55.64
when x.age <= 74 then 65.74
when x.age <= 84 then 75.84
when x.age >= 85 then 85.99
end as age14
,case
when x.hisp="H" then "H"
when x.r="R10" then "W"
when x.r="R02" then "B"
when x.r="R03" then "I"
when x.r="R04" then "S"
when x.r="R05" then "P"
when x.r="R06" then "O"
when x.r="R07" then "O"
end as r7
,case
when x.hisp="H" then "H"
when x.r="R10" then "W"
when x.r="R02" then "B"
when x.r="R03" then "AIAN"
when x.r="R04" then "API"
when x.r="R05" then "API"
when x.r="R06" then "O"
when x.r="R07" then "O"
end as r6
,count(x.hp_id) as hh_est
from sql_est.household_population as x
inner join sql_est.households as y
on x.hh_id = y.hh_id and x.yr=y.yr
where x.yr=2017 and x.role="H"
group by x.yr,y.ct,age14,r7,r6;
quit;

proc sql;
create table est_hh_1 as select yr,ct,r6,sum(hh_est) as hh
from est_hh_0 group by yr,ct,r6;
quit;

*/
