/*
Take total births from components of change projections
and allocate to race/ages using birth rates
*/

/* BIRTHS */
proc sql;
create table hp_fem_0 as select x.*

from (select hp_id,hh_id,age,
case 
when hisp="H" then "H"
when r="R10" then "W"
when r="R02" then "B"
when r="R04" then "S"
else "O" end as race
,ranuni(&yr + 100) as rn
from hp_base where sex = "F") as x

inner join (select hh_id from hh_base where 1 < size < 10) as z on x.hh_id = z.hh_id

inner join (select * from pdsr.birth_rates where yr = &yr and birth_rate_id = &br) as y
on x.age = y.age and x.race = y.race

where x.rn <= (y.birth_rate * 2) /* this is needed to oversample */
order by rn;
quit;

data hp_fem_0;set hp_fem_0;length i 4;
i+1;
run;

proc sql;
create table hp_fem(drop=i) as select x.*,monotonic() as b_id length=4
from hp_fem_0 as x
cross join (select * from sql_dof.&coc where county_name = "San Diego" and calendar_yr = &yr) as y
where x.i <= y.births;
quit;


data sd.hp_fem_&yr; set hp_fem;run;

%let t=%sysfunc(time(),time8.0);
%put ========== Finished Birth Assignment for year &yr at &t;

