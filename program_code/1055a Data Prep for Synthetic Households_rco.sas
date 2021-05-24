
%macro impacs;
%let l = %sysfunc(countw(&list2));

/* iteration over table names */
%do k=1 %to &l;
	%let name=%scan(&list2,&k);

proc sql;
CONNECT TO odbc(noprompt="driver=SQL Server; server=sql2014a8;Trusted_Connection=yes;");

create table &name._cn1_0 as select *
from connection to odbc
(select yr,line_number,line_desc,estimate,moe from census.acs.vw_summary_file
where yr=2018 and release_type='1Y' and summary_level='050' and st='06' and county='073' and subject_table = %Unquote(%bquote(')&name%bquote(')) );

create table &name._cn5_0 as select *
from connection to odbc
(select yr,line_number,line_desc,estimate,moe from census.acs.vw_summary_file
where yr=2018 and release_type='5Y' and summary_level='050' and st='06' and county='073' and subject_table = %Unquote(%bquote(')&name%bquote(')) );

create table &name._ct5_0 as select *
from connection to odbc
(select yr,tract as ct,line_number,line_desc,estimate,moe from census.acs.vw_summary_file
where yr=2018 and release_type='5Y' and summary_level='140' and county='073' and subject_table = %Unquote(%bquote(')&name%bquote(')) );

disconnect from odbc;
quit;

%end;

%mend impacs;

%let list2 = B23001 B23022 B23026 C24060 B15001 B14004 B14005 B14003;
%impacs;


proc sql;
create table B23001_lines as select distinct line_number,line_desc from B23001_cn1_0;
quit;

proc sql;
create table B23001_cn1_1 as select yr,estimate as est,moe
,case
when line_number in (5,7,8,9,91,93,94,95) then 1619
when line_number in (12,14,15,16,98,100,101,102) then 2021
when line_number in (19,21,22,23,105,107,108,109) then 2224
when line_number in (26,28,29,30,112,114,115,116) then 2529
when line_number in (33,35,36,37,119,121,122,123) then 3034
when line_number in (40,42,43,44,126,128,129,130) then 3544
when line_number in (47,49,50,51,133,135,136,137) then 4554
when line_number in (54,56,57,58,140,142,143,144) then 5559
when line_number in (61,63,64,65,147,149,150,151) then 6061
when line_number in (68,70,71,72,154,156,157,158) then 6264
when line_number in (75,76,77,161,162,163) then  6569
when line_number in (80,81,82,166,167,168) then  7074
when line_number in (85,86,87,171,172,173) then  7599
end as age13 length=3 /* 13 age groups */
,case
when line_number in (5,12,19,26,33,40,47,54,61,68) then "MIL"
when line_number in (7,14,21,28,35,42,49,56,63,70,75,80,85) then "EMP"
when line_number in (8,15,22,29,36,43,50,57,64,71,76,81,86) then "UNE"
when line_number in (9,16,23,30,37,44,51,58,65,72,77,82,87) then "NLF"
when line_number in (91,98,105,112,119,126,133,140,147,154) then "MIL"
when line_number in (93,100,107,114,121,128,135,142,149,156,161,166,171) then "EMP"
when line_number in (94,101,108,115,122,129,136,143,150,157,162,167,172) then "UNE"
when line_number in (95,102,109,116,123,130,137,144,151,158,163,168,173) then "NLF"
end as ws /* worker status */
,case when line_number<88 then "M" else "F" end as sex
from B23001_cn1_0 where calculated ws^="";

create table B23001_cn5_1 as select yr,estimate as est,moe
,case
when line_number in (5,7,8,9,91,93,94,95) then 1619
when line_number in (12,14,15,16,98,100,101,102) then 2021
when line_number in (19,21,22,23,105,107,108,109) then 2224
when line_number in (26,28,29,30,112,114,115,116) then 2529
when line_number in (33,35,36,37,119,121,122,123) then 3034
when line_number in (40,42,43,44,126,128,129,130) then 3544
when line_number in (47,49,50,51,133,135,136,137) then 4554
when line_number in (54,56,57,58,140,142,143,144) then 5559
when line_number in (61,63,64,65,147,149,150,151) then 6061
when line_number in (68,70,71,72,154,156,157,158) then 6264
when line_number in (75,76,77,161,162,163) then  6569
when line_number in (80,81,82,166,167,168) then  7074
when line_number in (85,86,87,171,172,173) then  7599
end as age13 length=3 /* 13 age groups */
,case
when line_number in (5,12,19,26,33,40,47,54,61,68) then "MIL"
when line_number in (7,14,21,28,35,42,49,56,63,70,75,80,85) then "EMP"
when line_number in (8,15,22,29,36,43,50,57,64,71,76,81,86) then "UNE"
when line_number in (9,16,23,30,37,44,51,58,65,72,77,82,87) then "NLF"
when line_number in (91,98,105,112,119,126,133,140,147,154) then "MIL"
when line_number in (93,100,107,114,121,128,135,142,149,156,161,166,171) then "EMP"
when line_number in (94,101,108,115,122,129,136,143,150,157,162,167,172) then "UNE"
when line_number in (95,102,109,116,123,130,137,144,151,158,163,168,173) then "NLF"
end as ws /* worker status */
,case when line_number<88 then "M" else "F" end as sex
from B23001_cn5_0 where calculated ws^="";

create table B23001_ct5_1 as select yr,ct,estimate as est,moe
,case
when line_number in (5,7,8,9,91,93,94,95) then 1619
when line_number in (12,14,15,16,98,100,101,102) then 2021
when line_number in (19,21,22,23,105,107,108,109) then 2224
when line_number in (26,28,29,30,112,114,115,116) then 2529
when line_number in (33,35,36,37,119,121,122,123) then 3034
when line_number in (40,42,43,44,126,128,129,130) then 3544
when line_number in (47,49,50,51,133,135,136,137) then 4554
when line_number in (54,56,57,58,140,142,143,144) then 5559
when line_number in (61,63,64,65,147,149,150,151) then 6061
when line_number in (68,70,71,72,154,156,157,158) then 6264
when line_number in (75,76,77,161,162,163) then  6569
when line_number in (80,81,82,166,167,168) then  7074
when line_number in (85,86,87,171,172,173) then  7599
end as age13 length=3 /* 13 age groups */
,case
when line_number in (5,12,19,26,33,40,47,54,61,68) then "MIL"
when line_number in (7,14,21,28,35,42,49,56,63,70,75,80,85) then "EMP"
when line_number in (8,15,22,29,36,43,50,57,64,71,76,81,86) then "UNE"
when line_number in (9,16,23,30,37,44,51,58,65,72,77,82,87) then "NLF"
when line_number in (91,98,105,112,119,126,133,140,147,154) then "MIL"
when line_number in (93,100,107,114,121,128,135,142,149,156,161,166,171) then "EMP"
when line_number in (94,101,108,115,122,129,136,143,150,157,162,167,172) then "UNE"
when line_number in (95,102,109,116,123,130,137,144,151,158,163,168,173) then "NLF"
end as ws /* worker status */
,case when line_number<88 then "M" else "F" end as sex
from B23001_ct5_0 where calculated ws^="";
quit;


proc sql;
create table B23022_cn1_1 as select yr,estimate as est,moe
,case
when line_number in (5,12,19,29,36,43) then "5052"
when line_number in (6,13,20,30,37,44) then "4849"
when line_number in (7,14,21,31,38,45) then "4047"
when line_number in (8,15,22,32,39,46) then "2739"
when line_number in (9,16,23,33,40,47) then "1426"
when line_number in (10,17,24,34,41,48) then "0113"
when line_number in (25,49) then "0000"
end as weeks_worked /* over last year (50 to 52) */

,case
when line_number in (5:10,29:34) then "3540"
when line_number in (12:17,36:41) then "1534"
when line_number in (19:24,43:48) then "0114"
end as hrs_worked /* usual hours worked per week (15 to 34) */

,case when line_number<26 then "M" else "F" end as sex
,1664 as age2 length=3
from B23022_cn1_0 where calculated hrs_worked^="";

create table B23022_cn5_1 as select yr,estimate as est,moe
,case
when line_number in (5,12,19,29,36,43) then "5052"
when line_number in (6,13,20,30,37,44) then "4849"
when line_number in (7,14,21,31,38,45) then "4047"
when line_number in (8,15,22,32,39,46) then "2739"
when line_number in (9,16,23,33,40,47) then "1426"
when line_number in (10,17,24,34,41,48) then "0113"
when line_number in (25,49) then "0000"
end as weeks_worked /* over last year (50 to 52) */

,case
when line_number in (5:10,29:34) then "3540"
when line_number in (12:17,36:41) then "1534"
when line_number in (19:24,43:48) then "0114"
end as hrs_worked /* usual hours worked per week (15 to 34) */

,case when line_number<26 then "M" else "F" end as sex
,1664 as age2 length=3
from B23022_cn5_0 where calculated hrs_worked^="";

create table B23022_ct5_1 as select yr,ct,estimate as est,moe
,case
when line_number in (5,12,19,29,36,43) then "5052"
when line_number in (6,13,20,30,37,44) then "4849"
when line_number in (7,14,21,31,38,45) then "4047"
when line_number in (8,15,22,32,39,46) then "2739"
when line_number in (9,16,23,33,40,47) then "1426"
when line_number in (10,17,24,34,41,48) then "0113"
when line_number in (25,49) then "0000"
end as weeks_worked /* over last year (50 to 52) */

,case
when line_number in (5:10,29:34) then "3540"
when line_number in (12:17,36:41) then "1534"
when line_number in (19:24,43:48) then "0114"
end as hrs_worked /* usual hours worked per week (15 to 34) */

,case when line_number<26 then "M" else "F" end as sex
,1664 as age2 length=3
from B23022_ct5_0 where calculated hrs_worked^="";
quit;


proc sql;
create table B23026_cn1_1 as select yr,estimate as est,moe
,case
when line_number in (5,12,19,29,36,43) then "5052"
when line_number in (6,13,20,30,37,44) then "4849"
when line_number in (7,14,21,31,38,45) then "4047"
when line_number in (8,15,22,32,39,46) then "2739"
when line_number in (9,16,23,33,40,47) then "1426"
when line_number in (10,17,24,34,41,48) then "0113"
when line_number in (25,49) then "0000"
end as weeks_worked /* over last year (50 to 52) */

,case
when line_number in (5:10,29:34) then "3540"
when line_number in (12:17,36:41) then "1534"
when line_number in (19:24,43:48) then "0114"
end as hrs_worked /* usual hours worked per week (15 to 34) */

,case when line_number<26 then "M" else "F" end as sex
,6599 as age2 length=3
from B23026_cn1_0 where calculated hrs_worked^="";

create table B23026_cn5_1 as select yr,estimate as est,moe
,case
when line_number in (5,12,19,29,36,43) then "5052"
when line_number in (6,13,20,30,37,44) then "4849"
when line_number in (7,14,21,31,38,45) then "4047"
when line_number in (8,15,22,32,39,46) then "2739"
when line_number in (9,16,23,33,40,47) then "1426"
when line_number in (10,17,24,34,41,48) then "0113"
when line_number in (25,49) then "0000"
end as weeks_worked /* over last year (50 to 52) */

,case
when line_number in (5:10,29:34) then "3540"
when line_number in (12:17,36:41) then "1534"
when line_number in (19:24,43:48) then "0114"
end as hrs_worked /* usual hours worked per week (15 to 34) */

,case when line_number<26 then "M" else "F" end as sex
,6599 as age2 length=3
from B23026_cn5_0 where calculated hrs_worked^="";

create table B23026_ct5_1 as select yr,ct,estimate as est,moe
,case
when line_number in (5,12,19,29,36,43) then "5052"
when line_number in (6,13,20,30,37,44) then "4849"
when line_number in (7,14,21,31,38,45) then "4047"
when line_number in (8,15,22,32,39,46) then "2739"
when line_number in (9,16,23,33,40,47) then "1426"
when line_number in (10,17,24,34,41,48) then "0113"
when line_number in (25,49) then "0000"
end as weeks_worked /* over last year (50 to 52) */

,case
when line_number in (5:10,29:34) then "3540"
when line_number in (12:17,36:41) then "1534"
when line_number in (19:24,43:48) then "0114"
end as hrs_worked /* usual hours worked per week (15 to 34) */

,case when line_number<26 then "M" else "F" end as sex
,6599 as age2 length=3
from B23026_ct5_0 where calculated hrs_worked^="";
quit;

proc sql;
create table B15001_cn1_1 as select yr,estimate as est,moe
,case
when line_number in (4,12,20,28,36,45,53,61,69,77) then "Less than 9th grade"
when line_number in (5,13,21,29,37,46,54,62,70,78) then "9th to 12th grade, no diploma"
when line_number in (6,14,22,30,38,47,55,63,71,79) then "High school graduate (includes equivalency)"
when line_number in (7,15,23,31,39,48,56,64,72,80) then "Some college, no degree"
when line_number in (8,16,24,32,40,49,57,65,73,81) then "Associate's degree"
when line_number in (9,17,25,33,41,50,58,66,74,82) then "Bachelor's degree"
when line_number in (10,18,26,34,42,51,59,67,75,83) then "Graduate or professional degree"
end as edu 

,case
when line_number in (4:10,45:51) then 1824
when line_number in (12:18,53:59) then 2534
when line_number in (20:26,61:67) then 3544
when line_number in (28:34,69:75) then 4564
when line_number in (36:42,77:83) then 6599
end as age5 length=3

,case when line_number<43 then "M" else "F" end as sex

from B15001_cn1_0 where calculated age5^=.;

create table B15001_cn5_1 as select yr,estimate as est,moe
,case
when line_number in (4,12,20,28,36,45,53,61,69,77) then "Less than 9th grade"
when line_number in (5,13,21,29,37,46,54,62,70,78) then "9th to 12th grade, no diploma"
when line_number in (6,14,22,30,38,47,55,63,71,79) then "High school graduate (includes equivalency)"
when line_number in (7,15,23,31,39,48,56,64,72,80) then "Some college, no degree"
when line_number in (8,16,24,32,40,49,57,65,73,81) then "Associate's degree"
when line_number in (9,17,25,33,41,50,58,66,74,82) then "Bachelor's degree"
when line_number in (10,18,26,34,42,51,59,67,75,83) then "Graduate or professional degree"
end as edu 

,case
when line_number in (4:10,45:51) then 1824
when line_number in (12:18,53:59) then 2534
when line_number in (20:26,61:67) then 3544
when line_number in (28:34,69:75) then 4564
when line_number in (36:42,77:83) then 6599
end as age5 length=3

,case when line_number<43 then "M" else "F" end as sex

from B15001_cn5_0 where calculated age5^=.;

create table B15001_ct5_1 as select yr,ct,estimate as est,moe
,case
when line_number in (4,12,20,28,36,45,53,61,69,77) then "Less than 9th grade"
when line_number in (5,13,21,29,37,46,54,62,70,78) then "9th to 12th grade, no diploma"
when line_number in (6,14,22,30,38,47,55,63,71,79) then "High school graduate (includes equivalency)"
when line_number in (7,15,23,31,39,48,56,64,72,80) then "Some college, no degree"
when line_number in (8,16,24,32,40,49,57,65,73,81) then "Associate's degree"
when line_number in (9,17,25,33,41,50,58,66,74,82) then "Bachelor's degree"
when line_number in (10,18,26,34,42,51,59,67,75,83) then "Graduate or professional degree"
end as edu 

,case
when line_number in (4:10,45:51) then 1824
when line_number in (12:18,53:59) then 2534
when line_number in (20:26,61:67) then 3544
when line_number in (28:34,69:75) then 4564
when line_number in (36:42,77:83) then 6599
end as age5 length=3

,case when line_number<43 then "M" else "F" end as sex

from B15001_ct5_0 where calculated age5^=.;
quit;

proc sql;
create table B14004_cn1_1 as select yr,estimate as est,moe
,case
when line_number in (4:7,20:23) then "Enrolled in public college or graduate school"
when line_number in (9:12,25:28) then "Enrolled in private college or graduate school"
when line_number in (14:17,30:33) then "Not enrolled in college or graduate school"
end as enr
,case
when line_number in (4,9,14,20,25,30) then 1517
when line_number in (5,10,15,21,26,31) then 1824
when line_number in (6,11,16,22,27,32) then 2534
when line_number in (7,12,17,23,28,33) then 3599
end as age4 length=3
,case when line_number<18 then "M" else "F" end as sex
from B14004_cn1_0 where calculated age4^=.;

create table B14004_cn5_1 as select yr,estimate as est,moe
,case
when line_number in (4:7,20:23) then "Enrolled in public college or graduate school"
when line_number in (9:12,25:28) then "Enrolled in private college or graduate school"
when line_number in (14:17,30:33) then "Not enrolled in college or graduate school"
end as enr
,case
when line_number in (4,9,14,20,25,30) then 1517
when line_number in (5,10,15,21,26,31) then 1824
when line_number in (6,11,16,22,27,32) then 2534
when line_number in (7,12,17,23,28,33) then 3599
end as age4 length=3
,case when line_number<18 then "M" else "F" end as sex
from B14004_cn5_0 where calculated age4^=.;

create table B14004_ct5_1 as select yr,ct,estimate as est,moe
,case
when line_number in (4:7,20:23) then "Enrolled in public college or graduate school"
when line_number in (9:12,25:28) then "Enrolled in private college or graduate school"
when line_number in (14:17,30:33) then "Not enrolled in college or graduate school"
end as enr
,case
when line_number in (4,9,14,20,25,30) then 1517
when line_number in (5,10,15,21,26,31) then 1824
when line_number in (6,11,16,22,27,32) then 2534
when line_number in (7,12,17,23,28,33) then 3599
end as age4 length=3
,case when line_number<18 then "M" else "F" end as sex
from B14004_ct5_0 where calculated age4^=.;
quit;

proc sql;
create table B14003_cn1_1 as select yr,estimate as est,moe
,case
when line_number in (4:11,32:39) then "Enrolled in public school"
when line_number in (13:20,41:48) then "Enrolled in private school"
when line_number in (22:29,50:57) then "Not enrolled in school"
end as enr
,case
when line_number in (4,13,22,32,41,50) then 0304
when line_number in (5,14,23,33,42,51) then 0509
when line_number in (6,15,24,34,43,52) then 1014
when line_number in (7,16,25,35,44,53) then 1517
when line_number in (8,17,26,36,45,54) then 1819
when line_number in (9,18,27,37,46,55) then 2024
when line_number in (10,19,28,38,47,56) then 2534
when line_number in (11,20,29,39,48,57) then 3599
end as age8 length=3
,case when line_number<30 then "M" else "F" end as sex
from B14003_cn1_0 where calculated age8^=.;

create table B14003_cn5_1 as select yr,estimate as est,moe
,case
when line_number in (4:11,32:39) then "Enrolled in public school"
when line_number in (13:20,41:48) then "Enrolled in private school"
when line_number in (22:29,50:57) then "Not enrolled in school"
end as enr
,case
when line_number in (4,13,22,32,41,50) then 0304
when line_number in (5,14,23,33,42,51) then 0509
when line_number in (6,15,24,34,43,52) then 1014
when line_number in (7,16,25,35,44,53) then 1517
when line_number in (8,17,26,36,45,54) then 1819
when line_number in (9,18,27,37,46,55) then 2024
when line_number in (10,19,28,38,47,56) then 2534
when line_number in (11,20,29,39,48,57) then 3599
end as age8 length=3
,case when line_number<30 then "M" else "F" end as sex
from B14003_cn5_0 where calculated age8^=.;

create table B14003_ct5_1 as select yr,ct,estimate as est,moe
,case
when line_number in (4:11,32:39) then "Enrolled in public school"
when line_number in (13:20,41:48) then "Enrolled in private school"
when line_number in (22:29,50:57) then "Not enrolled in school"
end as enr
,case
when line_number in (4,13,22,32,41,50) then 0304
when line_number in (5,14,23,33,42,51) then 0509
when line_number in (6,15,24,34,43,52) then 1014
when line_number in (7,16,25,35,44,53) then 1517
when line_number in (8,17,26,36,45,54) then 1819
when line_number in (9,18,27,37,46,55) then 2024
when line_number in (10,19,28,38,47,56) then 2534
when line_number in (11,20,29,39,48,57) then 3599
end as age8 length=3
,case when line_number<30 then "M" else "F" end as sex
from B14003_ct5_0 where calculated age8^=.;
quit;



proc sql;
create table B14005_cn1_1 as select yr,estimate as est,moe
,case
when line_number in (4:6,18:20) then "In School"
when line_number in (9:11,23:25) then "HS Grad"
when line_number in (13:15,27:29) then "Not HS Grad"
end as school_status
,case
when line_number in (4,9,13,18,23,27) then "Employed"
when line_number in (5,10,14,19,24,28) then "Unemployed"
when line_number in (6,11,15,20,25,29) then "Not in LF"
end as lf_status
,case when line_number<16 then "M" else "F" end as sex
from B14005_cn1_0 where calculated school_status^="";

create table B14005_cn5_1 as select yr,estimate as est,moe
,case
when line_number in (4:6,18:20) then "In School"
when line_number in (9:11,23:25) then "HS Grad"
when line_number in (13:15,27:29) then "Not HS Grad"
end as school_status
,case
when line_number in (4,9,13,18,23,27) then "Employed"
when line_number in (5,10,14,19,24,28) then "Unemployed"
when line_number in (6,11,15,20,25,29) then "Not in LF"
end as lf_status
,case when line_number<16 then "M" else "F" end as sex
from B14005_cn5_0 where calculated school_status^="";

create table B14005_ct5_1 as select yr,ct,estimate as est,moe
,case
when line_number in (4:6,18:20) then "In School"
when line_number in (9:11,23:25) then "HS Grad"
when line_number in (13:15,27:29) then "Not HS Grad"
end as school_status
,case
when line_number in (4,9,13,18,23,27) then "Employed"
when line_number in (5,10,14,19,24,28) then "Unemployed"
when line_number in (6,11,15,20,25,29) then "Not in LF"
end as lf_status
,case when line_number<16 then "M" else "F" end as sex
from B14005_ct5_0 where calculated school_status^="";
quit;



proc sql;
create table tab_wstat_cn1_1 as select yr,age13,sex
,case
when ws in ("MIL") then 9
when ws in ("EMP") then 1
when ws in ("UNE") then 3 else 6 end as wstat_new
,sum(est) as est,sum(moe) as moe
from B23001_cn1_1 group by yr,age13,sex,wstat_new;

create table tab_wstat_cn5_1 as select yr,age13,sex
,case
when ws in ("MIL") then 9
when ws in ("EMP") then 1
when ws in ("UNE") then 3 else 6 end as wstat_new
,sum(est) as est,sum(moe) as moe
from B23001_cn5_1 group by yr,age13,sex,wstat_new;

create table tab_wstat_ct5_1 as select yr,ct,age13,sex
,case
when ws in ("MIL") then 9
when ws in ("EMP") then 1
when ws in ("UNE") then 3 else 6 end as wstat_new
,sum(est) as est,sum(moe) as moe
from B23001_ct5_1 group by yr,ct,age13,sex,wstat_new;
quit;


/* weeks and hours worked are combined into a single table */
proc sql;
create table tab_weeks_cn1_1 as select yr,age2,sex
,case
when weeks_worked in ("5052","4849","4047","2739") then 1 else 5 end as weeks_worked_id
,case
when hrs_worked in ("3540") then 35 else 0 end as hours_worked
,put(calculated weeks_worked_id,1.0) || "_" || strip(put(calculated hours_worked,2.0)) as weeks_hours
,sum(est) as est,sum(moe) as moe
from (select * from B23022_cn1_1 union all select * from B23026_cn1_1)
group by yr,age2,sex,weeks_worked_id,hours_worked;

create table tab_weeks_cn5_1 as select yr,age2,sex
,case
when weeks_worked in ("5052","4849","4047","2739") then 1 else 5 end as weeks_worked_id
,case
when hrs_worked in ("3540") then 35 else 0 end as hours_worked
,put(calculated weeks_worked_id,1.0) || "_" || strip(put(calculated hours_worked,2.0)) as weeks_hours
,sum(est) as est,sum(moe) as moe
from (select * from B23022_cn5_1 union all select * from B23026_cn5_1)
group by yr,age2,sex,weeks_worked_id,hours_worked;

create table tab_weeks_ct5_1 as select yr,ct,age2,sex
,case
when weeks_worked in ("5052","4849","4047","2739") then 1 else 5 end as weeks_worked_id
,case
when hrs_worked in ("3540") then 35 else 0 end as hours_worked
,put(calculated weeks_worked_id,1.0) || "_" || strip(put(calculated hours_worked,2.0)) as weeks_hours
,sum(est) as est,sum(moe) as moe
from (select * from B23022_ct5_1 union all select * from B23026_ct5_1)
group by yr,ct,age2,sex,weeks_worked_id,hours_worked;
quit;

/* occupation for civilian population*/
/* there is no military */
proc sql;
create table tab_occupation_cn1_1 as select yr,estimate as est,moe
,case 
when line_number=2 then "White_Collar"
when line_number=3 then "Services"
when line_number=4 then "Sales_Clerical"
when line_number=5 then "Construction"
when line_number=6 then "Production" end as occ
,case
when line_number=2 then "11-1021"
when line_number=3 then "31-1010"
when line_number=4 then "41-1011"
when line_number=5 then "45-1010"
when line_number=6 then "51-1011" end as occsoc5
from C24060_cn1_0 where 2<=line_number<=6
order by yr,occ;

create table tab_occupation_cn5_1 as select yr,estimate as est,moe
,case 
when line_number=2 then "White_Collar"
when line_number=3 then "Services"
when line_number=4 then "Sales_Clerical"
when line_number=5 then "Construction"
when line_number=6 then "Production" end as occ
,case
when line_number=2 then "11-1021"
when line_number=3 then "31-1010"
when line_number=4 then "41-1011"
when line_number=5 then "45-1010"
when line_number=6 then "51-1011" end as occsoc5
from C24060_cn5_0 where 2<=line_number<=6
order by yr,occ;

create table tab_occupation_ct5_1 as select yr,ct,estimate as est,moe
,case 
when line_number=2 then "White_Collar"
when line_number=3 then "Services"
when line_number=4 then "Sales_Clerical"
when line_number=5 then "Construction"
when line_number=6 then "Production" end as occ
,case
when line_number=2 then "11-1021"
when line_number=3 then "31-1010"
when line_number=4 then "41-1011"
when line_number=5 then "45-1010"
when line_number=6 then "51-1011" end as occsoc5
from C24060_ct5_0 where 2<=line_number<=6
order by yr,ct,occ;
quit;

proc sql;
create table tab_edu_cn1_1 as select yr,age5,sex
,case
when edu in ("High school graduate (includes equivalency)","Some college, no degree","Associate's degree") then 9
when edu in ("Bachelor's degree","Graduate or professional degree") then 13
else 1 end as educ_id
,sum(est) as est,sum(moe) as moe
from B15001_cn1_1 group by yr,age5,sex,educ_id;

create table tab_edu_cn5_1 as select yr,age5,sex
,case
when edu in ("High school graduate (includes equivalency)","Some college, no degree","Associate's degree") then 9
when edu in ("Bachelor's degree","Graduate or professional degree") then 13
else 1 end as educ_id
,sum(est) as est,sum(moe) as moe
from B15001_cn5_1 group by yr,age5,sex,educ_id;

create table tab_edu_ct5_1 as select yr,ct,age5,sex
,case
when edu in ("High school graduate (includes equivalency)","Some college, no degree","Associate's degree") then 9
when edu in ("Bachelor's degree","Graduate or professional degree") then 13
else 1 end as educ_id
,sum(est) as est,sum(moe) as moe
from B15001_ct5_1 group by yr,ct,age5,sex,educ_id;
quit;

proc sql;
create table tab_cenr_cn1_1 as select yr,age4,sex
,case
when enr in ("Enrolled in public college or graduate school","Enrolled in private college or graduate school") then 1
else 0 end as enr_college
,sum(est) as est,sum(moe) as moe
from B14004_cn1_1 group by yr,age4,sex,enr_college;

create table tab_cenr_cn5_1 as select yr,age4,sex
,case
when enr in ("Enrolled in public college or graduate school","Enrolled in private college or graduate school") then 1
else 0 end as enr_college
,sum(est) as est,sum(moe) as moe
from B14004_cn5_1 group by yr,age4,sex,enr_college;

create table tab_cenr_ct5_1 as select yr,ct,age4,sex
,case
when enr in ("Enrolled in public college or graduate school","Enrolled in private college or graduate school") then 1
else 0 end as enr_college
,sum(est) as est,sum(moe) as moe
from B14004_ct5_1 group by yr,ct,age4,sex,enr_college;
quit;

proc sql;
create table tab_senr_cn1_1 as select yr,age8,sex
,case
when enr in ("Enrolled in public school","Enrolled in private school") then 1
else 0 end as enr_school
,sum(est) as est,sum(moe) as moe
from B14003_cn1_1 group by yr,age8,sex,enr_school;

create table tab_senr_cn5_1 as select yr,age8,sex
,case
when enr in ("Enrolled in public school","Enrolled in private school") then 1
else 0 end as enr_school
,sum(est) as est,sum(moe) as moe
from B14003_cn5_1 group by yr,age8,sex,enr_school;

create table tab_senr_ct5_1 as select yr,ct,age8,sex
,case
when enr in ("Enrolled in public school","Enrolled in private school") then 1
else 0 end as enr_school
,sum(est) as est,sum(moe) as moe
from B14003_ct5_1 group by yr,ct,age8,sex,enr_school;
quit;


proc sql;
create table tab_senr_cn1_2 as
select age8,sex,f,p_acs
from (select age8,sex,enr_school,est/sum(est) as f,sum(est) as p_acs from tab_senr_cn1_1 group by age8,sex)
where enr_school=1 and age8 not in (0304)
order by age8,sex;

create table tab_senr_cn5_2 as
select age8,sex,f,p_acs
from (select age8,sex,enr_school,est/sum(est) as f,sum(est) as p_acs from tab_senr_cn5_1 group by age8,sex)
where enr_school=1 and age8 not in (0304)
order by age8,sex;

create table tab_senr_ct5_2 as
select ct,age8,sex,f,p_acs
from (select ct,age8,sex,enr_school,est/sum(est) as f,sum(est) as p_acs from tab_senr_ct5_1 group by ct,age8,sex)
where enr_school=1 and age8 not in (0304)
order by ct,age8,sex;

create table tab_senr_cn5_3 as
select age8,enr_school,sum(est) as est from
tab_senr_cn5_1 where age8 not in (0304,.) group by age8,enr_school;

create table tab_senr_cn5_3a as select *,est/sum(est) as f
from tab_senr_cn5_3 group by age8;
quit;



proc sql;
create table tab_cenr_cn1_2 as
select age4,sex,f,p_acs
from (select age4,sex,enr_college,est/sum(est) as f,sum(est) as p_acs from tab_cenr_cn1_1 group by age4,sex)
where enr_college=1
order by age4,sex;

create table tab_cenr_cn5_2 as
select age4,sex,f,p_acs
from (select age4,sex,enr_college,est/sum(est) as f,sum(est) as p_acs from tab_cenr_cn5_1 group by age4,sex)
where enr_college=1
order by age4,sex;

create table tab_cenr_ct5_2 as
select ct,age4,sex,f,p_acs
from (select ct,age4,sex,enr_college,est/sum(est) as f,sum(est) as p_acs from tab_cenr_ct5_1 group by ct,age4,sex)
where enr_college=1
order by ct,age4,sex;

create table tab_cenr_cn5_3 as
select age4,enr_college,sum(est) as est from
tab_cenr_cn5_1 where age4 not in (.) group by age4,enr_college;

create table tab_cenr_cn5_3a as select *,est/sum(est) as f
from tab_cenr_cn5_3 group by age4;
quit;


proc sql;
create table tab_edu_cn1_2 as
select age5,sex,educ_id,est/sum(est) as f,est as p_acs
from tab_edu_cn1_1 group by age5,sex;

create table tab_edu_cn5_2 as
select age5,sex,educ_id,est/sum(est) as f,est as p_acs
from tab_edu_cn5_1 group by age5,sex;

create table tab_edu_ct5_2 as
select ct,age5,sex,educ_id,est/sum(est) as f,est as p_acs
from tab_edu_ct5_1 group by ct,age5,sex;
quit;

/* enrollment, education and work status for ages 16-19 */
proc sql;
create table tab_stat1_cn1_1 as select sex
,case
when school_status="In School" then 1 else 0 end as in_school
,case
when school_status="HS Grad" and lf_status="Employed" then 6
when school_status="HS Grad" and lf_status="Unemployed" then 5
when school_status="HS Grad" and lf_status="Not in LF" then 4
when school_status="Not HS Grad" and lf_status="Employed" then 3
when school_status="Not HS Grad" and lf_status="Unemployed" then 2
when school_status="Not HS Grad" and lf_status="Not in LF" then 1
when school_status="In School" and lf_status="Employed" then 30
when school_status="In School" and lf_status="Unemployed" then 20
when school_status="In School" and lf_status="Not in LF" then 10 end as stat1 length=3
,sum(est) as est,sum(moe) as moe
from B14005_cn1_1 group by sex,in_school,stat1;

/*
1 NILF HS NoGrad
2 Unemployed HS NoGrad
3 Employed HS NoGrad
4 NILF HS Grad
5 Unemployed HS Grad
6 Employed HS Grad

10 NILF student
20 Unemployed student
30 Employed student
*/

create table tab_stat1_cn5_1 as select yr,sex
,case
when school_status="In School" then 1 else 0 end as in_school
,case
when school_status="HS Grad" and lf_status="Employed" then 6
when school_status="HS Grad" and lf_status="Unemployed" then 5
when school_status="HS Grad" and lf_status="Not in LF" then 4
when school_status="Not HS Grad" and lf_status="Employed" then 3
when school_status="Not HS Grad" and lf_status="Unemployed" then 2
when school_status="Not HS Grad" and lf_status="Not in LF" then 1
when school_status="In School" and lf_status="Employed" then 30
when school_status="In School" and lf_status="Unemployed" then 20
when school_status="In School" and lf_status="Not in LF" then 10 end as stat1 length=3
,sum(est) as est,sum(moe) as moe
from B14005_cn5_1 group by sex,in_school,stat1;

create table tab_stat1_ct5_1 as select ct,sex
,case
when school_status="In School" then 1 else 0 end as in_school
,case
when school_status="HS Grad" and lf_status="Employed" then 6
when school_status="HS Grad" and lf_status="Unemployed" then 5
when school_status="HS Grad" and lf_status="Not in LF" then 4
when school_status="Not HS Grad" and lf_status="Employed" then 3
when school_status="Not HS Grad" and lf_status="Unemployed" then 2
when school_status="Not HS Grad" and lf_status="Not in LF" then 1
when school_status="In School" and lf_status="Employed" then 30
when school_status="In School" and lf_status="Unemployed" then 20
when school_status="In School" and lf_status="Not in LF" then 10 end as stat1 length=3
,sum(est) as est,sum(moe) as moe
from B14005_ct5_1 group by sex,ct,in_school,stat1;
quit;

proc sql;
create table tab_stat1_cn1_2 as
select sex,in_school,stat1,est/sum(est) as f,est as p_acs
from tab_stat1_cn1_1 group by sex,in_school;

create table tab_stat1_cn5_2 as
select sex,in_school,stat1,est/sum(est) as f,est as p_acs
from tab_stat1_cn5_1 group by sex,in_school;

create table tab_stat1_ct5_2 as
select ct,sex,in_school,stat1,est/sum(est) as f,est as p_acs
from tab_stat1_ct5_1 group by ct,sex,in_school;
quit;


proc sql;
create table tab_wstat_cn1_2 as
select sex,age13,wstat_new,est/sum(est) as f,est as p_acs
from tab_wstat_cn1_1 group by sex,age13;

create table tab_wstat_cn5_2 as
select sex,age13,wstat_new,est/sum(est) as f,est as p_acs
from tab_wstat_cn5_1 group by sex,age13;

create table tab_wstat_ct5_2 as
select ct,sex,age13,wstat_new,est/sum(est) as f,est as p_acs
from tab_wstat_ct5_1 group by ct,sex,age13;
quit;

/*------------------------*/
