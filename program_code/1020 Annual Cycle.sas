%macro forecast (yr=);

PROC DATASETS LIB=work Nolist MEMTYPE=data kill;
RUN; QUIT;

/* %let yr=2022; */
%let yrn=%eval(&yr + 1);

%include "T:\socioec\Current_Projects\&xver\program_code\1021 Program Part 1 (Opening).sas";

%include "T:\socioec\Current_Projects\&xver\program_code\1021a Birth Assignment.sas";

%include "T:\socioec\Current_Projects\&xver\program_code\1022 Program Part 2 (Births Deaths Demolitions).sas";

%include "T:\socioec\Current_Projects\&xver\program_code\1023 NEW Program Part 3 (Add Remove HH and HP).sas";

%include "T:\socioec\Current_Projects\&xver\program_code\1023a Macro--Jurswap.sas";
%jurswap (m=100); /* m controls maximum iterations */

%include "T:\socioec\Current_Projects\&xver\program_code\1024 NEW Program Part 4 (Closing).sas";

%mend forecast;

