/*
%madlib_sas_publish_logreg
This MACRO will publish SAS logistic regression models
to Greenplum/HAWQ/PostgreSQL database.

Contact
jvawdrey@pivotal.io

Input
* ParameterEstimates output of PROC logistic

Arguments
* modelDataset: SAS dataset contains parameterEstimates from PROC logistic
* modeTable: Output database table. Contains columns
	* coef double precision[] - coeficients (ordered by variable_order)
	* variables text[] - variables (ordered by variable_order)
	* variables_order integer[] - order of variables
* server: database address (e.g. '192.0.0.1')
* db: database name
* port: database port
* user: database username
* password: database password
* drop: drop any existing database table with same name as modelTable (MACRO
  will fail if modeTable exists and you do not drop first

Example
* Establish connection to database;
libname mydblib &engine. server=&server. port=&port. database=&database. user=&user. password=&password. schema=&schema.;

* Prep data for logistic - create binary column (BMW or Mercedes-Benz then yes) from make column;
proc sql;
  CREATE TABLE work.cars_binary AS
  SELECT *
        ,CASE WHEN make IN ('BMW','Mercedes-Benz') THEN 1 ELSE 0 END AS bmw_mb_flag
  FROM sashelp.cars;
quit;

* Logistic regression model predicting whether car is BMW or Mercedes-Benz;
proc logistic data=work.cars_binary;
  model bmw_mb_flag (event='1') = msrp enginesize cylinders weight;
  ods output parameterEstimates = sas_bmw_mb_flag_lgrm;
run;
quit;

* Run model publishing macro;
%madlib_sas_publish_logreg(
  modelDataset=sas_bmw_mb_flag_lgrm
 ,modelTable=&schema..db_bmw_mb_flag_lgrm
 ,server=&server
 ,db=&database
 ,port=&port
 ,user=&user
 ,password=&password
 ,drop=1
);

*/

%macro madlib_sas_publish_logreg(modelDataset,modelTable,server,db,port,user,password,drop);

  proc sql noprint;
    * Grab estimates from SAS model table;
    SELECT variable
          ,estimate
            INTO :vars separated by "','"
                ,:ests separated by ','
    FROM &modelDataset.;

	  * Capture total number of distinct variables (includes intercept);
    %let nvars=&sqlobs;
  quit;

  * Build a string which holds order of vars in database;
  %let str=1;
  %do i=2 %to &nvars;
    %let str=&str.,&i.;
  %end;

  * Vars is split by ',' ... we now need to add single quotes to each end of string;
  %let vars=%str(%')&vars.%str(%');

  * Publish model to database;
  proc sql noprint;
    * SQL procedure pass-through facility connection;
    CONNECT TO &engine. AS msplr_gpcon (server=&server. db=&db. port=&port. user=&user. password=&password.);

	  * If user requested then drop existing table;
	  * Else check if exists and exit if does;
    %if (%trim(&drop.)=1) %then
      %do;
	      EXECUTE (
	        DROP TABLE IF EXISTS &modelTable.;
	      ) BY msplr_gpcon;
      %end;
    %else
	    %do;

	      SELECT count(*) INTO :cnt
  	    FROM connection to msplr_gpcon (
          SELECT *
	        FROM &modelTable.
        );

        %if (%datatyp(&cnt.)=NUMERIC and %eval(&cnt.)>0) %then %do;
	        %put ERROR: Database table &modelTable. already exists;
          %put ERROR: Try changing modelTable name or setting argument drop equal to 1 and drop existing table;
          %put ERROR: Exiting MACRO!;
          %abort;
	    %end;
	  %end;

    * Add model table to database;
    EXECUTE (
      CREATE TABLE &modelTable. (
         coef double precision[]
        ,variables text[]
	      ,variables_order integer[]
      ) DISTRIBUTED RANDOMLY
    ) BY msplr_gpcon;

	  * Insert model data;
    EXECUTE (
      INSERT INTO &modelTable. VALUES (
         array[&ests.]
	      ,array[&vars.]
	      ,array[&str.]
      )
    ) BY msplr_gpcon;

    * Close open connections;
    DISCONNECT FROM msplr_gpcon;
  quit;

%mend madlib_sas_publish_logreg;
