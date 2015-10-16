/*
%madlib_sas_publish_linreg
This MACRO will publish SAS linear regression models
to Greenplum/HAWQ/PostgreSQL database.

Contact
jvawdrey@pivotal.io

Input
* ParameterEstimates output of PROC reg

Arguments
* modelDataset: SAS dataset contains parameterEstimates from PROC reg
* modeTable: Output database table. Contains columns
	* coef double precision[] - coeficients (ordered by variable_order)
	* variables text[] - variables (ordered by variable_order)
	* variables_order integer[] - order of variables
	* dependent text - name of dependent column
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

* Linear regression model predicting mpg_highway;
proc reg data=mydblib.cars;
  model mpg_highway=Horsepower Weight EngineSize;
  * Output parameter estimates to a dataset;
  ods output parameterEstimates = sas_mpg_highway_lrm;
run;
quit;

* Run model publishing macro;
%madlib_sas_publish_linreg(
  modelDataset=sas_mpg_highway_lrm
 ,modelTable=&schema..db_mpg_highway_lrm
 ,server=&server
 ,db=&database
 ,port=&port
 ,user=&user
 ,password=&password
 ,drop=1
);

*/
%macro madlib_sas_publish_linreg(modelDataset,modelTable,server,db,port,user,password,drop);

  proc sql noprint;
    * Grab estimates from SAS model table;
    SELECT variable
          ,estimate
          ,dependent
            INTO :vars separated by "','"
                ,:ests separated by ','
                ,:dep separated by '|'
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

  * Add single quotes around dependent name for text string input to database;
  %let dep=%str(%')%scan(&dep., 1, |)%str(%');

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
	      ,dependent text
      ) DISTRIBUTED RANDOMLY
    ) BY msplr_gpcon;

	  * Insert model data;
    EXECUTE (
      INSERT INTO &modelTable. VALUES (
         array[&ests.]
	      ,array[&vars.]
	      ,array[&str.]
	      ,&dep.
      )
    ) BY msplr_gpcon;

    * Close open connections;
    DISCONNECT FROM msplr_gpcon;
  quit;

%mend madlib_sas_publish_linreg;
