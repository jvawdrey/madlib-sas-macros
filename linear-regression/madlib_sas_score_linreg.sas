/*
%madlib_sas_score_linreg
MADlib linregr_predict function caller for in-database scoring

Contact
jvawdrey@pivotal.io

Input
* Output from %madlib_sas_publish_linreg MACRO

Arguments
* inTable: Database table containing data point to apply model to (score)
* outTable: Output table ... will contain all columns of inTable plus predict and optional residuals columns
* modelTable: Output database table from %madlib_sas_publish_lr MACRO
* server: database address (e.g. '192.0.0.1')
* db: database name
* port: database port
* user: database username
* password: database password
* predictColumnName: column name of predictions in outTable (Defaults to "predict")
* residualColumnName: column name of residuals (“observed” – “predicted”) in outTable
  * Requires observed values column of same name as dependent in original SAS model (dependent column in modelTable)
  * If argument is left null then this column will be left out of resulting outTable
* drop: drop any existing database table with same name as modelTable (MACRO
  will fail if modeTable exists and you do not drop first

Example
* Run model scoring macro;
%madlib_sas_score_linreg(
  inTable=&schema..cars
 ,outTable=&schema..cars_scored
 ,modelTable=&schema..db_mpg_highway_lrm
 ,server=&server
 ,db=&database
 ,port=&port
 ,user=&user
 ,password=&password
 ,predictColumnName=predict
 ,residualColumnName=err
 ,drop=1
);
*/

%macro madlib_sas_score_linreg(inTable,outTable,modelTable,server,db,port,user,password,predictColumnName,residualColumnName,drop);

  * Score data using published model in-database;
  proc sql noprint;
    * SQL Procedure Pass-Through Facility;
    CONNECT TO &engine. AS msplr_gpcon (server=&server. db=&database. port=&port. user=&user. password=&password.);

    * grab column names;
    SELECT variables into :vars separated by ","
    FROM connection to msplr_gpcon (
      SELECT unnest(variables) AS variables
	          ,unnest(variables_order) AS variables_order
	    FROM &modelTable.
    ) ORDER BY variables_order;

    * grab dependent name;
    SELECT dependent into :dep
    FROM connection to msplr_gpcon (
      SELECT dependent
	    FROM &modelTable.
    );


    * If user requested then drop existing table;
    * Else check if exists and exit if does;
    %if (%trim(&drop.)=1) %then
      %do;
        EXECUTE (
          DROP TABLE IF EXISTS &outTable.;
	      ) BY msplr_gpcon;
	    %end;
    %else
      %do;

	      SELECT count(*) INTO :cnt
  	    FROM connection to msplr_gpcon (
          SELECT *
	        FROM &outTable.
        );

        %if (%datatyp(&cnt.)=NUMERIC and %eval(&cnt.)>0) %then %do;
          %put ERROR: Database table &outTable. already exists;
          %put ERROR: Try changing outTable name or setting argument drop equal to 1 and drop existing table;
          %put ERROR: Exiting MACRO!;
          %abort;
	    %end;
	  %end;

    * Set a flag to add residuals column (1) or not (0);
    %let res=0;
    %if %length(&residualColumnName.)>0 %then %do;
      %let res=1;
	  %end;

    * Add default column name if predictColumnName not given;
    %if %length(&predictColumnName.)=0 %then %do;
      %let predictColumnName=predict;
	  %end;

    * Create output table with additional predicted and error columns;
    EXECUTE (
      CREATE TABLE &outTable. AS
      SELECT &inTable..*
            ,madlib.linregr_predict(ARRAY[&vars.],m.coef) AS &predictColumnName.
		    %if (%eval(&res.)=1) %then %do;
            	,&dep. - madlib.linregr_predict(ARRAY[&vars.],m.coef) AS &residualColumnName.
		    %end;
      FROM &inTable., &modelTable. m, (SELECT 1 AS intercept) i
      DISTRIBUTED RANDOMLY
    ) BY msplr_gpcon;

    * Close open connections;
    DISCONNECT FROM msplr_gpcon;
  quit;

%mend madlib_sas_score_linreg;
