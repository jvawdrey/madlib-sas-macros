/*
%madlib_sas_score_logreg
MADlib logregr_predict function caller for in-database scoring

Contact
jvawdrey@pivotal.io

Input
* Output from %madlib_sas_publish_logreg MACRO

Arguments
* inTable: Database table containing data point to apply model to (score)
* outTable: Output table ... will contain all columns of inTable plus predict
* modelTable: Output database table from %madlib_sas_publish_logreg MACRO
* server: database address (e.g. '192.0.0.1')
* db: database name
* port: database port
* user: database username
* password: database password
* predictColumnName: column name of predictions in outTable (Defaults to "predict")
* drop: drop any existing database table with same name as modelTable (MACRO
  will fail if modeTable exists and you do not drop first

Example
* Run model scoring macro;
%madlib_sas_score_logreg(
  inTable=&schema..cars
 ,outTable=&schema..cars_scored_lgrm
 ,modelTable=&schema..db_bmw_mb_flag_lgrm
 ,server=&server
 ,db=&database
 ,port=&port
 ,user=&user
 ,password=&password
 ,predictColumnName=predict
 ,drop=1
);

* View results;
proc print data=mydblib.cars_scored_lgrm;
run;

*/
%macro madlib_sas_score_logreg(inTable,outTable,modelTable,server,db,port,user,password,predictColumnName,drop);

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

    * Add default column name if predictColumnName not given;
    %if %length(&predictColumnName.)=0 %then %do;
      %let predictColumnName=predict;
	  %end;

    * Create output table with additional predicted;
    EXECUTE (
      CREATE TABLE &outTable. AS
      SELECT &inTable..*
            ,madlib.logregr_predict_prob(m.coef,ARRAY[&vars.]) AS &predictColumnName.
      FROM &inTable., &modelTable. m, (SELECT 1 AS intercept) i
      DISTRIBUTED RANDOMLY
    ) BY msplr_gpcon;

    * Close open connections;
    DISCONNECT FROM msplr_gpcon;
  quit;

%mend madlib_sas_score_logreg;
