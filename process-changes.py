# Databricks notebook source
import helpers.changes as ch

# COMMAND ----------

# DBTITLE 1,Keep changes logged in a selected table so that they are only processed once
changes_db = 'default'
changes_table = 'ddl_changes'

ch.trackChangesInit(spark, changes_db, changes_table)

# COMMAND ----------

# DBTITLE 1,Process DDL changes created as SQL code
## Take all SQL files with changes
all_changes = ch.getChangeSQLFiles('DDL/SQL/')

## Get files that have already been processed
processed_changes = spark.sql(f"select distinct file_processed from {changes_db}.{changes_table} where status = 'DONE'")

## Keep new change files only
new_changes =  [fl for fl in all_changes if fl not in [row.file_processed for row in processed_changes.collect()]]

for change_file in sorted(new_changes, key=str.lower):
  print(f"File to be executed: {change_file}")
  try:
    with open(change_file) as queryFile:
      queryText = queryFile.read()
      # note that there is no parsing of SQL here - all gets executed as-is
      results = spark.sql(queryText)
    ch.logChangeSuccess(spark, change_file, changes_db, changes_table)
  except Exception as e:
    print(f'error found: {e}')
    ch.logChangeFailure(spark, "ERROR", change_file, changes_db, changes_table)
    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC In the Python example below each change script must have `apply_changes()` function with 2 paramters:
# MAGIC 1. spark 
# MAGIC 2. database name 
# MAGIC 
# MAGIC This could be amended to provide more flexibility (e.g. by adding catalog to work with UC)

# COMMAND ----------

current_database_name = 'default'

# COMMAND ----------

# DBTITLE 1,Process DDL changes created as Python code
import importlib

## Take all Python files with changes
all_changes = ch.getChangePythonFiles('DDL/Python/')

## Get files that have already been processed
processed_changes = spark.sql(f"select distinct file_processed from {changes_db}.{changes_table} where status = 'DONE'")

## Keep new change files only
new_changes =  [fl for fl in all_changes if fl not in [row.file_processed for row in processed_changes.collect()]]

for change_file in sorted(new_changes, key=str.lower):
  print(f"File to be executed: {change_file}")
  try:
    # remove last 3 chars from filename (.py) and replace folder seperators by dots for library naming conventions
    changes_lib = importlib.import_module(change_file[:-3].replace('/','.'))
    # reload library in just in case
    importlib.reload(changes_lib)
    # run pre-defined function - has to be same for all DDL python files
    # pass spark as a parameter for not having to define it again
    changes_lib.apply_changes(spark, current_database_name)
    # log run as successful as long as there were no errors
    ch.logChangeSuccess(spark, change_file, changes_db, changes_table)
  except Exception as e:
    print(f'error found: {e}')
    # log run as failed - more error details could be good to capture here
    ch.logChangeFailure(spark, "ERROR", change_file, changes_db, changes_table) 


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from default.ddl_changes
