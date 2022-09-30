import glob
def getChangePythonFiles(change_folder = 'DDL/'):
  return glob.glob(f"{change_folder}*.py")

def getChangeSQLFiles(change_folder = 'DDL/'):
  return glob.glob(f"{change_folder}*.sql")

def trackChangesInit(spark, database = "default", table_name = "ddl_changes"):
  spark.sql(f"CREATE SCHEMA IF NOT EXISTS {database}")
  spark.sql(f"""CREATE TABLE IF NOT EXISTS {database}.{table_name} (file_processed string, status string)""")
  
def logChangeSuccess(spark, filename, database = "default", table_name = "ddl_changes"):
  spark.sql(f"""INSERT INTO TABLE {database}.{table_name} VALUES ('{filename}', 'DONE') """)
  
def logChangeFailure(spark, error_message, filename, database = "default", table_name = "ddl_changes"):
  spark.sql(f"""INSERT INTO TABLE {database}.{table_name} VALUES ('{filename}', '{error_message}') """)