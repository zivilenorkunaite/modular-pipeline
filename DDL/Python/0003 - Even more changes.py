def apply_changes(spark, database_name):
  df =  spark.table(f'{database_name}.ddl_changes')
  changes_logged = df.count()
  print(f"{changes_logged} have been logged at this stage.")
  # raise error
  print(1/0)