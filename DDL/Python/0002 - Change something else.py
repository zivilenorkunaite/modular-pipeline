def apply_changes(spark, database_name):
  print('Change 002')
  spark.sql(f"insert into {database_name}.change_0001 values (5,5,'five')")
  print('All done')