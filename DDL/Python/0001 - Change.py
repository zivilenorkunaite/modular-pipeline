def apply_changes(spark, database_name):
  print('This will be a change done via python file')

  print('all variables for this script are defined in change script itself')

  spark.sql(f"create table if not exists {database_name}.change_0001 (id int, some_col int, some_other_col string)")

  spark.sql(f"insert into {database_name}.change_0001 values (3, 3, 'run four')")