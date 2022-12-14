def get_my_secrets(key_name):
  return dbutils.secrets.get("zivile-scope",key_name)

def get_my_secrets2(spark, key_name):
  return dbutils.secrets.get("zivile-scope",key_name)

def get_my_secrets3(dbutils, key_name):
  return dbutils.secrets.get("zivile-scope",key_name)
