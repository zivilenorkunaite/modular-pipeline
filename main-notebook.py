# Databricks notebook source
import helpers.exports as e
import helpers.imports as i
import helpers.audit as aud

# COMMAND ----------

aud.log_process2()

# COMMAND ----------

aud.log_process("n/a","introduce_exports")
print(e.introduce_exports("Zivile"))

# COMMAND ----------

aud.log_process("n/a","import_some_data")
print(i.import_some_data())

# COMMAND ----------

variable = dbutils.secrets.get("zivile-scope","slack-url")
print(variable)

# COMMAND ----------

import helpers.secrets as ssshh

# COMMAND ----------

print(ssshh.get_my_secrets('slack-url'))

# COMMAND ----------

print(ssshh.get_my_secrets2(spark,'slack-url'))

# COMMAND ----------

print(ssshh.get_my_secrets3(dbutils,'slack-url'))
