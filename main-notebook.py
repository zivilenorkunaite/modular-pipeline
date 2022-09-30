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
