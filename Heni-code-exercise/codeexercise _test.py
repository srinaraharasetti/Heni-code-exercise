# Databricks notebook source
from runtime.nutterfixture import NutterFixture, tag
class MyTestFixture(NutterFixture):
   def run_test_name(self):
      dbutils.notebook.run('code exercise', 600)

   def assertion_test_name(self):
      some_tbl = sqlContext.sql('SELECT COUNT (*) as total  FROM blockchain_delta')
      first_row = some_tbl.first()
      assert (first_row[0] == 75773)

result = MyTestFixture().execute_tests()
print(result.to_string())

##result.exit(dbutils)

# COMMAND ----------

