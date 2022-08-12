# Databricks notebook source
import mlflow
import databricks.automl_runtime
from pyspark.sql.functions import *

# COMMAND ----------

automl_mlflow = "/Users/isaac.gritz+ucuc@databricks.com/databricks_automl/churn_hive_metastore.dbx_dbt_retail.churn_prediction_4"

automl_expId = mlflow.get_experiment_by_name(automl_mlflow).experiment_id

automl_mlflow_df = spark.read.format("mlflow-experiment").load(automl_expId)

refined_automl_mlflow_df = automl_mlflow_df.select(col('run_id'), col("experiment_id"), explode(map_concat(col("metrics"), col("params"))), col('start_time'), col("end_time")) \
                .filter("key != 'model'") \
                .select("run_id", "experiment_id", "key", col("value").cast("float"), col('start_time'), col("end_time")) \
                .groupBy("run_id", "experiment_id", "start_time", "end_time") \
                .pivot("key") \
                .sum("value") \
                .withColumn("trainingDuration", col("end_time").cast("integer")-col("start_time").cast("integer")) # example of added column

# COMMAND ----------

refined_automl_mlflow_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"hive_metastore.dbx_dbt_retail.automl_data_bronze")
