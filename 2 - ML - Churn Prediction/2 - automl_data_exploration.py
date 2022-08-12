# Databricks notebook source
# MAGIC %md
# MAGIC # Data Exploration
# MAGIC This notebook performs exploratory data analysis on the dataset.
# MAGIC To expand on the analysis, attach this notebook to the **isaac-cluster** cluster,
# MAGIC edit [the options of pandas-profiling](https://pandas-profiling.ydata.ai/docs/master/rtd/pages/advanced_usage.html), and rerun it.
# MAGIC - Explore completed trials in the [MLflow experiment](#mlflow/experiments/2916963709048647/s?orderByKey=metrics.%60val_roc_auc_score%60&orderByAsc=false)
# MAGIC - Navigate to the parent notebook [here](#notebook/2916963709048648) (If you launched the AutoML experiment using the Experiments UI, this link isn't very useful.)
# MAGIC 
# MAGIC Runtime Version: _11.1.x-cpu-ml-scala2.12_

# COMMAND ----------

import os
import uuid
import shutil
import pandas as pd
import databricks.automl_runtime

from mlflow.tracking import MlflowClient

# Download input data from mlflow into a pandas DataFrame
# Create temporary directory to download data
temp_dir = os.path.join(os.environ["SPARK_LOCAL_DIRS"], "tmp", str(uuid.uuid4())[:8])
os.makedirs(temp_dir)

# Download the artifact and read it
client = MlflowClient()
training_data_path = client.download_artifacts("65f13e3f2d12452cab78d5aae3e52b36", "data", temp_dir)
df = pd.read_parquet(os.path.join(training_data_path, "training_data"))

# Delete the temporary data
shutil.rmtree(temp_dir)

target_col = "churn"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Semantic Type Detection Alerts
# MAGIC 
# MAGIC For details about the definition of the semantic types and how to override the detection, see
# MAGIC [Databricks documentation on semantic type detection](https://docs.databricks.com/applications/machine-learning/automl.html#semantic-type-detection).
# MAGIC 
# MAGIC - Semantic type `categorical` detected for columns `Ankyo`, `Apson`, `Conan`, `Elpine`, `Karsair`, `Mannheiser`, `Mogitech`, `Mowepro`, `Olitscreens`, `Opple`, `Ramsung`, `Reagate`, `Rony`, `Sioneer`, `Zamaha`, `loyalty_segment`. Training notebooks will encode features based on categorical transformations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Profiling Results

# COMMAND ----------

from pandas_profiling import ProfileReport
df_profile = ProfileReport(df, title="Profiling Report", progress_bar=False, infer_dtypes=False)
profile_html = df_profile.to_html()

displayHTML(profile_html)
