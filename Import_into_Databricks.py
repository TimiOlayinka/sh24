import pandas as pd 
import numpy as np
from pyspark.sql import SparkSession
import boto3
from io import BytesIO
import openpyxl

# AWS Credentials
ACCESS_KEY = spark.conf.get("spark.aws.aws_access_key_id")
SECRET_KEY = spark.conf.get("spark.aws.aws_secret_access_key")

# Initialize boto3 client
s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)

# Get Excel file from S3
obj = s3_client.get_object(Bucket="databricks-workspace-stack-1a438-metastore-bucket", Key="data_eng_interview_task_data.xlsx")
data = obj['Body'].read()

# Sheets based on Gender, Sexual Orientation and Age Group
a_chlamydia = pd.read_excel(BytesIO(data), sheet_name = 'a_Chlamydia', skiprows=3, engine='openpyxl')
c_Gonorrhoea = pd.read_excel(BytesIO(data), sheet_name = 'c_Gonorrhoea', skiprows=3, engine='openpyxl')
e_Syphilis = pd.read_excel(BytesIO(data), sheet_name = 'e_Syphilis', skiprows=3, engine='openpyxl')

# Sheets based on Ethics Groups
b_chlamydia = pd.read_excel(BytesIO(data), sheet_name = 'b_Chlamydia', skiprows=3, engine='openpyxl')
d_Gonorrhoea = pd.read_excel(BytesIO(data), sheet_name = 'd_Gonorrhoea', skiprows=3, engine='openpyxl')
f_Syphilis = pd.read_excel(BytesIO(data), sheet_name = 'f_Syphilis', skiprows=3, engine='openpyxl')

# Population Data
g_Population_data = pd.read_excel(BytesIO(data), sheet_name = 'g_Population_data', skiprows=5, engine='openpyxl')
h_Population_data = pd.read_excel(BytesIO(data), sheet_name = 'h_Population_data', skiprows=5, engine='openpyxl')

# Gender, Sexual Orientation and Age Group
# Data Cleaning
column_names = list(a_chlamydia.columns)
column_names[3] = 'Age Group'

columns = []
for name in column_names:
    name = name.replace(' ', '_')
    columns.append(name)

# Update column names for each dataframe
a_chlamydia.columns = columns
c_Gonorrhoea.columns = columns
e_Syphilis.columns = columns

combined_df = pd.concat([a_chlamydia, c_Gonorrhoea, e_Syphilis], ignore_index=True)

# Replace [c], [x1], and [x] with NaN for the specified years
years = ['2018', '2019', '2020', '2021', '2022']
for year in years:
    combined_df[year] = combined_df[year].replace(['[c]', '[x1]', '[x]'], np.nan)

spark = SparkSession.builder.appName("PandasToSpark").getOrCreate()
sdf = spark.createDataFrame(combined_df)

spark.sql("CREATE SCHEMA IF NOT EXISTS sh24.staging")

table_name = "sh24.staging.GOA_Demographic_Line"
sdf.write.mode("overwrite").saveAsTable(table_name)

# Ethnic Groups
column_names_ethnics = list(d_Gonorrhoea.columns)

# Update column names for each dataframe
# Data Cleaning
columns_ethnics = []
for name in column_names_ethnics:
    name = name.replace(' ', '_')
    columns_ethnics.append(name)

b_chlamydia.columns = columns_ethnics
d_Gonorrhoea.columns = columns_ethnics
f_Syphilis.columns = columns_ethnics

combined_df_ethnics = pd.concat([b_chlamydia, d_Gonorrhoea, f_Syphilis], ignore_index=False)

sdf_ethnics = spark.createDataFrame(combined_df_ethnics)

ethnics_table_name = "sh24.staging.Ethnics_Demographic_Line"
sdf_ethnics.write.mode("overwrite").saveAsTable(ethnics_table_name)

# Population Data Clean
def clean_column_names(df):
    df.columns = [col.replace(' ', '_') for col in df.columns]
    return df
    
g_Population_data = clean_column_names(g_Population_data)
h_Population_data = clean_column_names(h_Population_data)


for year in years:
    g_Population_data[year] = g_Population_data[year].replace(['[c]', '[x1]', '[x]'], np.nan)
    h_Population_data[year] = h_Population_data[year].replace(['[c]', '[x1]', '[x]'], np.nan)

sdf_g_pop = spark.createDataFrame(g_Population_data)
sdf_h_pop = spark.createDataFrame(h_Population_data)

g_table_name = "sh24.staging.GOA_Population_Line"
sdf_g_pop.write.mode("overwrite").saveAsTable(g_table_name)

h_table_name = "sh24.staging.Ethnics_Population_Line"
sdf_h_pop.write.mode("overwrite").saveAsTable(h_table_name)
