import boto3
import pandas as pd

s3 = boto3.client('s3')

# Specify the S3 bucket and file key
bucket_name = 'salaries-data-lake'
file_key = 'raw-data/salaries.csv'

# Download the data from S3
response = s3.get_object(Bucket=bucket_name, Key=file_key)
data = response["Body"]
df = pd.read_csv(data)

# Process the data here
df_results = df.groupby(["work_year", "job_title"]).agg({"salary_in_usd": "mean", "remote_ratio": "mean"}).reset_index()

# Save the processed DataFrame as a CSV to a BytesIO object
result_data = df_results.to_csv(index=False)

# Specify the S3 destination for the processed data
result_key = 'resultados/results.csv'

# Upload the processed data to S3
s3.put_object(Bucket=bucket_name, Key=result_key, Body=result_data)

print("Data processed and saved to S3")