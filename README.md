# event_cleansing_stage

## Description

This is an example data prep and cleansing project. This 
The data files to cleanse are in JSONL format and are stored in s3.

I've chosen to implement this as an aws lambda function, triggered from the ObjectCreated:Put event raised by s3 on file upload / creation.

As the files are of arbitrary size, I've opted to parse these line by line rather than loading the files into memory. 
This could be sped up by multithreading or by splitting the files into smaller batches.

The cleansed files are uploaded to s3:/darrentighe-test/cleansed/ in csv format. 
CSVs in s3 can be uploaded to redshift using
```
COPY table_name
FROM 's3://<your-bucket-name>/load/file_name.csv'
```

Any json lines which could not be parsed are saved in s3:/darrentighe-test/broken/ along with the reason they could not be parsed. 
Saving them in this way prevents the rows from being lost. The rows can be manually fixed or additional data cleansing can be added to the process. The unprocessed rows can then be placed in the original bucket to re-trigger processing.

## Running as a local app

install dependencies
```
python3 -m pip install -r cleansing_stage/requirements.txt
```

run test entry point
```
python3 ./local_test_entrypoint.py --output_bucket <BUCKET_NAME>
```

## Running as a lambda

Building 
```
docker build -t event_cleansing_stage ./cleansing_stage/
```

upload to ecr
```
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin <YOUR ECR ADDRESS>.dkr.ecr.us-east-1.amazonaws.com
```

push image to ecr
```
docker tag  event_cleansing_stage:latest <YOUR ECR ADDRESS>.dkr.ecr.us-east-1.amazonaws.com/hello-world:latest
docker push <YOUR ECR ADDRESS>.dkr.ecr.us-east-1.amazonaws.com/event_cleansing_stage:latest        
```

