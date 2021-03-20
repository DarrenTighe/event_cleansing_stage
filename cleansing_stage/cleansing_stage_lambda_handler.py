import pandas as pd
import boto3
import os

from .data_reader import S3Reader
from .cleansing_stage import CleansingStage


def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        print(f'{bucket} {key}')

        reader = S3Reader(bucket, key)

        cleansing_stage = CleansingStage(reader)
        
        cleansing_stage.cleanse_data()
        
