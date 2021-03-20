#!/usr/bin/env python3
import pandas as pd
import boto3
import os

from .data_reader import S3Reader
from .cleansing_stage import CleansingStage
from .data_writer import S3Writer

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        
        output_bucket=getconfig('OUTPUT_BUCKET')

        reader = S3Reader(bucket, key)
        cleaned_writer = S3Writer(f's3://{output_bucket}/cleaned/{key}.csv')
        broken_writer = S3Writer(f's3://{output_bucket}/cleaned/{key}.csv')

        cleansing_stage = CleansingStage(reader, cleaned_writer, broken_writer)
        cleansing_stage.cleanse_data()
        
        
def getconfig(cfg_name):
    # This could be done with an ini file instead. 
    defaults = {
        'OUTPUT_BUCKET' : 'darrentighe-test'
    }
    return os.environ.get(cfg_name, defaults.get(cfg_name))