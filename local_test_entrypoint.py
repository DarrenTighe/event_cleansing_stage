#!/usr/bin/env python3
from cleansing_stage.cleansing_stage import CleansingStage
from cleansing_stage.data_reader import FileReader, S3Reader
from cleansing_stage.data_writer import ConsoleWriter, S3Writer
import argparse

# Providing this entrypoint so the project can be tested without building docker image,
# deploying as a lambda, wiring up to s3 events and retriggering the ObjectCreated:Put event.

# to get a copy of the data run cleansing_stage/test/get_data_from_bucket.sh 

bucket = 'dataeng-challenge'
testfiles=[
    '2020/01/01/0f6e0555e9f34063b711a676d13e38f9',
    '2020/01/01/564a546234b6483595cea2835608a112',
    '2020/01/01/24276288f3be4becb4af2558cb03a25e',
    '2020/01/01/b8bacbbdd4c24a14bf1e98a0c1defac8'
    ]

parser = argparse.ArgumentParser(description='Cleanse Data')
parser.add_argument('--output_bucket', type=str, default='darrentighe-test')
args = parser.parse_args()
output_bucket = args.output_bucket

for f in testfiles:
    print(f"processing {f}") 
    reader = S3Reader(bucket, f'8uht6u8bh/events/{f}.json')
    #reader = FileReader(f'cleansing_stage/test/{testdata}/{f}.json')
    clean_output = S3Writer(f's3://{output_bucket}/cleaned/{f}.csv')
    broken_output = S3Writer(f's3://{output_bucket}/broken/{f}.csv')

    cleansing_stage = CleansingStage(reader, clean_output, broken_output)
    cleansing_stage.cleanse_data()

