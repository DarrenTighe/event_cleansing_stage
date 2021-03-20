#!/usr/bin/env python3
import codecs
import boto3

class DataReader(object):
    def read_lines(self):
        pass


class FileReader(DataReader):
    def __init__(self, filename: str):
        self.filename = filename

    def read_lines(self):
        for line in open(self.filename, 'r'):
            yield line

class StringReader(DataReader):
    def __init__(self, data:str):
        self.data = data

    def read_lines(self):
        for line in self.data.splitlines():
            yield line


class S3Reader(DataReader):
    def __init__(self, bucket:str, key:str):
        s3 = boto3.resource('s3')
        self.s3object = s3.Object(bucket, key)

    def read_lines(self):
        line_stream = codecs.getreader("utf8")
        for line in line_stream(self.s3object.get()['Body']):
            yield line
        




