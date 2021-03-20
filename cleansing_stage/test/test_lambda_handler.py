import unittest

from ..cleansing_stage_lambda_handler import lambda_handler
from ..cleansing_stage import CleansingStage

class lambda_handler_test(unittest.TestCase):
    
    def setUp(self):
        self.bucket='dataeng-challenge'
        self.sample_file='8uht6u8bh/events/2020/01/01/0f6e0555e9f34063b711a676d13e38f9.json'
        self.sample_s3_event = {
        "Records":[
            {
            "eventVersion":"2.0",
            "eventSource":"aws:s3",
            "awsRegion":"us-west-2",
            "eventTime":"1970-01-01T00:00:00.000Z",
            "eventName":"ObjectCreated:Put",
            "userIdentity":{
                "principalId":"AIDAJDPLRKLG7UEXAMPLE"
            },
            "requestParameters":{
                "sourceIPAddress":"127.0.0.1"
            },
            "responseElements":{
                "x-amz-request-id":"C3D13FE58DE4C810",
                "x-amz-id-2":"FMyUVURIY8/IgAtTv8xRjskZQpcIZ9KG4V5Wp6S7S/JRWeUWerMUE5JgHvANOjpD"
            },
            "s3":{
                "s3SchemaVersion":"1.0",
                "configurationId":"testConfigRule",
                "bucket":{
                "name":f"{self.bucket}",
                "ownerIdentity":{
                    "principalId":"A3NL1KOZZKExample"
                },
                "arn":f"arn:aws:s3:::{self.bucket}"
                },
                "object":{
                "key":f"{self.sample_file}",
                "size":1024,
                "eTag":"d41d8cd98f00b204e9800998ecf8427e",
                "versionId":"096fKKXTRTtl3on89fVO.nfljtsv6qko"
                }
            }
            }
        ]
        }
    
    def test_lambda_handler(self):
        lambda_handler(self.sample_s3_event, None)


if __name__ =='__main__':
    unittest.main()