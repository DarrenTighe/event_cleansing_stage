import boto3
from cleansing_stage.data_reader import DataReader

class CleansingStage():

    def __init__(self, data_reader: DataReader):
        self.data_reader = data_reader

    def cleanse_data(self):
        for input_line in self.data_reader.read_lines():
            print(input_line)
