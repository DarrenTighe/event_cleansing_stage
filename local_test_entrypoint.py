from cleansing_stage.cleansing_stage import CleansingStage
from cleansing_stage.data_reader import FileReader
from cleansing_stage.data_writer import ConsoleWriter


reader = FileReader('cleansing_stage/test/testdata/2020/01/01/0f6e0555e9f34063b711a676d13e38f9.json')
clean_output = ConsoleWriter('clean: ')
broken_output = ConsoleWriter('broken: ')
cleansing_stage = CleansingStage(reader, clean_output, broken_output)
cleansing_stage.cleanse_data()