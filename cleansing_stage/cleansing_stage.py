import boto3
from .data_reader import DataReader, FileReader
from .data_writer import DataWriter
import pandas as pd
import io

class CleansingStage():

    def __init__(self, 
        data_reader: DataReader,
        clean_output_writer: DataWriter,
        error_output_writer: DataWriter
        ):
        self.data_reader = data_reader
        self.clean_output_writer = clean_output_writer
        self.error_output_writer = error_output_writer

    def cleanse_data(self):

        with self.clean_output_writer as clean_out:
            with self.error_output_writer as error_out:

                for input_line in self.data_reader.read_lines():
                    #print(input_line)
                    try:
                        line_series = pd.read_json(
                            path_or_buf=input_line,
                            typ='series',
                            dtype={
                                "id":'object',
                                "created_at":"datetime64",
                                "user_email":"object",
                                "ip":"object",
                                "event_name":"object",
                                "metadata":"object"
                            }
                        )
                    except ValueError as e:
                        error_out.write_line(f"{e} [{input_line}]")
                        continue

                    # print(pd.Dataframe(line_series))
                    outputstr = io.StringIO()
                    pd.DataFrame(line_series).T.to_csv(
                        path_or_buf = outputstr,
                        sep=',',
                        header=False,
                        index=False,
                    )
                    clean_out.write_line(outputstr.getvalue())








