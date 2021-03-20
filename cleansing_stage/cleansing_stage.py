import boto3
from .data_reader import DataReader, FileReader
from .data_writer import DataWriter
import pandas as pd
import io
from validate_email import validate_email

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
                lines_processed = 0
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
                        # Captures malformed rows - not valid json or not of the correct type
                        error_out.write_line(f"{e} [{input_line}]")
                        continue
                    
                    if line_series.isnull().values.any():
                        # Missing value
                        error_out.write_line(f"Missing Value [{input_line}]")
                    
                    if not validate_email(
                        email_address=line_series['user_email'], 
                        check_format=True, 
                        check_blacklist=False,
                        check_dns=False,
                        check_smtp=False):
                        # Going with a really simple validation here for the sake of speed. 
                        # If you wanted to validate emails by checking the domain exists or connecting with SMTP, 
                        # the best bet would be to cache results (Elasticache etc) so that repeated network traffic
                        # is minimized.
                        error_out.write_line(f"invalid email [{input_line}]")

                    line_df = pd.DataFrame(line_series).T
                    outputstr = io.StringIO()
                    line_df.to_csv(
                        path_or_buf = outputstr,
                        sep=',',
                        header=False,
                        index=False,
                    )
                    clean_out.write_line(outputstr.getvalue())

                    lines_processed += 1
                    if lines_processed % 1000 == 0:
                        print(f"{lines_processed} lines processed")

                print(f"{lines_processed} lines processed")









