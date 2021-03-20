import boto3
import smart_open

class DataWriter(object):

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def write_line(self, line):
        pass


class ConsoleWriter(DataWriter):
    # write lines to cloudwatch or local sysout
    def __init__(self, prefix=""):
        self.prefix = prefix

    def write_line(self, line):
        print(f"{self.prefix} {line}")


class FileWriter(DataWriter):
    # write lines to local file.
    def __init__(self, filename):
        self.fn = filename

    def __enter__(self):
        self.file = open(self.fn, 'w')
        return self

    def write_line(self, line):
        f.write(f"{line}\n")

    def __exit__(self, *args):
        close(self.f)
    

class S3Writer(DataWriter):
    # write lines to s3 directly 
    def __init__(self, s3url):
        self.s3url = s3url

    def __enter__(self):
        self.file = smart_open.open(s3url)
        return self

    def write_line(self, line):
        self.file.write(f"{line}\n")
    
    def __exit__(self, *args):
        smart_open.close(self.file)


