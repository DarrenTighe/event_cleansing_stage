#!/usr/bin/env python3
import unittest
from ..cleansing_stage import CleansingStage
from ..data_reader import StringReader
from ..data_writer import BufferWriter
from io import StringIO

class TestCleansingStage(unittest.TestCase):

    
    def test_straightforward_case(self):
        input_data = """{"id": "a2b6604c-913b-4598-858e-285253fe2721", "created_at": "2020-01-30 00:10:35", "user_email": "wallsdenise@thompson.com", "ip": "209.154.163.207", "event_name": "message_saved", "metadata": {"message_id": 66417}}"""

        expected_output = """a2b6604c-913b-4598-858e-285253fe2721,2020-01-30 00:10:35,wallsdenise@thompson.com,209.154.163.207,message_saved,{'message_id': 66417}\n"""

        expected_error_output = """"""

        reader = StringReader(input_data)
        
        clean_output = StringIO()
        clean_writer = BufferWriter(clean_output)

        broken_output = StringIO()
        broken_writer = BufferWriter(broken_output)

        cstage = CleansingStage(reader, clean_writer, broken_writer)
        cstage.cleanse_data()
        
        self.assertEqual(clean_output.getvalue(), expected_output)


    def test_rejection_not_json(self):
        input_data = """adsdasd"""

        expected_output = """"""

        expected_error_output = """Expected object or value [adsdasd]"""

        reader = StringReader(input_data)
        
        clean_output = StringIO()
        clean_writer = BufferWriter(clean_output)

        broken_output = StringIO()
        broken_writer = BufferWriter(broken_output)

        cstage = CleansingStage(reader, clean_writer, broken_writer)
        cstage.cleanse_data()

        self.assertEqual(clean_output.getvalue(), expected_output)
        self.assertEqual(broken_output.getvalue(), expected_error_output)

    def test_rejection_invalid_date(self):
            
        input_data = """{'id': 'ac2bde8e-c64d-406f-aaef-b9fbe6aa553c', 'created_at': datetime.datetime(2020, 1, 30, 2, 41, 28), 'user_email': 'wrightmary@robinson.info', 'ip': '213.77.36.45', 'event_name': 'meeting_invite', 'metadata': {'meeting_id': 65427, 'invited_user_email': 'tuckerraymond@briggs.org'}}"""

        expected_output = """"""

        expected_error_output = """Expected object or value [{'id': 'ac2bde8e-c64d-406f-aaef-b9fbe6aa553c', 'created_at': datetime.datetime(2020, 1, 30, 2, 41, 28), 'user_email': 'wrightmary@robinson.info', 'ip': '213.77.36.45', 'event_name': 'meeting_invite', 'metadata': {'meeting_id': 65427, 'invited_user_email': 'tuckerraymond@briggs.org'}}]"""

        reader = StringReader(input_data)
        
        clean_output = StringIO()
        clean_writer = BufferWriter(clean_output)

        broken_output = StringIO()
        broken_writer = BufferWriter(broken_output)

        cstage = CleansingStage(reader, clean_writer, broken_writer)
        cstage.cleanse_data()

        self.assertEqual(clean_output.getvalue(), expected_output)
        self.assertEqual(broken_output.getvalue(), expected_error_output)
        




