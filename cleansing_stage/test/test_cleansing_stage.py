import unittest

class TestCleansingStage(unittest.TestCase):

    
    def test_straightforward_case(self):
        input_data = """
        {"id": "a2b6604c-913b-4598-858e-285253fe2721", "created_at": "2020-01-30 00:10:35", "user_email": "wallsdenise@thompson.com", "ip": "209.154.163.207", "event_name": "message_saved", "metadata": {"message_id": 66417}}
        """

        expected_output = """
        "a2b6604c-913b-4598-858e-285253fe2721", "2020-01-30 00:10:35", "wallsdenise@thompson.com", "209.154.163.207", "message_saved", "{"message_id": 66417}"
        """

        #textwrap.dedent