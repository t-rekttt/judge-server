import datetime
import unittest

from dmoj.remote.storage import object_key, presign_get


class RemoteStorageTest(unittest.TestCase):
    def test_presign_matches_aws_documented_example(self):
        # Example from the AWS docs "Authenticating Requests: Using Query Parameters (AWS Signature Version 4)".
        url = presign_get(
            endpoint='https://examplebucket.s3.amazonaws.com',
            bucket='',
            key='test.txt',
            access_key='AKIAIOSFODNN7EXAMPLE',
            secret_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            region='us-east-1',
            expires=86400,
            now=datetime.datetime(2013, 5, 24, tzinfo=datetime.timezone.utc),
        )
        self.assertTrue(
            url.endswith('X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404'), url
        )

    def test_object_key(self):
        self.assertEqual(object_key('problems/', 'aplusb', None, 'files.zip'), 'problems/default/aplusb/files.zip')
        self.assertEqual(object_key('', 'aplusb', 'contest', 'archive.zip'), 'contest/aplusb/archive.zip')
