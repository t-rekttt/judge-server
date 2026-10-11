import datetime
import unittest

from dmoj.remote.storage import object_key, presign, presign_get


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
        self.assertEqual(object_key('tests/', 'aplusb', None, 'init.yml'), 'tests/aplusb/init.yml')
        self.assertEqual(object_key('', 'aplusb', 'contest', 'data/1.in'), 'contest/aplusb/data/1.in')

    def test_presign_signs_extra_query_parameters(self):
        url = presign(
            'GET',
            'https://s3.example',
            'bucket',
            '',
            'AK',
            'SK',
            'us-east-1',
            60,
            now=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            params={'list-type': '2', 'prefix': 'tests/'},
        )
        self.assertIn('list-type=2&prefix=tests%2F', url)
        self.assertTrue(url.startswith('https://s3.example/bucket?'))
