"""Test module for DBUtils"""
import unittest
from s3utils import S3Utils

class TestAWSUtils(unittest.TestCase):
    """Class for testing AWSUtils"""

    @unittest.skip("Skip test")
    def test_s3_write(self):
        """Test to check if we can write to s3"""
        with S3Utils() as s3obj:
            result = s3obj.put_object("davepulaskitest","mk1", "Makefile")

        self.assertEqual(result, True)

    def test_s3_read(self):
        """Test to check if we can read from s3"""
        with S3Utils() as s3obj:
            result = s3obj.get_object("davepulaskitest","BRANCH1/BRANCH11/file11.txt", "test_Sandeep.txt")

        self.assertEqual(result, True)



# TODO: Test select, insert, and update queries


# Run the tests
if __name__ == '__main__':
    unittest.main()
