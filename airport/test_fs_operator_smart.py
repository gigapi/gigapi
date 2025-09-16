
import unittest
from airport.fs_operator_smart import FsOperatorSmart

class TestFsOperatorSmart(unittest.TestCase):


    def test_copy_external(self):
        fs =  FsOperatorSmart("file:///home/hromozeka/QXIP/quackpipe/_testdata/hot")
        fs.copy_external("my_new_db/master/weather/metadata.db",
                         "s3://minioadmin:minioadmin@localhost:9000/gigapi/metadata.db?secure=false")

    # Add more tests for other methods as needed

if __name__ == '__main__':
    unittest.main()