import unittest
from airport.utils import LayerUrlHelper

class LayerUrlHelperTest(unittest.TestCase):
    def test_parse_url(self):
        h = LayerUrlHelper("file:///home/user/data")
        print(h.prefix)
        h = LayerUrlHelper("file://a/b")
        print(h.prefix)
        h = LayerUrlHelper("file://a")
        print(h.prefix)
        h = LayerUrlHelper("file://a")
        print(h.prefix)
        h.set_prefix("/home/aaa")
        print(h.prefix)
        print(h.string())
        print("====")
        h = LayerUrlHelper("s3://a:b@localhost:9191/mybucket/prefix/a/b/c")
        print(h.username)
        print(h.password)
        print(h.bucket_name)
        print(h.prefix)
        print("====")


if __name__ == '__main__':
    unittest.main()