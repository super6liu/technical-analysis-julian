import unittest

from src.configs import Configs


class TestConfigs(unittest.TestCase):
    def setUp(self) -> None:
        return super().setUp()

    def test_get(self):
        self.assertIsNotNone(Configs.configs("credentials", "mysql", "test")("db"))

    def test_fail(self):
        self.assertIsNone(Configs.configs("mock", "mysql", "test")("db"))
        self.assertIsNone(Configs.configs("credentials", "mock", "test")("db"))
        self.assertIsNone(Configs.configs("credentials", "mysql", "mock")("db"))
        self.assertIsNone(Configs.configs("credentials", "mysql", "test")("mock"))


if __name__ == '__main__':
    unittest.main()