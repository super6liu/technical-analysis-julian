import unittest
from unittest.mock import MagicMock

from src.services.service_datastore.datastore_service import DatastoreService

class TestDatastoreService(unittest.TestCase):
    def setUp(self) -> None:
        return super().setUp()
    
    # def test_read(self):
    #     ds = DatastoreService()
    #     pass

    def test_update(self):
        pass


if __name__ == '__main__':
    unittest.main()

